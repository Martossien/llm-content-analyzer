"""Lecture du CSV SMBeagle (19 colonnes, guillemets sélectifs).

SMBeagle (C#/Serilog) écrit un CSV particulier : seules les colonnes *string*
sont entourées de guillemets, les DateTime / bool / long / enum ne le sont pas,
et un guillemet interne est échappé `\\"` (et non `""` comme le veut la
RFC 4180). Le module accepte les deux échappements, ne charge jamais le fichier
entier en mémoire et ne perd aucune ligne en silence : une ligne illisible
ressort en `CsvLineError`.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from collections.abc import Callable, Iterator
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, TextIO

from docia.models import SmbeagleRow

logger = logging.getLogger(__name__)

if TYPE_CHECKING:  # pragma: no cover - import de typage uniquement
    from docia.db import Database

HEADER: tuple[str, ...] = (
    "Name",
    "Host",
    "Extension",
    "Username",
    "Hostname",
    "UNCDirectory",
    "CreationTime",
    "LastWriteTime",
    "Readable",
    "Writeable",
    "Deletable",
    "DirectoryType",
    "Base",
    "FileSize",
    "AccessTime",
    "FileAttributes",
    "Owner",
    "FastHash",
    "FileSignature",
)
"""Les 19 colonnes, dans l'ordre exact produit par SMBeagle."""

QUOTED_COLUMNS: frozenset[int] = frozenset({0, 1, 2, 3, 4, 5, 12, 15, 16, 17, 18})
"""Colonnes string : SMBeagle les entoure de guillemets (tolérance si absents)."""

UNQUOTED_COLUMNS: frozenset[int] = frozenset({6, 7, 8, 9, 10, 11, 13, 14})
"""Colonnes DateTime / bool / long / enum : jamais de guillemets."""

BATCH_SIZE = 1_000
"""Lignes accumulées avant un `upsert_files` (compromis mémoire / transactions)."""

MAX_KEPT_ERRORS = 200
"""Erreurs conservées dans le rapport ; les suivantes ne sont que comptées."""

PROGRESS_EVERY_BATCHES = 10
"""Lots entre deux appels au rappel d'avancement (10 × `BATCH_SIZE` = 10 000 lignes)."""

_POSITION_EVERY = 500
"""Lignes entre deux relevés de la position de lecture (octets)."""

_MAX_RAW = 500
"""Longueur maximale de la ligne brute mémorisée dans une erreur."""

SQLITE_INT_MIN = -(2**63)
SQLITE_INT_MAX = 2**63 - 1
"""Bornes d'un INTEGER SQLite. Un `FileSize` hors de cette plage n'est pas
stockable : `sqlite3` lève `OverflowError` au moment d'écrire le lot, donc très
loin de la ligne fautive — une seule ligne faisait perdre l'import entier. La
borne est donc contrôlée à la lecture, comme le reste du champ."""

_ROW_WRITE_ERRORS: tuple[type[Exception], ...] = (
    OverflowError,
    ValueError,
    TypeError,
    sqlite3.IntegrityError,
    sqlite3.InterfaceError,
    sqlite3.DataError,
    sqlite3.ProgrammingError,
)
"""Échecs d'écriture imputables à **une ligne** : le lot est rejoué ligne à ligne
et seules les fautives sont perdues. Tout le reste (`OperationalError` : disque
plein, base verrouillée ou corrompue) reste fatal — le rejouer ne ferait que
répéter l'échec, et poursuivre sur une base qui n'écrit plus serait un mensonge."""

_DATETIME_FORMATS: tuple[str, ...] = (
    "%d/%m/%Y %H:%M:%S",
    "%m/%d/%Y %H:%M:%S",
    "%d/%m/%Y %H:%M",
    "%m/%d/%Y %H:%M",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%d/%m/%Y",
    "%Y-%m-%d",
)


@dataclass(frozen=True)
class CsvLineError:
    """Une ligne rejetée : numéro, raison lisible, ligne brute.

    `line_number` vaut 1 pour l'en-tête et 0 pour une ligne rejetée à l'écriture
    en base (voir `import_csv.flush`), qui n'est plus localisable dans le fichier.
    """

    line_number: int
    reason: str
    raw: str


@dataclass
class ReadPosition:
    """Position de lecture d'un CSV, partagée entre le lecteur et son appelant.

    `read_smbeagle_csv` la met à jour toutes les `_POSITION_EVERY` lignes ; c'est
    ce qui permet d'annoncer une progression honnête (octets lus / taille du
    fichier) sans savoir à l'avance combien de lignes contient le fichier.
    """

    bytes_read: int = 0
    total_bytes: int = 0


@dataclass(frozen=True)
class ImportProgress:
    """Avancement d'un import en cours, passé au rappel `progress` d'`import_csv`.

    `rows` compte les lignes valides déjà écrites en base, `invalid` les lignes
    rejetées. Le pourcentage vient des octets lus : le nombre de lignes total est
    inconnu tant que le fichier n'est pas parcouru, la taille, elle, est connue.
    """

    rows: int
    invalid: int
    bytes_read: int
    total_bytes: int
    elapsed_s: float
    final: bool = False
    """Vrai pour le tout dernier appel : le bilan complet, jamais étranglé.

    Sans ce drapeau, un afficheur qui espace ses lignes (`import_progress_logger`)
    ravalait la dernière : un import de 934 028 lignes s'arrêtait sur « 900 000
    lignes — 96 % », un import de trois lignes sur « 0 lignes — 0 % ».
    """

    @property
    def percent(self) -> float:
        """Avancement estimé en pourcentage — 100 % dès que l'import est terminé.

        Un fichier vide ou dont la taille n'a pas pu être lue n'a pas de
        dénominateur : sans le cas `final`, la dernière ligne annoncerait « 0 % »
        pour un import pourtant achevé.
        """
        if self.total_bytes <= 0:
            return 100.0 if self.final else 0.0
        return min(100.0, 100.0 * self.bytes_read / self.total_bytes)


@dataclass(frozen=True)
class ImportReport:
    """Bilan d'un `import_csv`. `invalid` compte toutes les lignes rejetées,
    `errors` n'en garde que les `MAX_KEPT_ERRORS` premières."""

    scan_id: int
    total: int
    new: int
    updated: int
    unchanged: int
    invalid: int
    size_defaulted: int = 0
    """Lignes acceptées dont le `FileSize` était illisible : la taille retenue est
    0, pas la taille réelle (mode tolérant). Sans ce compteur, un fichier dont le
    scanner n'a pas pu lire la taille (ACL, verrou, montage cassé) sortait de
    l'audit — exclu « fichier trop petit » — sans laisser la moindre trace."""
    size_zero: int = 0
    """Lignes acceptées annonçant **exactement 0 octet** — taille lue, pas retombée.

    Un fichier vide, c'est banal ; *tout* un partage à 0 octet, non : c'est la
    signature d'un CSV produit sans `--sizefile` par un scanner qui écrit `0` au
    lieu d'un champ vide (SMBeagle ≤ v4.2.0, ou un outil tiers). Le cas est
    invité par le guide (« importer un CSV SMBeagle existant ») et vidait la
    campagne en silence, chaque fichier étant exclu « trop petit »."""
    mojibake: int = 0
    """Lignes acceptées (mode tolérant) dont le chemin contient un caractère de
    remplacement : le CSV n'était pas en UTF-8.

    Ces chemins ne désignent aucun fichier réel, mais partaient dans les exports
    comme candidats à la suppression sans que rien ne les distingue. En mode strict
    ils sont refusés ; en mode tolérant on les garde — l'audit reste exploitable —
    mais on les compte et on le dit."""
    errors: list[CsvLineError] = field(default_factory=list)


SUSPECT_ZERO_MIN = 20
"""En deçà, « tous les fichiers à 0 octet » reste un partage plausible (dossier de
test, arborescence de témoins) : on ne crie pas pour si peu."""

REPLACEMENT = "�"
"""Caractère de remplacement produit par `errors="replace"` sur un octet non UTF-8.

Sa présence dans un chemin n'est pas un détail d'affichage : le chemin stocké ne
désigne alors **aucun fichier réel**. Il ressortira quand même dans les exports,
comme candidat à la suppression, et rien ne dira qu'il est faux. Cas courant : un
CSV réenregistré depuis Excel en cp1252 (« Compté été » → « Compt� �t� »)."""


# --------------------------------------------------------------------- parsing


def _read_quoted(line: str, start: int) -> tuple[str, int, bool]:
    """Lit un champ quoté à partir de `start` (juste après le guillemet ouvrant).

    Renvoie la valeur, l'index suivant le guillemet fermant, et **si ce guillemet
    a bien été trouvé**. `\\"` et `""` valent un guillemet littéral ; `\\"` suivi
    d'une virgule ou d'une fin de ligne est en revanche un antislash final suivi
    du guillemet fermant (cas des chemins UNC : `"\\\\srv\\part$\\"`).

    Le troisième élément existe parce qu'un guillemet resté ouvert signifie que le
    champ **continue sur la ligne suivante** : sans lui, cette suite était relue
    comme un enregistrement à part entière et fabriquait un fichier fantôme.
    """
    out: list[str] = []
    i = start
    n = len(line)
    while i < n:
        char = line[i]
        if char == "\\" and i + 1 < n and line[i + 1] == '"':
            if i + 2 >= n or line[i + 2] == ",":
                out.append("\\")
                return "".join(out), i + 2, True
            out.append('"')
            i += 2
            continue
        if char == '"':
            if i + 1 < n and line[i + 1] == '"':
                out.append('"')
                i += 2
                continue
            return "".join(out), i + 1, True
        out.append(char)
        i += 1
    return "".join(out), i, False  # guillemet fermant manquant


def quote_left_open(line: str) -> bool:
    r"""Vrai si la ligne se termine **à l'intérieur** d'un champ quoté.

    SMBeagle écrit un enregistrement par ligne ; un guillemet resté ouvert signifie
    donc qu'un nom de fichier ou un chemin contient un saut de ligne — NTFS
    l'autorise. La suite du champ arrive alors sur la ligne suivante, que le
    lecteur relisait comme un enregistrement complet : `"rapport\nfinal.pdf"`
    produisait en base le chemin `\\srv\part$\docs\final.pdf"`, avec une taille
    plausible de 802 octets. Ce chemin ne désigne aucun fichier, et ressortait dans
    les candidats au nettoyage.

    Le parcours réutilise `_read_quoted`, seule définition des règles
    d'échappement (`\\"`, `""`, antislash final d'un chemin UNC) : les réécrire ici
    aurait créé une seconde vérité, qui aurait fini par diverger.
    """
    i, n = 0, len(line)
    while i < n:
        if line[i] == '"':
            _, i, ferme = _read_quoted(line, i + 1)
            if not ferme:
                return True
            while i < n and line[i] != ",":
                i += 1
        else:
            virgule = line.find(",", i)
            if virgule == -1:
                return False
            i = virgule
        if i < n and line[i] == ",":
            i += 1
    return False


def split_csv_line(line: str) -> list[str]:
    """Découpe une ligne SMBeagle en champs (virgule non protégée = séparateur).

    Les champs non quotés sont détourés des espaces ; le contenu d'un champ
    quoté est rendu tel quel.
    """
    if not line.strip():
        return []
    fields: list[str] = []
    i = 0
    n = len(line)
    while True:
        if i < n and line[i] == '"':
            value, i, _ = _read_quoted(line, i + 1)
            while i < n and line[i] != ",":  # ferraille après le guillemet fermant
                i += 1
        else:
            end = line.find(",", i)
            if end == -1:
                value, i = line[i:].strip(), n
            else:
                value, i = line[i:end].strip(), end
        fields.append(value)
        if i < n and line[i] == ",":
            i += 1
            continue
        break
    return fields


def validate_header(line: str) -> list[str]:
    """Vérifie l'en-tête (19 colonnes attendues, noms exacts à la casse près)."""
    errors: list[str] = []
    fields = split_csv_line(line.lstrip("﻿"))
    if len(fields) != len(HEADER):
        errors.append(f"en-tête : {len(fields)} colonnes au lieu de {len(HEADER)}")
        return errors
    for index, (found, expected) in enumerate(zip(fields, HEADER, strict=True)):
        if found.strip().lower() != expected.lower():
            errors.append(f"en-tête, colonne {index} : « {found} » au lieu de « {expected} »")
    return errors


def validate_csv_line_format(line: str, line_number: int) -> list[str]:
    """Contrôle les guillemets sélectifs d'une ligne de données (diagnostic).

    SMBeagle quote les colonnes `QUOTED_COLUMNS` et jamais les autres ; le
    parseur, lui, accepte les deux. Cette fonction sert au diagnostic d'un CSV
    douteux, pas au filtrage.
    """
    errors: list[str] = []
    fields = split_csv_line(line)
    if len(fields) != len(HEADER):
        errors.append(f"ligne {line_number} : {len(fields)} champs au lieu de {len(HEADER)}")
        return errors
    index = 0
    i = 0
    n = len(line)
    while index < len(HEADER) and i <= n:
        quoted = i < n and line[i] == '"'
        if quoted and index in UNQUOTED_COLUMNS:
            errors.append(f"ligne {line_number}, colonne {index} : guillemets inattendus")
        elif not quoted and index in QUOTED_COLUMNS:
            errors.append(f"ligne {line_number}, colonne {index} : guillemets manquants")
        if quoted:
            _, i, _ = _read_quoted(line, i + 1)
            while i < n and line[i] != ",":
                i += 1
        else:
            end = line.find(",", i)
            i = n if end == -1 else end
        i += 1  # sauter la virgule
        index += 1
    return errors


def _to_bool(value: str) -> bool:
    """`True` (toutes casses) → vrai ; tout le reste → faux."""
    return value.strip().lower() == "true"


def _normalize_extension(value: str) -> str:
    """Extension SMBeagle : minuscules, sans point (`"pdf"`)."""
    return value.strip().lower().lstrip(".")


def parse_line(line: str, line_number: int, *, strict: bool = False) -> SmbeagleRow:
    """Transforme une ligne de données en `SmbeagleRow`.

    Le `FileSize` connaît trois sorts, et aucun n'est silencieux :

    * **absent** (champ vide) — taille 0 et `size_unreadable`, dans les deux
      modes. En faire une erreur ferait échouer un scan entier, mais faire passer
      « inconnu » pour « 0 octet » ferait sortir le fichier de l'audit, exclu
      « trop petit », sans un mot. C'est ce qu'écrit SMBeagle depuis le 01/09
      quand la taille n'est pas collectée (sans `--sizefile`) ou pas lisible
      (ACL, verrou) — **les versions antérieures écrivaient `0`**, indiscernable
      d'un fichier vide : d'où le garde-fou `size_zero` de `import_csv` ;
    * **illisible** (non entier, ou hors de la plage des INTEGER SQLite) — refusé
      en mode strict, sinon taille 0 et `size_unreadable` ;
    * **lisible** — la taille, telle quelle.

    `import_csv` compte les retombées à zéro dans `ImportReport.size_defaulted`.

    Raises:
        ValueError: nombre de champs différent de 19 ; chemin non identifiant
            (`Name` ou `UNCDirectory` vide) ; `FileSize` illisible en mode strict.
    """
    fields = split_csv_line(line)
    if len(fields) != len(HEADER):
        raise ValueError(f"ligne {line_number} : {len(fields)} champs au lieu de {len(HEADER)}")
    raw_size = fields[13].strip()
    size_problem = ""
    file_size = 0
    if raw_size:
        try:
            file_size = int(raw_size)
        except ValueError:
            file_size, size_problem = 0, "non entier"
        else:
            if not SQLITE_INT_MIN <= file_size <= SQLITE_INT_MAX:
                file_size, size_problem = 0, "hors de la plage des entiers SQLite"
    if size_problem and strict:
        raise ValueError(f"ligne {line_number} : FileSize {size_problem} (« {fields[13]} »)")
    row = SmbeagleRow(
        name=fields[0],
        host=fields[1],
        extension=_normalize_extension(fields[2]),
        username=fields[3],
        hostname=fields[4],
        unc_directory=fields[5],
        creation_time=fields[6],
        last_write_time=fields[7],
        readable=_to_bool(fields[8]),
        writeable=_to_bool(fields[9]),
        deletable=_to_bool(fields[10]),
        directory_type=fields[11],
        base=fields[12],
        file_size=file_size,
        access_time=fields[14],
        file_attributes=fields[15],
        owner=fields[16],
        fast_hash=fields[17],
        file_signature=fields[18],
        size_unreadable=bool(size_problem) or not raw_size,
    )
    identity = row.identity_error()
    if identity:
        raise ValueError(f"ligne {line_number} : {identity}, chemin non identifiant")
    if strict and REPLACEMENT in row.path:
        raise ValueError(
            f"ligne {line_number} : chemin non décodable en UTF-8 "
            f"(« {row.path[:60]} ») — ce chemin ne désigne aucun fichier réel"
        )
    return row


@lru_cache(maxsize=8192)
def parse_smbeagle_datetime(text: str) -> datetime | None:
    """Date SMBeagle (`dd/MM/yyyy HH:mm:ss`) → `datetime` naïf, `None` si illisible.

    Tolère l'ordre américain (`MM/dd/yyyy`, machine en InvariantGlobalization)
    et l'ISO `yyyy-MM-dd`. Fonction pure, donc mémoïsée : les vues la rappellent
    des dizaines de milliers de fois sur un jeu de dates très répétitif, et un
    format non reconnu du premier coup coûte plusieurs `strptime` en échec.
    """
    value = text.strip()
    if not value:
        return None
    for fmt in _DATETIME_FORMATS:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed.replace(tzinfo=None) if parsed.tzinfo else parsed


def _byte_position(handle: TextIO) -> int:
    """Octets consommés dans le fichier, ou -1 si la position est indisponible.

    On interroge le tampon binaire : `TextIOWrapper.tell()` est interdit pendant
    une itération (`next()`). La valeur est en avance de la lecture décodée de la
    taille d'un tampon au plus — négligeable sur un fichier de plusieurs centaines
    de méga-octets, et l'usage est un pourcentage, pas une position exacte.
    """
    buffer = getattr(handle, "buffer", None)
    if buffer is None:
        return -1
    try:
        return int(buffer.tell())
    except (OSError, ValueError):  # pragma: no cover - flux non repositionnable
        return -1


def read_smbeagle_csv(
    path: Path, *, strict: bool = True, position: ReadPosition | None = None
) -> Iterator[SmbeagleRow | CsvLineError]:
    """Parcourt le CSV en flux et rend une `SmbeagleRow` ou une `CsvLineError` par ligne.

    Encodage UTF-8 (BOM toléré), fins de ligne CRLF ou LF, lignes vides
    ignorées. Un en-tête invalide arrête la lecture après l'erreur, **strict ou
    non** : les colonnes ne sont plus à la position attendue. Le mode tolérant
    est fait pour les lignes, pas pour la structure — poursuivre sur un en-tête
    décalé lisait par exemple `AccessTime` comme `FileSize` (toutes les tailles à
    0, toutes les dates dans les tailles) pour le seul signal d'« une ligne
    invalide » perdue dans le journal.

    Args:
        position: si fourni, reçoit la taille du fichier puis, toutes les
            `_POSITION_EVERY` lignes, le nombre d'octets lus — de quoi afficher
            une progression sans compter les lignes à l'avance.
    """
    if position is not None:
        try:
            position.total_bytes = path.stat().st_size
        except OSError:  # pragma: no cover - dépend du système de fichiers
            position.total_bytes = 0
        position.bytes_read = 0
    with path.open("r", encoding="utf-8-sig", errors="replace") as handle:
        header = handle.readline()
        if not header:
            yield CsvLineError(1, "fichier vide", "")
            return
        if REPLACEMENT in header or "\x00" in header:
            # Un CSV UTF-16 échouait déjà, mais sur un message de 1 400 caractères
            # de mojibake (« ��N a m e » au lieu de « Name »), qui ne nommait jamais
            # la seule cause : ce fichier n'est pas en UTF-8.
            yield CsvLineError(
                1,
                "en-tête illisible : ce fichier n'est pas encodé en UTF-8 "
                "(UTF-16 ou page de codes Windows ?). Réenregistrez-le en UTF-8, "
                "ou relancez le scan avec « docia scan »",
                header.rstrip("\r\n")[:80],
            )
            return
        header_errors = validate_header(header.rstrip("\r\n"))
        if header_errors:
            yield CsvLineError(1, " ; ".join(header_errors), header.rstrip("\r\n")[:_MAX_RAW])
            return
        suite_d_un_champ_ouvert = False
        for line_number, raw in enumerate(handle, start=2):
            if position is not None and line_number % _POSITION_EVERY == 0:
                offset = _byte_position(handle)
                if offset >= 0:
                    position.bytes_read = offset
            line = raw.rstrip("\r\n")
            if not line.strip():
                continue
            if suite_d_un_champ_ouvert:
                # Suite d'un champ quoté laissé ouvert : ce n'est pas un
                # enregistrement, c'est la fin du précédent. La relire comme une
                # ligne à part entière fabriquait un fichier fantôme — un chemin
                # tronqué, avec une taille plausible, qui ne désigne rien et
                # ressortait pourtant dans les candidats au nettoyage.
                suite_d_un_champ_ouvert = quote_left_open(line)
                yield CsvLineError(
                    line_number,
                    "suite d'un champ quoté non refermé à la ligne précédente : "
                    "un nom de fichier contient probablement un saut de ligne",
                    line[:_MAX_RAW],
                )
                continue
            if quote_left_open(line):
                suite_d_un_champ_ouvert = True
                yield CsvLineError(
                    line_number,
                    "champ quoté non refermé en fin de ligne",
                    line[:_MAX_RAW],
                )
                continue
            try:
                yield parse_line(line, line_number, strict=strict)
            except ValueError as exc:
                yield CsvLineError(line_number, str(exc), line[:_MAX_RAW])
        if position is not None:
            position.bytes_read = position.total_bytes


def import_csv(
    db: Database,
    path: Path,
    *,
    strict: bool = True,
    progress: Callable[[ImportProgress], None] | None = None,
    progress_every: int = PROGRESS_EVERY_BATCHES,
) -> ImportReport:
    """Importe un CSV SMBeagle dans la base : un `scan`, des `upsert_files` par lots.

    Un fichier déjà connu dont `fast_hash`, `size` ou `last_write_time` change
    est compté « modifié » (et repasse `pending` avec `content_version + 1`) ;
    les autres sont « inchangés ».

    Rien ne disparaît en silence : une ligne illisible est comptée `invalid` (avec
    sa raison dans `errors`), une taille illisible ramenée à 0 est comptée
    `size_defaulted`, et un lot refusé par la base est rejoué ligne à ligne au lieu
    d'emporter tout l'import.

    L'écriture est encadrée par `Database.bulk_load()` : les index secondaires de
    `files` sont retirés le temps du chargement puis reconstruits d'un bloc — cinq
    fois plus rapide qu'en les maintenant ligne à ligne sur un gros scan.

    Args:
        progress: rappel d'avancement, appelé au démarrage, tous les
            `progress_every` lots, puis une dernière fois à la fin — ce dernier
            appel porte `final=True` (bilan complet, 100 %). Un import de
            plusieurs minutes doit pouvoir se raconter : sans lui, l'interface
            affiche « intégration en cours » et rien d'autre.
        progress_every: nombre de lots entre deux appels.
    """
    scan_id = db.start_scan(str(path))
    total = new = updated = unchanged = invalid = size_defaulted = size_zero = mojibake = 0
    errors: list[CsvLineError] = []
    batch: list[SmbeagleRow] = []
    position = ReadPosition()
    started = time.monotonic()

    def notify(*, final: bool = False) -> None:
        """Rend compte de l'avancement — sans jamais mettre l'import en danger.

        Le rappel écrit dans une console, un journal ou une fenêtre : un tube fermé
        ou une fenêtre détruite ne doit pas faire perdre un import de dix minutes.
        `final=True` marque le dernier appel : l'afficheur ne doit pas l'étrangler.
        """
        if progress is None:
            return
        try:
            progress(
                ImportProgress(
                    rows=total,
                    invalid=invalid,
                    bytes_read=position.bytes_read,
                    total_bytes=position.total_bytes,
                    elapsed_s=time.monotonic() - started,
                    final=final,
                )
            )
        except Exception:  # noqa: BLE001 — l'affichage n'est jamais critique
            logger.debug("rappel de progression en échec, import poursuivi", exc_info=True)

    def flush() -> None:
        """Écrit le lot courant et **cumule** ses compteurs (une seule addition ici).

        Un lot refusé par la base n'emporte plus l'import : sa transaction est
        annulée, les lignes sont rejouées **une à une**, celles qui passent sont
        conservées et seules les fautives sont comptées `invalid` avec leur raison.
        Une ligne ne doit pas faire tomber le million (voir `_ROW_WRITE_ERRORS`
        pour ce qui reste fatal).
        """
        nonlocal new, updated, unchanged, total, invalid
        if not batch:
            return
        try:
            batch_new, batch_updated, batch_unchanged = db.upsert_files(batch, scan_id)
        except _ROW_WRITE_ERRORS as exc:
            logger.warning(
                "lot de %d ligne(s) refusé par la base (%s) : reprise ligne par ligne",
                len(batch),
                exc,
            )
            for row in batch:
                try:
                    row_new, row_updated, row_unchanged = db.upsert_files([row], scan_id)
                except _ROW_WRITE_ERRORS as row_exc:
                    total -= 1
                    invalid += 1
                    if len(errors) < MAX_KEPT_ERRORS:
                        errors.append(
                            CsvLineError(0, f"écriture refusée : {row_exc}", row.path[:_MAX_RAW])
                        )
                else:
                    new += row_new
                    updated += row_updated
                    unchanged += row_unchanged
        else:
            new += batch_new
            updated += batch_updated
            unchanged += batch_unchanged
        batch.clear()

    def close_scan() -> None:
        """Inscrit dans `scans` ce qui a réellement été écrit."""
        db.finish_scan(
            scan_id, total=total, new=new, updated=updated, unchanged=unchanged, invalid=invalid
        )

    try:
        with db.bulk_load(analyze=False):  # `finish_scan` rejoue `ANALYZE` juste après
            notify()
            batches = 0
            for item in read_smbeagle_csv(path, strict=strict, position=position):
                if isinstance(item, CsvLineError):
                    invalid += 1
                    if len(errors) < MAX_KEPT_ERRORS:
                        errors.append(item)
                    continue
                total += 1
                if item.size_unreadable:
                    size_defaulted += 1
                elif item.file_size == 0:
                    size_zero += 1
                if REPLACEMENT in item.name or REPLACEMENT in item.unc_directory:
                    mojibake += 1
                batch.append(item)
                if len(batch) >= BATCH_SIZE:
                    flush()
                    batches += 1
                    if progress_every > 0 and batches % progress_every == 0:
                        notify()
            flush()
            notify(final=True)
    except Exception:
        # L'import s'arrête, mais la ligne `scans` doit dire ce qui a été écrit : la
        # laisser à `rows_total=0` ferait passer un import interrompu pour un scan vide.
        with suppress(sqlite3.Error):
            close_scan()
        raise

    close_scan()
    if size_defaulted:
        logger.warning(
            "%s : %d ligne(s) sans FileSize lisible — taille ramenée à 0, "
            "donc exclusion « fichier trop petit » probable",
            path,
            size_defaulted,
        )
    if mojibake:
        logger.warning(
            "%s : %d chemin(s) non décodables en UTF-8 — ce CSV n'est pas en UTF-8 "
            "(réenregistré depuis Excel ?). Ces chemins ne désignent aucun fichier réel "
            "et ressortiront tels quels dans les exports.",
            path,
            mojibake,
        )
    if total >= SUSPECT_ZERO_MIN and size_zero == total:
        logger.warning(
            "%s : les %d fichiers annoncent 0 octet — ce CSV a très probablement été "
            "produit sans « --sizefile ». Toute la campagne sera exclue « fichier trop "
            "petit ». Relancez le scan avec l'option, ou utilisez « docia scan ».",
            path,
            total,
        )
    return ImportReport(
        scan_id=scan_id,
        total=total,
        new=new,
        updated=updated,
        unchanged=unchanged,
        invalid=invalid,
        size_defaulted=size_defaulted,
        size_zero=size_zero,
        mojibake=mojibake,
        errors=errors,
    )
