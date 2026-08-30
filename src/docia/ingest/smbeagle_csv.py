"""Lecture du CSV SMBeagle (19 colonnes, guillemets sélectifs).

SMBeagle (C#/Serilog) écrit un CSV particulier : seules les colonnes *string*
sont entourées de guillemets, les DateTime / bool / long / enum ne le sont pas,
et un guillemet interne est échappé `\\"` (et non `""` comme le veut la
RFC 4180). Le module accepte les deux échappements, ne charge jamais le fichier
entier en mémoire et ne perd aucune ligne en silence : une ligne illisible
ressort en `CsvLineError`.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from docia.models import SmbeagleRow

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

_MAX_RAW = 500
"""Longueur maximale de la ligne brute mémorisée dans une erreur."""

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
    """Une ligne rejetée : numéro (1 = en-tête), raison lisible, ligne brute."""

    line_number: int
    reason: str
    raw: str


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
    errors: list[CsvLineError] = field(default_factory=list)


# --------------------------------------------------------------------- parsing


def _read_quoted(line: str, start: int) -> tuple[str, int]:
    """Lit un champ quoté à partir de `start` (juste après le guillemet ouvrant).

    Renvoie la valeur et l'index suivant le guillemet fermant. `\\"` et `""`
    valent un guillemet littéral ; `\\"` suivi d'une virgule ou d'une fin de
    ligne est en revanche un antislash final suivi du guillemet fermant (cas
    des chemins UNC : `"\\\\srv\\part$\\"`).
    """
    out: list[str] = []
    i = start
    n = len(line)
    while i < n:
        char = line[i]
        if char == "\\" and i + 1 < n and line[i + 1] == '"':
            if i + 2 >= n or line[i + 2] == ",":
                out.append("\\")
                return "".join(out), i + 2
            out.append('"')
            i += 2
            continue
        if char == '"':
            if i + 1 < n and line[i + 1] == '"':
                out.append('"')
                i += 2
                continue
            return "".join(out), i + 1
        out.append(char)
        i += 1
    return "".join(out), i  # guillemet fermant manquant : on tolère


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
            value, i = _read_quoted(line, i + 1)
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
            _, i = _read_quoted(line, i + 1)
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

    Raises:
        ValueError: nombre de champs différent de 19, ou `FileSize` non entier
            en mode strict (sinon la taille vaut 0).
    """
    fields = split_csv_line(line)
    if len(fields) != len(HEADER):
        raise ValueError(f"ligne {line_number} : {len(fields)} champs au lieu de {len(HEADER)}")
    try:
        file_size = int(fields[13].strip() or "0")
    except ValueError:
        if strict:
            raise ValueError(
                f"ligne {line_number} : FileSize non entier (« {fields[13]} »)"
            ) from None
        file_size = 0
    return SmbeagleRow(
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
    )


def parse_smbeagle_datetime(text: str) -> datetime | None:
    """Date SMBeagle (`dd/MM/yyyy HH:mm:ss`) → `datetime` naïf, `None` si illisible.

    Tolère l'ordre américain (`MM/dd/yyyy`, machine en InvariantGlobalization)
    et l'ISO `yyyy-MM-dd`.
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


def read_smbeagle_csv(path: Path, *, strict: bool = True) -> Iterator[SmbeagleRow | CsvLineError]:
    """Parcourt le CSV en flux et rend une `SmbeagleRow` ou une `CsvLineError` par ligne.

    Encodage UTF-8 (BOM toléré), fins de ligne CRLF ou LF, lignes vides
    ignorées. En mode strict, un en-tête invalide arrête la lecture après
    l'erreur : les colonnes ne sont plus à la position attendue.
    """
    with path.open("r", encoding="utf-8-sig", errors="replace") as handle:
        header = handle.readline()
        if not header:
            yield CsvLineError(1, "fichier vide", "")
            return
        header_errors = validate_header(header.rstrip("\r\n"))
        if header_errors:
            yield CsvLineError(1, " ; ".join(header_errors), header.rstrip("\r\n")[:_MAX_RAW])
            if strict:
                return
        for line_number, raw in enumerate(handle, start=2):
            line = raw.rstrip("\r\n")
            if not line.strip():
                continue
            try:
                yield parse_line(line, line_number, strict=strict)
            except ValueError as exc:
                yield CsvLineError(line_number, str(exc), line[:_MAX_RAW])


def import_csv(db: Database, path: Path, *, strict: bool = True) -> ImportReport:
    """Importe un CSV SMBeagle dans la base : un `scan`, des `upsert_files` par lots.

    Un fichier déjà connu dont `fast_hash`, `size` ou `last_write_time` change
    est compté « modifié » (et repasse `pending` avec `content_version + 1`) ;
    les autres sont « inchangés ».
    """
    scan_id = db.start_scan(str(path))
    total = new = updated = unchanged = invalid = 0
    errors: list[CsvLineError] = []
    batch: list[SmbeagleRow] = []

    def flush() -> tuple[int, int, int]:
        if not batch:
            return 0, 0, 0
        counts = db.upsert_files(batch, scan_id)
        batch.clear()
        return counts

    for item in read_smbeagle_csv(path, strict=strict):
        if isinstance(item, CsvLineError):
            invalid += 1
            if len(errors) < MAX_KEPT_ERRORS:
                errors.append(item)
            continue
        total += 1
        batch.append(item)
        if len(batch) >= BATCH_SIZE:
            batch_new, batch_updated, batch_unchanged = flush()
            new += batch_new
            updated += batch_updated
            unchanged += batch_unchanged
    batch_new, batch_updated, batch_unchanged = flush()
    new += batch_new
    updated += batch_updated
    unchanged += batch_unchanged

    db.finish_scan(
        scan_id, total=total, new=new, updated=updated, unchanged=unchanged, invalid=invalid
    )
    return ImportReport(
        scan_id=scan_id,
        total=total,
        new=new,
        updated=updated,
        unchanged=unchanged,
        invalid=invalid,
        errors=errors,
    )
