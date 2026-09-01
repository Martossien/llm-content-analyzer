"""Pilotage de SMBeagle_enriched en sous-processus : étape 0 d'une campagne.

Le scanner (`SMBeagle.exe`, C#) reste un programme séparé — c'est lui qui a le
code SMB/Win32 ; docia le trouve (config, à côté de l'exécutable, PATH), construit
la ligne de commande à partir d'un `ScanProfile`, lit sa progression (`--progress-json`
quand disponible, sinon les lignes texte) et récupère le CSV 19 colonnes + le
manifeste. Aucun import Tk ni argparse ici : la CLI, la GUI et le serveur web
(v4) appellent `run_scan` de la même façon.
"""

from __future__ import annotations

import json
import logging
import os
import queue
import shutil
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import IO

logger = logging.getLogger(__name__)

SMBEAGLE_EXE = "SMBeagle.exe" if os.name == "nt" else "SMBeagle"
ENRICHMENT_FLAGS = (
    "--sizefile",
    "--access-time",
    "--fileattributes",
    "--ownerfile",
    "--fasthash",
    "--file-signature",
)
"""Les 6 colonnes enrichies dont docia a besoin (taille, dates d'accès, attributs,
propriétaire, empreinte pour les doublons, signature)."""

EXIT_OK = 0
EXIT_ARGS = 2
EXIT_NOTHING = 3
EXIT_PARTIAL = 4
"""Scan terminé, CSV bon, mais **une cible demandée n'a pas été scannée**.

Le scanner sort en 4 dès que son manifeste porte un `skipped` non vide (ACL qui
refuse un partage, montage cassé). Ce n'est pas un échec : les lignes écrites
sont exactes et s'importent normalement — seul le périmètre est amputé. Traiter
ce code comme fatal transformerait un scan parfaitement exploitable en échec dur.
"""

EXIT_ACCEPTED = (EXIT_OK, EXIT_NOTHING, EXIT_PARTIAL)
"""Codes de retour qui ne sont pas des échecs : CSV exploitable dans les trois cas."""

CANCEL_POLL_S = 0.25
"""Délai d'attente d'une ligne de sortie avant de reconsulter `cancel`.

SMBeagle est lancé avec `-q` : il n'écrit rien pendant l'énumération d'un gros
partage, et rien du tout pendant le délai TCP d'un hôte injoignable. Attendre la
prochaine ligne pour regarder `cancel`, c'est ignorer « Arrêter » pendant des
minutes — l'utilisateur tue alors l'exécutable, et le CSV partiel que l'on prend
soin de conserver part avec."""


class ScanError(Exception):
    """Scanner introuvable, arguments invalides ou scan échoué (message lisible)."""


def _is_absolute(path: str) -> bool:
    """Chemin complet, forme Windows (`D:\\x`, `\\\\srv\\part`) ou POSIX (`/mnt/x`).

    Un chemin relatif serait résolu par le scanner contre SON répertoire courant —
    donc un tout autre dossier que celui attendu. Le scanner le refuse (code 2) ;
    docia le dit plus tôt et plus clairement.
    """
    text = path.strip()
    if not text:
        return False
    return PureWindowsPath(text).is_absolute() or PurePosixPath(text).is_absolute()


@dataclass
class ScanProfile:
    """Ce que l'utilisateur choisit : un périmètre et les options d'enrichissement."""

    local_paths: list[str] = field(default_factory=list)
    """Dossiers locaux ou chemins UNC montés (`--local-path`, plusieurs acceptés)."""
    hosts: list[str] = field(default_factory=list)
    """Serveurs SMB à scanner (`--host`), découverte réseau désactivée."""
    shares: list[str] = field(default_factory=list)
    """Partages à retenir (`--share`) ; vide = tous les partages visibles."""
    exclude_shares: list[str] = field(default_factory=list)
    exclude_hidden_shares: bool = True
    enrich: bool = True
    """Ajoute les 6 colonnes enrichies (toujours vrai pour une campagne docia)."""
    preserve_access_time: bool = True
    """`--preserve-access-time` : restaure la date d'accès après lecture (hachage,
    signature), pour ne pas fausser « non accédé depuis X ans »."""
    skip_acls: bool = False
    """`-A` : sans énumération des ACL (plus rapide ; colonnes lecture/écriture vides)."""
    domain: str = ""
    username: str = ""
    password: str = ""
    """Hors Windows (ou compte explicite) : identifiants SMB."""
    extra_args: list[str] = field(default_factory=list)

    def validate(self) -> list[str]:
        errors: list[str] = []
        if not self.local_paths and not self.hosts:
            errors.append("indique au moins un dossier (local ou UNC monté) ou un serveur SMB")
        if self.local_paths and self.hosts:
            errors.append("dossiers locaux et serveurs SMB ne se combinent pas dans un même scan")
        errors += [
            f"chemin non absolu : « {p} » — indique un chemin complet "
            "(`D:\\dossier`, `\\\\serveur\\partage` ou `/mnt/partage`)"
            for p in self.local_paths
            if not _is_absolute(p)
        ]
        if bool(self.username) != bool(self.password):
            errors.append("identifiant et mot de passe vont ensemble")
        return errors

    def targets(self) -> list[str]:
        return list(self.local_paths) or list(self.hosts)


@dataclass
class ScanEvent:
    """Progression lisible par une barre : étape, compteurs, temps écoulé, message."""

    stage: str
    message: str
    hosts: int = 0
    shares: int = 0
    files: int = 0
    elapsed_s: float = 0.0


@dataclass
class ScanResult:
    """Bilan d'un scan. `command` est **déjà masquée** (voir `redact_args`) : le
    mot de passe SMB ne doit pas survivre dans un objet qu'un journal, un
    manifeste ou un rapport futur recopierait tel quel.

    Les trois champs de **périmètre** (`skipped`, `cancelled`, `expected_files`)
    existent parce qu'un audit sert à décider de suppressions : « je n'ai pas
    tout vu » doit être un fait transmis à l'appelant, pas une ligne perdue dans
    la sortie du scanner. Ils sont recopiés en base par `service.scan_campaign`
    et survivent donc au manifeste.
    """

    csv_path: Path
    manifest_path: Path | None
    exit_code: int
    files: int
    elapsed_s: float
    command: list[str]
    manifest: dict[str, object] = field(default_factory=dict)
    tail: list[str] = field(default_factory=list)
    """Dernières lignes de sortie (diagnostic en cas d'échec)."""
    skipped: list[str] = field(default_factory=list)
    """Cibles demandées que le scanner n'a **pas** scannées (`manifest.skipped`).

    Vide sur un scan normal, et vide aussi avec un scanner antérieur au code 4 :
    l'absence de la clé n'invente jamais un périmètre incomplet."""
    cancelled: bool = False
    """L'utilisateur a demandé l'arrêt : le CSV ne porte qu'un fragment du périmètre."""
    expected_files: int = 0
    """Nombre de fichiers **annoncé** par le scanner ; 0 quand il n'a rien annoncé.

    Voir `expected_file_count` pour la source retenue et pourquoi."""

    @property
    def missing_files(self) -> int:
        """Lignes annoncées mais absentes du CSV (0 si le compte est bon ou inconnu)."""
        return max(self.expected_files - self.files, 0)

    @property
    def complete(self) -> bool:
        """Vrai si le scan couvre **tout** ce qui a été demandé.

        Faux dès qu'une cible a été écartée, que l'utilisateur a arrêté le scan,
        ou que le CSV compte moins de lignes que le scanner n'en a annoncé."""
        return not self.skipped and not self.cancelled and self.missing_files == 0


def find_smbeagle(configured: str = "") -> Path | None:
    """Chemin du scanner : config, puis à côté de l'exécutable docia (exe PyInstaller)
    ou du dépôt, puis PATH. None si introuvable."""
    candidates: list[Path] = []
    if configured:
        candidates.append(Path(configured))
    exe_dir = Path(sys.executable).resolve().parent if getattr(sys, "frozen", False) else None
    if exe_dir is not None:
        candidates += [exe_dir / SMBEAGLE_EXE, exe_dir / "smbeagle" / SMBEAGLE_EXE]
    candidates += [Path.cwd() / SMBEAGLE_EXE, Path.cwd() / "smbeagle" / SMBEAGLE_EXE]
    for c in candidates:
        if c.is_file():
            return c
    found = shutil.which(SMBEAGLE_EXE) or shutil.which("SMBeagle")
    return Path(found) if found else None


def build_command(
    exe: Path,
    profile: ScanProfile,
    csv_out: Path,
    *,
    manifest_out: Path | None = None,
    progress_json: bool = True,
) -> list[str]:
    """Ligne de commande SMBeagle_enriched pour ce profil (sans shell).

    Les options à valeurs multiples (`--local-path`, `-h`, `-s`, `-S`) attendent
    leurs valeurs **à la suite** (`--local-path A B`) : SMBeagle les déclare en
    séquences CommandLineParser, qui refuse l'option répétée
    (« Option 'local-path' is defined multiple times », code 2). Répéter l'option
    faisait donc échouer tout scan portant sur plus d'un dossier.
    """
    cmd: list[str] = [str(exe), "-c", str(csv_out), "-q"]
    if profile.local_paths:
        cmd += ["--local-path", *profile.local_paths]
    else:
        cmd.append("-D")  # pas de découverte réseau : périmètre explicite
        if profile.hosts:
            cmd += ["-h", *profile.hosts]
        if profile.shares:
            cmd += ["-s", *profile.shares]
        if profile.exclude_shares:
            cmd += ["-S", *profile.exclude_shares]
        if profile.exclude_hidden_shares:
            cmd.append("-E")
    if profile.enrich:
        cmd += list(ENRICHMENT_FLAGS)
        if profile.preserve_access_time:
            cmd.append("--preserve-access-time")
    if profile.skip_acls:
        cmd.append("-A")
    if profile.domain:
        cmd += ["-d", profile.domain]
    if profile.username:
        cmd += ["-u", profile.username, "-p", profile.password]
    if progress_json:
        cmd.append("--progress-json")
    if manifest_out is not None:
        cmd += ["--manifest", str(manifest_out)]
    cmd += list(profile.extra_args)
    return cmd


def redact_args(cmd: list[str]) -> list[str]:
    """Copie de la ligne de commande, la valeur de `-p` remplacée par `••••`.

    C'est cette forme-là que l'on garde (`ScanResult.command`) et que l'on
    affiche : un mot de passe SMB conservé en clair dans un objet de résultat
    finit tôt ou tard dans un journal ou un manifeste.
    """
    out: list[str] = []
    hide_next = False
    for part in cmd:
        out.append("••••" if hide_next else part)
        hide_next = part == "-p"
    return out


def redact(cmd: list[str]) -> str:
    """Ligne de commande affichable (mot de passe masqué)."""
    return " ".join(f'"{p}"' if " " in p else p for p in redact_args(cmd))


def parse_progress_line(line: str) -> ScanEvent | None:
    """Ligne `--progress-json` → événement ; None pour une ligne texte ordinaire.

    Le contrat du scanner (programme C# versionné à part) n'est pas verrouillé :
    un `files` sérialisé avec séparateur de milliers (`"1 234"`, ce que rend
    `ToString("N0")` en fr-FR), un `elapsed_s` en `"00:01:23"` ou un flottant
    infini suffisent à rendre la ligne inexploitable. Une ligne de progression
    illisible est alors traitée comme du texte ordinaire (`None`) : c'est une
    ligne perdue, jamais un scan de plusieurs heures perdu sur une trace Python.
    """
    text = line.strip()
    if not text.startswith("{"):
        return None
    try:
        data = json.loads(text)
        if not isinstance(data, dict) or "event" not in data:
            return None
        kind = str(data.get("event"))
        if kind == "error":
            return ScanEvent(stage="error", message=str(data.get("message", "erreur du scanner")))
        return ScanEvent(
            stage=str(data.get("stage") or kind),
            message=str(data.get("message") or ""),
            hosts=int(data.get("hosts") or 0),
            shares=int(data.get("shares") or 0),
            files=int(data.get("files") or 0),
            elapsed_s=float(data.get("elapsed_s") or 0.0),
        )
    except (ValueError, TypeError, OverflowError):
        logger.debug("ligne de progression illisible, traitée comme du texte : %s", text[:200])
        return None


def count_csv_rows(csv_path: Path) -> int:
    """Lignes de données du CSV (en-tête exclu) ; 0 si absent.

    Le comptage est **physique** (une ligne du fichier = une ligne de données) :
    un nom de fichier contenant un saut de ligne — légal sous POSIX — est écrit
    entre guillemets et compte alors pour deux. Ce comptage ne peut donc que
    **surestimer** le nombre réel d'enregistrements, jamais le sous-estimer :
    c'est ce qui rend le contrôle de troncature (`files < expected_files`) sûr
    contre les faux positifs. Mesuré sur les fixtures du dépôt smbeagle : 7
    fichiers ordinaires → 7 ; un nom avec saut de ligne parmi 2 fichiers → 3.
    """
    if not csv_path.is_file():
        return 0
    with csv_path.open("rb") as fh:
        n = sum(1 for _ in fh)
    return max(0, n - 1)


def manifest_skipped(manifest: dict[str, object]) -> list[str]:
    """Cibles écartées déclarées par le manifeste (`skipped`), liste vide sinon.

    Tolère un manifeste sans la clé (`SMBeagle.exe` antérieur au code 4) comme un
    manifeste dont la clé n'est pas une liste : dans les deux cas le périmètre est
    réputé **entier**. Inventer une cible manquante serait pire que de n'en
    signaler aucune.
    """
    raw = manifest.get("skipped")
    if not isinstance(raw, list):
        return []
    return [str(item) for item in raw if str(item).strip()]


def expected_file_count(manifest: dict[str, object], last_files: int) -> int:
    """Nombre de fichiers que le scanner dit avoir produit ; 0 s'il n'a rien dit.

    Trois sources annoncent ce compte, et **le manifeste fait foi** :

    - `manifest.counts.files` est écrit *après* la fermeture et le vidage du CSV
      (`Finish()` : `OutputHelper.CloseAndFlush()` puis `manifest.Write()`), à
      partir du même compteur dédoublonné (`FileFinder.FileCount`) qui a produit
      une ligne CSV par fichier. C'est donc le seul chiffre qui décrive le fichier
      **tel qu'il a été refermé** ; vérifié sur les fixtures du dépôt smbeagle,
      `counts.files` = 7 pour 7 lignes de données, sans décalage d'unité.
    - l'événement `done` porte exactement la même valeur (`ProgressReporter.Done`
      reçoit `manifest.Files`), mais transite par la sortie standard : un tube
      coupé ou une ligne tronquée la perd.
    - `last_files` (le maximum vu en progression) n'est qu'un compteur *en cours* :
      il monte pendant l'énumération et n'est pas un décompte de lignes écrites.

    Le repli sur `last_files` ne sert donc que si le manifeste manque — scanner
    lancé sans `--manifest`, arrêt avant l'écriture du manifeste — et vaut alors
    ce que vaut la progression : un minorant du travail annoncé, jamais un chiffre
    de trop. Renvoie 0 quand aucune source ne s'est prononcée : pas d'annonce,
    pas de contrôle.
    """
    counts = manifest.get("counts")
    if isinstance(counts, dict):
        raw = counts.get("files")
        if isinstance(raw, bool):  # `True` vaudrait 1 : ce n'est pas un compte
            return max(last_files, 0)
        if isinstance(raw, int):
            return max(raw, 0)
        if isinstance(raw, (float, str)):
            try:
                return max(int(float(raw)), 0)
            except (TypeError, ValueError):
                logger.debug("manifeste : counts.files illisible (%r)", raw)
    return max(last_files, 0)


def scope_warnings(
    *, skipped: Sequence[str], cancelled: bool, expected_files: int, files: int
) -> list[str]:
    """Phrases d'avertissement sur le périmètre, pour un administrateur pressé.

    Une phrase par fait constaté : ce qui manque, puis quoi faire. Aucun code de
    retour brut, aucun nom de champ. Liste vide quand le périmètre est entier —
    c'est le cas normal, et il ne doit rien afficher du tout.

    Prend des faits nus plutôt qu'un objet : le même texte doit sortir juste après
    le scan (depuis un `ScanResult`) et des mois plus tard dans un rapport (depuis
    la table `scans`), sans que les deux formulations puissent diverger.
    """
    messages: list[str] = []
    if skipped:
        # Les trois premiers noms suffisent à situer le problème dans une ligne de
        # journal ou un bandeau ; la liste entière est portée à part (`skipped`),
        # là où un rapport peut l'afficher proprement.
        noms = ", ".join(skipped[:3])
        reste = len(skipped) - 3
        if reste > 0:
            noms += f" et {reste} autre(s)"
        messages.append(
            f"Périmètre incomplet : {len(skipped)} emplacement(s) demandé(s) n'ont pas pu "
            f"être parcourus ({noms}). Les fichiers qu'ils contiennent sont absents de "
            "l'audit. Vérifiez les droits d'accès (ou que le partage est bien monté), "
            "puis relancez le scan avant de décider la moindre suppression."
        )
    if cancelled:
        detail = (
            f"{files} fichier(s) sur les {expected_files} déjà repérés"
            if expected_files > files
            else f"{files} fichier(s)"
        )
        messages.append(
            f"Scan arrêté en cours de route : l'audit ne porte que sur {detail}. "
            "Relancez un scan complet avant de décider la moindre suppression."
        )
    elif expected_files > files:
        messages.append(
            f"Inventaire incomplet : le scanner annonçait {expected_files} fichier(s) et "
            f"n'en a écrit que {files}. Le scan est à refaire."
        )
    return messages


def _notify_event(on_event: Callable[[ScanEvent], None] | None, event: ScanEvent) -> None:
    """Transmet un événement de progression — sans jamais mettre le scan en danger.

    Le rappel écrit dans une console ou une fenêtre : une fenêtre détruite ne doit
    pas faire perdre un scan de plusieurs heures (même garde que `import_csv.notify`).
    """
    if on_event is None:
        return
    try:
        on_event(event)
    except Exception:  # noqa: BLE001 — l'affichage n'est jamais critique
        logger.debug("rappel d'événement en échec, scan poursuivi", exc_info=True)


def _notify_line(on_line: Callable[[str], None] | None, text: str) -> None:
    """Transmet une ligne de journal, sous la même garde que `_notify_event`."""
    if on_line is None:
        return
    try:
        on_line(text)
    except Exception:  # noqa: BLE001 — l'affichage n'est jamais critique
        logger.debug("rappel de ligne en échec, scan poursuivi", exc_info=True)


def _pump_stdout(stream: IO[str], sink: queue.Queue[str | None]) -> None:
    """Lit la sortie du scanner dans un fil et la dépose dans `sink` (None = fin).

    `for raw in proc.stdout:` bloque tant que le scanner se tait ; le déporter ici
    laisse la boucle principale consulter `cancel` à intervalle fixe.
    """
    try:
        for raw in stream:
            sink.put(raw.rstrip("\r\n"))
    except (OSError, ValueError):  # tube fermé par un terminate() : fin de lecture
        logger.debug("lecture de la sortie du scanner interrompue", exc_info=True)
    finally:
        sink.put(None)


def run_scan(
    profile: ScanProfile,
    csv_out: Path,
    *,
    exe: Path | None = None,
    configured_exe: str = "",
    on_event: Callable[[ScanEvent], None] | None = None,
    on_line: Callable[[str], None] | None = None,
    cancel: threading.Event | None = None,
    write_manifest: bool = True,
) -> ScanResult:
    """Lance le scanner et attend la fin. `cancel` arrête le processus proprement
    (le CSV partiel est conservé : il reste importable).

    `cancel` est consulté toutes les `CANCEL_POLL_S`, y compris pendant les longs
    silences du scanner, et les rappels `on_event` / `on_line` sont protégés :
    ni un « Arrêter » ignoré ni une fenêtre détruite ne doivent coûter un scan.

    Le résultat dit toujours si le périmètre est entier (`ScanResult.complete`) :
    cibles écartées par le scanner (`skipped`, code de retour 4), arrêt demandé
    (`cancelled`), ou CSV plus court que ce que le scanner a annoncé.

    Raises:
        ScanError: profil invalide, scanner introuvable ou impossible à lancer,
            code de sortie inattendu (le 4 « périmètre incomplet » n'en est pas
            un), aucun CSV produit, ou CSV plus court que le compte annoncé
            **sans** arrêt demandé (écriture interrompue).
    """
    errors = profile.validate()
    if errors:
        raise ScanError(" ; ".join(errors))
    scanner = exe or find_smbeagle(configured_exe)
    if scanner is None:
        raise ScanError(
            f"scanner introuvable : place {SMBEAGLE_EXE} à côté de l'exécutable docia "
            "ou renseigne scan.smbeagle_path"
        )
    csv_out = csv_out.resolve()
    csv_out.parent.mkdir(parents=True, exist_ok=True)
    manifest_out = csv_out.with_suffix(".manifest.json") if write_manifest else None
    cmd = build_command(scanner, profile, csv_out, manifest_out=manifest_out)
    _notify_line(on_line, f"scan : {redact(cmd)}")
    started = time.monotonic()
    tail: list[str] = []
    last_files = 0
    try:
        proc = subprocess.Popen(  # noqa: S603 — arguments construits par build_command, sans shell
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            cwd=str(csv_out.parent),
        )
    except OSError as exc:
        raise ScanError(f"lancement du scanner impossible : {exc}") from exc
    assert proc.stdout is not None
    lines: queue.Queue[str | None] = queue.Queue()
    reader = threading.Thread(
        target=_pump_stdout, args=(proc.stdout, lines), name="docia-scan-stdout", daemon=True
    )
    reader.start()
    cancelled = False
    try:
        while True:
            if cancel is not None and cancel.is_set():
                cancelled = True
                proc.terminate()
                _notify_line(on_line, "scan : arrêt demandé — CSV partiel conservé")
                break
            try:
                line = lines.get(timeout=CANCEL_POLL_S)
            except queue.Empty:
                continue  # le scanner se tait (énumération, hôte injoignable) : on repasse par `cancel`
            if line is None:  # fin de la sortie : le scanner a terminé
                break
            event = parse_progress_line(line)
            if event is not None:
                event.elapsed_s = event.elapsed_s or (time.monotonic() - started)
                last_files = max(last_files, event.files)
                _notify_event(on_event, event)
                continue
            if line.strip():
                tail.append(line)
                del tail[:-40]
                _notify_line(on_line, f"scan : {line.strip()}")
    finally:
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        # Le fil de lecture est un démon : on lui laisse le temps de finir sur une
        # sortie normale (il a déjà rendu la main), sans jamais rallonger un arrêt
        # demandé — un petit-fils orphelin peut garder le tube ouvert.
        reader.join(timeout=CANCEL_POLL_S)
    elapsed = time.monotonic() - started
    manifest: dict[str, object] = {}
    if manifest_out is not None and manifest_out.is_file():
        try:
            loaded = json.loads(manifest_out.read_text(encoding="utf-8"))
            manifest = loaded if isinstance(loaded, dict) else {}
        except ValueError:
            logger.warning("manifeste illisible : %s", manifest_out)
    # Le nombre de fichiers est **toujours** celui du CSV : le chiffre annoncé par la
    # progression n'est pas un décompte de lignes, et le faire passer pour tel a déjà
    # annoncé « scan terminé : 42 000 fichiers » sur un CSV de 0 octet.
    files = count_csv_rows(csv_out)
    cancelled = cancelled or (cancel is not None and cancel.is_set())
    skipped = manifest_skipped(manifest)
    result = ScanResult(
        csv_path=csv_out,
        manifest_path=manifest_out if manifest else None,
        exit_code=proc.returncode,
        files=files,
        elapsed_s=elapsed,
        command=redact_args(cmd),
        manifest=manifest,
        tail=tail,
        skipped=skipped,
        cancelled=cancelled,
        expected_files=expected_file_count(manifest, last_files),
    )
    if proc.returncode not in EXIT_ACCEPTED and not cancelled:
        detail = " | ".join(tail[-3:]) if tail else "aucune sortie"
        raise ScanError(f"scanner terminé avec le code {proc.returncode} : {detail}")
    if not csv_out.is_file():
        raise ScanError(
            "scan arrêté avant que le scanner n'écrive le CSV : rien à importer"
            if cancelled
            else "le scanner n'a produit aucun CSV"
        )
    if result.missing_files and not cancelled:
        # Le scanner a annoncé plus de lignes qu'il n'en a écrit et n'a pas été
        # arrêté : il est mort en écrivant (disque plein, partage coupé). Le CSV
        # est un fragment muet — l'importer donnerait un audit qui se croit
        # exhaustif. Le cas « aucune ligne du tout » n'est que le cas extrême de
        # celui-ci ; le contrôle ne s'y limite plus.
        raise ScanError(
            f"le scanner annonçait {result.expected_files} fichier(s) mais {csv_out} "
            f"n'en contient que {files} : écriture interrompue (disque plein, partage "
            "coupé, scanner tombé). Le scan est à refaire, ce CSV n'est pas exploitable."
        )
    if skipped:
        _notify_line(
            on_line,
            "scan : périmètre incomplet — non scanné(s) : "
            + ", ".join(skipped)
            + " (accès refusé ou emplacement injoignable)",
        )
        logger.warning("scan au périmètre incomplet, cibles écartées : %s", ", ".join(skipped))
    return result
