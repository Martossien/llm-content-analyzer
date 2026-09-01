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
from collections.abc import Callable
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
    manifeste ou un rapport futur recopierait tel quel."""

    csv_path: Path
    manifest_path: Path | None
    exit_code: int
    files: int
    elapsed_s: float
    command: list[str]
    manifest: dict[str, object] = field(default_factory=dict)
    tail: list[str] = field(default_factory=list)
    """Dernières lignes de sortie (diagnostic en cas d'échec)."""


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
    """Lignes de données du CSV (en-tête exclu) ; 0 si absent."""
    if not csv_path.is_file():
        return 0
    with csv_path.open("rb") as fh:
        n = sum(1 for _ in fh)
    return max(0, n - 1)


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

    Raises:
        ScanError: profil invalide, scanner introuvable ou impossible à lancer,
            code de sortie inattendu, aucun CSV produit, ou CSV vide alors que la
            progression annonçait des fichiers (écriture interrompue).
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
    result = ScanResult(
        csv_path=csv_out,
        manifest_path=manifest_out if manifest else None,
        exit_code=proc.returncode,
        files=files,
        elapsed_s=elapsed,
        command=redact_args(cmd),
        manifest=manifest,
        tail=tail,
    )
    cancelled = cancelled or (cancel is not None and cancel.is_set())
    if proc.returncode not in (EXIT_OK, EXIT_NOTHING) and not cancelled:
        detail = " | ".join(tail[-3:]) if tail else "aucune sortie"
        raise ScanError(f"scanner terminé avec le code {proc.returncode} : {detail}")
    if not csv_out.is_file():
        raise ScanError(
            "scan arrêté avant que le scanner n'écrive le CSV : rien à importer"
            if cancelled
            else "le scanner n'a produit aucun CSV"
        )
    if files == 0 and last_files > 0 and not cancelled:
        raise ScanError(
            f"le scanner annonçait {last_files} fichier(s) mais {csv_out} ne contient "
            "aucune ligne de données : écriture interrompue (disque plein, partage coupé, "
            "scanner tombé). Le scan est à refaire, ce CSV n'a rien à importer."
        )
    return result
