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
import shutil
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

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


class ScanError(Exception):
    """Scanner introuvable, arguments invalides ou scan échoué (message lisible)."""


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
    """Ligne de commande SMBeagle_enriched pour ce profil (sans shell)."""
    cmd: list[str] = [str(exe), "-c", str(csv_out), "-q"]
    if profile.local_paths:
        for p in profile.local_paths:
            cmd += ["--local-path", p]
    else:
        cmd.append("-D")  # pas de découverte réseau : périmètre explicite
        for h in profile.hosts:
            cmd += ["-h", h]
        for s in profile.shares:
            cmd += ["-s", s]
        for s in profile.exclude_shares:
            cmd += ["-S", s]
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


def redact(cmd: list[str]) -> str:
    """Ligne de commande affichable (mot de passe masqué)."""
    out: list[str] = []
    hide_next = False
    for part in cmd:
        out.append("••••" if hide_next else part)
        hide_next = part == "-p"
    return " ".join(f'"{p}"' if " " in p else p for p in out)


def parse_progress_line(line: str) -> ScanEvent | None:
    """Ligne `--progress-json` → événement ; None pour une ligne texte ordinaire."""
    text = line.strip()
    if not text.startswith("{"):
        return None
    try:
        data = json.loads(text)
    except ValueError:
        return None
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


def count_csv_rows(csv_path: Path) -> int:
    """Lignes de données du CSV (en-tête exclu) ; 0 si absent."""
    if not csv_path.is_file():
        return 0
    with csv_path.open("rb") as fh:
        n = sum(1 for _ in fh)
    return max(0, n - 1)


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
    (le CSV partiel est conservé : il reste importable)."""
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
    if on_line is not None:
        on_line(f"scan : {redact(cmd)}")
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
    try:
        for raw in proc.stdout:
            line = raw.rstrip("\r\n")
            if cancel is not None and cancel.is_set():
                proc.terminate()
                if on_line is not None:
                    on_line("scan : arrêt demandé — CSV partiel conservé")
                break
            event = parse_progress_line(line)
            if event is not None:
                event.elapsed_s = event.elapsed_s or (time.monotonic() - started)
                last_files = max(last_files, event.files)
                if on_event is not None:
                    on_event(event)
                continue
            if line.strip():
                tail.append(line)
                del tail[:-40]
                if on_line is not None:
                    on_line(f"scan : {line.strip()}")
    finally:
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
    elapsed = time.monotonic() - started
    manifest: dict[str, object] = {}
    if manifest_out is not None and manifest_out.is_file():
        try:
            loaded = json.loads(manifest_out.read_text(encoding="utf-8"))
            manifest = loaded if isinstance(loaded, dict) else {}
        except ValueError:
            logger.warning("manifeste illisible : %s", manifest_out)
    files = count_csv_rows(csv_out) or last_files
    result = ScanResult(
        csv_path=csv_out,
        manifest_path=manifest_out if manifest else None,
        exit_code=proc.returncode,
        files=files,
        elapsed_s=elapsed,
        command=cmd,
        manifest=manifest,
        tail=tail,
    )
    cancelled = cancel is not None and cancel.is_set()
    if proc.returncode not in (EXIT_OK, EXIT_NOTHING) and not cancelled:
        detail = " | ".join(tail[-3:]) if tail else "aucune sortie"
        raise ScanError(f"scanner terminé avec le code {proc.returncode} : {detail}")
    if not csv_out.is_file():
        raise ScanError("le scanner n'a produit aucun CSV")
    return result
