"""Socle du service : erreurs, dataclasses de résultat, constantes, dossier de configuration."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from docia.config import Config
from docia.db import Database
from docia.llm.schema import prompt_hash
from docia.pipeline import resolve_system_prompt
from docia.views import RunStat

logger = logging.getLogger(__name__)


HOME_ENV = "DOCIA_HOME"
"""Variable d'environnement qui redirige le dossier de configuration (tests, poste verrouillé)."""

RECENT_FILE = "recent.json"
MAX_RECENT = 20
DEFAULT_KEEP_BACKUPS = 10
"""Sauvegardes **courantes** conservées par la rotation — seule source de vérité.

`cli.py` doit importer cette valeur (`--keep`) plutôt que d'en redéfinir une : elle
gouverne ce que la rotation supprime, et deux valeurs qui divergent, c'est une
campagne effacée un jour où l'on croyait en garder dix.
"""
BACKUP_SUFFIX = ".sqlite"
SAFETY_LABEL_PREFIX = "avant_"
"""Étiquette d'une **copie de sûreté** : filet posé juste avant une opération
destructrice (`avant_migration_*`, `avant_restauration`, `avant_reanalyse_*`).

Ce ne sont pas des sauvegardes courantes : elles n'entrent jamais dans le vivier
des `DEFAULT_KEEP_BACKUPS` copies tournantes. Une rotation qui les emportait
supprimait précisément le filet dont on a besoin quand l'opération a mal tourné.
Elles restent listées par `list_backups` (l'utilisateur doit pouvoir les
restaurer) et se suppriment à la main.
"""
REANALYZE_SCOPES = ("all", "errors", "pending_only", "filter")
WHERE_KEYS = ("security", "rgpd", "owner", "extension", "path_like")


class ServiceError(Exception):
    """Erreur métier destinée à l'utilisateur (message en français, sans trace)."""


# --------------------------------------------------------------------- modèles


@dataclass(frozen=True)
class CampaignStatus:
    """Photographie d'une campagne : avancement, risques, prompt actif, dernier run."""

    db_path: Path
    files: int
    pending: int
    queued: int
    done: int
    error: int
    excluded: int
    analyses: int
    blocks_built: int
    blocks_sent: int
    blocks_done: int
    blocks_error: int
    reviewed: int
    to_review: int
    security: dict[str, int]
    rgpd: dict[str, int]
    active_prompt: str
    last_run: RunStat | None
    schema_version: int

    @property
    def percent_done(self) -> float:
        """Part des fichiers analysés parmi ceux qui doivent l'être (hors exclus)."""
        target = self.files - self.excluded
        return round(100.0 * self.done / target, 1) if target > 0 else 0.0


@dataclass(frozen=True)
class RunEvent:
    """Un événement de progression d'un run (ce que voit une barre d'avancement)."""

    kind: str
    """`info` | `block_done` | `block_error` | `file_error` | `cancelled` | `finished`."""
    message: str
    files_done: int
    files_total: int
    files_error: int
    blocks_done: int
    blocks_total: int
    elapsed_s: float
    eta_s: float | None
    files_per_hour: float | None


@dataclass(frozen=True)
class RecentCampaign:
    """Une campagne récemment ouverte (liste `recent.json`)."""

    db_path: Path
    csv_path: Path | None
    last_opened: str
    label: str


# ------------------------------------------------------------------- helpers


def _now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def _stamp() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S")


def _slug(text: str) -> str:
    """Étiquette réduite aux caractères sûrs dans un nom de fichier Windows."""
    keep = [c if (c.isalnum() or c in "-_") else "_" for c in text.strip()]
    return "".join(keep).strip("_")[:40]


def _effective_keys(db: Database, cfg: Config) -> tuple[str, str]:
    """(empreinte de prompt effective, modèle courant) — la clé d'une analyse."""
    return prompt_hash(resolve_system_prompt(db, cfg), cfg.llm.model), cfg.llm.model


def docia_home() -> Path:
    """Dossier de configuration : `$DOCIA_HOME`, `%APPDATA%/docia` ou `~/.config/docia`."""
    override = os.environ.get(HOME_ENV, "").strip()
    if override:
        return Path(override)
    appdata = os.environ.get("APPDATA", "").strip()
    if os.name == "nt" and appdata:
        return Path(appdata) / "docia"
    return Path.home() / ".config" / "docia"


# -------------------------------------------------------------------- statut
