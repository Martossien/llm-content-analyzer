"""Dossier de configuration de docia (`recent.json`, `pacer.json`).

Ici et pas dans `service/` : le pipeline y mémorise le budget adaptatif, et le
service dépend du pipeline — jamais l'inverse.
"""

from __future__ import annotations

import os
from pathlib import Path

HOME_ENV = "DOCIA_HOME"
"""Variable d'environnement qui remplace le dossier par défaut (tests, portable)."""


def docia_home() -> Path:
    """Dossier de configuration : `$DOCIA_HOME`, `%APPDATA%/docia` ou `~/.config/docia`."""
    override = os.environ.get(HOME_ENV, "").strip()
    if override:
        return Path(override)
    appdata = os.environ.get("APPDATA", "").strip()
    if os.name == "nt" and appdata:
        return Path(appdata) / "docia"
    return Path.home() / ".config" / "docia"
