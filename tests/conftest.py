"""Fixtures partagées : serveur OpenAI factice (`tests/fake_openai.py`) et isolation
du dossier de configuration.

`DOCIA_HOME` est forcé vers un dossier temporaire pour **chaque** test : sans cela,
`service.remember_campaign` (appelé par les tests de scan et de fenêtre) écrivait
les campagnes de pytest dans le vrai `~/.config/docia/recent.json` de la machine —
l'accueil de l'utilisateur listait des bases `/tmp/pytest-of-…` disparues.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytest_plugins = ["tests.fake_openai"]


@pytest.fixture(autouse=True)
def _docia_home_isole(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DOCIA_HOME", str(tmp_path / "config_docia"))
