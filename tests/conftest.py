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


def prompt_court(tmp_path: Path) -> Path:
    """Prompt système **court** pour les tests qui simulent un serveur à petit contexte.

    Le prompt embarqué fait ~2 000 tokens : sur un contexte simulé de 4 000 ou 12 000
    tokens, il ne laisserait rien aux blocs. Ces tests éprouvent la mécanique
    (découpage, reprise, banc), pas le prompt : ils en prennent un de 200 caractères.
    """
    path = tmp_path / "prompt_court.md"
    path.write_text(
        "Tu es un analyste documentaire. Pour chaque fichier du corpus, rends une entrée "
        "JSON avec file_ref, resume, security, rgpd, finance, legal et retention, "
        "sans commentaire.\n",
        encoding="utf-8",
    )
    return path
