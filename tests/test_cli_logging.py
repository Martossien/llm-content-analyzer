"""Journalisation de la CLI : console lisible, détail complet sur disque.

Une campagne réelle rencontre toujours des fichiers illisibles ; sans ce partage,
l'utilisateur voit défiler des dizaines de traces Python et croit à un plantage.
"""

from __future__ import annotations

import argparse
import logging
from collections.abc import Iterator
from pathlib import Path

import pytest

import docia.cli as cli


@pytest.fixture
def racine_propre() -> Iterator[logging.Logger]:
    """Isole le logger racine (pytest y installe ses propres gestionnaires)."""
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    root.handlers = []
    cli._JOURNAL, cli._LOGGING_CONFIGURED = None, False
    try:
        yield root
    finally:
        for h in root.handlers:
            h.close()
        root.handlers, root.level = handlers, level
        cli._JOURNAL, cli._LOGGING_CONFIGURED = None, False


def _args(tmp_path: Path) -> argparse.Namespace:
    return argparse.Namespace(verbose=False, config=tmp_path / "docia.toml")


def test_console_sans_pile_journal_avec_pile(
    racine_propre: logging.Logger, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    journal = cli._setup_logging(_args(tmp_path))
    assert journal == tmp_path / "docia.log"

    try:
        raise ValueError("mail illisible")
    except ValueError:
        logging.getLogger("docfuse.extractors.msg").warning(
            "Erreur extraction MSG %s", "D:\\part\\note.msg", exc_info=True
        )
    for handler in racine_propre.handlers:
        handler.flush()

    console = capsys.readouterr().err
    assert "Erreur extraction MSG D:\\part\\note.msg" in console
    assert "Traceback" not in console, "la pile d'appels n'a rien à faire dans la console"
    assert console.strip().count("\n") == 0, "un incident = une ligne"

    contenu = journal.read_text(encoding="utf-8")
    assert "Erreur extraction MSG" in contenu
    assert "Traceback (most recent call last)" in contenu
    assert "ValueError: mail illisible" in contenu


def test_configuration_idempotente(racine_propre: logging.Logger, tmp_path: Path) -> None:
    """La fenêtre rappelle `main()` pour produire ses documents : pas de doublons."""
    first = cli._setup_logging(_args(tmp_path))
    count = len(racine_propre.handlers)
    assert cli._setup_logging(_args(tmp_path)) == first
    assert len(racine_propre.handlers) == count


def test_journal_impossible_ne_bloque_pas(
    racine_propre: logging.Logger, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Dossier en lecture seule : on garde la console, on ne plante pas."""
    monkeypatch.setattr(
        cli, "_log_file", lambda _c: tmp_path / "inexistant" / "sous-dossier" / "docia.log"
    )
    monkeypatch.setattr(Path, "resolve", Path.absolute)
    assert cli._setup_logging(_args(tmp_path)) is None
    assert racine_propre.handlers, "la console reste branchée"
