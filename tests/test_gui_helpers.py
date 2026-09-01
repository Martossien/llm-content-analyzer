"""Fonctions pures de la GUI (testables sans fenêtre) et import du cœur sans Tk."""

from __future__ import annotations

import subprocess
import sys
import tomllib
from pathlib import Path

from docia.config import Config, load_config
from docia.gui import config_to_toml, parse_int

ROOT = Path(__file__).resolve().parent.parent


def test_config_to_toml_roundtrip(tmp_path: Path) -> None:
    cfg = Config(db_path="x.sqlite")
    cfg.llm.transport = "openwebui"
    cfg.llm.base_url = "http://srv:8080/api"
    cfg.llm.max_in_flight = 3
    cfg.blocks.block_tokens = 16000
    cfg.filter.excluded_extensions = [".zip", ".log"]
    text = config_to_toml(cfg)
    tomllib.loads(text)
    (tmp_path / "docia.toml").write_text(text, encoding="utf-8")
    back = load_config(tmp_path / "docia.toml")
    assert back.llm.transport == "openwebui"
    assert back.llm.base_url == "http://srv:8080/api"
    assert (back.llm.max_in_flight, back.blocks.block_tokens) == (3, 16000)
    assert back.filter.excluded_extensions == [".zip", ".log"]
    assert back.validate() == []


def test_parse_int() -> None:
    assert parse_int("42", 1) == 42
    assert parse_int("1 000", 1) == 1000
    assert parse_int("abc", 7) == 7
    assert parse_int("0", 5) == 5
    assert parse_int("0", 5, minimum=0) == 0
    assert parse_int("500", 1000, minimum=1000) == 1000


def test_core_imports_without_customtkinter() -> None:
    """Le cœur (cli, pipeline, db) doit s'importer même si customtkinter est absent."""
    code = (
        "import sys; sys.modules['customtkinter'] = None; "
        "import docia.cli, docia.pipeline, docia.db, docia.gui; print('ok')"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert out.stdout.strip() == "ok"


def test_main_without_args_reports_missing_gui_cleanly() -> None:
    code = "import sys; sys.modules['customtkinter'] = None; sys.argv=['docia']; from docia.__main__ import main; main()"
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert out.returncode == 1
    assert "docia[gui]" in out.stderr


# ------------------------------------------------ filet d'exception de la fenêtre
def test_crash_line_dit_tout_en_une_ligne_sans_pile() -> None:
    """L'utilisateur reçoit une phrase, pas une trace Python."""
    from docia.gui.app import crash_line

    ligne = crash_line("action de la fenêtre", KeyError("colonne 'sécu'"), "C:\\docia\\docia.log")

    assert "\n" not in ligne
    assert "Traceback" not in ligne
    assert "action de la fenêtre" in ligne
    assert "KeyError" in ligne
    assert "C:\\docia\\docia.log" in ligne


def test_crash_line_supporte_une_exception_muette_et_bavarde() -> None:
    from docia.gui.app import crash_line

    assert "RuntimeError" in crash_line("x", RuntimeError(), "docia.log")
    longue = crash_line("x", ValueError("détail " * 200), "docia.log")
    assert len(longue) < 400
    assert "\n" not in longue


def test_journal_de_la_fenetre_recoit_les_couches_basses() -> None:
    """`docia.service`, `docia.db`, DocFuse : leurs avertissements atteignent l'écran.

    Avant, ils n'allaient qu'au fichier `docia.log` et à une console que l'exe
    fenêtré n'affiche pas : l'utilisateur ne voyait rien.
    """
    import logging

    from docia.gui.app import WINDOW_SKIP, _JournalToWindow

    vues: list[str] = []
    handler = _JournalToWindow(vues.append)
    racine = logging.getLogger()
    niveau = racine.level
    racine.setLevel(logging.DEBUG)
    racine.addHandler(handler)
    try:
        logging.getLogger("docia.service").info("détail sans intérêt pour l'utilisateur")
        logging.getLogger("docia.db").warning("base verrouillée par une autre instance")
        try:
            raise ValueError("panne interne")
        except ValueError:
            logging.getLogger("docfuse.extractors").exception("extraction impossible")
        logging.getLogger("docia.gui.app").error(
            "déjà dit par la fenêtre", extra={WINDOW_SKIP: True}
        )
    finally:
        racine.removeHandler(handler)
        racine.setLevel(niveau)

    assert any("base verrouillée" in v for v in vues), vues
    assert any("extraction impossible" in v for v in vues), vues
    assert not any("sans intérêt" in v for v in vues), "INFO ne doit pas inonder la fenêtre"
    assert not any("déjà dit" in v for v in vues), "pas de doublon avec le message de la fenêtre"
    assert all("Traceback" not in v and "\n" not in v for v in vues), vues


def test_journal_de_la_fenetre_ne_leve_jamais() -> None:
    """Un puits cassé ne doit pas faire tomber l'émetteur du message."""
    import logging

    from docia.gui.app import _JournalToWindow

    def puits(_: str) -> None:
        raise RuntimeError("file pleine")

    handler = _JournalToWindow(puits)
    handler.handleError = lambda _record: None  # type: ignore[method-assign]
    handler.emit(logging.LogRecord("docia.db", logging.WARNING, __file__, 1, "message", None, None))
