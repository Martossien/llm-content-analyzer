"""Fonctions pures de la GUI (testables sans fenêtre) et import du cœur sans Tk."""

from __future__ import annotations

import sqlite3
import subprocess
import sys
import tomllib
from pathlib import Path

from docia.config import Config, load_config
from docia.gui import config_to_toml, parse_int, result_rows, status_lines

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


def test_status_lines() -> None:
    counts = {
        "files": 10,
        "pending": 2,
        "queued": 1,
        "done": 6,
        "excluded": 1,
        "error": 0,
        "blocks_built": 0,
        "blocks_sent": 1,
        "blocks_done": 3,
        "blocks_error": 0,
        "analyses": 6,
    }
    lines = status_lines(counts, {"security": {"C0": 4, "C3": 2}, "rgpd": {}})
    assert lines[0].startswith("fichiers : 10")
    assert "analyses : 6" in lines[1]
    assert lines[2] == "sécurité : C0 4, C3 2"
    assert len(lines) == 3


def test_result_rows_limit_and_fallbacks() -> None:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE t(name, security_classification, rgpd_risk_level, finance_document_type, "
        "legal_contract_type, resume, status, exclusion_reason)"
    )
    conn.execute("INSERT INTO t VALUES('a.txt','C2','low','none','none','résumé a','done',NULL)")
    conn.execute("INSERT INTO t VALUES('b.txt',NULL,NULL,NULL,NULL,NULL,'error','introuvable')")
    conn.execute("INSERT INTO t VALUES('c.txt',NULL,NULL,NULL,NULL,NULL,'pending',NULL)")
    rows = result_rows(conn.execute("SELECT * FROM t"), limit=2)
    assert rows == [
        ("a.txt", "C2", "low", "none", "none", "résumé a"),
        ("b.txt", "error", "", "", "", "introuvable"),
    ]


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
