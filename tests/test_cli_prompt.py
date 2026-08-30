"""CLI : profils de prompt et revue."""

from __future__ import annotations

from pathlib import Path

import pytest

from docia.cli import main
from docia.db import Database
from docia.llm.schema import load_system_prompt


def test_prompt_lifecycle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.chdir(tmp_path)
    assert main(["init"]) == 0
    capsys.readouterr()
    assert main(["prompt", "show"]) == 0
    assert capsys.readouterr().out.strip() == load_system_prompt(None).strip()

    custom = tmp_path / "mon_prompt.md"
    custom.write_text(
        load_system_prompt(None) + "\nAjoute un champ dans le résumé : la langue du document.\n",
        encoding="utf-8",
    )
    assert main(["prompt", "save", "langue", str(custom), "--use"]) == 0
    assert main(["prompt", "list"]) == 0
    out = capsys.readouterr().out
    assert "* langue" in out
    assert main(["prompt", "show"]) == 0
    assert "langue du document" in capsys.readouterr().out
    with Database("docia.sqlite") as db:
        assert db.active_prompt() is not None
    assert main(["prompt", "reset"]) == 0
    assert main(["prompt", "export", str(tmp_path / "out.md"), "--name", "langue"]) == 0
    assert "langue du document" in (tmp_path / "out.md").read_text(encoding="utf-8")
    assert main(["prompt", "use", "inconnu"]) == 1
    assert main(["prompt", "delete", "langue"]) == 0
    short = tmp_path / "court.md"
    short.write_text("trop court", encoding="utf-8")
    assert main(["prompt", "save", "x", str(short)]) == 1


def test_review_requires_known_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.chdir(tmp_path)
    assert main(["init"]) == 0
    assert main(["review", "42", "--status", "validated"]) == 1
    capsys.readouterr()
