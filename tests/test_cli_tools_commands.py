"""`docia quick`, `docia scan`, `docia bench` par leurs fonctions de commande.

`quick --dry-run` traverse DocFuse pour de vrai (extraction, blocs) sans LLM ;
`scan` pilote le faux scanner de `tests/test_scan.py` ; `bench` reçoit un rapport
doublé. Ce sont les trois commandes que la CI Windows n'exerce que par l'exe.
"""

from __future__ import annotations

import argparse
import json
import types
from pathlib import Path
from typing import Any

import pytest

from docia import cli_tools
from docia.config import Config
from tests.test_scan import _fake_scanner


def _cfg(tmp_path: Path) -> Config:
    cfg = Config()
    cfg.db_path = str(tmp_path / "campagne.sqlite")
    cfg.llm.base_url = "http://127.0.0.1:9/v1"
    return cfg


def test_quick_dry_run_construit_les_blocs_sans_llm(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    dossier = tmp_path / "docs"
    dossier.mkdir()
    (dossier / "note.txt").write_text(
        "Contrat de travail de Jean Dupont, salaire 2 345 EUR.\n" * 20, encoding="utf-8"
    )
    (dossier / "vide.txt").write_text("", encoding="utf-8")
    args = argparse.Namespace(paths=[dossier], keep_db=None, json=True, dry_run=True)
    code = cli_tools.cmd_quick(args, _cfg(tmp_path))
    report = json.loads(capsys.readouterr().out)
    assert report["dry_run"] is True
    assert report["requested"] == 2
    assert report["blocks_built"] >= 1
    assert code in (0, 2)  # 2 si le fichier vide est compté en échec d'extraction


def test_quick_chemin_introuvable_sort_en_1(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    args = argparse.Namespace(paths=[tmp_path / "absent"], keep_db=None, json=False, dry_run=True)
    assert cli_tools.cmd_quick(args, _cfg(tmp_path)) == 1
    assert "introuvable" in capsys.readouterr().err


def test_scan_pilote_le_scanner_et_prepare(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    exe = _fake_scanner(tmp_path, rows=3)
    cfg = _cfg(tmp_path)
    cfg.scan.smbeagle_path = str(exe)
    cible = tmp_path / "partage"
    cible.mkdir()
    args = argparse.Namespace(
        local_path=[str(cible)],
        host=[],
        share=[],
        exclude_share=[],
        domain="",
        username="",
        csv=None,
        json=True,
        no_plan=False,
    )
    code = cli_tools.cmd_scan(args, cfg)
    out = capsys.readouterr().out
    summary = json.loads(out)
    assert code == 0, out
    assert summary["files"] == 3
    assert summary["new"] == 3
    assert summary["complete"] is True
    assert summary["skipped"] == []


def test_scan_perimetre_amputé_sort_en_2(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    exe = _fake_scanner(tmp_path, rows=2, exit_code=4, skipped=["\\\\srv\\finance"])
    cfg = _cfg(tmp_path)
    cfg.scan.smbeagle_path = str(exe)
    cible = tmp_path / "partage"
    cible.mkdir()
    args = argparse.Namespace(
        local_path=[str(cible)],
        host=[],
        share=[],
        exclude_share=[],
        domain="",
        username="",
        csv=None,
        json=True,
        no_plan=True,
    )
    code = cli_tools.cmd_scan(args, cfg)
    summary = json.loads(capsys.readouterr().out)
    assert code == 2
    assert summary["complete"] is False
    assert summary["skipped"] == ["\\\\srv\\finance"]


def test_scan_profil_invalide_sort_en_2(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    args = argparse.Namespace(
        local_path=["relatif/partage"],
        host=[],
        share=[],
        exclude_share=[],
        domain="",
        username="",
        csv=None,
        json=False,
        no_plan=False,
    )
    assert cli_tools.cmd_scan(args, _cfg(tmp_path)) == 2
    assert "chemin non absolu" in capsys.readouterr().err


def _bench_report(*, ok: bool = True, errors: int = 0) -> Any:
    return types.SimpleNamespace(
        ok=ok,
        errors=errors,
        message="serveur injoignable" if not ok else "",
        as_dict=lambda: {"ok": ok, "errors": errors},
        as_lines=lambda: ["banc : ok"],
    )


@pytest.mark.parametrize(("ok", "errors", "attendu"), [(True, 0, 0), (True, 2, 2), (False, 0, 1)])
def test_bench_codes_de_retour(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    ok: bool,
    errors: int,
    attendu: int,
) -> None:
    import docia.bench

    monkeypatch.setattr(
        docia.bench, "run_bench", lambda _cfg, **_k: _bench_report(ok=ok, errors=errors)
    )
    args = argparse.Namespace(
        blocks=1, block_tokens=1000, files_per_block=2, in_flight=1, json=True
    )
    assert cli_tools.cmd_bench(args, _cfg(tmp_path)) == attendu
    captured = capsys.readouterr()
    assert json.loads(captured.out)["ok"] is ok
    if not ok:
        assert "injoignable" in captured.err
