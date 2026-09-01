"""Analyse immédiate (`docia.quick`) : dossier, fichier seul, reprise, erreurs."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import pytest

from docia.cli_tools import register
from docia.config import Config
from docia.quick import QuickReport, csv_rows_from_paths, quick_analyze

TEXTS = {
    "contrat_dupont.txt": (
        "Contrat de travail à durée indéterminée conclu entre la société ACME et "
        "Monsieur Dupont, ingénieur, à compter du 1er septembre 2026. "
    ),
    "facture_0912.txt": (
        "Facture n° 0912 — maintenance du parc informatique. Montant HT 18 640,00 €, "
        "TVA 20 %, total TTC 22 368,00 €. Échéance à trente jours. "
    ),
    "procedure.txt": (
        "Procédure de sauvegarde des serveurs : copie quotidienne vers le NAS, "
        "rétention trente jours, test de restauration mensuel. "
    ),
}


@pytest.fixture
def dossier(tmp_path: Path) -> Path:
    """Trois documents texte distincts et un fichier écarté, dans un dossier local.

    D-109 : une image matricielle est désormais océrisée, donc analysée. Le
    fichier écarté est ici un `.ico` — icône d'interface, qu'aucun moteur OCR ne
    lit — pour que ce test garde un cas d'exclusion à vérifier.
    """
    src = tmp_path / "partage"
    src.mkdir()
    for name, text in TEXTS.items():
        (src / name).write_text(text * 4, encoding="utf-8")
    (src / "icone.ico").write_bytes(b"\x00\x00\x01\x00" + b"0" * 400)
    return src


def _config(tmp_path: Path, base_url: str) -> Config:
    cfg = Config(db_path=str(tmp_path / "ignore.sqlite"))
    cfg.llm.base_url = base_url
    cfg.llm.transport = "vllm"
    cfg.llm.max_in_flight = 3
    cfg.llm.timeout_s = 30
    cfg.llm.max_retries = 1
    cfg.blocks.block_tokens = 100_000
    return cfg


def test_rows_from_directory_skip_unreadable(dossier: Path) -> None:
    unreadable: list[str] = []
    rows = list(csv_rows_from_paths([dossier], unreadable=unreadable))
    assert {r.name for r in rows} == {*TEXTS, "icone.ico"}
    assert unreadable == []
    row = next(r for r in rows if r.name == "icone.ico")
    assert (row.extension, row.directory_type, row.host) == ("ico", "LOCAL_FIXED", "localhost")
    assert row.fast_hash
    assert row.file_size > 0
    assert Path(row.path) == dossier / "icone.ico"


def test_quick_analyzes_a_directory(tmp_path: Path, dossier: Path, fake_server) -> None:  # type: ignore[no-untyped-def]
    cfg = _config(tmp_path, fake_server.base_url_vllm)
    report = quick_analyze(cfg, [dossier])

    assert report.ok, report.message
    assert (report.requested, report.analyzed, report.errors) == (4, 3, 0)
    assert report.excluded == 1
    done = {f.name: f for f in report.files if f.status == "done"}
    assert set(done) == set(TEXTS)
    first = done["contrat_dupont.txt"]
    assert [first.security, first.rgpd, first.finance, first.legal] == [
        "C1",
        "low",
        "invoice",
        "none",
    ]
    assert first.retention.startswith("fiscal")
    assert first.resume
    excluded = next(f for f in report.files if f.status == "excluded")
    assert excluded.name == "icone.ico"
    assert "extension exclue" in excluded.reason

    lines = report.as_lines()
    assert all(len(line) <= 120 for line in lines)
    assert lines[0].startswith("fichier")
    assert any("icone.ico" in line and "excluded" in line for line in lines)
    assert report.as_dict()["analyzed"] == 3


def test_quick_analyzes_a_single_file(tmp_path: Path, dossier: Path, fake_server) -> None:  # type: ignore[no-untyped-def]
    cfg = _config(tmp_path, fake_server.base_url_vllm)
    report = quick_analyze(cfg, [dossier / "facture_0912.txt"])

    assert report.ok
    assert (report.requested, report.analyzed) == (1, 1)
    assert report.files[0].name == "facture_0912.txt"


def test_quick_reports_missing_path(tmp_path: Path, fake_server) -> None:  # type: ignore[no-untyped-def]
    cfg = _config(tmp_path, fake_server.base_url_vllm)
    report = quick_analyze(cfg, [tmp_path / "nulle_part"])

    assert report.ok is False
    assert "introuvable" in report.message
    assert "nulle_part" in report.message
    assert fake_server.post_count == 0


def test_quick_keeps_db_and_resumes_without_llm_calls(
    tmp_path: Path, dossier: Path, fake_server
) -> None:  # type: ignore[no-untyped-def]
    cfg = _config(tmp_path, fake_server.base_url_vllm)
    db_path = tmp_path / "quick.sqlite"

    first = quick_analyze(cfg, [dossier], db_path=db_path)
    assert (first.ok, first.analyzed) == (True, 3)
    assert db_path.exists()
    calls = fake_server.post_count
    assert calls > 0

    second = quick_analyze(cfg, [dossier], db_path=db_path)
    assert (second.ok, second.analyzed) == (True, 3)
    assert fake_server.post_count == calls  # reprise : plus rien à envoyer
    assert second.kept_db is True
    assert second.db_path == str(db_path)
    assert "base conservée" in "\n".join(second.as_lines())


def test_quick_reports_unreachable_server(tmp_path: Path, dossier: Path) -> None:
    cfg = _config(tmp_path, "http://127.0.0.1:1/v1")
    report = quick_analyze(cfg, [dossier])

    assert report.ok is False
    assert "injoignable" in report.message


def test_quick_handler_returns_zero_and_prints_table(
    tmp_path: Path, dossier: Path, fake_server, capsys: pytest.CaptureFixture[str]
) -> None:  # type: ignore[no-untyped-def]
    """Le gestionnaire `quick` de la CLI : code 0 et tableau à l'écran."""
    parser = argparse.ArgumentParser(prog="docia")
    handlers = register(parser.add_subparsers(dest="command", required=True))
    args = parser.parse_args(["quick", str(dossier)])
    assert handlers["quick"](args, _config(tmp_path, fake_server.base_url_vllm)) == 0
    out = capsys.readouterr().out
    assert "contrat_dupont.txt" in out
    assert "icone.ico" in out

    args = parser.parse_args(["quick", str(tmp_path / "nulle_part")])
    assert handlers["quick"](args, _config(tmp_path, fake_server.base_url_vllm)) == 1


def test_quick_dry_run_builds_blocks_without_llm(tmp_path: Path) -> None:
    """`--dry-run` : extraction + blocs, aucun appel LLM (serveur inexistant)."""
    from docia.cli import main as cli_main
    from docia.config import Config
    from docia.quick import quick_analyze

    src = tmp_path / "docs"
    src.mkdir()
    for i in range(3):
        (src / f"note{i}.txt").write_text("contrat de prestation " * 40, encoding="utf-8")
    cfg = Config()
    cfg.llm.base_url = "http://127.0.0.1:1/v1"
    cfg.filter.excluded_dir_markers = []
    cfg.filter.min_size_bytes = 1
    report = quick_analyze(cfg, [src], dry_run=True)
    assert report.ok, report.message
    assert report.dry_run
    assert report.blocks_built >= 1
    assert report.extraction_errors == 0
    assert report.as_lines()[0].startswith("extraction seule")
    cfg_path = tmp_path / "docia.toml"
    cfg_path.write_text(
        'db_path = "x.sqlite"\n[llm]\nbase_url = "http://127.0.0.1:1/v1"\n'
        "[filter]\nexcluded_dir_markers = []\nmin_size_bytes = 1\n",
        encoding="utf-8",
    )
    assert cli_main(["--config", str(cfg_path), "quick", "--dry-run", str(src)]) == 0


@pytest.mark.skipif(
    os.name == "nt" or getattr(os, "getuid", lambda: 1)() == 0,
    reason="les permissions POSIX ne bloquent ni Windows ni root",
)
def test_quick_compte_et_signale_les_dossiers_refuses(dossier: Path) -> None:
    """MOYEN 9 : un dossier refusé disparaissait, ni compté ni signalé.

    `Path.rglob` avale les `PermissionError` : `docia quick \\\\srv\\partage\\Compta`
    avec un compte sans droits sur un sous-dossier — le cas normal d'un partage
    cloisonné — annonçait « 3 fichiers : 3 analysés, 0 illisible », le quatrième
    n'existant nulle part. Pour un audit de conformité, c'est la pire des sorties.
    """
    prive = dossier / "prive"
    prive.mkdir()
    (prive / "secret.txt").write_text("dossier interdit", encoding="utf-8")
    prive.chmod(0o000)
    unreadable: list[str] = []
    denied: list[str] = []
    try:
        rows = list(csv_rows_from_paths([dossier], unreadable=unreadable, denied_dirs=denied))
    finally:
        prive.chmod(0o755)

    assert {r.name for r in rows} == {*TEXTS, "icone.ico"}
    assert denied == [str(prive)], "le dossier refusé doit être remonté à l'appelant"
    assert unreadable == []

    rapport = QuickReport(requested=len(rows), analyzed=len(rows), denied_dirs=len(denied))
    assert any("1 dossier(s) refusé(s)" in ligne for ligne in rapport.as_lines())
