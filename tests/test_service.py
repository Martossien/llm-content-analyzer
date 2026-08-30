"""Couche service : événements de run, réanalyse ciblée, sauvegarde, campagnes récentes.

Les fixtures `corpus` (six fichiers + CSV SMBeagle) et `_config` viennent du test
bout en bout ; `fake_server` du plugin `tests.fake_openai`.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from docia.cli import DEFAULT_KEEP_BACKUPS as CLI_KEEP_BACKUPS
from docia.cli import main
from docia.config import Config
from docia.db import _SCHEMA_V1, SCHEMA_VERSION, Database, backup_dir_for
from docia.models import DomainAnalysis, FileAnalysis, FileStatus
from docia.service import (
    DEFAULT_KEEP_BACKUPS,
    RunEvent,
    ServiceError,
    backup_database,
    campaign_status,
    forget_campaign,
    import_scan,
    list_backups,
    plan,
    reanalyze,
    recent_campaigns,
    remember_campaign,
    restore_database,
    run_campaign,
)
from tests.test_pipeline_e2e import _config, corpus  # noqa: F401


@pytest.fixture(autouse=True)
def _docia_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Isole `recent.json` : aucun test n'écrit dans le vrai dossier de config."""
    monkeypatch.setenv("DOCIA_HOME", str(tmp_path / "config_docia"))


def _prepare(tmp_path: Path, csv_path: Path, base_url: str) -> tuple[Config, Database]:
    """Base importée et planifiée, prête pour un run."""
    cfg = _config(tmp_path, base_url, block_tokens=1_200)
    db = Database(cfg.db_path)
    report = import_scan(db, csv_path)
    assert (report.new, report.invalid) == (6, 0)
    assert plan(db, cfg).pending == 6
    return cfg, db


def _c3(ref: str) -> FileAnalysis:
    """Analyse forcée en C3 pour vérifier une réanalyse ciblée sur la sécurité."""
    return FileAnalysis(
        file_ref=ref,
        resume="r",
        security=DomainAnalysis("C3", 90, {"justification": "secret"}),
        rgpd=DomainAnalysis("high", 80, {"data_types": ["sante"]}),
        finance=DomainAnalysis("none", 90, {"amounts": []}),
        legal=DomainAnalysis("none", 90, {"parties": []}),
        raw={"file_ref": ref},
    )


# --------------------------------------------------------------------- run


def test_run_campaign_emits_coherent_events(
    tmp_path: Path,
    corpus: tuple[Path, Path],  # noqa: F811
    fake_server,  # type: ignore[no-untyped-def]
) -> None:
    _src, csv_path = corpus
    cfg, db = _prepare(tmp_path, csv_path, fake_server.base_url_vllm)
    events: list[RunEvent] = []
    with db:
        report = run_campaign(db, cfg, on_event=events.append)
        state = campaign_status(db)

    assert events, "aucun événement de progression"
    assert events[0].kind == "info"
    assert events[0].files_total == 6
    assert events[-1].kind == "finished"
    assert events[-1].files_done == report.files_done == 6
    assert events[-1].blocks_done == report.blocks_done
    assert any(e.kind == "block_done" for e in events)
    assert all(e.elapsed_s >= 0.0 for e in events)
    assert all(e.eta_s is None or e.eta_s >= 0.0 for e in events)
    assert all(e.files_per_hour is None or e.files_per_hour > 0.0 for e in events)
    # la progression est monotone
    assert [e.files_done for e in events] == sorted(e.files_done for e in events)
    assert (state.done, state.analyses, state.error) == (6, 6, 0)
    assert state.active_prompt == "(embarqué)"
    assert state.last_run is not None
    assert state.last_run.run_id == report.run_id
    assert state.schema_version == SCHEMA_VERSION


def test_run_campaign_without_callback_stays_silent(
    tmp_path: Path,
    corpus: tuple[Path, Path],  # noqa: F811
    fake_server,  # type: ignore[no-untyped-def]
) -> None:
    _src, csv_path = corpus
    cfg, db = _prepare(tmp_path, csv_path, fake_server.base_url_vllm)
    with db:
        assert run_campaign(db, cfg, dry_run=True).files_selected == 6


# --------------------------------------------------------------- réanalyse


def test_reanalyze_all_then_second_run_redoes_everything(
    tmp_path: Path,
    corpus: tuple[Path, Path],  # noqa: F811
    fake_server,  # type: ignore[no-untyped-def]
) -> None:
    _src, csv_path = corpus
    cfg, db = _prepare(tmp_path, csv_path, fake_server.base_url_vllm)
    with db:
        run_campaign(db, cfg)
        assert db.counts()["analyses"] == 6

        assert reanalyze(db, cfg, scope="all") == 6
        counts = db.counts()
        assert (counts["pending"], counts["done"], counts["analyses"]) == (6, 0, 0)

        again = run_campaign(db, cfg)
        assert (again.files_selected, again.files_done) == (6, 6)
        assert db.counts()["analyses"] == 6


def test_reanalyze_where_targets_only_matching_files(
    tmp_path: Path,
    corpus: tuple[Path, Path],  # noqa: F811
    fake_server,  # type: ignore[no-untyped-def]
) -> None:
    _src, csv_path = corpus
    cfg, db = _prepare(tmp_path, csv_path, fake_server.base_url_vllm)
    with db:
        run_campaign(db, cfg)
        phash = str(next(iter(db.query("SELECT prompt_hash AS h FROM analyses")))["h"])

        # le serveur factice classe tout en C1 : on force un seul fichier en C3
        contrat = next(f for f in db.iter_files() if f.name == "contrat_dupont.txt")
        db.store_analysis(
            contrat.id,
            None,
            contrat.content_version,
            prompt_hash=phash,
            model=cfg.llm.model,
            analysis=_c3(contrat.path),
        )

        assert reanalyze(db, cfg, scope="filter", where={"security": "C3"}) == 1
        pending = [f.path for f in db.iter_files(FileStatus.PENDING)]
        assert pending == [contrat.path]
        assert db.counts()["analyses"] == 5

        # un critère sur le chemin cible les deux fichiers du dossier « rh »
        run_campaign(db, cfg)
        assert reanalyze(db, cfg, scope="filter", where={"path_like": "%rh%"}) == 2
        assert {Path(p).name for p in (f.path for f in db.iter_files(FileStatus.PENDING))} == {
            "contrat_dupont.txt",
            "note.txt",
        }

        # une extension inconnue ne remet rien à analyser
        assert reanalyze(db, cfg, scope="filter", where={"extension": "docx"}) == 0

        with pytest.raises(ServiceError):
            reanalyze(db, cfg, scope="filter", where={"couleur": "bleu"})
        with pytest.raises(ServiceError):
            reanalyze(db, cfg, scope="filter", where=None)
        with pytest.raises(ServiceError):
            reanalyze(db, cfg, scope="inconnu")


def test_reanalyze_errors_only_requeues_failed_files(
    tmp_path: Path,
    corpus: tuple[Path, Path],  # noqa: F811
    fake_server,  # type: ignore[no-untyped-def]
) -> None:
    _src, csv_path = corpus
    cfg, db = _prepare(tmp_path, csv_path, fake_server.base_url_vllm)
    with db:
        run_campaign(db, cfg)
        victim = next(f for f in db.iter_files() if f.name == "budget.txt")
        db.set_file_status(victim.id, FileStatus.ERROR, "essai")

        assert reanalyze(db, cfg, scope="errors") == 1
        counts = db.counts()
        assert (counts["pending"], counts["error"], counts["done"]) == (1, 0, 5)
        assert counts["analyses"] == 6  # aucune analyse supprimée


def test_reanalyze_pending_only_clears_their_analyses(
    tmp_path: Path,
    corpus: tuple[Path, Path],  # noqa: F811
    fake_server,  # type: ignore[no-untyped-def]
) -> None:
    _src, csv_path = corpus
    cfg, db = _prepare(tmp_path, csv_path, fake_server.base_url_vllm)
    with db:
        run_campaign(db, cfg, limit=2)
        assert db.counts()["pending"] == 4
        assert reanalyze(db, cfg, scope="pending_only") == 4
        assert db.counts()["analyses"] == 2


# -------------------------------------------------------------- sauvegarde


def test_backup_taken_before_reanalyze(
    tmp_path: Path,
    corpus: tuple[Path, Path],  # noqa: F811
    fake_server,  # type: ignore[no-untyped-def]
) -> None:
    _src, csv_path = corpus
    cfg, db = _prepare(tmp_path, csv_path, fake_server.base_url_vllm)
    with db:
        run_campaign(db, cfg)
        assert list_backups(db.path) == []
        reanalyze(db, cfg, scope="all")

    saved = list_backups(Path(cfg.db_path))
    assert len(saved) == 1
    assert saved[0].parent == backup_dir_for(Path(cfg.db_path))
    assert "avant_reanalyse_all" in saved[0].name
    with Database(saved[0]) as copy:
        counts = copy.counts()
    assert (counts["done"], counts["analyses"]) == (6, 6)


def test_backup_rotation_keeps_only_the_newest(tmp_path: Path) -> None:
    db_path = tmp_path / "docia.sqlite"
    with Database(db_path) as db:
        db.start_scan("a.csv")
    first = backup_database(db_path, keep=2)
    second = backup_database(db_path, keep=2)
    third = backup_database(db_path, label="dernière", keep=2)

    kept = list_backups(db_path)
    assert kept == [third, second]
    assert not first.exists()
    assert "derni" in third.name  # l'étiquette est reprise, ramenée à des caractères sûrs

    with pytest.raises(ServiceError):
        backup_database(tmp_path / "absente.sqlite")


def test_restore_puts_back_counters_and_saves_current(tmp_path: Path) -> None:
    db_path = tmp_path / "docia.sqlite"
    with Database(db_path) as db:
        db.start_scan("a.csv")
        db.save_prompt("metier", "x" * 60, activate=True)
    snapshot = backup_database(db_path, label="reference")

    with Database(db_path) as db:
        db.delete_prompt("metier")
        assert db.active_prompt() is None

    restored = restore_database(db_path, snapshot)
    assert restored == db_path
    with Database(db_path) as db:
        active = db.active_prompt()
    assert active is not None
    assert active[0] == "metier"
    assert any("avant_restauration" in p.name for p in list_backups(db_path))

    with pytest.raises(ServiceError):
        restore_database(db_path, tmp_path / "inconnue.sqlite")
    illisible = tmp_path / "pas_une_base.sqlite"
    illisible.write_text("ceci n'est pas une base", encoding="utf-8")
    with pytest.raises(ServiceError):
        restore_database(db_path, illisible)


def test_migration_is_backed_up_automatically(tmp_path: Path) -> None:
    old = tmp_path / "ancienne.sqlite"
    conn = sqlite3.connect(str(old))
    conn.executescript(_SCHEMA_V1)
    conn.execute("INSERT INTO meta(key, value) VALUES('schema_version', '1')")
    conn.commit()
    conn.close()

    with Database(old) as db:
        assert db.schema_version == SCHEMA_VERSION
    copies = list(backup_dir_for(old).glob("*_avant_migration_v*.sqlite"))
    assert len(copies) == 1
    with sqlite3.connect(str(copies[0])) as check:
        version = check.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
    assert version[0] == "1"

    # ré-ouverture d'une base à jour : aucune nouvelle copie
    with Database(old):
        pass
    assert len(list(backup_dir_for(old).glob("*_avant_migration_v*.sqlite"))) == 1


def test_fresh_database_is_not_backed_up(tmp_path: Path) -> None:
    with Database(tmp_path / "neuve.sqlite"):
        pass
    assert not backup_dir_for(tmp_path / "neuve.sqlite").exists()


# ---------------------------------------------------------------- campagnes


def test_recent_campaigns_roundtrip(tmp_path: Path) -> None:
    first = tmp_path / "campagne_a.sqlite"
    second = tmp_path / "campagne_b.sqlite"
    first.touch()
    second.touch()
    assert recent_campaigns() == []

    remember_campaign(first, tmp_path / "scan_a.csv", label="RH")
    remember_campaign(second)
    entries = recent_campaigns()
    assert [e.db_path for e in entries] == [second.resolve(), first.resolve()]
    assert entries[1].csv_path == (tmp_path / "scan_a.csv").resolve()
    assert entries[1].label == "RH"
    assert entries[0].csv_path is None

    # remise en tête : l'étiquette et le CSV connus sont conservés
    remember_campaign(first)
    entries = recent_campaigns()
    assert entries[0].db_path == first.resolve()
    assert entries[0].label == "RH"

    forget_campaign(first)
    assert [e.db_path for e in recent_campaigns()] == [second.resolve()]


def test_recent_campaigns_tolerates_broken_file(tmp_path: Path) -> None:
    home = tmp_path / "config_docia"
    home.mkdir(parents=True, exist_ok=True)
    (home / "recent.json").write_text("{ pas du JSON", encoding="utf-8")
    assert recent_campaigns() == []
    remember_campaign(tmp_path / "c.sqlite")
    assert len(recent_campaigns()) == 1


def test_recent_campaigns_are_capped(tmp_path: Path) -> None:
    for i in range(25):
        remember_campaign(tmp_path / f"c{i:02d}.sqlite")
    entries = recent_campaigns()
    assert len(entries) == 20
    assert entries[0].db_path == (tmp_path / "c24.sqlite").resolve()


def test_import_scan_remembers_the_campaign(
    tmp_path: Path,
    corpus: tuple[Path, Path],  # noqa: F811
) -> None:
    _src, csv_path = corpus
    cfg = _config(tmp_path, "http://127.0.0.1:1")
    with Database(cfg.db_path) as db:
        import_scan(db, csv_path)
    entries = recent_campaigns()
    assert len(entries) == 1
    assert entries[0].db_path == Path(cfg.db_path).resolve()
    assert entries[0].csv_path == csv_path.resolve()

    with Database(cfg.db_path) as db, pytest.raises(ServiceError):
        import_scan(db, tmp_path / "absent.csv")


# ---------------------------------------------------------------------- CLI


def test_cli_backup_restore_reanalyze_campaigns(
    tmp_path: Path,
    corpus: tuple[Path, Path],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    fake_server,  # type: ignore[no-untyped-def]
) -> None:
    _src, csv_path = corpus
    monkeypatch.chdir(tmp_path)
    cfg, db = _prepare(tmp_path, csv_path, fake_server.base_url_vllm)
    with db:
        run_campaign(db, cfg)
    base = ["--db", cfg.db_path]
    capsys.readouterr()

    assert main([*base, "backup", "--label", "manuelle"]) == 0
    saved = list_backups(Path(cfg.db_path))
    assert len(saved) == 1
    assert "manuelle" in saved[0].name

    assert main([*base, "reanalyze"]) == 1  # aucun critère
    assert main([*base, "reanalyze", "--where", "nimportequoi"]) == 1  # critère mal formé
    assert main([*base, "reanalyze", "--all", "--where", "security=C1"]) == 1  # incompatibles
    assert main([*base, "reanalyze", "--where", "security=C1"]) == 0
    with Database(cfg.db_path) as check:
        assert check.counts()["analyses"] == 0

    assert main([*base, "restore", str(saved[0])]) == 1  # sans --yes : rien n'est fait
    with Database(cfg.db_path) as check:
        assert check.counts()["analyses"] == 0
    assert main([*base, "restore", str(saved[0]), "--yes"]) == 0
    with Database(cfg.db_path) as check:
        assert check.counts()["analyses"] == 6

    assert main([*base, "campaigns"]) == 0
    assert str(Path(cfg.db_path).resolve()) in capsys.readouterr().out

    assert main([*base, "status", "--json"]) == 0
    status = json.loads(capsys.readouterr().out)
    assert status["counts"]["analyses"] == 6
    assert status["active_prompt"] == "(embarqué)"
    assert status["schema_version"] == SCHEMA_VERSION
    assert status["last_run"]["run_id"] >= 1
    assert status["reviews"] == {"reviewed": 0, "to_review": 0}
    assert main([*base, "status"]) == 0
    assert "prompt actif" in capsys.readouterr().out


def test_cli_keeps_the_same_rotation_default() -> None:
    """La valeur par défaut de `--keep` ne doit pas diverger de celle du service."""
    assert CLI_KEEP_BACKUPS == DEFAULT_KEEP_BACKUPS


def test_cli_campaigns_without_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.chdir(tmp_path)
    assert main(["campaigns"]) == 0
    assert "aucune campagne récente" in capsys.readouterr().out
