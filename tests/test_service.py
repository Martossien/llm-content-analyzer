"""Couche service : événements de run, réanalyse ciblée, sauvegarde, campagnes récentes.

Les fixtures `corpus` (six fichiers + CSV SMBeagle) et `_config` viennent du test
bout en bout ; `fake_server` du plugin `tests.fake_openai`.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import sqlite3
import subprocess
import sys
from collections.abc import Iterator, Sequence
from pathlib import Path
from textwrap import dedent

import pytest

from docia import journal as journal_mod
from docia.cli import main
from docia.config import Config
from docia.db import _SCHEMA_V1, SCHEMA_VERSION, Database, backup_dir_for
from docia.gui.service_shim import GuiService
from docia.ingest.smbeagle_csv import ImportReport
from docia.models import DomainAnalysis, FileAnalysis, FileStatus, SmbeagleRow
from docia.service import (
    DEFAULT_KEEP_BACKUPS,
    ImportProgress,
    RunEvent,
    ServiceError,
    _effective_keys,  # noqa: PLC2701 - la clé (empreinte de prompt, modèle) d'une analyse
    backup_database,
    campaign_status,
    forget_campaign,
    format_import_report,
    import_progress_logger,
    import_scan,
    list_backups,
    plan,
    reanalyze,
    recent_campaigns,
    remember_campaign,
    restore_database,
    run_campaign,
    set_review,
)
from docia.views import format_int
from tests.test_pipeline_e2e import _config, corpus  # noqa: F401


@pytest.fixture(autouse=True)
def _docia_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Isole `recent.json` : aucun test n'écrit dans le vrai dossier de config."""
    monkeypatch.setenv("DOCIA_HOME", str(tmp_path / "config_docia"))


@pytest.fixture
def journal_isole() -> Iterator[None]:
    """Isole la journalisation le temps d'un test qui appelle `main()`.

    `_setup_logging` n'agit qu'une fois par processus : sans cette remise à zéro,
    le gestionnaire console d'un test précédent écrit dans un flux capturé déjà
    refermé, et pytest voit passer des « Logging error » qui ne viennent pas du code.
    """
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    root.handlers = []
    journal_mod.reset()
    try:
        yield
    finally:
        for handler in root.handlers:
            handler.close()
        root.handlers, root.level = handlers, level
        journal_mod.reset()


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


def test_reanalyse_ciblee_ignore_une_classification_perimee(tmp_path: Path) -> None:
    """`--where security=C3` ne doit viser que les fichiers dont l'analyse **du contenu
    actuel** dit C3. Un fichier modifié depuis (content_version 2) dont seule la
    version 1 était C3 n'a plus de classification : il ne doit pas être ciblé, et
    un fichier dont la version courante est C3 doit l'être."""
    from docia.db import Database
    from docia.models import DomainAnalysis, FileAnalysis
    from docia.service import reanalyze

    def analysis(ref: str, label: str) -> FileAnalysis:
        dom = DomainAnalysis
        return FileAnalysis(
            file_ref=ref,
            resume="r",
            security=dom(label, 90, {"justification": ""}),
            rgpd=dom("none", 90, {"data_types": []}),
            finance=dom("none", 90, {"amounts": []}),
            legal=dom("none", 90, {"parties": []}),
            raw={},
            retention=dom("none", 90, {"required": False, "years": 0, "justification": ""}),
        )

    cfg = Config()
    with Database(tmp_path / "c.sqlite") as db:
        scan = db.start_scan("x.csv")
        db.upsert_files([_fichier("a.txt"), _fichier("b.txt")], scan)
        a, b = (db.query("SELECT id FROM files ORDER BY path")[i]["id"] for i in (0, 1))
        db.store_analysis(a, None, 1, prompt_hash="h", model="m", analysis=analysis("a", "C3"))
        db.store_analysis(b, None, 1, prompt_hash="h", model="m", analysis=analysis("b", "C3"))
        # `a` change de contenu : sa classification C3 ne décrit plus rien.
        db._conn.execute(
            "UPDATE files SET content_version = 2, status = 'pending' WHERE id = ?", (a,)
        )
        db._conn.execute("UPDATE files SET status = 'done' WHERE id = ?", (b,))
        db._conn.commit()
        count = reanalyze(db, cfg, scope="filter", where={"security": "C3"}, backup=False)
        assert count == 1, "seul b porte une analyse C3 valable"


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


def test_import_scan_journalise_une_progression_espacee(
    tmp_path: Path,
    corpus: tuple[Path, Path],  # noqa: F811
) -> None:
    """Le rappel de progression traverse bien `import_scan` jusqu'au journal."""
    _src, csv_path = corpus
    cfg = _config(tmp_path, "http://127.0.0.1:1")
    lignes: list[str] = []
    with Database(cfg.db_path) as db:
        report = import_scan(
            db,
            csv_path,
            progress=import_progress_logger(lignes.append, min_seconds=0.0, min_rows=0),
        )
    assert lignes, "aucune progression journalisée"
    assert all(ligne.startswith("intégration : ") for ligne in lignes)
    assert lignes[0].startswith("intégration : 0 lignes")
    assert f"{report.total} lignes" in lignes[-1]
    assert "100 %" in lignes[-1]


def test_import_progress_logger_espace_les_lignes() -> None:
    """Cent appels rapprochés ne produisent qu'une ligne : le journal n'est pas inondé."""
    lignes: list[str] = []
    emit = import_progress_logger(lignes.append, min_seconds=60.0, min_rows=1_000_000)
    for i in range(100):
        emit(
            ImportProgress(
                rows=i * 1_000, invalid=0, bytes_read=i, total_bytes=100_000, elapsed_s=0.1
            )
        )
    assert len(lignes) == 1

    lignes.clear()
    emit2 = import_progress_logger(lignes.append, min_seconds=60.0, min_rows=10_000)
    for i in range(100):
        emit2(
            ImportProgress(
                rows=i * 1_000, invalid=0, bytes_read=i, total_bytes=100_000, elapsed_s=0.1
            )
        )
    assert len(lignes) == 10


def test_import_progress_logger_emet_toujours_la_ligne_finale() -> None:
    """L'étranglement ne doit jamais ravaler le dernier appel.

    Sur les 934 028 lignes du scan réel, la dernière ligne affichée était
    « 900 000 lignes — 96 % » ; sur un import de trois lignes, l'utilisateur ne
    voyait que « 0 lignes — 0 % ». `ImportProgress.final` court-circuite les seuils.
    """
    octets = 251_868_508  # taille du gros scan de référence
    for total_lignes in (3, 934_028):
        lignes: list[str] = []
        emit = import_progress_logger(lignes.append, min_seconds=3_600.0, min_rows=1_000_000)
        emit(ImportProgress(rows=0, invalid=0, bytes_read=0, total_bytes=octets, elapsed_s=0.0))
        for rows in range(10_000, total_lignes, 10_000):  # les lots intermédiaires, tous étranglés
            emit(
                ImportProgress(
                    rows=rows,
                    invalid=0,
                    bytes_read=octets * rows // total_lignes,
                    total_bytes=octets,
                    elapsed_s=1.0,
                )
            )
        emit(
            ImportProgress(
                rows=total_lignes,
                invalid=0,
                bytes_read=octets,
                total_bytes=octets,
                elapsed_s=42.0,
                final=True,
            )
        )
        assert lignes[-1] == f"intégration : {format_int(total_lignes)} lignes — 100 % — 42 s"


def test_import_progress_logger_ne_repete_pas_la_ligne_finale() -> None:
    """Le dernier lot a déjà tout dit : ne pas écrire deux fois la même ligne."""
    lignes: list[str] = []
    emit = import_progress_logger(lignes.append, min_seconds=0.0, min_rows=0)
    emit(ImportProgress(rows=0, invalid=0, bytes_read=0, total_bytes=10, elapsed_s=0.0))
    emit(ImportProgress(rows=63, invalid=0, bytes_read=10, total_bytes=10, elapsed_s=0.1))
    emit(
        ImportProgress(rows=63, invalid=0, bytes_read=10, total_bytes=10, elapsed_s=0.1, final=True)
    )
    assert lignes == [
        "intégration : 0 lignes — 0 % — 0 s",
        "intégration : 63 lignes — 100 % — 0 s",
    ]


def test_import_dun_petit_csv_finit_sur_cent_pour_cent(tmp_path: Path) -> None:
    """Trois lignes, seuils par défaut : l'utilisateur doit voir autre chose que « 0 % »."""
    source = Path(__file__).parent / "fixtures" / "scan_local_mini.csv"
    entete, *donnees = source.read_text(encoding="utf-8").splitlines()
    petit = tmp_path / "trois_lignes.csv"
    petit.write_text("\n".join([entete, *donnees[:3]]) + "\n", encoding="utf-8")
    lignes: list[str] = []
    with Database(tmp_path / "docia.sqlite") as db:
        report = import_scan(db, petit, progress=import_progress_logger(lignes.append))
    assert report.total == 3
    assert lignes[-1] == "intégration : 3 lignes — 100 % — 0 s"


def test_format_import_report_est_la_seule_formulation_du_bilan() -> None:
    """Le bilan d'import n'a qu'une écriture : `cli`, `cli_tools` et la fenêtre la partagent."""
    report = ImportReport(scan_id=7, total=63, new=60, updated=2, unchanged=1, invalid=0)
    assert (
        format_import_report(report)
        == "import : 63 lignes — 60 nouveaux, 2 modifiés, 1 inchangés, 0 invalides"
    )
    assert format_import_report(report, prefix=f"scan {report.scan_id}").startswith("scan 7 : ")


# ---------------------------------------------------------------------- CLI


def test_cli_ingest_memorise_la_campagne(
    tmp_path: Path,
    corpus: tuple[Path, Path],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    journal_isole: None,  # noqa: ARG001 - remet la journalisation à zéro
) -> None:
    """`docia ingest` doit passer par `service.import_scan`, comme la fenêtre.

    En appelant `import_csv` en direct, la CLI sautait `remember_campaign` :
    après un `ingest` pourtant réussi, `docia campaigns` répondait « aucune
    campagne récente ». La ligne de bilan, elle, ne doit pas changer d'un iota.
    """
    _src, csv_path = corpus
    monkeypatch.chdir(tmp_path)
    db_path = str(tmp_path / "docia.sqlite")
    assert main(["--db", db_path, "ingest", str(csv_path)]) == 0
    assert (
        capsys.readouterr().out.strip()
        == "scan 1 : 6 lignes — 6 nouveaux, 0 modifiés, 0 inchangés, 0 invalides"
    )
    assert [e.db_path for e in recent_campaigns()] == [Path(db_path).resolve()]
    assert main(["--db", db_path, "campaigns"]) == 0
    sortie = capsys.readouterr().out
    assert "aucune campagne récente" not in sortie
    assert str(Path(db_path).resolve()) in sortie


def test_cli_ingest_csv_illisible_rend_un_message_francais(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    journal_isole: None,  # noqa: ARG001 - remet la journalisation à zéro
) -> None:
    """Un CSV présent mais illisible : `ServiceError`, pas une trace Python brute."""
    monkeypatch.chdir(tmp_path)
    illisible = tmp_path / "scan.csv"
    illisible.mkdir()  # existe, mais refuse de s'ouvrir
    assert main(["--db", str(tmp_path / "docia.sqlite"), "ingest", str(illisible)]) == 1
    erreur = capsys.readouterr().err
    assert "Traceback" not in erreur
    assert "lecture impossible du scan" in erreur


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


def test_cli_campaigns_without_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.chdir(tmp_path)
    assert main(["campaigns"]) == 0
    assert "aucune campagne récente" in capsys.readouterr().out


# ------------------------- sauvegarde : cloisonnement, filets, interruptions
#
# Le triptyque sauvegarde / restauration / réanalyse est ce sur quoi on se rabat
# quand quelque chose a mal tourné : les tests ci-dessous coupent pour de vrai
# (processus tué, rotation réelle) plutôt que de simuler poliment.


def _base(tmp_path: Path, name: str) -> Path:
    """Petite base réelle, prête à être sauvegardée."""
    path = tmp_path / name
    with Database(path) as db:
        db.start_scan("a.csv")
    return path


def _vieillit(paths: Sequence[Path]) -> None:
    """Horodate les copies dans l'ordre donné : la première est la plus ancienne."""
    for n, path in enumerate(paths):
        stamp = 1_600_000_000_000_000_000 + n * 1_000_000_000
        os.utime(path, ns=(stamp, stamp))


def test_restauration_ne_detruit_pas_la_sauvegarde_restauree(tmp_path: Path) -> None:
    """C1 : la sauvegarde préalable tournait et emportait le fichier même visé.

    Dix copies, on restaure la plus ancienne : la rotation de la copie
    « avant_restauration » la supprimait *avant* que la source ne soit lue. La
    restauration échouait (« No such file or directory ») et la copie était perdue.
    """
    db_path = _base(tmp_path, "campagne.sqlite")
    with Database(db_path) as db:
        db.save_prompt("origine", "x" * 60, activate=True)
    copies = [backup_database(db_path, keep=0, label=f"s{n}") for n in range(DEFAULT_KEEP_BACKUPS)]
    _vieillit(copies)
    plus_ancienne = copies[0]
    with Database(db_path) as db:
        db.delete_prompt("origine")

    restaure = restore_database(db_path, plus_ancienne)

    assert plus_ancienne.exists(), "la copie restaurée a été emportée par la rotation"
    with Database(restaure) as db:
        actif = db.active_prompt()
    assert actif is not None
    assert actif[0] == "origine"
    assert any("avant_restauration" in p.name for p in list_backups(db_path))


def test_restauration_aboutit_meme_si_la_rotation_emporte_la_source(tmp_path: Path) -> None:
    """C1 (suite) : au-delà des dix, la source est légitimement reprise — sans échec.

    Onze copies courantes : celle qu'on restaure sort de la fenêtre de rétention
    pendant l'opération. La copie vers `<base>.tmp` étant faite *avant* la rotation,
    la restauration aboutit quand même et ne laisse aucun `.tmp` derrière elle.
    """
    db_path = _base(tmp_path, "campagne.sqlite")
    with Database(db_path) as db:
        db.save_prompt("origine", "x" * 60, activate=True)
    copies = [
        backup_database(db_path, keep=0, label=f"s{n}") for n in range(DEFAULT_KEEP_BACKUPS + 1)
    ]
    _vieillit(copies)
    with Database(db_path) as db:
        db.delete_prompt("origine")

    restaure = restore_database(db_path, copies[0])

    with Database(restaure) as db:
        actif = db.active_prompt()
    assert actif is not None
    assert actif[0] == "origine"
    assert not copies[0].exists()  # onzième copie courante : la rotation l'a reprise
    assert not db_path.with_name(db_path.name + ".tmp").exists()


def test_la_rotation_ne_voit_que_sa_propre_campagne(tmp_path: Path) -> None:
    """C2 : `audit` supprimait les sauvegardes d'`audit_2024_direction`.

    Le motif n'était pas ancré sur le nom complet. Dans le dossier commun que
    l'écran Rapports invite à choisir, la rotation d'une campagne emportait celles
    d'une autre, et `list_backups` les présentait comme siennes.
    """
    audit = _base(tmp_path, "audit.sqlite")
    direction = _base(tmp_path, "audit_2024_direction.sqlite")
    commun = backup_dir_for(audit)
    voisines = [backup_database(direction, out_dir=commun, keep=0) for _ in range(3)]
    siennes = [
        backup_database(audit, keep=DEFAULT_KEEP_BACKUPS) for _ in range(DEFAULT_KEEP_BACKUPS)
    ]

    manquantes = [p.name for p in voisines if not p.exists()]
    assert not manquantes, (
        f"rotation d'audit : sauvegardes d'une autre campagne effacées {manquantes}"
    )
    assert set(list_backups(audit)) == set(siennes)


def test_les_copies_de_surete_echappent_a_la_rotation(tmp_path: Path) -> None:
    """C3 : les filets (`avant_migration`, `avant_restauration`, `avant_reanalyse`)
    entraient dans le vivier des dix et disparaissaient les premiers, étant les
    plus anciens. Ils restent listés — un utilisateur doit pouvoir les restaurer."""
    db_path = _base(tmp_path, "campagne.sqlite")
    migration = (
        backup_dir_for(db_path)
        / f"campagne_avant_migration_v{SCHEMA_VERSION}_20200101T000000.sqlite"
    )
    filets = [
        backup_database(db_path, keep=0, label="avant_restauration"),
        backup_database(db_path, keep=0, label="avant_reanalyse_all"),
    ]
    shutil.copy2(filets[0], migration)
    filets.append(migration)
    _vieillit(filets)  # les plus anciennes du dossier : les premières servies

    courantes = [backup_database(db_path, keep=2) for _ in range(3)]

    emportes = [p.name for p in filets if not p.exists()]
    assert not emportes, f"copies de sûreté emportées par la rotation : {emportes}"
    assert not courantes[0].exists(), "la rotation doit tourner les copies courantes"
    assert set(list_backups(db_path)) == {*filets, *courantes[1:]}


def test_une_sauvegarde_tuee_en_plein_vol_ne_laisse_pas_de_cadavre(tmp_path: Path) -> None:
    """Copie interrompue par un arrêt brutal : rien de tronqué ne doit être listé.

    Le processus est réellement tué (`os._exit`) au milieu de l'écriture : aucun
    `finally`, aucun `except` ne s'exécute. Le fichier laissé derrière doit être un
    `.sqlite.tmp` — jamais un `.sqlite` de 932 Mo que `list_backups` présenterait
    comme « la plus récente », donc celle qu'un utilisateur restaurerait.
    """
    db_path = _base(tmp_path, "campagne.sqlite")
    script = tmp_path / "coupure_sauvegarde.py"
    script.write_text(
        dedent(f"""
        import os
        from pathlib import Path

        import docia.db as db_module
        from docia.service import backup_database

        base = Path({str(db_path)!r})

        def _tronque(self, path):
            Path(path).write_bytes(base.read_bytes()[:512])  # copie partielle
            os._exit(9)  # ... et la machine s'éteint

        db_module.Database.backup_to = _tronque
        with db_module.Database(base) as db:
            backup_database(base, db=db)
        """),
        encoding="utf-8",
    )
    fini = subprocess.run(  # noqa: S603 - interpréteur courant, script écrit par le test
        [sys.executable, str(script)], capture_output=True, check=False
    )
    assert fini.returncode == 9, fini.stderr.decode("utf-8", "replace")

    assert list_backups(db_path) == [], "un cadavre tronqué est présenté comme une sauvegarde"
    restes = sorted(p.name for p in backup_dir_for(db_path).iterdir())
    assert restes, "le test n'a rien interrompu"
    assert all(n.endswith(".sqlite.tmp") for n in restes), restes


# ------------------------------------------- réanalyse : atomicité et réparation


def _fichier(name: str) -> SmbeagleRow:
    return SmbeagleRow(
        name=name,
        host="srv",
        extension=name.rsplit(".", 1)[-1],
        username="u",
        hostname="srv.dom",
        unc_directory="\\\\srv\\part\\dossier",
        creation_time="01/01/2025 10:00:00",
        last_write_time="01/01/2026 10:00:00",
        readable=True,
        writeable=False,
        deletable=False,
        directory_type="SMB",
        base="\\\\srv\\part\\",
        file_size=1000,
        access_time="02/01/2026 10:00:00",
        file_attributes="Archive",
        owner="DOM\\x",
        fast_hash="aaaa",
        file_signature="unknown",
    )


def _campagne_analysee(tmp_path: Path, combien: int = 6) -> tuple[Config, Path]:
    """Base où `combien` fichiers sont `done` avec une analyse C3 chacun."""
    db_path = tmp_path / "campagne.sqlite"
    cfg = Config(db_path=str(db_path))
    with Database(db_path) as db:
        scan = db.start_scan("a.csv")
        db.upsert_files([_fichier(f"f{n}.pdf") for n in range(combien)], scan)
        phash, model = _effective_keys(db, cfg)
        for row in db.query("SELECT id, content_version FROM files ORDER BY id"):
            db.store_analysis(
                int(row["id"]),
                None,
                int(row["content_version"]),
                prompt_hash=phash,
                model=model,
                analysis=_c3(f"f{row['id']}"),
            )
        assert (db.counts()["done"], db.counts()["analyses"]) == (combien, combien)
    return cfg, db_path


def test_une_reanalyse_tuee_en_plein_vol_ne_laisse_aucune_trace(tmp_path: Path) -> None:
    """Une réanalyse interrompue laisse la campagne **exactement** telle qu'elle était.

    Historique de ce test, parce qu'il dit quelque chose sur la méthode. Il vérifiait
    d'abord qu'un état intermédiaire restait *réparable* : `reanalyze` faisait deux
    écritures séparées, une coupure entre les deux laissait des fichiers `done` sans
    analyse, la campagne annonçait « 100 % analysé » là où le rapport disait
    « 0 analysé », et seul `reanalyze --all` rattrapait le coup. L'ordre des deux
    écritures avait été inversé pour rendre cet état honnête et réparable.

    C'était un pis-aller : `Database.reset_for_reanalysis` — une seule transaction
    pour les deux écritures — existait déjà, testée, et **personne ne l'appelait**.
    Maintenant qu'elle est branchée, il n'y a plus d'état intermédiaire du tout, donc
    plus rien à réparer. Le test tue le processus au cœur de la transaction et vérifie
    la propriété forte : **rien n'a bougé**.
    """
    cfg, db_path = _campagne_analysee(tmp_path)
    script = tmp_path / "coupure_reanalyse.py"
    script.write_text(
        dedent(f"""
        import os
        from pathlib import Path

        import docia.db as db_module
        from docia.config import Config
        from docia.service import reanalyze

        base = Path({str(db_path)!r})
        # La machine s'éteint **au cœur** de la transaction : les statuts viennent
        # de passer `pending`, la suppression des analyses n'a pas encore eu lieu.
        # Sans transaction unique, c'est exactement l'état bâtard d'avant.
        from contextlib import contextmanager

        _transaction = db_module.Database.transaction

        # `sqlite3.Connection` est immuable : on l'enveloppe pour couper dedans.
        class _Mandataire:
            def __init__(self, conn):
                self._conn = conn

            def execute(self, sql, *a, **k):
                if sql.lstrip().upper().startswith("DELETE FROM ANALYSES"):
                    os._exit(9)
                return self._conn.execute(sql, *a, **k)

            def __getattr__(self, nom):
                return getattr(self._conn, nom)

        @contextmanager
        def _coupe_au_milieu(self):
            with _transaction(self) as conn:
                yield _Mandataire(conn)

        db_module.Database.transaction = _coupe_au_milieu

        with db_module.Database(base) as db:
            reanalyze(
                db,
                Config(db_path=str(base)),
                scope="filter",
                where={{"security": "C3"}},
                backup=False,
            )
        """),
        encoding="utf-8",
    )
    fini = subprocess.run(  # noqa: S603 - interpréteur courant, script écrit par le test
        [sys.executable, str(script)], capture_output=True, check=False
    )
    assert fini.returncode == 9, fini.stderr.decode("utf-8", "replace")

    with Database(db_path) as db:
        interrompu = campaign_status(db)
        assert (interrompu.done, interrompu.pending, interrompu.analyses) == (6, 0, 6), (
            "la transaction n'a pas été annulée : la coupure a laissé une trace"
        )
        # et la réanalyse, relancée, fait son travail normalement
        repris = reanalyze(db, cfg, scope="filter", where={"security": "C3"}, backup=False)
        apres = campaign_status(db)
        phash, model = _effective_keys(db, cfg)
        selectionnables = db.select_pending_ids(1000, prompt_hash=phash, model=model)

    assert repris == 6
    assert (apres.done, apres.pending, apres.analyses) == (0, 6, 0)
    assert len(selectionnables) == 6, "`docia run` retrouve bien les fichiers à refaire"


# ----------------------------------------------------- vérification humaine


def test_set_review_enregistre_et_relit_la_fiche(tmp_path: Path) -> None:
    cfg, db_path = _campagne_analysee(tmp_path, combien=1)
    with Database(db_path) as db:
        file_id = int(db.query("SELECT id FROM files")[0]["id"])
        fiche = set_review(db, file_id, "corrected", corrected_security="C2", reviewer="moi")
        assert fiche is not None
        assert db.review_counts()["corrected"] == 1
        with pytest.raises(ServiceError):
            set_review(db, file_id, "n_importe_quoi")


def test_le_pont_gui_delegue_la_verification_au_service(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """La doctrine du pont — « toute écriture passe par le service » — sans exception.

    `GuiService.set_review` écrivait directement dans `Database`, seule écriture de
    la fenêtre à court-circuiter `docia.service` : l'API REST de la v4 n'aurait eu
    aucune revue à exposer.
    """
    from docia.gui import service_shim

    _cfg, db_path = _campagne_analysee(tmp_path, combien=1)
    with Database(db_path) as db:
        file_id = int(db.query("SELECT id FROM files")[0]["id"])
    appels: list[tuple[int, str]] = []
    vrai = service_shim.service.set_review

    def espion(db: Database, fid: int, status: str, **kwargs: object) -> sqlite3.Row | None:
        appels.append((fid, status))
        return vrai(db, fid, status, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(service_shim.service, "set_review", espion)
    fiche = GuiService(lambda: Database(db_path)).set_review(file_id, "validated", reviewer="moi")

    assert appels == [(file_id, "validated")]
    assert fiche is not None
    with Database(db_path) as db:
        assert db.review_counts()["validated"] == 1
