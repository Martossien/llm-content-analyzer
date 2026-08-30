"""Tests des exclusions, du score de priorité et du plan."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pytest

from docia.config import FilterConfig
from docia.db import Database
from docia.filter import exclusion_reason, plan_files, priority_score
from docia.ingest.smbeagle_csv import import_csv
from docia.models import FileRow, FileStatus

FIXTURE = Path(__file__).parent / "fixtures" / "scan_local_mini.csv"
NOW = datetime(2026, 8, 30, 12, 0, 0)


def make_row(
    *,
    name: str = "rapport.pdf",
    extension: str = "pdf",
    size_bytes: int = 50_000,
    path: str = r"\\srv\part$\docs\rapport.pdf",
    last_write_time: str = "01/08/2026 10:00:00",
    status: FileStatus = FileStatus.PENDING,
) -> FileRow:
    """Construit un `FileRow` minimal pour les fonctions pures."""
    return FileRow(
        id=1,
        path=path,
        name=name,
        extension=extension,
        size_bytes=size_bytes,
        fast_hash="deadbeef",
        last_write_time=last_write_time,
        content_version=1,
        status=status,
    )


def permissive_config() -> FilterConfig:
    """Config qui ne rejette rien : sert de base aux tests ciblés."""
    return FilterConfig(
        excluded_extensions=[],
        min_size_bytes=0,
        max_size_bytes=1 << 40,
        excluded_dir_markers=[],
    )


# ------------------------------------------------------------------ exclusions


def test_aucune_exclusion() -> None:
    assert exclusion_reason(make_row(), permissive_config()) is None


@pytest.mark.parametrize("extension", ["jpg", "JPG", ".Jpg"])
def test_exclusion_par_extension(extension: str) -> None:
    cfg = permissive_config()
    cfg.excluded_extensions = [".jpg"]
    reason = exclusion_reason(make_row(extension=extension), cfg)
    assert reason is not None
    assert "extension exclue" in reason


def test_exclusion_extension_sans_point_dans_la_config() -> None:
    cfg = permissive_config()
    cfg.excluded_extensions = ["PNG"]
    assert exclusion_reason(make_row(extension="png"), cfg) is not None


def test_exclusion_fichier_trop_petit() -> None:
    cfg = permissive_config()
    cfg.min_size_bytes = 100
    reason = exclusion_reason(make_row(size_bytes=4), cfg)
    assert reason is not None
    assert "trop petit" in reason


def test_exclusion_fichier_trop_volumineux() -> None:
    cfg = permissive_config()
    cfg.max_size_bytes = 1_000
    reason = exclusion_reason(make_row(size_bytes=2_000), cfg)
    assert reason is not None
    assert "trop volumineux" in reason


@pytest.mark.parametrize(
    "path",
    [
        r"\\srv\c$\Windows\System32\notes.txt",
        r"\\srv\c$\WINDOWS\System32\notes.txt",
        "//srv/c$/Windows/System32/notes.txt",
    ],
)
def test_exclusion_par_marqueur_de_dossier(path: str) -> None:
    cfg = permissive_config()
    cfg.excluded_dir_markers = ["\\Windows\\"]
    reason = exclusion_reason(make_row(path=path, extension="txt"), cfg)
    assert reason is not None
    assert "dossier exclu" in reason


def test_ordre_des_regles_extension_avant_taille() -> None:
    cfg = FilterConfig(
        excluded_extensions=[".log"],
        min_size_bytes=1_000_000,
        max_size_bytes=1 << 40,
        excluded_dir_markers=[],
    )
    reason = exclusion_reason(make_row(extension="log", size_bytes=1), cfg)
    assert reason is not None
    assert "extension exclue" in reason


def test_fichier_sans_extension_non_exclu_par_extension() -> None:
    cfg = permissive_config()
    cfg.excluded_extensions = [".log"]
    assert exclusion_reason(make_row(extension="", name="LICENCE"), cfg) is None


# ----------------------------------------------------------------------- score


def test_score_maximum() -> None:
    row = make_row(name="contrat_cadre.docx", extension="docx", size_bytes=200_000)
    assert priority_score(row, NOW) == 100  # 40 + 30 + 20 + 10


def test_score_famille_bureautique_sans_mot_cle() -> None:
    row = make_row(name="notes.docx", extension="docx", size_bytes=200_000)
    assert priority_score(row, NOW) == 90


def test_score_famille_texte() -> None:
    row = make_row(name="notes.txt", extension="txt", size_bytes=200_000)
    assert priority_score(row, NOW) == 75  # 25 + 30 + 20


def test_score_famille_inconnue() -> None:
    row = make_row(name="donnees.bin", extension="bin", size_bytes=200_000)
    assert priority_score(row, NOW) == 60  # 10 + 30 + 20


@pytest.mark.parametrize(
    ("size_bytes", "attendu"),
    [(500, 10), (10 * 1024, 30), (5 * 1024 * 1024, 30), (20 * 1024 * 1024, 15)],
)
def test_score_taille(size_bytes: int, attendu: int) -> None:
    base = priority_score(make_row(extension="bin", size_bytes=200_000, name="x.bin"), NOW)
    row = make_row(extension="bin", size_bytes=size_bytes, name="x.bin")
    assert priority_score(row, NOW) == base - 30 + attendu


@pytest.mark.parametrize(
    ("jours", "attendu"),
    [(30, 20), (400, 12), (2_000, 5)],
)
def test_score_age(jours: int, attendu: int) -> None:
    modifie = (NOW - timedelta(days=jours)).strftime("%d/%m/%Y %H:%M:%S")
    row = make_row(extension="bin", size_bytes=200_000, name="x.bin", last_write_time=modifie)
    assert priority_score(row, NOW) == 10 + 30 + attendu


def test_score_age_inconnu() -> None:
    row = make_row(extension="bin", size_bytes=200_000, name="x.bin", last_write_time="n/a")
    assert priority_score(row, NOW) == 10 + 30 + 10


@pytest.mark.parametrize(
    "name",
    ["facture_2026.bin", "MOT DE PASSE.bin", "mot_de_passe.bin", "Bilan.bin", "rgpd-note.bin"],
)
def test_score_mots_cles(name: str) -> None:
    row = make_row(extension="bin", size_bytes=200_000, name=name)
    assert priority_score(row, NOW) == 70


def test_score_horodatage_avec_fuseau() -> None:
    """Un `now` aware ne doit pas faire exploser la comparaison."""
    aware = NOW.astimezone()
    assert priority_score(make_row(), aware) > 0


# ------------------------------------------------------------------------ plan


def test_plan_fixture_tout_pending(tmp_path: Path) -> None:
    with Database(tmp_path / "docia.sqlite") as db:
        import_csv(db, FIXTURE)
        report = plan_files(db, permissive_config())
        assert report.pending == 63
        assert report.excluded == 0
        assert report.by_reason == {}
        assert db.counts()["pending"] == 63


def test_plan_fixture_exclusions_par_defaut(tmp_path: Path) -> None:
    """La fixture est presque entièrement sous `\\admin$\\` : 62 exclus, 1 retenu."""
    with Database(tmp_path / "docia.sqlite") as db:
        import_csv(db, FIXTURE)
        report = plan_files(db, FilterConfig())
        assert report.pending + report.excluded == 63
        assert report.excluded == 62
        assert sum(report.by_reason.values()) == 62
        assert report.by_reason["dossier exclu (\\admin$\\)"] == 40
        counts = db.counts()
        assert counts["excluded"] == 62
        assert counts["pending"] == 1


def test_plan_fixture_taille_minimale(tmp_path: Path) -> None:
    cfg = permissive_config()
    cfg.min_size_bytes = 100
    with Database(tmp_path / "docia.sqlite") as db:
        import_csv(db, FIXTURE)
        report = plan_files(db, cfg)
        assert report.pending + report.excluded == 63
        assert report.excluded > 0
        assert all("trop petit" in reason for reason in report.by_reason)
        counts = db.counts()
        assert counts["pending"] == report.pending
        assert counts["excluded"] == report.excluded


def test_plan_attribue_les_scores(tmp_path: Path) -> None:
    with Database(tmp_path / "docia.sqlite") as db:
        import_csv(db, FIXTURE)
        plan_files(db, permissive_config())
        assert all(row.priority_score > 0 for row in list(db.iter_files()))


def test_plan_ne_retrograde_pas_un_fichier_done(tmp_path: Path) -> None:
    with Database(tmp_path / "docia.sqlite") as db:
        import_csv(db, FIXTURE)
        plan_files(db, permissive_config())
        cible = next(
            row for row in list(db.iter_files()) if row.path.endswith(r"\addins\fxsext.ecf")
        )
        db.set_file_status(cible.id, FileStatus.DONE)

        plan_files(db, FilterConfig())  # tout serait exclu

        apres = db.get_file(cible.id)
        assert apres is not None
        assert apres.status == FileStatus.DONE
