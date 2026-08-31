"""Vues partagées : doublons, ancienneté, matrices, conservation, nettoyage, revues."""

from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import fields, is_dataclass
from datetime import date
from pathlib import Path

import pytest

from docia import views
from docia.db import Database
from docia.models import DomainAnalysis, FileAnalysis, FileStatus, SmbeagleRow

TODAY = date(2026, 6, 30)
"""Date de référence injectée dans toutes les vues datées."""

SNAPSHOT = Path(__file__).parent / "fixtures" / "views_snapshot.json"
"""Empreinte des vues, produite avant l'optimisation (schéma v5) : référence de non-régression."""


def _row(
    name: str,
    *,
    fast_hash: str = "aaaa",
    size: int = 1000,
    lwt: str = "01/01/2026 10:00:00",
    access: str = "02/01/2026 10:00:00",
    owner: str = "DOM\\alice",
    directory: str = "\\\\srv\\part\\dossier",
    base: str = "\\\\srv\\part",
    extension: str | None = None,
) -> SmbeagleRow:
    return SmbeagleRow(
        name=name,
        host="srv",
        extension=extension if extension is not None else name.rsplit(".", 1)[-1],
        username="u",
        hostname="srv.dom",
        unc_directory=directory,
        creation_time="01/01/2020 10:00:00",
        last_write_time=lwt,
        readable=True,
        writeable=False,
        deletable=False,
        directory_type="SMB",
        base=base,
        file_size=size,
        access_time=access,
        file_attributes="Archive",
        owner=owner,
        fast_hash=fast_hash,
        file_signature="unknown",
    )


def _analysis(
    ref: str,
    *,
    security: str = "C1",
    rgpd: str = "low",
    retention: bool = False,
    years: int = 0,
    basis: str = "none",
) -> FileAnalysis:
    return FileAnalysis(
        file_ref=ref,
        resume=f"résumé de {ref}",
        security=DomainAnalysis(security, 80, {"justification": "parce que"}),
        rgpd=DomainAnalysis(rgpd, 70, {"data_types": ["identite"]}),
        finance=DomainAnalysis("none", 90, {"amounts": []}),
        legal=DomainAnalysis("none", 90, {"parties": []}),
        raw={"file_ref": ref},
        retention=DomainAnalysis(
            basis,
            60,
            {"required": retention, "years": years, "justification": "obligation"},
        ),
    )


@pytest.fixture
def db(tmp_path: Path) -> Iterator[Database]:
    """Base de démonstration : 3 doublons, des âges variés, 5 analyses, 2 revues."""
    database = Database(tmp_path / "views.sqlite")
    scan = database.start_scan("scan.csv")
    database.upsert_files(
        [
            # famille de doublons : 3 exemplaires de 1 000 octets
            _row("copie1.pdf", fast_hash="dup", size=1000),
            _row("copie2.pdf", fast_hash="dup", size=1000, directory="\\\\srv\\part\\autre"),
            _row(
                "copie3.pdf",
                fast_hash="dup",
                size=1000,
                directory="\\\\srv\\rh\\dossier",
                base="\\\\srv\\rh",
            ),
            # famille de 2 exemplaires de 500 octets
            _row("note1.docx", fast_hash="dup2", size=500),
            _row("note2.docx", fast_hash="dup2", size=500, directory="\\\\srv\\part\\autre"),
            # unique, ancien (dernier accès et écriture en 2018)
            _row(
                "vieux.txt",
                fast_hash="old",
                size=2_000_000,
                lwt="05/03/2018 08:00:00",
                access="06/03/2018 08:00:00",
                owner="DOM\\bob",
            ),
            # unique, récent, gros, propriétaire bob
            _row("gros.xlsx", fast_hash="big", size=200_000_000, owner="DOM\\bob"),
            # fichier vide
            _row("vide.txt", fast_hash="empty", size=0),
        ],
        scan,
    )
    files = {f.name: f for f in database.iter_files()}
    database.store_analysis(
        files["copie1.pdf"].id,
        None,
        1,
        prompt_hash="p",
        model="m",
        analysis=_analysis("copie1.pdf", security="C3", rgpd="critical"),
    )
    database.store_analysis(
        files["copie3.pdf"].id,
        None,
        1,
        prompt_hash="p",
        model="m",
        analysis=_analysis(
            "copie3.pdf", security="C2", rgpd="high", retention=True, years=5, basis="rh"
        ),
    )
    database.store_analysis(
        files["vieux.txt"].id,
        None,
        1,
        prompt_hash="p",
        model="m",
        analysis=_analysis("vieux.txt", security="C0", rgpd="none"),
    )
    database.store_analysis(
        files["gros.xlsx"].id,
        None,
        1,
        prompt_hash="p",
        model="m",
        analysis=_analysis(
            "gros.xlsx", security="C1", rgpd="low", retention=True, years=10, basis="fiscal"
        ),
    )
    database.store_analysis(
        files["note1.docx"].id,
        None,
        1,
        prompt_hash="p",
        model="m",
        analysis=_analysis("note1.docx"),
    )
    database.set_file_status(files["vide.txt"].id, FileStatus.EXCLUDED, "fichier vide")
    database.set_review(
        files["copie1.pdf"].id, "corrected", corrected_security="C2", reviewer="moi"
    )
    database.set_review(files["copie3.pdf"].id, "validated", reviewer="moi")
    yield database
    database.close()


# --------------------------------------------------------------- non-régression


def _plain(value: object) -> object:
    """Dataclasses, dates et conteneurs → types directement comparables au JSON."""
    if is_dataclass(value) and not isinstance(value, type):
        return {field.name: _plain(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, list | tuple):
        return [_plain(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _plain(item) for key, item in value.items()}
    return value


def snapshot(database: Database) -> dict[str, object]:
    """Résultat de toutes les vues sur une base, sous forme comparable."""
    overview = _plain(views.overview(database, today=TODAY, stale_years=3))
    assert isinstance(overview, dict)
    overview["db_path"] = "(chemin temporaire)"  # dépend du dossier de test
    return {
        "overview": overview,
        "by_extension": _plain(views.by_extension(database)),
        "by_owner": _plain(views.by_owner(database)),
        "by_share": _plain(views.by_share(database)),
        "by_directory_1": _plain(views.by_directory(database, depth=1)),
        "by_directory_2": _plain(views.by_directory(database, depth=2)),
        "size_buckets": _plain(views.size_buckets(database)),
        "empty_or_tiny": _plain(views.empty_or_tiny(database)),
        "status_summary": _plain(views.status_summary(database)),
        "duplicates": _plain(views.duplicates(database)),
        "duplicates_min3": _plain(views.duplicates(database, min_copies=3)),
        "stale_files": _plain(views.stale_files(database, today=TODAY)),
        "cleanup_3": _plain(views.cleanup_candidates(database, years=3, today=TODAY)),
        "cleanup_10": _plain(views.cleanup_candidates(database, years=10, today=TODAY)),
        "top_sensitive": _plain(views.top_sensitive(database, limit=10)),
        "matrix_share": _plain(views.classification_matrix(database, axis="share")),
        "matrix_owner": _plain(views.classification_matrix(database, axis="owner")),
        "matrix_directory": _plain(views.classification_matrix(database, axis="directory")),
        "matrix_extension": _plain(views.classification_matrix(database, axis="extension")),
        "retention_plan": _plain(views.retention_plan(database, today=TODAY)),
        "review_progress": _plain(views.review_progress(database)),
        "runs_summary": _plain(views.runs_summary(database)),
        "classification_summary": _plain(database.classification_summary()),
        "counts": _plain(database.counts()),
    }


def test_views_snapshot_is_unchanged(db: Database) -> None:
    """Chaque valeur rendue par les vues est comparée à l'empreinte de référence.

    L'empreinte a été produite avec le code d'avant l'optimisation (schéma v5) :
    elle vérifie que les requêtes réécrites et les clés de date normalisées ne
    changent aucun chiffre.
    """
    assert snapshot(db) == json.loads(SNAPSHOT.read_text(encoding="utf-8"))


# ------------------------------------------------------------------ hygiène


def test_duplicates_families_and_reclaimable_bytes(db: Database) -> None:
    report = views.duplicates(db)
    assert report.total_families == 2
    assert report.total_copies == 5
    # 1000 × (3−1) + 500 × (2−1) = 2 500
    assert report.total_reclaimable_bytes == 2500
    first = report.families[0]
    assert (first.copies, first.size_bytes, first.reclaimable_bytes) == (3, 1000, 2000)
    assert len(first.paths) == 3
    assert len(first.file_ids) == 3
    assert first.family_id == "dup-1000"
    # un seuil plus haut ne garde que la famille de 3
    assert views.duplicates(db, min_copies=3).total_families == 1


def test_stale_files_thresholds(db: Database) -> None:
    buckets = {b.years: b for b in views.stale_files(db, years=(1, 3, 5, 10), today=TODAY)}
    assert buckets[1].cutoff == date(2025, 6, 30)
    # seul vieux.txt (2018) est antérieur à 2025-06-30 et à 2023-06-30
    assert (buckets[1].not_accessed_files, buckets[1].not_accessed_bytes) == (1, 2_000_000)
    assert buckets[3].not_accessed_files == 1
    assert buckets[5].not_accessed_files == 1  # 2018 < 2021-06-30
    assert buckets[10].not_accessed_files == 0  # 2018 > 2016-06-30
    assert buckets[3].not_modified_files == 1


def test_by_extension_owner_share_and_sizes(db: Database) -> None:
    extensions = {g.label: g for g in views.by_extension(db)}
    assert extensions["pdf"].files == 3
    assert extensions["xlsx"].bytes == 200_000_000

    owners = {g.label: g for g in views.by_owner(db)}
    assert owners["DOM\\bob"].files == 2
    assert owners["DOM\\alice"].files == 6

    shares = {g.label: g for g in views.by_share(db)}
    assert set(shares) == {"\\\\srv\\part", "\\\\srv\\rh"}
    assert shares["\\\\srv\\rh"].files == 1

    buckets = {g.label: g for g in views.size_buckets(db)}
    assert buckets["> 100 Mo"].files == 1
    assert sum(g.files for g in views.size_buckets(db)) == 8

    tiny = views.empty_or_tiny(db)
    assert (tiny.files, tiny.empty_files) == (1, 1)


def test_status_summary_reports_exclusion_reasons(db: Database) -> None:
    summary = views.status_summary(db)
    assert summary.total_files == 8
    assert summary.counts["done"] == 5
    assert summary.counts["excluded"] == 1
    assert [(g.label, g.files) for g in summary.reasons] == [("fichier vide", 1)]


def test_by_directory_groups_two_levels(db: Database) -> None:
    rows = {r.label: r for r in views.by_directory(db, depth=1)}
    assert "\\\\srv\\part\\dossier" in rows
    assert "\\\\srv\\rh\\dossier" in rows
    assert rows["\\\\srv\\rh\\dossier"].files == 1


# ------------------------------------------------------------------ risque


def test_classification_matrix_by_owner(db: Database) -> None:
    rows = {r.label: r for r in views.classification_matrix(db, axis="owner")}
    alice = rows["DOM\\alice"]
    assert alice.analyzed == 3  # copie1 (C3), copie3 (C2), note1 (C1)
    assert alice.security["C3"] == 1
    assert alice.security["C2"] == 1
    assert alice.security["C1"] == 1
    assert alice.rgpd["critical"] == 1
    assert alice.sensitive == 2
    bob = rows["DOM\\bob"]
    assert bob.analyzed == 2
    assert bob.sensitive == 0
    with pytest.raises(ValueError, match="axe inconnu"):
        views.classification_matrix(db, axis="bidon")


def test_classification_matrix_by_share_and_extension(db: Database) -> None:
    shares = {r.label: r for r in views.classification_matrix(db, axis="share")}
    assert shares["\\\\srv\\rh"].security["C2"] == 1
    extensions = {r.label: r for r in views.classification_matrix(db, axis="extension")}
    assert extensions["pdf"].analyzed == 2


def test_top_sensitive_order_and_content(db: Database) -> None:
    top = views.top_sensitive(db, limit=10)
    assert [f.security for f in top] == ["C3", "C2"]
    assert top[0].rgpd == "critical"
    assert top[0].path.endswith("copie1.pdf")
    assert top[0].review_status == "corrected"
    assert "résumé" in top[0].resume
    assert top[0].justification == "parce que"


def test_retention_plan_end_dates(db: Database) -> None:
    plan = views.retention_plan(db, today=TODAY)
    assert plan.total_files == 2
    by_path = {r.path.rsplit("\\", 1)[-1]: r for r in plan.rows}
    # copie3.pdf : dernière écriture 01/01/2026 + 5 ans
    assert by_path["copie3.pdf"].end_date == date(2031, 1, 1)
    assert by_path["copie3.pdf"].years == 5
    assert by_path["copie3.pdf"].basis == "rh"
    assert by_path["copie3.pdf"].expired is False
    assert by_path["gros.xlsx"].end_date == date(2036, 1, 1)
    assert plan.expired_files == 0
    assert {g.label for g in plan.by_basis} == {"ressources humaines", "obligation fiscale"}


def test_cleanup_candidates(db: Database) -> None:
    # vieux.txt : C0, sans conservation, dernier accès en 2018 → candidat à 3 ans
    report = views.cleanup_candidates(db, years=3, today=TODAY)
    assert report.total_files == 1
    assert report.total_bytes == 2_000_000
    assert report.rows[0].path.endswith("vieux.txt")
    assert report.cutoff == date(2023, 6, 30)
    # à 10 ans, plus aucun candidat
    assert views.cleanup_candidates(db, years=10, today=TODAY).total_files == 0


def test_review_progress_and_discrepancies(db: Database) -> None:
    progress = views.review_progress(db)
    assert (progress.validated, progress.corrected, progress.to_review) == (1, 1, 0)
    assert progress.analyzed == 5
    assert progress.not_reviewed == 3
    assert progress.percent_reviewed == 40.0
    assert len(progress.discrepancies) == 1
    gap = progress.discrepancies[0]
    assert (gap.llm_security, gap.corrected_security) == ("C3", "C2")


def test_runs_summary(db: Database) -> None:
    assert views.runs_summary(db) == []
    run = db.start_run(model="m", prompt_hash="p", config_json="{}")
    db.finish_run(run)
    stats = views.runs_summary(db)
    assert len(stats) == 1
    assert (stats[0].run_id, stats[0].model, stats[0].blocks) == (run, "m", 0)
    assert stats[0].tokens_per_file == 0.0


def test_overview_aggregates(db: Database) -> None:
    o = views.overview(db, today=TODAY, stale_years=3)
    assert o.total_files == 8
    assert o.analyzed == 5
    assert o.excluded == 1
    assert o.duplicate_families == 2
    assert o.duplicate_reclaimable_bytes == 2500
    assert o.stale_files == 1
    assert o.sensitive_files == 2
    assert o.rgpd_at_risk == 2
    assert o.retention_files == 2
    assert o.cleanup_files == 1
    assert o.reviewed == 2
    assert o.model == "m"


# ------------------------------------------------------------------ utilitaires


@pytest.mark.parametrize(
    ("base", "directory", "expected"),
    [
        ("\\\\srv\\part", "\\\\srv\\part\\a\\b", "\\\\srv\\part"),
        ("", "\\\\srv\\part\\a\\b", "\\\\srv\\part"),
        ("", "\\\\srv", "\\\\srv"),
        ("", "", "(inconnu)"),
    ],
)
def test_share_label(base: str, directory: str, expected: str) -> None:
    assert views.share_label(base, directory) == expected


DIRECTORIES: tuple[tuple[str, str], ...] = (
    ("", ""),
    ("", "\\\\srv"),
    ("", "\\\\srv\\part"),
    ("", "\\\\srv\\part\\a"),
    ("", "\\\\srv\\part\\a\\b"),
    ("", "\\\\srv\\part\\a\\b\\c"),
    ("", "\\\\srv\\part\\a\\bb\\c"),
    ("", "\\\\srv\\part\\a\\b\\c\\d\\e"),
    ("", "\\\\srv\\part\\a\\b/c\\d"),
    ("", "//srv/part/a/b/c"),
    ("", "C:\\data\\a\\b"),
    ("", "C:\\data\\a\\bb"),
    ("   ", "\\\\srv\\part\\x\\y\\z"),
    ("\\\\srv\\part", "\\\\srv\\part\\a\\b\\c"),
    ("\\\\srv\\part", "\\\\srv\\part\\a\\b\\c\\d"),
    ("\\\\srv\\part", "\\\\SRV\\PART\\a\\b\\c"),
    ("\\\\srv\\part\\", "\\\\srv\\part\\a"),
    ("autre", "\\\\srv\\part\\a\\b\\c"),
    ("autre", "\\\\srv\\part\\a\\b\\cc"),
)
"""Répertoires piégeux : profondeurs, séparateurs, casse, espaces, `base` vide ou non."""


@pytest.mark.parametrize("depth", [1, 2, 3, 5])
def test_axis_labeller_matches_direct_labels(depth: int) -> None:
    """La réutilisation de préfixe rend exactement les étiquettes calculées une à une.

    Les lignes sont triées comme le fait l'index couvrant : c'est le cas où
    l'étiquette précédente est réutilisée.
    """
    rows = sorted(DIRECTORIES)
    for axis, expected in (
        ("share", [views.share_label(base, path) for base, path in rows]),
        ("directory", [views.directory_label(base, path, depth) for base, path in rows]),
    ):
        label_of = views._axis_labeller(axis, depth)  # noqa: SLF001
        assert [label_of(row) for row in rows] == expected


def test_directory_label_depth() -> None:
    assert (
        views.directory_label("\\\\srv\\part", "\\\\srv\\part\\a\\b\\c", 2) == "\\\\srv\\part\\a\\b"
    )
    assert views.directory_label("\\\\srv\\part", "\\\\srv\\part", 2) == "\\\\srv\\part"


def test_shift_years_handles_29_february() -> None:
    assert views.shift_years(date(2024, 2, 29), -1) == date(2023, 2, 28)
    assert views.shift_years(date(2024, 2, 29), 4) == date(2028, 2, 29)


def test_format_helpers() -> None:
    assert views.format_bytes(512) == "512 o"
    assert views.format_bytes(1536) == "1,5 Ko"
    assert views.format_int(1234567).replace(" ", " ") == "1 234 567"
    assert views.percent(1, 4) == 25.0
    assert views.percent(1, 0) == 0.0


def test_empty_database_is_safe(tmp_path: Path) -> None:
    with Database(tmp_path / "vide.sqlite") as empty:
        assert views.duplicates(empty).families == []
        assert views.by_extension(empty) == []
        assert views.top_sensitive(empty) == []
        assert views.retention_plan(empty, today=TODAY).total_files == 0
        assert views.overview(empty, today=TODAY).total_files == 0
