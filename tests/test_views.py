"""Vues partagées : doublons, ancienneté, matrices, conservation, nettoyage, revues."""

from __future__ import annotations

import json
import sys
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
    resume: str | None = None,
    justification: str = "parce que",
) -> FileAnalysis:
    """Analyse de démonstration. `resume` et `justification` sont paramétrables :
    ce sont les deux textes que la LLM rend sans contrainte de caractères."""
    return FileAnalysis(
        file_ref=ref,
        resume=resume if resume is not None else f"résumé de {ref}",
        security=DomainAnalysis(security, 80, {"justification": justification}),
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


def test_status_summary_borne_les_raisons_sans_le_taire(tmp_path: Path) -> None:
    """La borne sur les raisons d'exclusion est explicite, paramétrable, et annoncée.

    Elle valait `LIMIT 10` en dur dans le SQL, sans que rien ne signale la coupe :
    un rapport qui justifie des suppressions taisait des motifs de **non**-analyse.
    Les raisons d'erreur sont du texte libre (`Database.set_file_status`), donc de
    cardinalité non bornée : la borne reste — mais elle se dit.
    """
    with Database(tmp_path / "raisons.sqlite") as database:
        scan = database.start_scan("scan.csv")
        database.upsert_files([_row(f"f{i}.pdf", fast_hash=f"h{i}") for i in range(25)], scan)
        database.finish_scan(scan, total=25, new=25, updated=0, unchanged=0, invalid=0)
        for i, fichier in enumerate(sorted(database.iter_files(), key=lambda f: f.name)):
            database.set_file_status(fichier.id, FileStatus.ERROR, f"extraction : panne n°{i:02d}")

        defaut = views.status_summary(database)
        assert len(defaut.reasons) == views.REASON_TOP == 10
        assert defaut.reasons_total == 25
        assert defaut.reasons_hidden == 15, "le rapport peut dire ce qu'il ne montre pas"

        trois = views.status_summary(database, reason_limit=3)
        assert len(trois.reasons) == 3
        assert (trois.reasons_total, trois.reasons_hidden) == (25, 22)

        tout = views.status_summary(database, reason_limit=None)
        assert len(tout.reasons) == 25
        assert (tout.reasons_total, tout.reasons_hidden) == (25, 0)


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
    # Séparateurs vides : c'est là que compter les antislashs bruts décalait le
    # préfixe mémoïsé et fusionnait deux partages distincts sous une seule étiquette.
    ("", "\\\\\\srv\\part\\a"),
    ("", "\\\\\\srv\\AUTRE\\b"),
    ("", "\\\\srv\\\\part\\a"),
    ("", "\\\\srv\\\\AUTRE\\b"),
    ("", "\\\\srv\\part\\\\a\\b"),
    ("", "\\\\srv\\part\\\\a\\bb"),
    ("", "//srv//part//a//b"),
    ("", "//srv//AUTRE//a//b"),
)
"""Répertoires piégeux : profondeurs, séparateurs, casse, espaces, `base` vide ou non."""


def _generated_directories(count: int, seed: int = 11) -> list[tuple[str, str]]:
    """Chemins engendrés : séparateurs doublés ou manquants, slashs, espaces, casse mêlée.

    Les 19 cas choisis à la main ne suffisaient pas : le défaut ne se déclenche que
    sur un séparateur vide, forme qu'aucun d'eux ne portait.

    Les `base` engendrées descendent aussi **dans** l'arborescence
    (`\\\\srv\\part\\Direction\\RH`), la forme que prend une campagne cadrée sur un
    sous-arbre. C'est la profondeur que la mémoïsation supposait toujours égale à
    deux — 2 000 chemins n'en rendaient plus que 304 étiquettes distinctes sur 500.
    """
    import random

    rng = random.Random(seed)
    serveurs = ["srv", "SRV", "srv-fichiers", "192.168.1.72"]
    partages = ["part", "PART", "partage 1", "part,age", "admin$", "AUTRE"]
    niveaux = ["a", "b", "dossier 1", "Direction Générale", "sous dossier", "x", ""]
    out: list[tuple[str, str]] = []
    for _ in range(count):
        head = rng.choice(["\\\\", "\\\\\\", "//", "\\", ""])
        sep = rng.choice(["\\", "\\\\", "/"])
        parts = [rng.choice(serveurs), rng.choice(partages)]
        parts += [rng.choice(niveaux) for _ in range(rng.randint(0, 5))]
        directory = head + sep.join(parts)
        if rng.random() < 0.15:
            directory += rng.choice(["\\", "/", " "])
        profond = "\\\\" + "\\".join(p for p in parts[:4] if p)
        base = rng.choice(
            ["", "", "", f"\\\\{parts[0]}\\{parts[1]}", parts[1], "X", profond, profond]
        )
        out.append((base, directory))
    return out


@pytest.mark.parametrize("depth", [0, 1, 2, 3, 5])
def test_axis_labeller_matches_generated_labels(depth: int) -> None:
    """Propriété : sur 2 000 chemins engendrés, la mémoïsation ne change aucune étiquette.

    Un préfixe trop court réutilisait l'étiquette précédente et **additionnait des
    fichiers de partages différents** sur une même ligne de statistiques.
    """
    rows = sorted(_generated_directories(2_000))
    for axis, expected in (
        ("share", [views.share_label(base, path) for base, path in rows]),
        ("directory", [views.directory_label(base, path, depth) for base, path in rows]),
    ):
        label_of = views._axis_labeller(axis, depth)  # noqa: SLF001
        assert [label_of(row) for row in rows] == expected


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


@pytest.mark.parametrize("depth", [0, 1, 2, 3, 5])
def test_le_regroupement_memoise_egale_le_regroupement_ligne_a_ligne(depth: int) -> None:
    """Propriété : le tableau « Répertoires » compte comme un regroupement naïf.

    Le test frère compare des **étiquettes** ; celui-ci compare le **regroupement**,
    c'est-à-dire ce que la ligne du tableau annonce : combien de répertoires
    distincts, et combien de fichiers sur chacun. Les deux se lisent différemment —
    une étiquette réutilisée à tort ne fait pas qu'égarer un nom, elle **additionne
    les fichiers d'un répertoire sur la ligne d'un autre**, et fait disparaître le
    second du tableau.

    Mesuré sur ces 2 000 chemins avant la correction, à `depth=1` : 304 lignes au
    lieu de 500, soit 196 répertoires évaporés, leurs fichiers reportés ailleurs.
    Sur une sortie qui justifie des suppressions, se tromper de répertoire est
    exactement l'erreur à ne pas faire.
    """
    from collections import Counter

    rows = sorted(_generated_directories(2_000))
    label_of = views._axis_labeller("directory", depth)  # noqa: SLF001
    memoise = Counter(label_of(row) for row in rows)
    ligne_a_ligne = Counter(views.directory_label(base, path, depth) for base, path in rows)
    assert memoise == ligne_a_ligne
    assert len(memoise) == len(ligne_a_ligne), "des répertoires distincts ont fusionné"


def test_le_tableau_repertoires_ne_fusionne_pas_un_sous_arbre(tmp_path: Path) -> None:
    """Campagne cadrée sur un sous-arbre : chaque répertoire garde sa ligne.

    `base` vaut alors `\\\\srv\\part\\Direction\\RH` — quatre niveaux, pas deux. Le
    préfixe mémoïsé n'en couvrait que `depth + 2`, donc deux de moins que
    l'étiquette n'en consomme : `…\\RH\\Paie` héritait de l'étiquette de
    `…\\RH\\Contrats`. Le tableau annonçait **une** ligne de quatre fichiers là où
    il y a quatre répertoires d'un fichier.
    """
    base = "\\\\srv\\part\\Direction\\RH"
    dossiers = ("Contrats", "Paie", "Recrutement", "Sanctions")
    with Database(tmp_path / "sous_arbre.sqlite") as database:
        scan = database.start_scan("scan.csv")
        database.upsert_files(
            [
                _row(f"{nom}.pdf", base=base, directory=f"{base}\\{nom}", fast_hash=f"h{i}")
                for i, nom in enumerate(dossiers)
            ],
            scan,
        )
        database.finish_scan(scan, total=4, new=4, updated=0, unchanged=0, invalid=0)
        lignes = views.by_directory(database, depth=1)
    assert sorted(ligne.label for ligne in lignes) == [f"{base}\\{nom}" for nom in dossiers]
    assert [ligne.files for ligne in lignes] == [1, 1, 1, 1]


def test_directory_label_depth() -> None:
    assert (
        views.directory_label("\\\\srv\\part", "\\\\srv\\part\\a\\b\\c", 2) == "\\\\srv\\part\\a\\b"
    )
    assert views.directory_label("\\\\srv\\part", "\\\\srv\\part", 2) == "\\\\srv\\part"


def test_shift_years_handles_29_february() -> None:
    assert views.shift_years(date(2024, 2, 29), -1) == date(2023, 2, 28)
    assert views.shift_years(date(2024, 2, 29), 4) == date(2028, 2, 29)


@pytest.mark.parametrize(
    ("depart", "annees", "attendu"),
    [
        (date(9999, 1, 1), 10, date.max),  # DateTime.MaxValue + conservation légale
        (date(9999, 12, 31), 1, date.max),
        (date(9999, 2, 28), 100, date.max),  # durée maximale acceptée par le parseur
        (date(1, 1, 1), -1, date.min),  # FILETIME nul, décalé vers le passé
        (date(2026, 6, 30), -5, date(2021, 6, 30)),  # cas courant : inchangé
    ],
)
def test_shift_years_borne_les_dates_hors_plage(depart: date, annees: int, attendu: date) -> None:
    """Une date aberrante ne doit jamais coûter le rapport.

    `date.replace(year=10009)` lève `ValueError: year 10009 is out of range` :
    un seul fichier daté 9999 (FILETIME saturé, `DateTime.MaxValue` de .NET) avec
    une conservation légale rendait `html`, `markdown`, `powerbi` et `xlsx`
    impossibles pour la campagne entière.
    """
    assert views.shift_years(depart, annees) == attendu


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


# ------------------------------- D1 : « à conserver 0 an » n'est pas « échu »


def _base_conservation(path: Path, *, years: int, written: str = "01/01/2010 08:00:00") -> Database:
    """Un fichier « à conserver » pendant `years` années, écrit il y a longtemps."""
    database = Database(path)
    scan = database.start_scan("scan.csv")
    database.upsert_files([_row("dossier.pdf", lwt=written, access=written)], scan)
    database.finish_scan(scan, total=1, new=1, updated=0, unchanged=0, invalid=0)
    for file_row in database.iter_files():
        database.store_analysis(
            file_row.id,
            None,
            1,
            prompt_hash="p",
            model="m",
            analysis=_analysis(
                file_row.name, retention=True, years=years, basis="legal", security="C1"
            ),
        )
    return database


def test_conservation_sans_duree_nest_jamais_echue(tmp_path: Path) -> None:
    """`retention_required=1` avec `years=0` : durée non déterminée, pas échéance immédiate.

    Le schéma LLM autorise `years: 0` (`minimum: 0`) et l'analyseur l'accepte.
    Calculée, la fin de conservation valait « dernière écriture + 0 an », donc la
    date d'écriture elle-même : tout fichier écrit avant aujourd'hui devenait
    « échu : oui ». Sur une base réelle de 280 208 fichiers, 155 218 étaient ainsi
    déclarés échus à tort — et « échu » est l'indicateur sur lequel un agent
    décide de supprimer.
    """
    with _base_conservation(tmp_path / "zero.sqlite", years=0) as database:
        plan = views.retention_plan(database, today=TODAY)
    ligne = plan.rows[0]
    assert ligne.years == 0
    assert ligne.undetermined is True
    assert ligne.end_date is None
    assert ligne.expired is False
    assert plan.expired_files == 0
    assert plan.undetermined_files == 1


def test_conservation_avec_duree_reste_echue_quand_elle_lest(tmp_path: Path) -> None:
    """Le correctif ne doit pas éteindre l'indicateur : une durée réelle échue le reste."""
    with _base_conservation(tmp_path / "cinq.sqlite", years=5) as database:
        plan = views.retention_plan(database, today=TODAY)
    ligne = plan.rows[0]
    assert (ligne.years, ligne.undetermined) == (5, False)
    assert ligne.end_date == date(2015, 1, 1)
    assert ligne.expired is True
    assert (plan.expired_files, plan.undetermined_files) == (1, 0)


# ------------------------- un fichier sensible n'est jamais candidat au nettoyage


@pytest.mark.parametrize("classe", ["C2", "C3", "N/A", ""])
def test_un_fichier_sensible_ou_non_classe_nest_jamais_candidat_au_nettoyage(
    tmp_path: Path, classe: str
) -> None:
    """Seules C0 et C1 peuvent entrer dans la liste des candidats à la suppression.

    Ajouter les C2 à `_CLEANUP_WHERE` passait la suite complète : rien ne
    vérifiait la seule garantie de sûreté de cette vue. Un fichier non classé
    (`N/A`, ou vide parce que l'analyse a échoué) ne doit pas y entrer non plus :
    la clause est une liste blanche, pas une liste noire.
    """
    with Database(tmp_path / f"nettoyage_{classe or 'vide'}.sqlite") as database:
        scan = database.start_scan("scan.csv")
        database.upsert_files(
            [_row("ancien.pdf", lwt="01/01/2010 08:00:00", access="01/01/2010 08:00:00")], scan
        )
        database.finish_scan(scan, total=1, new=1, updated=0, unchanged=0, invalid=0)
        file_id = next(iter(database.iter_files())).id
        database.store_analysis(
            file_id,
            None,
            1,
            prompt_hash="p",
            model="m",
            analysis=_analysis("ancien.pdf", security="C1", rgpd="none"),
        )
        # la classification est écrasée directement : `C1` seul est un candidat
        database.query("SELECT 1")
        database._conn.execute(  # noqa: SLF001
            "UPDATE analyses SET security_classification=?", (classe,)
        )
        database._conn.commit()  # noqa: SLF001

        rapport = views.cleanup_candidates(database, years=1, today=TODAY)
        assert rapport.rows == []
        assert (rapport.total_files, rapport.total_bytes) == (0, 0)
        assert views.overview(database, today=TODAY, stale_years=1).cleanup_files == 0

        # preuve que la base était bien candidate avec une classe autorisée
        database._conn.execute("UPDATE analyses SET security_classification='C1'")  # noqa: SLF001
        database._conn.commit()  # noqa: SLF001
        assert views.cleanup_candidates(database, years=1, today=TODAY).total_files == 1


# --------------------- la règle « dernière analyse » n'existe qu'à un seul endroit


def _sql_normalise(text: str) -> str:
    """Texte SQL comparable : blancs ôtés (`file_id=f.id` ≡ `file_id = f.id`)."""
    return "".join(text.split())


def test_la_regle_derniere_analyse_est_la_meme_partout() -> None:
    """Les trois formulations de « la dernière analyse » doivent rester identiques.

    Elle décide quelle analyse fait foi : classification, RGPD, conservation,
    candidats au nettoyage. Elle vivait en trois exemplaires — `docia.views`,
    `docia.db` et `docia.report.powerbi` — sans qu'aucun test ne les compare : le
    rapport, l'écran Résultats et l'export Power BI pouvaient diverger sans que
    rien n'échoue.
    """
    from docia import db as db_module
    from docia.report import powerbi

    attendu_a = _sql_normalise(views.latest_analysis_sql("a.file_id"))
    attendu_f = _sql_normalise(views.latest_analysis_sql("f.id"))

    assert attendu_a in _sql_normalise(views._IS_LATEST)  # noqa: SLF001
    assert attendu_f in _sql_normalise(powerbi._LATEST_ANALYSIS_JOIN)  # noqa: SLF001
    # `docia.db` ne peut pas importer `docia.views` (le cycle est dans l'autre sens) :
    # sa copie est comparée mot à mot, en attendant qu'elle descende dans `docia.db`.
    assert attendu_f in _sql_normalise(db_module._LATEST_JOINS)  # noqa: SLF001
    # Quatrième formulation : celle des compteurs (`classification_summary`), qui
    # partait des analyses et comptait donc **toutes** les lignes de la table.
    assert attendu_a in _sql_normalise(db_module._IS_LATEST)  # noqa: SLF001


def test_une_analyse_sans_classe_reste_dans_la_matrice(tmp_path: Path) -> None:
    """MOYEN : une analyse sans classe de sécurité était jetée de la matrice.

    `_fold_risk` faisait `continue` sur une classification vide : la ligne entière
    disparaissait, **son niveau RGPD compris**. La synthèse et la matrice du même
    rapport annonçaient alors des chiffres différents :

        synthèse : analysés=3  RGPD à risque=2
        matrice  : analysés=2  RGPD à risque=1

    Un fichier au risque RGPD critique s'évaporait du tableau parce que sa classe de
    sécurité n'était pas renseignée — le cas existe quand le modèle répond
    partiellement. C'est précisément celui qu'il faut voir.
    """
    with Database(tmp_path / "sans_classe.sqlite") as database:
        scan = database.start_scan("s")
        database.upsert_files([_row(f"d{i}.pdf", fast_hash=f"h{i}") for i in range(3)], scan)
        database.finish_scan(scan, total=3, new=3, updated=0, unchanged=0, invalid=0)
        fichiers = sorted(database.iter_files(), key=lambda f: f.name)
        for fichier, classe, niveau in zip(
            fichiers, ("C3", "C0", ""), ("critical", "low", "critical"), strict=True
        ):
            database.store_analysis(
                fichier.id,
                None,
                1,
                prompt_hash="p",
                model="m",
                analysis=_analysis(fichier.name, security=classe, rgpd=niveau),
            )
        apercu = views.overview(database, today=TODAY)
        matrice = views.classification_matrix(database, axis="share")

    assert sum(ligne.analyzed for ligne in matrice) == apercu.analyzed == 3
    risque = sum(ligne.rgpd.get("high", 0) + ligne.rgpd.get("critical", 0) for ligne in matrice)
    assert risque == apercu.rgpd_at_risk == 2, "le RGPD ne disparaît pas avec la classe"
    assert matrice[0].security["N/A"] == 1, "la classe absente se voit, elle ne s'efface pas"


def test_l_avancement_de_verification_ne_depasse_pas_cent_pour_cent(tmp_path: Path) -> None:
    """MOYEN : l'avancement de la vérification humaine montait à 400 %.

    `_review_counts` comptait **toute** la table `reviews`, sans lien avec le
    dénominateur (`_analyzed_files`). `set_review` accepte n'importe quel identifiant,
    y compris un fichier jamais analysé : un analysé et quatre revues affichaient
    « 4 sur 1 analysés », soit 400 %, et `not_reviewed` était ramené à 0 par un
    `max(..., 0)` qui masquait l'incohérence au lieu de la signaler.
    """
    with Database(tmp_path / "revues.sqlite") as database:
        scan = database.start_scan("s")
        database.upsert_files([_row(f"d{i}.pdf", fast_hash=f"h{i}") for i in range(4)], scan)
        database.finish_scan(scan, total=4, new=4, updated=0, unchanged=0, invalid=0)
        fichiers = sorted(database.iter_files(), key=lambda f: f.name)
        database.store_analysis(
            fichiers[0].id,
            None,
            1,
            prompt_hash="p",
            model="m",
            analysis=_analysis(fichiers[0].name, security="C0", rgpd="low"),
        )
        for fichier in fichiers:  # les quatre sont « vérifiés », un seul est analysé
            database.set_review(fichier.id, "validated", reviewer="moi")
        avancement = views.review_progress(database)

    revus = avancement.to_review + avancement.validated + avancement.corrected
    assert avancement.analyzed == 1
    assert revus == 1, "seules les revues de fichiers analysés comptent"
    assert views.percent(revus, avancement.analyzed) == 100.0
    assert avancement.not_reviewed == 0


def test_une_analyse_perimee_ne_decide_plus_d_une_suppression(tmp_path: Path) -> None:
    """GRAVE : un fichier modifié depuis son analyse restait candidat au nettoyage.

    Les vues retenaient « la dernière analyse » sans jamais vérifier qu'elle portait
    sur le **contenu actuel**. Un fichier ré-scanné après modification repasse pourtant
    `pending` avec `content_version + 1` : la chaîne sait que son analyse est périmée,
    et les rapports s'en servaient quand même.

    Preuve d'origine — un fichier passé de 2 à 9 Mo, contenu tout autre :

        avant le re-scan : analysés=1  nettoyage=1 fichier / 2 000 000 o
        après le re-scan : analysés=1  nettoyage=1 fichier / 9 000 000 o   ← ancienne
                           classification C0 combinée à la **nouvelle** taille

    La nouvelle taille servait donc à chiffrer un gain de place sur la foi d'une classe
    de sécurité établie sur un contenu qui n'existe plus.
    """
    with Database(tmp_path / "perime.sqlite") as database:
        scan = database.start_scan("s1")
        database.upsert_files(
            [_row("vieux.txt", fast_hash="a", size=2_000_000, access="01/01/2010 08:00:00")], scan
        )
        database.finish_scan(scan, total=1, new=1, updated=0, unchanged=0, invalid=0)
        fichier = next(iter(database.iter_files()))
        database.store_analysis(
            fichier.id,
            None,
            1,
            prompt_hash="p",
            model="m",
            analysis=_analysis("vieux.txt", security="C0", rgpd="low"),
        )
        avant = views.overview(database, stale_years=1, today=TODAY)
        assert (avant.analyzed, avant.cleanup_files, avant.cleanup_bytes) == (1, 1, 2_000_000)

        rescan = database.start_scan("s2")  # le fichier a changé : empreinte et taille
        database.upsert_files(
            [_row("vieux.txt", fast_hash="AUTRE", size=9_000_000, access="01/01/2010 08:00:00")],
            rescan,
        )
        database.finish_scan(rescan, total=1, new=0, updated=1, unchanged=0, invalid=0)
        modifie = next(iter(database.iter_files()))
        assert (modifie.content_version, modifie.status) == (2, "pending")

        apres = views.overview(database, stale_years=1, today=TODAY)
        assert apres.cleanup_files == 0, "une classification périmée ne justifie aucune suppression"
        assert apres.cleanup_bytes == 0
        assert apres.analyzed == 0, "le fichier attend d'être réanalysé : il n'est pas « analysé »"
        assert views.cleanup_candidates(database, years=1, today=TODAY).rows == []
        assert database.count_analyzed_files() == 0, "la CLI doit dire la même chose"


def test_les_trois_ecrans_annoncent_les_memes_comptes_de_classification(tmp_path: Path) -> None:
    """`docia status`, le rapport et l'onglet Risque, sur une base réanalysée.

    Les trois chemins comptaient différemment : `classification_summary` et
    `counts()["analyses"]` totalisaient les **lignes** de la table `analyses`,
    historique des réanalyses compris, quand le rapport comptait les **fichiers**
    dont l'analyse fait foi. Une campagne entièrement réanalysée — le cas d'un
    changement de prompt ou de modèle — affichait donc le double dans la console
    et dans la fenêtre, et le bon chiffre dans le rapport HTML, sur la même base.
    Chaque écran justifie des suppressions : ils doivent compter la même chose.
    """
    from docia.service import campaign_status

    classes = ("C0", "C1", "C2", "C3")
    with Database(tmp_path / "reanalysee.sqlite") as database:
        scan = database.start_scan("scan.csv")
        database.upsert_files(
            [_row(f"doc{i}.pdf", fast_hash=f"h{i}") for i in range(len(classes))], scan
        )
        database.finish_scan(scan, total=4, new=4, updated=0, unchanged=0, invalid=0)
        fichiers = sorted(database.iter_files(), key=lambda f: f.name)
        # Toute la campagne est analysée deux fois, avec un **prompt différent** : c'est
        # ce que fait `docia reanalyze`. `content_version` reste 1, celle des fichiers —
        # elle ne bouge que si le fichier lui-même change, et une analyse portant sur un
        # contenu révolu n'a plus voix au chapitre (voir `views._FROM_LATEST`).
        for prompt in ("p1", "p2"):
            for fichier, classe in zip(fichiers, classes, strict=True):
                database.store_analysis(
                    fichier.id,
                    None,
                    1,
                    prompt_hash=prompt,
                    model="m",
                    analysis=_analysis(fichier.name, security=classe, rgpd="high"),
                )
        assert database.query_values("SELECT COUNT(*) FROM analyses")[0][0] == 8, "8 lignes"

        repartition = database.classification_summary()
        etat = campaign_status(database)
        apercu = views.overview(database, today=TODAY)
        matrice = views.classification_matrix(database, axis="share")

    # 1. la répartition ne compte chaque fichier qu'une fois
    assert repartition["security"] == dict.fromkeys(classes, 1)
    assert sum(repartition["security"].values()) == 4
    assert sum(repartition["rgpd"].values()) == 4

    # 2. `docia status`, `docia status --json` et l'onglet Risque lisent celle-là
    assert etat.security == repartition["security"]
    assert etat.rgpd == repartition["rgpd"]
    assert etat.analyses == 4

    # 3. le rapport HTML tombe sur les mêmes chiffres, dérivés des mêmes classes
    assert apercu.analyzed == etat.analyses
    assert apercu.sensitive_files == repartition["security"]["C2"] + repartition["security"]["C3"]
    assert apercu.rgpd_at_risk == repartition["rgpd"].get("high", 0)

    # 4. et la matrice de risque du rapport, quatrième formulation, aussi
    assert sum(ligne.analyzed for ligne in matrice) == 4
    assert sum(ligne.sensitive for ligne in matrice) == apercu.sensitive_files


def test_les_vues_le_journal_et_lexport_designent_la_meme_derniere_analyse(
    tmp_path: Path,
) -> None:
    """Trois analyses du même fichier : les trois chemins de lecture doivent choisir la même.

    Les deux départages sont couverts : `created_at` d'abord, puis `id` décroissant
    à horodatage égal (réanalyse dans la même seconde).
    """
    from docia.report import powerbi

    with Database(tmp_path / "derniere.sqlite") as database:
        scan = database.start_scan("scan.csv")
        database.upsert_files([_row("doc.pdf")], scan)
        database.finish_scan(scan, total=1, new=1, updated=0, unchanged=0, invalid=0)
        file_id = next(iter(database.iter_files())).id
        # Trois analyses du **même contenu** (`content_version=1`, celle du fichier),
        # distinguées par le prompt : trois passes de `docia reanalyze`. Faire varier
        # `content_version` à la place décrirait un fichier modifié trois fois, dont
        # les analyses antérieures ne font justement plus foi.
        for prompt, (classe, horodatage) in zip(
            ("p1", "p2", "p3"),
            (
                ("C0", "2026-01-01T08:00:00+00:00"),  # la plus ancienne
                ("C3", "2026-03-01T08:00:00+00:00"),  # la plus récente : celle qui fait foi
                ("C1", "2026-01-01T08:00:00+00:00"),  # id plus grand, mais horodatage ancien
            ),
            strict=True,
        ):
            database.store_analysis(
                file_id,
                None,
                1,
                prompt_hash=prompt,
                model="m",
                analysis=_analysis("doc.pdf", security=classe, rgpd="critical"),
            )
            database._conn.execute(  # noqa: SLF001
                "UPDATE analyses SET created_at=? WHERE id=(SELECT MAX(id) FROM analyses)",
                (horodatage,),
            )
            database._conn.commit()  # noqa: SLF001

        assert [f.security for f in views.top_sensitive(database)] == ["C3"]
        assert [r["security_classification"] for r in database.latest_analyses()] == ["C3"]
        powerbi.export_powerbi(database, tmp_path / "pbi", today=TODAY)
    lignes = (tmp_path / "pbi" / "analyses.csv").read_text(encoding="utf-8-sig").splitlines()
    entetes = lignes[0].split(";")
    assert lignes[1].split(";")[entetes.index("security_classification")] == "C3"


# ------------------------------------------- coût mémoire et coût en requêtes


def _taille_profonde(value: object, seen: set[int] | None = None) -> int:
    """Octets réellement retenus par une structure imbriquée."""
    seen = set() if seen is None else seen
    if id(value) in seen:
        return 0
    seen.add(id(value))
    total = sys.getsizeof(value)
    if isinstance(value, dict):
        total += sum(
            _taille_profonde(k, seen) + _taille_profonde(v, seen) for k, v in value.items()
        )
    elif isinstance(value, list | tuple | set):
        total += sum(_taille_profonde(item, seen) for item in value)
    return total


def test_le_repli_du_risque_ne_retient_pas_une_ligne_par_ligne_source() -> None:
    """Ce que `_fold_risk` garde dépend du nombre d'étiquettes, jamais du nombre de lignes.

    La version précédente empilait un tuple Python par ligne lue : 934 028 tuples
    retenus pour rendre 5 862 lignes, soit 444 Mo, sur une campagne réelle.
    """
    ligne = ("cle", "C1", "low", 1)

    def label_of(_row: tuple[object, ...]) -> str:
        return "unique"

    une = views._fold_risk([ligne], 1, label_of)  # noqa: SLF001
    beaucoup = views._fold_risk([ligne] * 50_000, 1, label_of)  # noqa: SLF001

    assert len(une) == len(beaucoup) == 1
    # cinquante mille fois plus de lignes, et le repli ne grossit que de la taille
    # des entiers cumulés — pas d'un objet par ligne (≈ 5 Mo dans l'ancienne version)
    croissance = _taille_profonde(beaucoup) - _taille_profonde(une)
    assert 0 <= croissance < 1024, croissance
    # le résultat, lui, est bien cumulé
    assert beaucoup["unique"][0]["C1"] == 50_000
    assert beaucoup["unique"][1]["low"] == 50_000
    assert beaucoup["unique"][2][0] == 50_000


def _base_arborescente(path: Path, *, directories: int, base: str = "\\\\srv\\part") -> Database:
    """Un fichier analysé par répertoire, tous sous le même partage."""
    database = Database(path)
    scan = database.start_scan("scan.csv")
    database.upsert_files(
        [
            _row(f"f{i}.pdf", fast_hash=f"h{i}", base=base, directory=f"{base}\\niveau1\\d{i}")
            for i in range(directories)
        ],
        scan,
    )
    database.finish_scan(
        scan, total=directories, new=directories, updated=0, unchanged=0, invalid=0
    )
    for file_row in database.iter_files():
        database.store_analysis(
            file_row.id, None, 1, prompt_hash="p", model="m", analysis=_analysis(file_row.name)
        )
    return database


def test_une_seule_base_vide_ne_fait_pas_basculer_laxe_partage(tmp_path: Path) -> None:
    """Un enregistrement à `base` vide ne doit pas regrouper tout le parc par répertoire.

    `share_label` ne lit `unc_directory` que si `base` est vide ; le regroupement
    SQL en tenait compte, mais **globalement** : un seul enregistrement fautif et
    l'axe entier passait d'un groupe par partage à un groupe par répertoire —
    521 718 groupes au lieu de 6 sur une campagne de 934 028 fichiers, soit ×24
    en mémoire. Le repli est désormais borné aux seules lignes à `base` vide.
    """
    with _base_arborescente(tmp_path / "partage.sqlite", directories=60) as database:
        sain = views.classification_matrix(database, axis="share")
        groupes_sains = len(views._axis_volumes(database, views._axis_group(database, "share")))  # noqa: SLF001

        # un unique enregistrement dont la colonne `base` ne nomme rien
        database._conn.execute(  # noqa: SLF001
            "UPDATE files SET base='\\' WHERE id=(SELECT MIN(id) FROM files)"
        )
        database._conn.commit()  # noqa: SLF001
        abime = views.classification_matrix(database, axis="share")
        groupes_abimes = len(views._axis_volumes(database, views._axis_group(database, "share")))  # noqa: SLF001

    assert groupes_sains == 1  # un seul partage, un seul groupe SQL
    # l'enregistrement fautif fait son propre groupe ; les 59 autres restent groupés
    # ensemble. Avant, l'axe entier retombait sur `unc_directory` : 60 groupes.
    assert groupes_abimes == 2
    # et le résultat rendu, lui, ne change pas d'un iota
    assert {r.label for r in sain} == {"\\\\srv\\part"}
    assert sain == abime


def test_duplicates_ne_lance_pas_une_requete_par_famille(tmp_path: Path) -> None:
    """Le détail des familles est lu par paquets, pas une famille à la fois.

    Sans limite, c'était un `N+1` : 150 001 requêtes sur une campagne réelle —
    et `report.powerbi` l'appelait ainsi, sans limite, à chaque export.
    """
    familles = 500
    with Database(tmp_path / "doublons.sqlite") as database:
        scan = database.start_scan("scan.csv")
        database.upsert_files(
            [
                _row(
                    f"f{i}.pdf", fast_hash=f"h{i // 2}", size=1000, directory=f"\\\\srv\\part\\d{i}"
                )
                for i in range(familles * 2)
            ],
            scan,
        )
        database.finish_scan(
            scan, total=familles * 2, new=familles * 2, updated=0, unchanged=0, invalid=0
        )

        appels: list[str] = []
        vraie = database.query_values

        def espion(sql: str, params: tuple[object, ...] = ()) -> list[tuple[object, ...]]:
            appels.append(sql)
            return vraie(sql, params)

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(database, "query_values", espion)
            rapport = views.duplicates(database)
            en_flux = list(views.iter_duplicate_families(database))

    assert rapport.total_families == familles
    assert rapport.total_copies == familles * 2
    assert len(rapport.families) == familles
    assert all(len(f.paths) == 2 and len(f.file_ids) == 2 for f in rapport.families)
    # une requête d'agrégation + un paquet toutes les `MEMBER_BATCH` familles, deux fois
    attendu = 2 * (1 + -(-familles // views.MEMBER_BATCH))
    assert len(appels) == attendu, appels[:5]
    assert attendu < familles  # et surtout : pas une requête par famille
    # le flux rend exactement les mêmes familles, dans le même ordre
    assert [f.family_id for f in en_flux] == [f.family_id for f in rapport.families]
    assert [f.paths for f in en_flux] == [f.paths for f in rapport.families]
