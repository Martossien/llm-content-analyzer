"""Calculs de l'écran Statistiques : `compute_*` doit être **pur** (aucun Tk), pour
tourner dans un thread de fond — c'est ce qui empêche la fenêtre de geler sur une
grande campagne. Ces tests s'exécutent sans écran : s'ils devaient importer Tk, ils
échoueraient, ce qui est exactement la garantie recherchée.
"""

from __future__ import annotations

import pytest

from docia import views
from docia.db import Database
from docia.gui.tab_stats import (
    SectionData,
    compute_hygiene,
    compute_retention,
    compute_review,
    compute_risk,
)
from tests.test_views import db  # noqa: F401 — fixture réutilisée (campagne de démonstration)


def test_hygiene_doublons(db: Database) -> None:  # noqa: F811
    data = compute_hygiene(views, db, "Doublons (espace récupérable)", 5)
    assert set(data.tiles) == {"dup", "stale", "cleanup"}
    assert data.cols == ["copies", "taille", "récupérable", "chemins"]
    assert data.rows, "la campagne de démonstration contient des doublons"
    assert "familles" in data.summary
    assert data.chart_unit == "Mo"


def test_hygiene_anciennete_et_nettoyage(db: Database) -> None:  # noqa: F811
    stale = compute_hygiene(db=db, views=views, view_name="Ancienneté (non accédés)", years=5)
    assert stale.cols[0] == "depuis (ans)"
    assert len(stale.rows) == len(views.stale_files(db))
    cleanup = compute_hygiene(views, db, "Candidats au nettoyage", 5)
    assert "libérables" in cleanup.summary


def test_hygiene_repartitions(db: Database) -> None:  # noqa: F811
    for view_name, first_col in (
        ("Extensions", "extension"),
        ("Propriétaires", "propriétaire"),
        ("Partages", "partage"),
        ("Tailles", "taille"),
    ):
        data = compute_hygiene(views, db, view_name, 5)
        assert data.cols[0] == first_col
        assert data.rows
    repertoires = compute_hygiene(views, db, "Répertoires", 5)
    assert repertoires.cols[0] == "répertoire"


def test_risque_fichiers_sensibles(db: Database) -> None:  # noqa: F811
    data = compute_risk(views, db, "Fichiers sensibles", 5)
    assert data.tiles["c3"] == "1"
    assert data.rows
    assert data.tags
    assert data.tags[0] == "C3"
    assert data.rows[0][0] == "C3"


def test_risque_matrices(db: Database) -> None:  # noqa: F811
    for view_name, label in (
        ("Classification × partage", "partage"),
        ("Classification × propriétaire", "propriétaire"),
        ("Classification × répertoire", "répertoire"),
    ):
        data = compute_risk(views, db, view_name, 5)
        assert data.cols[0] == label
        assert data.cols[-1] == "RGPD élevé+"
        assert data.rows


def test_conservation(db: Database) -> None:  # noqa: F811
    data = compute_retention(views, db, "Plan de conservation", 5)
    assert "à conserver" in data.summary
    assert data.cols[:3] == ["fin", "ans", "fondement"]
    assert len(data.tags or []) == len(data.rows)


def test_verification(db: Database) -> None:  # noqa: F811
    ecarts = compute_review(views, db, "Écarts LLM / humain", 5)
    assert ecarts.tiles["pct"].endswith("%")
    assert "analysés" in ecarts.summary
    runs = compute_review(views, db, "Runs", 5)
    assert runs.summary.startswith("historique des runs")


def test_section_vide_par_defaut() -> None:
    """Une `SectionData` sans données s'affiche sans lever (campagne absente)."""
    data = SectionData(summary="aucune campagne ouverte")
    assert data.tiles == {}
    assert data.rows == []
    assert data.tags is None


@pytest.mark.parametrize(
    "compute", [compute_hygiene, compute_risk, compute_retention, compute_review]
)
def test_aucun_appel_tk(compute: object) -> None:
    """Les fonctions de calcul ne doivent dépendre d'aucun widget : elles ne reçoivent
    que `views`, la base, le nom de la vue et le seuil d'ancienneté."""
    from inspect import signature

    assert list(signature(compute).parameters)[:2] == ["views", "db"]  # type: ignore[arg-type]
