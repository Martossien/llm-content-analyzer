"""Collecte des vues nécessaires à un rapport, en une passe.

`collect()` est appelé une fois ; HTML, Markdown et Excel consomment le même
`ReportData` : **les chiffres** des trois rendus ne peuvent donc pas diverger.
Leur *forme*, elle, diverge délibérément — le HTML, destiné à la direction,
numérote une section « Répartition RGPD par partage » que le Markdown, fait pour
un mail ou un ticket, n'a pas : leurs numéros de sous-sections décalent d'une
unité à partir de 3.3. Les deux publics ne sont pas les mêmes ; les fusionner
pour l'esthétique coûterait plus que cela ne rapporte.

**Rien n'est coupé en silence.** Deux bornes distinctes, parce que deux natures
de tableaux :

- `top` borne les **classements** (doublons, top sensible, extensions,
  propriétaires, répertoires, écarts de revue). Un classement est borné par
  définition ; le rendu doit dire sur combien il porte — c'est à quoi sert
  `ReportData.totals` / `ReportData.hidden`.
- `actions` borne les deux tableaux dont chaque ligne est **une décision de
  suppression** : le plan de conservation et les candidats au nettoyage. Le
  classeur Excel les prend entiers (`actions=None`), comme l'export Power BI et
  le CSV : c'est vers eux que renvoient les rapports, il faut donc qu'ils portent
  la totalité de ce que le total annonce.

Les vues concernées construisent de toute façon la liste entière avant de la
couper : lever la borne ne coûte ni une requête ni un octet de mémoire de pointe.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from docia import views
from docia.db import Database

TOP_ROWS = 50
"""Nombre de lignes détaillées gardées dans les tableaux « top »."""

_XLSX = "le classeur Excel (« export --format xlsx »)"
_DU_XLSX = "du classeur Excel (« export --format xlsx »)"
_PBI = "l'export Power BI (« export --format powerbi »)"

FULL_LISTING: dict[str, str] = {
    "duplicates": f"{_PBI}, fichier duplicates.csv",
    "sensitive": f"l'onglet « Fichiers » {_DU_XLSX} et {_PBI}, fichier analyses.csv",
    "retention": f"l'onglet « Conservation » {_DU_XLSX} et {_PBI}",
    "cleanup": f"l'onglet « Nettoyage » {_DU_XLSX} et {_PBI}",
    "discrepancies": f"{_PBI}, fichier reviews.csv",
}
"""Où est la liste complète d'un tableau borné — **une seule fois**, pour le HTML et le Markdown.

Une note de troncature qui envoie au mauvais endroit ne vaut pas mieux qu'une
troncature silencieuse : la destination est nommée table par table."""

DEFAULT_LISTING = (
    f"{_XLSX} et {_PBI}, à regrouper depuis les tables complètes files.csv et analyses.csv"
)
"""Destination des répartitions (extensions, propriétaires, répertoires) : aucun onglet
ne les porte en entier, elles se recalculent depuis les tables de faits."""


def listing_of(name: str) -> str:
    """Où trouver la totalité du tableau `name`."""
    return FULL_LISTING.get(name, DEFAULT_LISTING)


@dataclass(frozen=True)
class ReportData:
    """Toutes les vues d'un rapport, déjà triées et bornées."""

    overview: views.Overview
    status: views.StatusSummary
    duplicates: views.DuplicateReport
    stale: list[views.StaleBucket]
    extensions: list[views.GroupStat]
    owners: list[views.GroupStat]
    shares: list[views.GroupStat]
    directories: list[views.AxisRow]
    sizes: list[views.GroupStat]
    tiny: views.TinyReport
    by_share: list[views.AxisRow]
    by_owner: list[views.AxisRow]
    sensitive: list[views.SensitiveFile]
    retention: views.RetentionPlan
    cleanup: views.CleanupReport
    reviews: views.ReviewProgress
    runs: list[views.RunStat] = field(default_factory=list)
    totals: dict[str, int] = field(default_factory=dict)
    """Nombre réel de lignes **avant** la coupe, par nom de champ."""

    def hidden(self, name: str, shown: int) -> int:
        """Lignes de `name` que le rendu ne montre pas (0 s'il les montre toutes)."""
        return max(self.totals.get(name, shown) - shown, 0)


def collect(
    db: Database,
    *,
    today: date | None = None,
    top: int | None = TOP_ROWS,
    actions: int | None = TOP_ROWS,
    cleanup_years: int = 5,
) -> ReportData:
    """Exécute toutes les vues du rapport pour la base `db`.

    `top` borne les classements, `actions` le plan de conservation et les
    candidats au nettoyage ; `None` lève la borne (voir le docstring du module).
    """
    half = None if top is None else top // 2
    extensions = views.by_extension(db)
    owners = views.by_owner(db)
    directories = views.by_directory(db, depth=2)
    by_owner = views.classification_matrix(db, axis="owner")
    duplicates = views.duplicates(db, limit=top)
    sensitive = views.top_sensitive(db, limit=top)
    retention = views.retention_plan(db, today=today, limit=actions)
    cleanup = views.cleanup_candidates(db, years=cleanup_years, today=today, limit=actions)
    reviews = views.review_progress(db, limit=top)
    return ReportData(
        overview=views.overview(db, today=today, stale_years=cleanup_years),
        status=views.status_summary(db),
        duplicates=duplicates,
        stale=views.stale_files(db, today=today),
        extensions=extensions if half is None else extensions[:half],
        owners=owners if half is None else owners[:half],
        shares=views.by_share(db),
        directories=directories if half is None else directories[:half],
        sizes=views.size_buckets(db),
        tiny=views.empty_or_tiny(db),
        by_share=views.classification_matrix(db, axis="share"),
        by_owner=by_owner if half is None else by_owner[:half],
        sensitive=sensitive,
        retention=retention,
        cleanup=cleanup,
        reviews=reviews,
        runs=views.runs_summary(db),
        totals={
            "extensions": len(extensions),
            "owners": len(owners),
            "directories": len(directories),
            "by_owner": len(by_owner),
            "duplicates": duplicates.total_families,
            "sensitive": views.count_sensitive(db),
            "retention": retention.total_files,
            "cleanup": cleanup.total_files,
            "discrepancies": reviews.total_discrepancies,
        },
    )
