"""Compteurs et répartitions de la campagne (`docia status`, onglet Risque)."""

from __future__ import annotations

from docia.db.core import _DatabaseCore
from docia.db.sql import (
    _IS_LATEST,
)
from docia.models import (
    BlockStatus,
    FileStatus,
)


class StatsOps(_DatabaseCore):
    # ------------------------------------------------------------------ stats
    def counts(self) -> dict[str, int]:
        """Compteurs de la campagne — `analyses` compte des **fichiers**, pas des lignes.

        `analyses` est le nombre de fichiers dont une analyse fait foi, c'est-à-dire
        exactement ce que le rapport appelle « analysés » (`views.overview.analyzed`)
        et le total de `classification_summary`. La table, elle, garde l'historique :
        un fichier réanalysé (nouvelle version de contenu, nouveau prompt, nouveau
        modèle) y laisse une ligne de plus. Compter ces lignes affichait jusqu'au
        **double** du nombre de fichiers analysés dans `docia status`, dans
        `docia status --json` et dans l'onglet Risque, pendant que le rapport HTML
        annonçait le bon chiffre sur la même base. Pour un outil dont la sortie
        justifie des suppressions, les trois écrans doivent compter la même chose.
        """
        counts = {s.value: 0 for s in FileStatus}
        for r in self._conn.execute("SELECT status, COUNT(*) AS n FROM files GROUP BY status"):
            counts[r["status"]] = int(r["n"])
        counts["files"] = sum(counts[s.value] for s in FileStatus)
        counts["analyses"] = self.count_analyzed_files()
        for status in BlockStatus:
            counts[f"blocks_{status.value}"] = int(
                self._conn.execute(
                    "SELECT COUNT(*) FROM blocks WHERE status=?", (status.value,)
                ).fetchone()[0]
            )
        return counts

    def count_analyzed_files(self) -> int:
        """Fichiers analysés **pour leur contenu actuel** (miroir de `views._analyzed_files`).

        `COUNT(DISTINCT file_id) FROM analyses` comptait aussi les fichiers modifiés
        depuis leur analyse. Ceux-là sont repassés `pending` et attendent d'être
        réanalysés : les compter « analysés » faisait dépasser le total de la campagne
        (`analysés + à analyser + exclus + erreurs` &gt; nombre de fichiers) et laissait
        une classification périmée décider d'une suppression. Voir `views._FROM_LATEST`.
        """
        return int(
            self._conn.execute(
                "SELECT COUNT(DISTINCT a.file_id) FROM analyses a"
                " JOIN files f ON f.id = a.file_id AND a.content_version = f.content_version"
            ).fetchone()[0]
        )

    _SUMMARY_COLUMNS = (
        ("security_classification", "security"),
        ("rgpd_risk_level", "rgpd"),
        ("finance_document_type", "finance"),
        ("legal_contract_type", "legal"),
    )
    """Colonnes de `analyses` réparties par `classification_summary` : (colonne, clé rendue)."""

    def classification_summary(self) -> dict[str, dict[str, int]]:
        """Répartition des classes sur la **dernière analyse** de chaque fichier.

        Même règle que les vues du rapport (`views._IS_LATEST`) : l'historique des
        réanalyses ne compte pas deux fois. Chaque répartition totalise donc
        `counts()["analyses"]`.

        Les quatre répartitions sortent d'**une seule** requête croisée, repliée
        ensuite en Python. Retenir la dernière analyse coûte une sous-requête
        corrélée par ligne : une requête par colonne, c'était quatre fois ce
        balayage (mesuré sur 80 000 analyses : 246 ms contre 116 ms ici, coût
        linéaire). Le croisement ne pèse rien en mémoire : les quatre vocabulaires
        sont fermés (classes de sécurité, niveaux RGPD, types de document), donc
        le nombre de groupes est borné quelle que soit la taille de la campagne.
        """
        out: dict[str, dict[str, int]] = {key: {} for _, key in self._SUMMARY_COLUMNS}
        columns = ", ".join(column for column, _ in self._SUMMARY_COLUMNS)
        for row in self._conn.execute(
            f"SELECT {columns}, COUNT(*) FROM analyses a"  # noqa: S608 — colonnes internes
            " JOIN files f ON f.id = a.file_id"
            f" WHERE {_IS_LATEST} GROUP BY 1, 2, 3, 4"
        ):
            number = int(row[len(self._SUMMARY_COLUMNS)])
            for position, (_, key) in enumerate(self._SUMMARY_COLUMNS):
                bucket = out[key]
                bucket[row[position]] = bucket.get(row[position], 0) + number
        return out
