"""Tables `analyses` et `segment_analyses` : écriture, agrégation, copie, dernière analyse qui fait foi."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator, Sequence

from docia.db.core import _DatabaseCore, _now
from docia.db.sql import (
    _DISPLAY_ORDER_SQL,
    _LATEST_FROM,
    _LATEST_JOINS,
    _LATEST_SELECT,
    _REVIEWS_JOIN,
    _latest_filters,
    _needs_analysis,
)
from docia.models import (
    FileAnalysis,
    FileStatus,
)


class AnalysesOps(_DatabaseCore):
    # ------------------------------------------------------------------ analyses
    def store_analysis(
        self,
        file_id: int,
        block_id: int | None,
        content_version: int,
        *,
        prompt_hash: str,
        model: str,
        analysis: FileAnalysis,
        segments: int = 1,
    ) -> None:
        """Insère (ou remplace) l'analyse d'un fichier et le passe `done`, en une transaction."""
        with self.transaction() as conn:
            conn.execute(
                """INSERT INTO analyses(file_id, block_id, content_version, prompt_hash, model, resume,
                   security_classification, security_confidence, security_justification,
                   rgpd_risk_level, rgpd_data_types, rgpd_confidence,
                   finance_document_type, finance_amounts, finance_confidence,
                   legal_contract_type, legal_parties, legal_confidence, raw_json, created_at,
                   segments, retention_required, retention_years, retention_basis,
                   retention_justification, retention_confidence)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(file_id, content_version, prompt_hash, model) DO UPDATE SET
                   block_id=excluded.block_id, resume=excluded.resume,
                   security_classification=excluded.security_classification, security_confidence=excluded.security_confidence,
                   security_justification=excluded.security_justification, rgpd_risk_level=excluded.rgpd_risk_level,
                   rgpd_data_types=excluded.rgpd_data_types, rgpd_confidence=excluded.rgpd_confidence,
                   finance_document_type=excluded.finance_document_type, finance_amounts=excluded.finance_amounts,
                   finance_confidence=excluded.finance_confidence, legal_contract_type=excluded.legal_contract_type,
                   legal_parties=excluded.legal_parties, legal_confidence=excluded.legal_confidence,
                   raw_json=excluded.raw_json, created_at=excluded.created_at,
                   segments=excluded.segments, retention_required=excluded.retention_required,
                   retention_years=excluded.retention_years, retention_basis=excluded.retention_basis,
                   retention_justification=excluded.retention_justification,
                   retention_confidence=excluded.retention_confidence""",
                (
                    file_id,
                    block_id,
                    content_version,
                    prompt_hash,
                    model,
                    analysis.resume,
                    analysis.security.label,
                    analysis.security.confidence,
                    str(analysis.security.details.get("justification", "")),
                    analysis.rgpd.label,
                    json.dumps(analysis.rgpd.details.get("data_types", []), ensure_ascii=False),
                    analysis.rgpd.confidence,
                    analysis.finance.label,
                    json.dumps(analysis.finance.details.get("amounts", []), ensure_ascii=False),
                    analysis.finance.confidence,
                    analysis.legal.label,
                    json.dumps(analysis.legal.details.get("parties", []), ensure_ascii=False),
                    analysis.legal.confidence,
                    json.dumps(analysis.raw, ensure_ascii=False),
                    _now(),
                    segments,
                    int(bool(analysis.retention.details.get("required", False))),
                    int(str(analysis.retention.details.get("years", 0)) or 0),
                    analysis.retention.label,
                    str(analysis.retention.details.get("justification", "")),
                    analysis.retention.confidence,
                ),
            )
            conn.execute(
                "UPDATE files SET status='done', exclusion_reason=NULL, updated_at=? WHERE id=?",
                (_now(), file_id),
            )

    def store_segment_analysis(
        self,
        file_id: int,
        block_id: int | None,
        content_version: int,
        *,
        prompt_hash: str,
        model: str,
        segment_index: int,
        segment_count: int,
        raw: dict[str, object],
    ) -> None:
        """Analyse d'un segment d'un fichier découpé (le fichier reste `queued`
        jusqu'à l'agrégation des K segments).

        Les segments d'un **autre** découpage du même contenu (K différent) sont
        supprimés au passage : un run précédent, plus fin ou plus grossier, laissait
        sinon des lignes périmées que le pipeline comptait comme faites — le fichier
        était déclaré `done` avec 20 % de son contenu analysé.
        """
        with self.transaction() as conn:
            conn.execute(
                """DELETE FROM segment_analyses WHERE file_id=? AND content_version=?
                   AND prompt_hash=? AND model=? AND segment_count<>?""",
                (file_id, content_version, prompt_hash, model, segment_count),
            )
            conn.execute(
                """INSERT INTO segment_analyses(file_id, block_id, content_version, prompt_hash,
                   model, segment_index, segment_count, raw_json, created_at)
                   VALUES(?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(file_id, content_version, prompt_hash, model, segment_index)
                   DO UPDATE SET block_id=excluded.block_id, raw_json=excluded.raw_json,
                   segment_count=excluded.segment_count, created_at=excluded.created_at""",
                (
                    file_id,
                    block_id,
                    content_version,
                    prompt_hash,
                    model,
                    segment_index,
                    segment_count,
                    json.dumps(raw, ensure_ascii=False),
                    _now(),
                ),
            )

    def segment_analyses(
        self,
        file_id: int,
        content_version: int,
        *,
        prompt_hash: str,
        model: str,
        segment_count: int | None = None,
    ) -> list[tuple[int, int, dict[str, object]]]:
        """Segments déjà analysés : (index, count, JSON brut), triés par index.

        `segment_count` restreint au découpage demandé : sans lui, la méthode rend
        **toutes** les lignes du couple (fichier, version, prompt, modèle), quel que
        soit le découpage sous lequel elles ont été écrites.
        """
        sql = """SELECT segment_index, segment_count, raw_json FROM segment_analyses
                 WHERE file_id=? AND content_version=? AND prompt_hash=? AND model=?"""
        params: tuple[object, ...] = (file_id, content_version, prompt_hash, model)
        if segment_count is not None:
            sql += " AND segment_count=?"
            params = (*params, segment_count)
        rows = self._conn.execute(sql + " ORDER BY segment_index", params).fetchall()
        out: list[tuple[int, int, dict[str, object]]] = []
        for r in rows:
            raw = json.loads(r["raw_json"])
            out.append(
                (
                    int(r["segment_index"]),
                    int(r["segment_count"]),
                    raw if isinstance(raw, dict) else {},
                )
            )
        return out

    def copy_analysis(
        self,
        src_file_id: int,
        dst_file_id: int,
        dst_content_version: int,
        *,
        prompt_hash: str,
        model: str,
    ) -> bool:
        """Copie l'analyse courante de `src` vers `dst` (contenu identique — doublon
        DocFuse) et passe `dst` en `done`. False si `src` n'a pas d'analyse."""
        src = self._conn.execute(
            """SELECT * FROM analyses WHERE file_id=? AND prompt_hash=? AND model=?
               AND content_version=(SELECT content_version FROM files WHERE id=?)
               ORDER BY id DESC LIMIT 1""",
            (src_file_id, prompt_hash, model, src_file_id),
        ).fetchone()
        if src is None:
            return False
        # sqlite3.Row : itérer la ligne donne les VALEURS, pas les clés → `.keys()` obligatoire.
        skip = ("id", "file_id", "content_version", "created_at")
        cols = [k for k in src.keys() if k not in skip]  # noqa: SIM118
        with self.transaction() as conn:
            conn.execute(
                "DELETE FROM analyses WHERE file_id=? AND content_version=? AND prompt_hash=? AND model=?",
                (dst_file_id, dst_content_version, prompt_hash, model),
            )
            conn.execute(
                f"INSERT INTO analyses(file_id, content_version, created_at, {', '.join(cols)}) "  # noqa: S608
                f"VALUES(?, ?, ?, {', '.join('?' for _ in cols)})",
                (dst_file_id, dst_content_version, _now(), *[src[c] for c in cols]),
            )
            conn.execute(
                "UPDATE files SET status='done', exclusion_reason=NULL, updated_at=? WHERE id=?",
                (_now(), dst_file_id),
            )
        return True

    def delete_analyses(self, file_ids: Sequence[int], *, prompt_hash: str, model: str) -> int:
        """Supprime analyses et segments de ces fichiers pour ce prompt et ce modèle.

        Une seule transaction (par paquets de 500 identifiants, limite SQLite sur
        le nombre de paramètres). Rend le nombre de lignes `analyses` supprimées.
        """
        if not file_ids:
            return 0
        deleted = 0
        with self.transaction() as conn:
            for start in range(0, len(file_ids), 500):
                chunk = tuple(file_ids[start : start + 500])
                marks = ",".join("?" for _ in chunk)
                params: tuple[object, ...] = (*chunk, prompt_hash, model)
                conn.execute(
                    f"DELETE FROM segment_analyses WHERE file_id IN ({marks})"  # noqa: S608
                    " AND prompt_hash=? AND model=?",
                    params,
                )
                cur = conn.execute(
                    f"DELETE FROM analyses WHERE file_id IN ({marks})"  # noqa: S608
                    " AND prompt_hash=? AND model=?",
                    params,
                )
                deleted += int(cur.rowcount)
        return deleted

    def reset_for_reanalysis(self, file_ids: Sequence[int], *, prompt_hash: str, model: str) -> int:
        """Remet des fichiers `pending` **et** supprime leurs analyses, en une transaction.

        Rend le nombre de lignes `analyses` supprimées.

        C'est l'opération que `service.reanalyze` faisait en deux écritures, donc en
        deux transactions : une coupure entre les deux laissait la campagne dans un
        état intermédiaire. Dans un sens, des fichiers `done` sans analyse —
        `done=60, analyses=0`, une campagne qui s'annonce à 100 % et que plus aucune
        commande ordinaire ne reprend. Dans l'autre, des fichiers `pending` dont les
        analyses subsistent — visible et réparable en rejouant la commande, mais une
        fenêtre quand même. Ici, il n'y a plus de fenêtre : soit les deux écritures
        passent, soit la base reste exactement dans son état d'avant.

        `transaction()` n'est pas réentrante — un `BEGIN` imbriqué lève — donc les
        deux méthodes existantes ne peuvent pas se composer : leurs corps sont
        réunis ici, et `delete_analyses` comme `set_files_status` restent utiles
        seules (suppression sans remise à `pending`, changement de statut sans
        suppression).
        """
        if not file_ids:
            return 0
        deleted = 0
        now = _now()
        with self.transaction() as conn:
            conn.executemany(
                "UPDATE files SET status=?, exclusion_reason=?, updated_at=? WHERE id=?",
                [(str(FileStatus.PENDING), None, now, fid) for fid in file_ids],
            )
            for start in range(0, len(file_ids), 500):
                chunk = tuple(file_ids[start : start + 500])
                marks = ",".join("?" for _ in chunk)
                params: tuple[object, ...] = (*chunk, prompt_hash, model)
                conn.execute(
                    f"DELETE FROM segment_analyses WHERE file_id IN ({marks})"  # noqa: S608
                    " AND prompt_hash=? AND model=?",
                    params,
                )
                cur = conn.execute(
                    f"DELETE FROM analyses WHERE file_id IN ({marks})"  # noqa: S608
                    " AND prompt_hash=? AND model=?",
                    params,
                )
                deleted += int(cur.rowcount)
        return deleted

    def latest_analyses(
        self,
        *,
        file_id: int | None = None,
        security: str | None = None,
        rgpd: str | None = None,
        review: str | None = None,
        search: str | None = None,
        limit: int | None = None,
        display_order: bool = False,
    ) -> Iterator[sqlite3.Row]:
        """Dernière analyse de chaque fichier (jointe au fichier), pour l'export.

        Sans argument : toute la campagne, triée par chemin — c'est la forme
        qu'attendent les exports, qui la consomment **en curseur**.

        `file_id` : une seule fiche. L'écran Résultats relisait toute la campagne à
        chaque clic sur une ligne (plusieurs secondes sur 200 000 fichiers).

        Filtres et `limit` : ceux de l'écran Résultats, descendus en SQL. Il relisait
        les 934 028 lignes d'une campagne pour en afficher 1 000 (9,3 s, 950 Mo) —
        et recommençait à chaque changement de filtre comme après chaque validation.
        Sémantique reprise telle quelle de l'écran :

        * `security` / `rgpd` : égalité stricte sur la dernière analyse, `''`
          désignant les fichiers sans analyse ;
        * `review` : égalité stricte sur le statut de vérification, `''` = non vérifié
          (aucune ligne dans `reviews`) ;
        * `search` : sous-chaîne, insensible à la casse, cherchée dans
          `chemin + résumé + propriétaire` ; `%`, `_` et `\\` y sont littéraux
          (l'écran comparait des sous-chaînes, pas des motifs : « 100 % » ne doit
          pas ramener la campagne entière).

        `display_order` trie comme l'écran (analysés d'abord, du plus sensible au
        moins sensible, puis `error`, `done`, le reste ; à égalité, nom en
        minuscules). Compromis assumé : `LOWER()` de SQLite ne replie que l'ASCII —
        « Étude » et « étude » ne se rangent pas ensemble comme le ferait `str.lower`
        de Python. Le tri SQL n'est donc qu'**approché** ; l'appelant re-trie
        exactement, en Python, les ≤ `limit` lignes rendues (`gui/tab_results.py`).
        La même limite vaut pour `search` : `LIKE` replie la casse ASCII, pas les
        lettres accentuées — « ETUDE » retrouve « etude », « étude » ne retrouve pas
        « Étude ».
        """
        if file_id is not None:
            return iter(
                self._conn.execute(
                    f"{_LATEST_SELECT}{_LATEST_FROM} WHERE f.id = ? ORDER BY f.path", (file_id,)
                )
            )
        where, params = _latest_filters(security, rgpd, review, search)
        order = _DISPLAY_ORDER_SQL if display_order else "f.path"
        if limit is None:
            return iter(
                self._conn.execute(
                    f"{_LATEST_SELECT}{_LATEST_FROM}{where} ORDER BY {order}", tuple(params)
                )
            )
        # Deux étages : le premier balaie la campagne mais ne trie que (clés, id) et
        # s'arrête à `limit` lignes ; le second ne rapporte les 38 colonnes que pour
        # celles-là. En un seul étage, SQLite trierait 934 028 lignes complètes.
        return iter(
            self._conn.execute(
                f"{_LATEST_SELECT}"
                f" FROM (SELECT f.id AS sel_id{_LATEST_FROM}{where}"
                f"       ORDER BY {order} LIMIT ?) sel"
                f" JOIN files f ON f.id = sel.sel_id{_LATEST_JOINS}"
                f" ORDER BY {order}",
                (*params, limit),
            )
        )

    def count_latest_analyses(
        self,
        *,
        security: str | None = None,
        rgpd: str | None = None,
        review: str | None = None,
        search: str | None = None,
    ) -> int:
        """Nombre de fichiers retenus par les filtres de `latest_analyses`.

        C'est le total affiché par l'écran Résultats à côté des 1 000 lignes rendues.
        Sans aucun filtre, il se lit directement dans `files` ; un filtre qui ne porte
        que sur la vérification humaine évite la jointure sur la dernière analyse —
        c'est elle qui coûte (une sous-requête corrélée par fichier).
        """
        where, params = _latest_filters(security, rgpd, review, search)
        if not where:
            return int(self._conn.execute("SELECT COUNT(*) FROM files").fetchone()[0])
        joins = _LATEST_JOINS if _needs_analysis(security, rgpd, search) else _REVIEWS_JOIN
        row = self._conn.execute(
            f"SELECT COUNT(*) FROM files f{joins}{where}", tuple(params)
        ).fetchone()
        return int(row[0])
