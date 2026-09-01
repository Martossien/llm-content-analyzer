"""Tables `scans` et `files` : import par lots, sélection des fichiers à analyser, statuts, plan."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable, Iterator, Sequence

from docia.db.core import _TOUCH_FLUSH, APPLY_PLAN_BATCH, ITER_FILES_BATCH, _DatabaseCore, _now
from docia.db.sql import (
    _PENDING_WHERE,
    _PLAN_EXCLUDE_SQL,
    _PLAN_KEEP_SQL,
    _TOUCH_SQL,
    date_key,
)
from docia.models import (
    FileRow,
    FileStatus,
    SmbeagleRow,
    path_key,
)


class FilesOps(_DatabaseCore):
    # ------------------------------------------------------------------ scans
    def start_scan(self, csv_path: str, *, kind: str = "import") -> int:
        cur = self._conn.execute(
            "INSERT INTO scans(csv_path, imported_at, kind) VALUES(?, ?, ?)",
            (csv_path, _now(), kind),
        )
        self._conn.commit()
        return int(cur.lastrowid or 0)

    def annotate_scan(
        self,
        scan_id: int,
        *,
        manifest_json: str,
        scanner_elapsed_s: float,
        skipped: Sequence[str] = (),
        cancelled: bool = False,
        exit_code: int = 0,
        expected_files: int = -1,
    ) -> None:
        """Attache au scan importé le manifeste **et le périmètre réellement couvert**.

        `complete` est déduit ici, une seule fois, des trois faits qui amputent un
        périmètre : une cible écartée, un arrêt demandé, ou un CSV plus court que
        le compte annoncé. Les rapports et l'interface lisent cette colonne plutôt
        que de refaire le raisonnement chacun de leur côté.

        Les valeurs par défaut décrivent un scan complet d'un scanner antérieur au
        code 4 (aucun `skipped`, aucun compte annoncé) : appelée comme avant, la
        méthode ne marque donc jamais une campagne incomplète à tort.
        """
        rows = self.query_values("SELECT rows_total FROM scans WHERE id=?", (scan_id,))
        rows_total = int(rows[0][0]) if rows else 0
        truncated = expected_files >= 0 and expected_files > rows_total
        complete = not skipped and not cancelled and not truncated
        self._conn.execute(
            "UPDATE scans SET kind='scan', manifest_json=?, scanner_elapsed_s=?,"
            " complete=?, skipped_json=?, cancelled=?, exit_code=?, expected_files=?"
            " WHERE id=?",
            (
                manifest_json,
                scanner_elapsed_s,
                int(complete),
                json.dumps(list(skipped), ensure_ascii=False) if skipped else "",
                int(cancelled),
                exit_code,
                expected_files,
                scan_id,
            ),
        )
        self._conn.commit()

    def last_scan(self) -> sqlite3.Row | None:
        row = self._conn.execute("SELECT * FROM scans ORDER BY id DESC LIMIT 1").fetchone()
        return row if isinstance(row, sqlite3.Row) else None

    def incomplete_scans(self) -> list[sqlite3.Row]:
        """Scans de la campagne dont le périmètre n'est **pas** entier, du plus ancien au plus récent.

        C'est la réponse durable à « cette campagne porte-t-elle sur tout ce qu'on
        a demandé ? » : elle ne dépend ni du manifeste, ni du CSV, ni de la session
        pendant laquelle le scan a tourné.
        """
        return list(self._conn.execute("SELECT * FROM scans WHERE complete=0 ORDER BY id"))

    def finish_scan(
        self, scan_id: int, *, total: int, new: int, updated: int, unchanged: int, invalid: int
    ) -> None:
        """Clôt un scan et rafraîchit les statistiques d'index.

        `ANALYZE` (moins d'une seconde pour 200 000 fichiers) donne au planificateur
        les cardinalités réelles : sans elles, plusieurs vues statistiques
        choisissent un index moins bon que le balayage couvrant attendu.
        """
        self._conn.execute(
            "UPDATE scans SET rows_total=?, rows_new=?, rows_updated=?, rows_unchanged=?, rows_invalid=? WHERE id=?",
            (total, new, updated, unchanged, invalid, scan_id),
        )
        self._conn.execute("ANALYZE")
        self._conn.commit()

    def upsert_files(self, rows: Iterable[SmbeagleRow], scan_id: int) -> tuple[int, int, int]:
        """Insère ou met à jour des lignes SMBeagle.

        Un fichier connu dont `fast_hash`, `size` ou `last_write_time` change
        prend `content_version + 1` et repasse `pending` (sauf s'il est
        `excluded`, l'exclusion étant une règle, pas un état de contenu).

        `access_key` / `write_key` (schéma v6) sont recalculées à chaque écriture :
        elles doivent rester le reflet exact de `COALESCE(NULLIF(access_time_first,
        ''), access_time)` et de `last_write_time`.

        Les fichiers inchangés (le cas de masse d'un rescan) ne sont pas mis à jour
        un par un : leurs `UPDATE` sont accumulés puis joués en `executemany`. Ils
        n'écrivent que des colonnes qu'aucun `SELECT` de la boucle ne relit, l'ordre
        reste donc celui du fichier ; par prudence le tampon est vidé avant toute
        écriture directe visant un fichier qui s'y trouve déjà (même chemin présent
        deux fois dans le même lot).

        Returns:
            (nouveaux, modifiés, inchangés).
        """
        new = updated = unchanged = 0
        now = _now()
        with self.transaction() as conn:
            touched: list[tuple[object, ...]] = []
            touched_ids: set[int] = set()

            def flush_touched() -> None:
                if touched:
                    conn.executemany(_TOUCH_SQL, touched)
                    touched.clear()
                    touched_ids.clear()

            for row in rows:
                key = path_key(row.path)
                existing = conn.execute(
                    "SELECT id, fast_hash, size_bytes, last_write_time, access_time_first, status,"
                    " content_version FROM files WHERE path_key=?",
                    (key,),
                ).fetchone()
                if existing is None:
                    conn.execute(
                        """INSERT INTO files(path_key, path, name, extension, host, hostname, username,
                           unc_directory, base, directory_type, size_bytes, creation_time, last_write_time,
                           access_time, access_time_first, file_attributes, owner, fast_hash, file_signature,
                           readable, writeable, deletable, first_seen_scan_id, last_seen_scan_id,
                           access_key, write_key,
                           content_version, status, updated_at)
                           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,'pending',?)""",
                        (
                            key,
                            row.path,
                            row.name,
                            row.extension,
                            row.host,
                            row.hostname,
                            row.username,
                            row.unc_directory,
                            row.base,
                            row.directory_type,
                            row.file_size,
                            row.creation_time,
                            row.last_write_time,
                            row.access_time,
                            row.access_time,
                            row.file_attributes,
                            row.owner,
                            row.fast_hash,
                            row.file_signature,
                            int(row.readable),
                            int(row.writeable),
                            int(row.deletable),
                            scan_id,
                            scan_id,
                            date_key(row.access_time),
                            date_key(row.last_write_time),
                            now,
                        ),
                    )
                    new += 1
                    continue
                changed = (
                    existing["fast_hash"] != row.fast_hash
                    or int(existing["size_bytes"]) != row.file_size
                    or existing["last_write_time"] != row.last_write_time
                )
                if changed:
                    if int(existing["id"]) in touched_ids:
                        flush_touched()
                    new_status = (
                        existing["status"]
                        if existing["status"] == FileStatus.EXCLUDED
                        else FileStatus.PENDING
                    )
                    conn.execute(
                        """UPDATE files SET size_bytes=?, creation_time=?, last_write_time=?, access_time=?,
                           access_time_first=?, access_key=?, write_key=?,
                           file_attributes=?, owner=?, fast_hash=?, file_signature=?, readable=?, writeable=?,
                           deletable=?, last_seen_scan_id=?, content_version=content_version+1, status=?,
                           exclusion_reason=CASE WHEN ?='excluded' THEN exclusion_reason ELSE NULL END, updated_at=?
                           WHERE id=?""",
                        (
                            row.file_size,
                            row.creation_time,
                            row.last_write_time,
                            row.access_time,
                            row.access_time,
                            date_key(row.access_time),
                            date_key(row.last_write_time),
                            row.file_attributes,
                            row.owner,
                            row.fast_hash,
                            row.file_signature,
                            int(row.readable),
                            int(row.writeable),
                            int(row.deletable),
                            scan_id,
                            str(new_status),
                            str(new_status),
                            now,
                            existing["id"],
                        ),
                    )
                    updated += 1
                else:
                    # `access_time_first` ne bouge pas : la clé d'accès ne retombe sur
                    # `access_time` que si la première observation manque.
                    first_access = str(existing["access_time_first"]) or row.access_time
                    touched.append(
                        (scan_id, row.access_time, date_key(first_access), now, existing["id"])
                    )
                    touched_ids.add(int(existing["id"]))
                    unchanged += 1
                    if len(touched) >= _TOUCH_FLUSH:
                        flush_touched()
            flush_touched()
        return new, updated, unchanged

    # ------------------------------------------------------------------ files
    @staticmethod
    def _file_row(r: sqlite3.Row) -> FileRow:
        return FileRow(
            id=int(r["id"]),
            path=r["path"],
            name=r["name"],
            extension=r["extension"],
            size_bytes=int(r["size_bytes"]),
            fast_hash=r["fast_hash"],
            last_write_time=r["last_write_time"],
            content_version=int(r["content_version"]),
            status=FileStatus(r["status"]),
            exclusion_reason=r["exclusion_reason"],
            priority_score=int(r["priority_score"]),
            owner=r["owner"],
            host=r["host"],
            unc_directory=r["unc_directory"],
        )

    def get_file(self, file_id: int) -> FileRow | None:
        r = self._conn.execute("SELECT * FROM files WHERE id=?", (file_id,)).fetchone()
        return self._file_row(r) if r else None

    def iter_files(
        self,
        status: FileStatus | None = None,
        *,
        ordered: bool = True,
        batch: int = ITER_FILES_BATCH,
    ) -> Iterator[FileRow]:
        """Parcourt les fichiers, éventuellement filtrés par statut.

        `ordered` (défaut, comportement historique) trie par `priority_score DESC,
        path` : l'ordre attendu de tout appelant qui présente les fichiers à un
        humain. Ce tri oblige SQLite à matérialiser la table entière avant de
        rendre la première ligne — plus d'une seconde et plusieurs centaines de
        mégaoctets sur 934 000 fichiers, pour rien quand l'appelant les traite
        *tous* sans se soucier de l'ordre (c'est le cas de `filter.plan_files`).

        `ordered=False` parcourt donc par `id` croissant, **par tranches de
        `batch` lignes** : aucun curseur de lecture ne reste ouvert entre deux
        tranches, ce qui autorise l'appelant à écrire dans `files` pendant le
        parcours (SQLite refuse un `COMMIT` tant qu'une lecture est en cours sur
        la même connexion) sans jamais sauter ni revoir une ligne, les `id` étant
        immuables.
        """
        params: tuple[object, ...] = ()
        where = ""
        if status is not None:
            where = " WHERE status=?"
            params = (str(status),)
        if ordered:
            sql = f"SELECT * FROM files{where} ORDER BY priority_score DESC, path"  # noqa: S608
            for r in self._conn.execute(sql, params):
                yield self._file_row(r)
            return
        clause = f"{where} AND id > ?" if status is not None else " WHERE id > ?"
        paged = f"SELECT * FROM files{clause} ORDER BY id LIMIT ?"  # noqa: S608
        last = -1
        while True:
            rows = self._conn.execute(paged, (*params, last, batch)).fetchall()
            if not rows:
                return
            for r in rows:
                yield self._file_row(r)
            last = int(rows[-1]["id"])
            if len(rows) < batch:
                return

    def select_pending(self, limit: int, *, prompt_hash: str, model: str) -> list[FileRow]:
        """Fichiers à analyser : `pending`, sans analyse pour leur version de contenu
        courante avec ce prompt et ce modèle. Ordre : priorité, puis chemin.

        Charge **tout** en mémoire (1,7 Go pour 700 000 fichiers) : réservé aux
        petites sélections. Le pipeline passe par `select_pending_ids` puis
        `files_by_ids`.
        """
        rows = self._conn.execute(
            f"SELECT f.* FROM files f{_PENDING_WHERE}"
            " ORDER BY f.priority_score DESC, f.path LIMIT ?",
            (prompt_hash, model, limit),
        ).fetchall()
        return [self._file_row(r) for r in rows]

    def select_pending_ids(self, limit: int, *, prompt_hash: str, model: str) -> list[int]:
        """Identifiants des fichiers à analyser, dans l'ordre de `select_pending`.

        Même sélection, même tri, mais un entier par fichier au lieu d'une
        `FileRow` : 28 Mo au lieu de 1 722 Mo pour 700 797 fichiers — et cette
        liste est gardée du début à la fin d'un run qui dure des heures, sur un
        serveur de 8 à 16 Go.

        Pourquoi une liste et non un curseur ouvert : le run **écrit** dans `files`
        (`queued`, `done`, `error`) au fil des lots, sur la même connexion. Un
        curseur laissé ouvert empêcherait ces validations, et le simple fait de
        changer les statuts déplacerait les lignes hors de la sélection — le
        parcours sauterait des fichiers. La liste d'identifiants, elle, est un
        instantané : elle reste exacte quoi qu'il advienne des statuts pendant le
        run, exactement comme la liste de `FileRow` qu'elle remplace.

        Le curseur est consommé au fil de l'eau, sans `fetchall()` et sans
        `sqlite3.Row` : matérialiser d'abord les 700 797 lignes rendait au pic
        113 Mo au lieu de 46 Mo, pour une liste d'entiers.
        """
        cursor = self._conn.cursor()
        cursor.row_factory = None
        try:
            return [
                int(r[0])
                for r in cursor.execute(
                    f"SELECT f.id FROM files f{_PENDING_WHERE}"
                    " ORDER BY f.priority_score DESC, f.path LIMIT ?",
                    (prompt_hash, model, limit),
                )
            ]
        finally:
            cursor.close()

    def count_pending(self, *, prompt_hash: str, model: str, limit: int | None = None) -> int:
        """Nombre de fichiers à analyser (mêmes critères que `select_pending`).

        `limit` plafonne le compte comme la sélection le ferait, pour que le
        compteur affiché corresponde à ce qui sera réellement traité.
        """
        if limit is not None:
            row = self._conn.execute(
                f"SELECT COUNT(*) FROM (SELECT 1 FROM files f{_PENDING_WHERE} LIMIT ?)",
                (prompt_hash, model, limit),
            ).fetchone()
        else:
            row = self._conn.execute(
                f"SELECT COUNT(*) FROM files f{_PENDING_WHERE}", (prompt_hash, model)
            ).fetchone()
        return int(row[0])

    def files_by_ids(self, file_ids: Sequence[int]) -> list[FileRow]:
        """Fichiers désignés par leurs identifiants, **dans l'ordre demandé**.

        Le pipeline s'en sert pour charger un lot (`blocks.batch_files`) à la fois
        à partir de `select_pending_ids` : la mémoire d'un run ne dépend plus de la
        taille de la campagne mais de celle d'un lot. Les identifiants sont envoyés
        par paquets de 500 (limite raisonnable sur le nombre de paramètres SQLite) ;
        un identifiant disparu de `files` est simplement absent du résultat.
        """
        if not file_ids:
            return []
        found: dict[int, FileRow] = {}
        for start in range(0, len(file_ids), 500):
            chunk = tuple(file_ids[start : start + 500])
            marks = ",".join("?" for _ in chunk)
            for r in self._conn.execute(f"SELECT * FROM files WHERE id IN ({marks})", chunk):
                found[int(r["id"])] = self._file_row(r)
        return [row for fid in file_ids if (row := found.get(int(fid))) is not None]

    def set_file_status(self, file_id: int, status: FileStatus, reason: str | None = None) -> None:
        self._conn.execute(
            "UPDATE files SET status=?, exclusion_reason=?, updated_at=? WHERE id=?",
            (str(status), reason, _now(), file_id),
        )
        self._conn.commit()

    def set_files_status(
        self, file_ids: Sequence[int], status: FileStatus, reason: str | None = None
    ) -> None:
        now = _now()
        with self.transaction() as conn:
            conn.executemany(
                "UPDATE files SET status=?, exclusion_reason=?, updated_at=? WHERE id=?",
                [(str(status), reason, now, fid) for fid in file_ids],
            )

    def apply_plan(
        self,
        decisions: Iterable[tuple[int, FileStatus, str | None, int]],
        *,
        batch: int = APPLY_PLAN_BATCH,
    ) -> tuple[int, int]:
        """Applique les décisions du filtre : (file_id, statut, raison, score).
        Ne touche pas aux fichiers `done`/`error` sauf pour le score.

        Les `UPDATE` sont regroupés par `executemany` — deux ordres SQL distincts
        selon la décision (`_PLAN_EXCLUDE_SQL`, `_PLAN_KEEP_SQL`) — et envoyés par
        tranches de `batch` : un aller-retour SQLite pour des milliers de lignes
        au lieu d'un par fichier. `decisions` peut être un flux, rien n'est
        accumulé au-delà d'une tranche. Chaque décision porte sur un `id`
        distinct : regrouper ne change donc pas le résultat.

        Returns:
            (fichiers pending, fichiers exclus).
        """
        pending = excluded = 0
        now = _now()
        exclude: list[tuple[object, ...]] = []
        keep: list[tuple[object, ...]] = []

        def flush(conn: sqlite3.Connection) -> None:
            if exclude:
                conn.executemany(_PLAN_EXCLUDE_SQL, exclude)
                exclude.clear()
            if keep:
                conn.executemany(_PLAN_KEEP_SQL, keep)
                keep.clear()

        with self.transaction() as conn:
            for file_id, status, reason, score in decisions:
                if status == FileStatus.EXCLUDED:
                    exclude.append((reason, score, now, file_id))
                    excluded += 1
                else:
                    keep.append((score, now, file_id))
                    pending += 1
                if len(exclude) + len(keep) >= batch:
                    flush(conn)
            flush(conn)
        return pending, excluded

    def reset_errors(self) -> int:
        cur = self._conn.execute(
            "UPDATE files SET status='pending', exclusion_reason=NULL, updated_at=? WHERE status='error'",
            (_now(),),
        )
        self._conn.commit()
        return int(cur.rowcount)

    def requeue_stale(self) -> int:
        """Fichiers `queued` sans bloc en vol (interruption) → `pending`."""
        cur = self._conn.execute(
            """UPDATE files SET status='pending', updated_at=? WHERE status='queued' AND id NOT IN (
                 SELECT bf.file_id FROM block_files bf JOIN blocks b ON b.id=bf.block_id WHERE b.status IN ('built','sent'))""",
            (_now(),),
        )
        self._conn.commit()
        return int(cur.rowcount)
