"""Tables `runs`, `blocks` et `block_files` : cycle de vie d'un bloc envoyé à la LLM."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from docia.db.core import _DatabaseCore, _now
from docia.models import (
    BlockFile,
    BlockSpec,
    LLMUsage,
)


class BlocksOps(_DatabaseCore):
    # ------------------------------------------------------------------ runs
    """Tables `runs`, `blocks` et `block_files` : un run, ses blocs envoyés à la LLM, l'issue par fichier."""

    def start_run(self, *, model: str, prompt_hash: str, config_json: str) -> int:
        """Ouvre une ligne `runs` (modèle, empreinte du prompt, config figée) et rend son identifiant."""
        cur = self._conn.execute(
            "INSERT INTO runs(started_at, model, prompt_hash, config_json) VALUES(?,?,?,?)",
            (_now(), model, prompt_hash, config_json),
        )
        self._conn.commit()
        return int(cur.lastrowid or 0)

    def finish_run(self, run_id: int, status: str = "done") -> None:
        """Clôt un run avec son statut (`done`, `error`, `cancelled`, `dry-run`)."""
        self._conn.execute(
            "UPDATE runs SET finished_at=?, status=? WHERE id=?", (_now(), status, run_id)
        )
        self._conn.commit()

    # ------------------------------------------------------------------ blocks
    def create_block(self, run_id: int, spec: BlockSpec, *, prompt_hash: str, model: str) -> int:
        """Enregistre un bloc construit et ses fichiers (passés `queued`), en une transaction."""
        with self.transaction() as conn:
            cur = conn.execute(
                """INSERT INTO blocks(run_id, path, tokens_estimated, tokens_with_margin, file_count, oversized,
                   status, prompt_hash, model, created_at) VALUES(?,?,?,?,?,?,'built',?,?,?)""",
                (
                    run_id,
                    str(spec.path),
                    spec.tokens_estimated,
                    spec.tokens_with_margin,
                    len(spec.files),
                    int(spec.oversized),
                    prompt_hash,
                    model,
                    _now(),
                ),
            )
            block_id = int(cur.lastrowid or 0)
            conn.executemany(
                "INSERT INTO block_files(block_id, file_id, file_ref, content_version, oversized,"
                " segment_index, segment_count) VALUES(?,?,?,?,?,?,?)",
                [
                    (
                        block_id,
                        bf.file_id,
                        bf.file_ref,
                        bf.content_version,
                        int(bf.oversized),
                        bf.segment_index,
                        bf.segment_count,
                    )
                    for bf in spec.files
                ],
            )
            conn.executemany(
                "UPDATE files SET status='queued', updated_at=? WHERE id=?",
                [(_now(), bf.file_id) for bf in spec.files],
            )
        spec.block_id = block_id
        return block_id

    def mark_block_sent(self, block_id: int) -> None:
        """Le bloc part vers la LLM : statut `sent`, tentative comptée."""
        self._conn.execute(
            "UPDATE blocks SET status='sent', attempts=attempts+1, sent_at=? WHERE id=?",
            (_now(), block_id),
        )
        self._conn.commit()

    def mark_block_done(self, block_id: int, usage: LLMUsage | None) -> None:
        """Le bloc a une réponse exploitable : statut `done`, consommation de tokens."""
        self._conn.execute(
            """UPDATE blocks SET status='done', completed_at=?, usage_prompt_tokens=?, usage_completion_tokens=?,
               latency_ms=?, error=NULL WHERE id=?""",
            (
                _now(),
                usage.prompt_tokens if usage else None,
                usage.completion_tokens if usage else None,
                usage.latency_ms if usage else None,
                block_id,
            ),
        )
        self._conn.commit()

    def mark_block_error(self, block_id: int, error: str) -> None:
        """Le bloc a échoué : statut `error` et message (tronqué à 2 000 caractères)."""
        self._conn.execute(
            "UPDATE blocks SET status='error', completed_at=?, error=? WHERE id=?",
            (_now(), error[:2000], block_id),
        )
        self._conn.commit()

    def file_attempts(
        self, file_id: int, *, segment_index: int | None = None, segment_count: int | None = None
    ) -> int:
        """Nombre de blocs dans lesquels ce fichier a déjà été envoyé (tentatives).

        `segment_index` / `segment_count` restreignent le compte à **un segment** d'un
        fichier découpé. Sans cela un fichier en K parties comptait K tentatives dès le
        premier run (une par bloc-segment) : au-delà de `MAX_FILE_ATTEMPTS`, un seul
        segment refusé (un 503 pendant un redémarrage du serveur) condamnait tout le
        fichier, y compris les K−1 segments déjà payés.
        """
        sql = """SELECT COUNT(*) FROM block_files bf JOIN blocks b ON b.id=bf.block_id
                 WHERE bf.file_id=? AND b.status IN ('sent','done','error')"""
        params: tuple[object, ...] = (file_id,)
        if segment_index is not None and segment_count is not None:
            sql += " AND bf.segment_index=? AND bf.segment_count=?"
            params = (file_id, segment_index, segment_count)
        row = self._conn.execute(sql, params).fetchone()
        return int(row[0])

    def unfinished_files(
        self, file_ids: Sequence[int], *, sample: int = 5
    ) -> tuple[int, list[str]]:
        """Parmi ces identifiants, ceux qui ne sont ni `done`, ni `error`, ni `excluded`.

        Rend `(nombre, quelques noms)` sans jamais matérialiser de `FileRow` (une
        campagne fait 700 000 fichiers). Le pipeline s'en sert en fin de run : un run
        qui laisse des fichiers engagés dans un bloc sans résultat ni erreur ne doit
        jamais être clos « done » — sinon plus rien ne signale qu'ils sont en plan.
        """
        total = 0
        noms: list[str] = []
        for start in range(0, len(file_ids), 500):
            chunk = tuple(file_ids[start : start + 500])
            marks = ",".join("?" for _ in chunk)
            for row in self._conn.execute(
                f"SELECT name FROM files WHERE id IN ({marks})"  # noqa: S608
                " AND status NOT IN ('done','error','excluded')",
                chunk,
            ):
                total += 1
                if len(noms) < sample:
                    noms.append(str(row["name"]))
        return total, noms

    def set_block_file_outcome(self, block_id: int, file_id: int, outcome: str) -> None:
        """Issue d'un fichier dans un bloc (`done`, `failed: …`, `segment done`…)."""
        self._conn.execute(
            "UPDATE block_files SET outcome=? WHERE block_id=? AND file_id=?",
            (outcome, block_id, file_id),
        )
        self._conn.commit()

    def pending_blocks(self, *, prompt_hash: str, model: str) -> list[BlockSpec]:
        """Blocs `built`/`sent` d'un run précédent, à (re)envoyer — reprise.

        Deux requêtes en tout — les blocs, puis **tous** leurs fichiers d'un coup —
        au lieu d'une par bloc : reprendre une campagne interrompue avec 20 000
        blocs en attente coûtait 20 001 allers-retours SQLite avant le premier envoi.
        """
        rows = self._conn.execute(
            "SELECT * FROM blocks WHERE status IN ('built','sent') AND prompt_hash=? AND model=?"
            " ORDER BY id",
            (prompt_hash, model),
        ).fetchall()
        if not rows:
            return []
        files_by_block: dict[int, list[BlockFile]] = {int(r["id"]): [] for r in rows}
        for r in self._conn.execute(
            "SELECT bf.block_id, bf.file_id, bf.file_ref, bf.content_version, bf.oversized,"
            " bf.segment_index, bf.segment_count FROM block_files bf"
            " JOIN blocks b ON b.id = bf.block_id"
            " WHERE b.status IN ('built','sent') AND b.prompt_hash=? AND b.model=?"
            " ORDER BY bf.block_id, bf.rowid",
            (prompt_hash, model),
        ):
            files_by_block[int(r["block_id"])].append(
                BlockFile(
                    int(r["file_id"]),
                    r["file_ref"],
                    int(r["content_version"]),
                    bool(r["oversized"]),
                    int(r["segment_index"]),
                    int(r["segment_count"]),
                )
            )
        return [
            BlockSpec(
                path=Path(r["path"]),
                files=files_by_block[int(r["id"])],
                tokens_estimated=int(r["tokens_estimated"]),
                tokens_with_margin=int(r["tokens_with_margin"]),
                oversized=bool(r["oversized"]),
                block_id=int(r["id"]),
            )
            for r in rows
        ]
