"""Tables `prompts` et `reviews` : profils de prompt, vérification humaine."""

from __future__ import annotations

import sqlite3

from docia.db.core import REVIEW_STATUSES, _DatabaseCore, _now


class PromptsOps(_DatabaseCore):
    # ------------------------------------------------------------------ prompts
    def save_prompt(self, name: str, text: str, *, activate: bool = False) -> int:
        """Crée ou met à jour un profil de prompt nommé."""
        import hashlib

        digest = hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]
        now = _now()
        with self.transaction() as conn:
            conn.execute(
                """INSERT INTO prompts(name, text, hash, active, created_at, updated_at)
                   VALUES(?,?,?,0,?,?)
                   ON CONFLICT(name) DO UPDATE SET text=excluded.text, hash=excluded.hash,
                   updated_at=excluded.updated_at""",
                (name, text, digest, now, now),
            )
            if activate:
                conn.execute("UPDATE prompts SET active=0")
                conn.execute("UPDATE prompts SET active=1 WHERE name=?", (name,))
            row = conn.execute("SELECT id FROM prompts WHERE name=?", (name,)).fetchone()
        return int(row["id"])

    def list_prompts(self) -> list[sqlite3.Row]:
        return list(
            self._conn.execute(
                "SELECT id, name, hash, active, length(text) AS chars, created_at, updated_at"
                " FROM prompts ORDER BY name"
            )
        )

    def get_prompt(self, name: str) -> str | None:
        row = self._conn.execute("SELECT text FROM prompts WHERE name=?", (name,)).fetchone()
        return str(row["text"]) if row else None

    def set_active_prompt(self, name: str | None) -> bool:
        """Active un profil (None = aucun : prompt embarqué). False si inconnu."""
        with self.transaction() as conn:
            conn.execute("UPDATE prompts SET active=0")
            if name is None:
                return True
            cur = conn.execute("UPDATE prompts SET active=1 WHERE name=?", (name,))
            return cur.rowcount == 1

    def active_prompt(self) -> tuple[str, str] | None:
        """(nom, texte) du profil actif, ou None (prompt embarqué)."""
        row = self._conn.execute("SELECT name, text FROM prompts WHERE active=1 LIMIT 1").fetchone()
        return (str(row["name"]), str(row["text"])) if row else None

    def delete_prompt(self, name: str) -> bool:
        cur = self._conn.execute("DELETE FROM prompts WHERE name=?", (name,))
        self._conn.commit()
        return cur.rowcount == 1

    # ------------------------------------------------------------------ reviews
    def set_review(
        self,
        file_id: int,
        status: str,
        *,
        comment: str = "",
        reviewer: str = "",
        corrected_security: str | None = None,
        corrected_rgpd: str | None = None,
        corrected_retention_years: int | None = None,
    ) -> None:
        """Statut de vérification humaine d'un fichier (`to_review` / `validated` / `corrected`)."""
        if status not in REVIEW_STATUSES:
            raise ValueError(f"statut de revue inconnu : {status}")
        self._conn.execute(
            """INSERT INTO reviews(file_id, status, comment, corrected_security, corrected_rgpd,
               corrected_retention_years, reviewer, updated_at) VALUES(?,?,?,?,?,?,?,?)
               ON CONFLICT(file_id) DO UPDATE SET status=excluded.status, comment=excluded.comment,
               corrected_security=excluded.corrected_security, corrected_rgpd=excluded.corrected_rgpd,
               corrected_retention_years=excluded.corrected_retention_years,
               reviewer=excluded.reviewer, updated_at=excluded.updated_at""",
            (
                file_id,
                status,
                comment,
                corrected_security,
                corrected_rgpd,
                corrected_retention_years,
                reviewer,
                _now(),
            ),
        )
        self._conn.commit()

    def review_counts(self) -> dict[str, int]:
        out = dict.fromkeys(REVIEW_STATUSES, 0)
        for r in self._conn.execute("SELECT status, COUNT(*) AS n FROM reviews GROUP BY status"):
            out[str(r["status"])] = int(r["n"])
        return out
