"""Base SQLite : schéma, upsert avec versions de contenu, sélection, reprise, analyses."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from docia.db import (
    _MIGRATIONS,  # noqa: PLC2701 - le test rejoue les migrations réelles jusqu'à la v5
    SCHEMA_VERSION,
    Database,
    backup_dir_for,
    date_key,
    date_key_sql,
)
from docia.models import (
    BlockFile,
    BlockSpec,
    DomainAnalysis,
    FileAnalysis,
    FileStatus,
    LLMUsage,
    SmbeagleRow,
)


def _row(
    name: str,
    *,
    fast_hash: str = "aaaa",
    size: int = 1000,
    lwt: str = "01/01/2026 10:00:00",
    access: str = "02/01/2026 10:00:00",
) -> SmbeagleRow:
    return SmbeagleRow(
        name=name,
        host="srv",
        extension=name.rsplit(".", 1)[-1],
        username="u",
        hostname="srv.dom",
        unc_directory="\\\\srv\\part\\dossier",
        creation_time="01/01/2025 10:00:00",
        last_write_time=lwt,
        readable=True,
        writeable=False,
        deletable=False,
        directory_type="SMB",
        base="\\\\srv\\part\\",
        file_size=size,
        access_time=access,
        file_attributes="Archive",
        owner="DOM\\x",
        fast_hash=fast_hash,
        file_signature="unknown",
    )


def _analysis(ref: str) -> FileAnalysis:
    return FileAnalysis(
        file_ref=ref,
        resume="r",
        security=DomainAnalysis("C1", 80, {"justification": "j"}),
        rgpd=DomainAnalysis("low", 70, {"data_types": ["identite"]}),
        finance=DomainAnalysis("none", 90, {"amounts": []}),
        legal=DomainAnalysis("none", 90, {"parties": []}),
        raw={"file_ref": ref},
    )


def test_schema_and_path(tmp_path: Path) -> None:
    with Database(tmp_path / "x.sqlite") as db:
        assert db.schema_version == SCHEMA_VERSION
        assert db.counts()["files"] == 0
    # ré-ouverture : pas de nouvelle migration
    with Database(tmp_path / "x.sqlite") as db:
        assert db.schema_version == SCHEMA_VERSION


def test_upsert_new_updated_unchanged_and_content_version(tmp_path: Path) -> None:
    with Database(tmp_path / "x.sqlite") as db:
        scan1 = db.start_scan("a.csv")
        assert db.upsert_files([_row("a.txt"), _row("b.pdf")], scan1) == (2, 0, 0)
        a = next(f for f in db.iter_files() if f.name == "a.txt")
        assert (a.content_version, a.status, a.path) == (
            1,
            FileStatus.PENDING,
            "\\\\srv\\part\\dossier\\a.txt",
        )

        db.set_file_status(a.id, FileStatus.DONE)
        scan2 = db.start_scan("a.csv")
        # même contenu → inchangé, statut conservé ; b modifié → version 2, pending
        assert db.upsert_files([_row("a.txt"), _row("b.pdf", fast_hash="bbbb")], scan2) == (0, 1, 1)
        a2 = db.get_file(a.id)
        assert a2 is not None
        assert (a2.status, a2.content_version) == (FileStatus.DONE, 1)
        b = next(f for f in db.iter_files() if f.name == "b.pdf")
        assert (b.content_version, b.status) == (2, FileStatus.PENDING)

        # casse différente du chemin → même fichier (path_key)
        row_upper = _row("A.TXT")
        assert db.upsert_files([row_upper], scan2) == (0, 0, 1)


def test_excluded_file_stays_excluded_when_content_changes(tmp_path: Path) -> None:
    with Database(tmp_path / "x.sqlite") as db:
        scan = db.start_scan("a.csv")
        db.upsert_files([_row("a.zip")], scan)
        f = next(db.iter_files())
        db.set_file_status(f.id, FileStatus.EXCLUDED, "extension exclue")
        db.upsert_files([_row("a.zip", fast_hash="zzzz")], scan)
        f2 = db.get_file(f.id)
        assert f2 is not None
        assert (f2.status, f2.exclusion_reason, f2.content_version) == (
            FileStatus.EXCLUDED,
            "extension exclue",
            2,
        )


def test_select_pending_skips_files_already_analyzed_for_current_version(tmp_path: Path) -> None:
    with Database(tmp_path / "x.sqlite") as db:
        scan = db.start_scan("a.csv")
        db.upsert_files([_row("a.txt"), _row("b.txt")], scan)
        files = list(db.iter_files())
        a, b = files[0], files[1]
        db.store_analysis(
            a.id, None, a.content_version, prompt_hash="p1", model="m", analysis=_analysis("a.txt")
        )
        assert db.get_file(a.id).status == FileStatus.DONE  # type: ignore[union-attr]
        # a est done → non sélectionné ; b pending → sélectionné
        assert [f.id for f in db.select_pending(10, prompt_hash="p1", model="m")] == [b.id]
        # a repasse pending (ex. retry) mais a déjà une analyse pour p1/m/v1 → toujours pas sélectionné
        db.set_file_status(a.id, FileStatus.PENDING)
        assert [f.id for f in db.select_pending(10, prompt_hash="p1", model="m")] == [b.id]
        # autre prompt → a redevient à analyser
        assert {f.id for f in db.select_pending(10, prompt_hash="p2", model="m")} == {a.id, b.id}


def test_store_analysis_upserts_and_marks_done(tmp_path: Path) -> None:
    with Database(tmp_path / "x.sqlite") as db:
        scan = db.start_scan("a.csv")
        db.upsert_files([_row("a.txt")], scan)
        a = next(db.iter_files())
        db.store_analysis(a.id, None, 1, prompt_hash="p", model="m", analysis=_analysis("a.txt"))
        db.store_analysis(a.id, None, 1, prompt_hash="p", model="m", analysis=_analysis("a.txt"))
        assert db.counts()["analyses"] == 1
        rows = list(db.latest_analyses())
        assert rows[0]["security_classification"] == "C1"
        assert rows[0]["rgpd_data_types"] == '["identite"]'
        assert db.classification_summary()["security"] == {"C1": 1}


def test_blocks_lifecycle_and_resume(tmp_path: Path) -> None:
    with Database(tmp_path / "x.sqlite") as db:
        scan = db.start_scan("a.csv")
        db.upsert_files([_row("a.txt"), _row("b.txt"), _row("c.txt")], scan)
        a, b, c = list(db.iter_files())
        run = db.start_run(model="m", prompt_hash="p", config_json="{}")
        spec = BlockSpec(
            path=tmp_path / "b1.md",
            files=[BlockFile(a.id, "a.txt", 1), BlockFile(b.id, "b.txt", 1)],
            tokens_estimated=10,
            tokens_with_margin=12,
        )
        block_id = db.create_block(run, spec, prompt_hash="p", model="m")
        assert spec.block_id == block_id
        assert db.get_file(a.id).status == FileStatus.QUEUED  # type: ignore[union-attr]
        assert db.file_attempts(a.id) == 0  # bloc construit mais pas envoyé

        # reprise : le bloc built est renvoyé par pending_blocks, c (jamais mis en bloc) est requeue-able
        pending = db.pending_blocks(prompt_hash="p", model="m")
        assert [p.block_id for p in pending] == [block_id]
        assert [bf.file_ref for bf in pending[0].files] == ["a.txt", "b.txt"]
        db.set_file_status(c.id, FileStatus.QUEUED)
        assert db.requeue_stale() == 1  # c n'appartient à aucun bloc en vol
        assert db.get_file(c.id).status == FileStatus.PENDING  # type: ignore[union-attr]

        db.mark_block_sent(block_id)
        assert db.file_attempts(a.id) == 1
        db.mark_block_done(block_id, LLMUsage(100, 20, 1500, "m"))
        assert db.counts()["blocks_done"] == 1
        assert db.pending_blocks(prompt_hash="p", model="m") == []

        db.mark_block_error(block_id, "boom")
        assert db.counts()["blocks_error"] == 1


def test_apply_plan_and_reset_errors(tmp_path: Path) -> None:
    with Database(tmp_path / "x.sqlite") as db:
        scan = db.start_scan("a.csv")
        db.upsert_files([_row("a.txt"), _row("b.zip"), _row("c.txt")], scan)
        a, b, c = list(db.iter_files())
        db.set_file_status(c.id, FileStatus.DONE)
        pending, excluded = db.apply_plan(
            [
                (a.id, FileStatus.PENDING, None, 55),
                (b.id, FileStatus.EXCLUDED, "zip", 0),
                (c.id, FileStatus.PENDING, None, 40),
            ]
        )
        assert (pending, excluded) == (2, 1)
        assert db.get_file(b.id).exclusion_reason == "zip"  # type: ignore[union-attr]
        assert db.get_file(c.id).status == FileStatus.DONE  # type: ignore[union-attr]
        assert db.get_file(a.id).priority_score == 55  # type: ignore[union-attr]
        db.set_file_status(a.id, FileStatus.ERROR, "x")
        assert db.reset_errors() == 1
        assert db.get_file(a.id).status == FileStatus.PENDING  # type: ignore[union-attr]


def test_prompt_profiles_and_reviews(tmp_path: Path) -> None:
    with Database(tmp_path / "x.sqlite") as db:
        assert db.schema_version == SCHEMA_VERSION
        assert db.active_prompt() is None
        db.save_prompt("audit-rh", "Prompt RH " * 20)
        db.save_prompt("audit-fin", "Prompt finance " * 20, activate=True)
        assert db.active_prompt() is not None
        assert db.active_prompt()[0] == "audit-fin"  # type: ignore[index]
        names = [r["name"] for r in db.list_prompts()]
        assert names == ["audit-fin", "audit-rh"]
        assert db.set_active_prompt("audit-rh") is True
        assert db.set_active_prompt("inconnu") is False
        assert db.active_prompt() is None  # l'échec a désactivé tout profil
        db.save_prompt("audit-rh", "Prompt RH v2 " * 20)  # mise à jour, même nom
        assert len(db.list_prompts()) == 2
        assert db.get_prompt(
            "audit-rh",
        ).startswith("Prompt RH v2")  # type: ignore[union-attr]
        assert db.delete_prompt("audit-fin") is True
        assert db.delete_prompt("audit-fin") is False

        scan = db.start_scan("a.csv")
        db.upsert_files([_row("a.txt")], scan)
        a = next(db.iter_files())
        db.store_analysis(a.id, None, 1, prompt_hash="p", model="m", analysis=_analysis("a.txt"))
        db.set_review(
            a.id, "corrected", comment="C2 plutôt", corrected_security="C2", reviewer="moi"
        )
        rows = list(db.latest_analyses())
        assert rows[0]["review_status"] == "corrected"
        assert rows[0]["corrected_security"] == "C2"
        assert rows[0]["retention_basis"] == "none"
        assert db.review_counts()["corrected"] == 1
        with pytest.raises(ValueError, match="statut de revue inconnu"):
            db.set_review(a.id, "bidon")


# ------------------------------------------------------- clés de date (v6)

DATES: tuple[str, ...] = (
    "",
    "n/a",
    "05/03/2018 08:00:00",
    "31/12/2026 23:59:59",
    "08/31/2026 10:00:00",
    "1/2/2026 10:00",
    "2026-08-31",
    "2026-08-31T10:00:00",
    "2026-08-31 10:00:00+02:00",
    "2026/08/31 10:00:00",
    "20260831",
    "  2026-08-31",
)
"""Dates de toutes les formes rencontrées : SMBeagle, américaine, ISO, illisibles."""


def test_date_key_matches_sql(tmp_path: Path) -> None:
    """`date_key` (écriture, Python) et `date_key_sql` (migration) rendent la même clé."""
    with Database(tmp_path / "x.sqlite") as db:
        for value in DATES:
            expected = db.query_values(
                f"SELECT {date_key_sql('v')} FROM (SELECT ? AS v)", (value,)
            )[0][0]
            assert date_key(value) == expected, value


def _v5_database(path: Path) -> None:
    """Crée une base au schéma v5 remplie de fichiers aux dates variées."""
    conn = sqlite3.connect(path)
    for _version, sql in _MIGRATIONS[:5]:
        conn.executescript(sql)
    conn.execute("INSERT INTO meta(key, value) VALUES('schema_version', '5')")
    conn.execute("INSERT INTO scans(csv_path, imported_at) VALUES('a.csv', '2026-01-01')")
    conn.executemany(
        """INSERT INTO files(path_key, path, name, last_write_time, access_time,
           access_time_first, size_bytes, first_seen_scan_id, last_seen_scan_id, updated_at)
           VALUES(?,?,?,?,?,?,?,1,1,'2026-01-01')""",
        [
            # accès rajeuni par l'audit : la clé doit suivre la première observation
            (
                "a",
                "a",
                "a",
                "05/03/2018 08:00:00",
                "30/08/2026 09:00:00",
                "06/03/2018 08:00:00",
                10,
            ),
            # première observation absente (fichier importé avant la v5)
            ("b", "b", "b", "2020-07-04T10:00:00", "2021-09-08T11:00:00", "", 20),
            # dates illisibles
            ("c", "c", "c", "n/a", "", "", 30),
        ],
    )
    conn.commit()
    conn.close()


def test_migration_v5_to_v6_backfills_date_keys(tmp_path: Path) -> None:
    """Une base v5 remplie migre seule en v6 : clés rétro-remplies, données intactes."""
    path = tmp_path / "campagne.sqlite"
    _v5_database(path)
    with Database(path) as db:
        assert db.schema_version == SCHEMA_VERSION
        keys = {
            str(r[0]): (str(r[1]), str(r[2]))
            for r in db.query_values("SELECT name, access_key, write_key FROM files")
        }
        assert keys == {
            "a": ("20180306", "20180305"),
            "b": ("20210908", "20200704"),
            "c": ("", ""),
        }
        assert db.counts()["files"] == 3
        assert [int(r[0]) for r in db.query_values("SELECT size_bytes FROM files ORDER BY id")] == [
            10,
            20,
            30,
        ]
    backup = backup_dir_for(path) / f"campagne_avant_migration_v{SCHEMA_VERSION}.sqlite"
    assert backup.exists()
    with Database(backup) as saved:  # la sauvegarde migre à son tour, sans perte
        assert saved.counts()["files"] == 3


def test_date_keys_follow_upserts(tmp_path: Path) -> None:
    """Insertion, rescan sans changement et changement de contenu gardent les clés justes."""
    with Database(tmp_path / "x.sqlite") as db:

        def keys() -> tuple[str, str]:
            row = db.query_values("SELECT access_key, write_key FROM files")[0]
            return str(row[0]), str(row[1])

        scan1 = db.start_scan("a.csv")
        db.upsert_files([_row("a.txt")], scan1)
        assert keys() == ("20260102", "20260101")

        # rescan : l'audit a rajeuni la date d'accès, la clé suit la première observation
        scan2 = db.start_scan("a.csv")
        assert db.upsert_files([_row("a.txt", access="30/08/2026 09:00:00")], scan2) == (0, 0, 1)
        assert keys() == ("20260102", "20260101")

        # contenu modifié : les deux clés repartent des dates du scan
        scan3 = db.start_scan("a.csv")
        assert db.upsert_files(
            [
                _row(
                    "a.txt",
                    fast_hash="bbbb",
                    lwt="04/07/2026 10:00:00",
                    access="05/07/2026 10:00:00",
                )
            ],
            scan3,
        ) == (0, 1, 0)
        assert keys() == ("20260705", "20260704")
