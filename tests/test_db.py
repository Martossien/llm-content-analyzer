"""Base SQLite : schéma, upsert avec versions de contenu, sélection, reprise, analyses."""

from __future__ import annotations

import itertools
import os
import random
import sqlite3
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

import docia.db as db_module
from docia.db import (
    _MIGRATIONS,  # noqa: PLC2701 - le test rejoue les migrations réelles jusqu'à la v5
    BULK_LOCK_KEY,
    BULK_LOCK_TTL_S,
    FILES_INDEXES,
    SCHEMA_VERSION,
    Database,
    MigrationBackupError,
    backup_dir_for,
    date_key,
    date_key_sql,
    normalize_index_sql,
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


_FUZZ_ALPHABET = "0123456789/-: TZ+.éab\r\n\t\x00"
"""Caractères injectés dans les dates engendrées (chiffres, séparateurs, ferraille)."""


def _fuzz_dates(count: int = 4000) -> list[str]:
    """Milliers de dates : les formes réelles, toutes leurs mutations d'un caractère,
    toutes les chaînes courtes d'un petit alphabet, puis du bruit aléatoire.

    Une clé de date fausse ne se voit pas : elle décale silencieusement les
    statistiques d'ancienneté. Le corpus vise donc les frontières de
    `date_key` — position des séparateurs, longueur 9/10/11, dates tronquées.
    """
    rng = random.Random(20260831)
    values: list[str] = list(DATES)
    for template in DATES:
        for position in range(len(template) + 1):
            for char in _FUZZ_ALPHABET:
                values.append(template[:position] + char + template[position + 1 :])
                values.append(template[:position] + char + template[position:])
            values.append(template[:position] + template[position + 1 :])
    for length in range(12):
        for tup in itertools.product("0/-", repeat=min(length, 5)):
            values.append("".join(tup))
    values += [
        "".join(rng.choice(_FUZZ_ALPHABET) for _ in range(rng.randint(0, 16))) for _ in range(count)
    ]
    return values


def test_date_key_matches_sql_on_generated_dates(tmp_path: Path) -> None:
    """Même clé en Python et en SQL sur des milliers de dates engendrées.

    `date_key` remplit `files.access_key` / `files.write_key` à chaque écriture,
    `date_key_sql` les rétro-remplit à la migration : le moindre écart rendrait
    les vues d'ancienneté fausses sur les bases migrées, et seulement là.
    """
    values = _fuzz_dates()
    assert len(values) > 5000, "corpus trop maigre pour un fuzzing utile"
    sql = f"SELECT {date_key_sql('v')} FROM (SELECT ? AS v)"
    with Database(tmp_path / "x.sqlite") as db:
        divergences = [
            (value, date_key(value), expected)
            for value, expected in (
                (value, str(db.query_values(sql, (value,))[0][0])) for value in values
            )
            if date_key(value) != expected
        ]
    assert divergences == [], divergences[:5]


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
    # Nom horodaté : une nouvelle tentative d'ouverture ne doit jamais écraser la
    # sauvegarde saine par une base à moitié migrée.
    sauvegardes = sorted(
        backup_dir_for(path).glob(f"campagne_avant_migration_v{SCHEMA_VERSION}_*.sqlite")
    )
    assert len(sauvegardes) == 1, sauvegardes
    with Database(sauvegardes[0]) as saved:  # la sauvegarde migre à son tour, sans perte
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


# --------------------------------------------------- chargement en masse (index)


def _index_names(db: Database) -> set[str]:
    """Index secondaires de `files` (ceux qui ont une définition SQL)."""
    return {
        str(r[0])
        for r in db.query_values(
            "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='files'"
            " AND sql IS NOT NULL"
        )
    }


def _index_definitions(db: Database) -> dict[str, str]:
    """{nom: définition normalisée} des index secondaires de `files` réellement présents."""
    return {
        str(r[0]): normalize_index_sql(str(r[1]))
        for r in db.query_values(
            "SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name='files'"
            " AND sql IS NOT NULL"
        )
    }


def test_files_indexes_constant_matches_schema(tmp_path: Path) -> None:
    """`FILES_INDEXES` décrit exactement les index que les migrations laissent en place.

    **Colonnes comprises** : comparer les seuls noms ne prouvait rien. En écrivant
    `idx_files_share_size ON files(owner)` dans la constante, le test passait, et
    `_ensure_files_indexes` recréait tranquillement un index sur les mauvaises
    colonnes après un import interrompu — les vues repassaient en balayage complet
    et rien ne le signalait.
    """
    with Database(tmp_path / "x.sqlite") as db:
        attendu = {name: normalize_index_sql(sql) for name, sql in FILES_INDEXES.items()}
        assert _index_definitions(db) == attendu


def test_un_index_declare_sur_les_mauvaises_colonnes_est_detecte(tmp_path: Path) -> None:
    """Le garde-fou du test précédent : falsifier les colonnes doit le faire échouer."""
    with Database(tmp_path / "x.sqlite") as db:
        falsifie = dict(FILES_INDEXES)
        falsifie["idx_files_share_size"] = (
            "CREATE INDEX IF NOT EXISTS idx_files_share_size ON files(owner)"
        )
        attendu = {name: normalize_index_sql(sql) for name, sql in falsifie.items()}
        assert _index_definitions(db) != attendu
        assert set(_index_definitions(db)) == set(attendu), "les noms, eux, coïncident toujours"


def test_normalize_index_sql_ignore_la_mise_en_forme_mais_pas_les_colonnes() -> None:
    """Casse, espaces et `IF NOT EXISTS` ne comptent pas ; les colonnes, si."""
    stocke = "CREATE INDEX idx_x ON files(base, unc_directory, size_bytes)"
    declare = "CREATE  INDEX  IF NOT EXISTS  idx_x\n  ON files ( base ,unc_directory, size_bytes );"
    assert normalize_index_sql(stocke) == normalize_index_sql(declare)
    autre = "CREATE INDEX idx_x ON files(base, size_bytes, unc_directory)"
    assert normalize_index_sql(stocke) != normalize_index_sql(autre)


def test_bulk_load_ne_supprime_jamais_un_index_unique(tmp_path: Path) -> None:
    """Un index UNIQUE est une contrainte : le retirer laisserait entrer des doublons.

    `bulk_load` supprimait tout index dont `sql` n'était pas nul. Sur une base
    portant un `CREATE UNIQUE INDEX` (une migration future en ajoutera), l'import
    pouvait insérer les doublons que la contrainte refuse, et le `CREATE UNIQUE
    INDEX` de sortie échouait alors sur ces doublons : contrainte perdue, doublons
    installés, et un simple message dans le journal.
    """
    with Database(tmp_path / "x.sqlite") as db:
        db._conn.execute("CREATE UNIQUE INDEX idx_files_unique_essai ON files(path)")  # noqa: SLF001
        db._conn.commit()  # noqa: SLF001
        scan = db.start_scan("a.csv")
        db.upsert_files([_row("a.txt")], scan)
        with db.bulk_load(analyze=False):
            assert "idx_files_unique_essai" in _index_names(db), "la contrainte doit tenir"
            with pytest.raises(sqlite3.IntegrityError):
                db._conn.execute(  # noqa: SLF001
                    "INSERT INTO files(path_key, path, name, first_seen_scan_id,"
                    " last_seen_scan_id, updated_at)"
                    " VALUES('autre', '\\\\srv\\part\\dossier\\a.txt', 'a.txt', ?, ?, 'now')",
                    (scan, scan),
                )
        assert "idx_files_unique_essai" in _index_names(db)
        assert _index_definitions(db)["idx_files_unique_essai"] == normalize_index_sql(
            "CREATE UNIQUE INDEX idx_files_unique_essai ON files(path)"
        )


def test_bulk_load_drops_then_recreates_indexes(tmp_path: Path) -> None:
    """Pendant le chargement : plus d'index secondaire ; après : tous, à l'identique."""
    with Database(tmp_path / "x.sqlite") as db:
        avant = {
            str(r[0]): str(r[1])
            for r in db.query_values(
                "SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name='files'"
                " AND sql IS NOT NULL"
            )
        }
        with db.bulk_load():
            assert _index_names(db) == set()
            # l'index UNIQUE implicite de `path_key` reste : l'upsert s'en sert
            scan = db.start_scan("a.csv")
            assert db.upsert_files([_row("a.txt"), _row("a.txt")], scan) == (1, 0, 1)
        apres = {
            str(r[0]): str(r[1])
            for r in db.query_values(
                "SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name='files'"
                " AND sql IS NOT NULL"
            )
        }
        assert apres == avant
        assert db.counts()["files"] == 1


def _chargement_qui_echoue(db: Database) -> None:
    """Chargement en masse qui écrit une ligne puis part en erreur."""
    with db.bulk_load():
        db.upsert_files([_row("a.txt")], db.start_scan("a.csv"))
        raise RuntimeError("import interrompu")


def test_bulk_load_recreates_indexes_after_failure(tmp_path: Path) -> None:
    """Un chargement qui échoue laisse quand même la base avec tous ses index."""
    with Database(tmp_path / "x.sqlite") as db:
        with pytest.raises(RuntimeError, match="import interrompu"):
            _chargement_qui_echoue(db)
        assert _index_names(db) == set(FILES_INDEXES)
        assert db.counts()["files"] == 1


def test_missing_indexes_are_rebuilt_when_the_database_is_reopened(tmp_path: Path) -> None:
    """Processus tué pendant un import : la réouverture reconstruit les index manquants."""
    path = tmp_path / "x.sqlite"
    with Database(path) as db:
        db.upsert_files([_row("a.txt")], db.start_scan("a.csv"))
    # simulation d'une interruption : les index secondaires ont disparu
    conn = sqlite3.connect(path)
    for name in FILES_INDEXES:
        conn.execute(f"DROP INDEX {name}")
    conn.commit()
    conn.close()

    with Database(path) as db:
        assert _index_names(db) == set(FILES_INDEXES)
        assert db.counts()["files"] == 1


def test_unchanged_updates_are_batched_without_changing_the_result(tmp_path: Path) -> None:
    """Les mises à jour « inchangé » différées gardent l'ordre du fichier.

    Même chemin deux fois dans le même lot, la seconde fois avec un contenu
    modifié : c'est la dernière ligne qui doit gagner.
    """
    with Database(tmp_path / "x.sqlite") as db:
        scan1 = db.start_scan("a.csv")
        db.upsert_files([_row("a.txt")], scan1)
        scan2 = db.start_scan("b.csv")
        assert db.upsert_files(
            [
                _row("a.txt", access="03/01/2026 10:00:00"),
                _row("a.txt", fast_hash="cccc", access="04/01/2026 10:00:00"),
            ],
            scan2,
        ) == (0, 1, 1)
        row = db.query("SELECT access_time, access_key, content_version FROM files")[0]
        assert str(row["access_time"]) == "04/01/2026 10:00:00"
        assert str(row["access_key"]) == "20260104"
        assert int(row["content_version"]) == 2


def test_iter_files_non_trie_parcourt_tout_par_tranches(tmp_path: Path) -> None:
    """`ordered=False` : mêmes fichiers, par `id` croissant, quelle que soit la tranche."""
    with Database(tmp_path / "x.sqlite") as db:
        scan = db.start_scan("a.csv")
        db.upsert_files([_row(f"f{i}.txt", size=100 + i) for i in range(25)], scan)
        trie = {f.id for f in db.iter_files()}
        for batch in (1, 7, 25, 1000):
            rows = list(db.iter_files(ordered=False, batch=batch))
            assert [f.id for f in rows] == sorted(f.id for f in rows), batch
            assert {f.id for f in rows} == trie, batch
        # le filtre par statut tient aussi la pagination
        premier = next(db.iter_files(ordered=False))
        db.set_file_status(premier.id, FileStatus.DONE)
        restants = list(db.iter_files(FileStatus.PENDING, ordered=False, batch=2))
        assert len(restants) == 24
        assert premier.id not in {f.id for f in restants}


def test_iter_files_non_trie_supporte_les_ecritures_pendant_le_parcours(tmp_path: Path) -> None:
    """Le plan écrit pendant qu'il lit : aucune ligne sautée ni revue."""
    with Database(tmp_path / "x.sqlite") as db:
        scan = db.start_scan("a.csv")
        db.upsert_files([_row(f"f{i}.txt") for i in range(20)], scan)
        vus: list[int] = []
        for row in db.iter_files(ordered=False, batch=3):
            vus.append(row.id)
            db.apply_plan([(row.id, FileStatus.EXCLUDED, "essai", 7)])
        assert vus == sorted(vus)
        assert len(vus) == len(set(vus)) == 20
        assert all(f.status == FileStatus.EXCLUDED for f in db.iter_files())


def test_apply_plan_regroupe_sans_changer_le_resultat(tmp_path: Path) -> None:
    """`executemany` par tranches : mêmes statuts, mêmes scores, mêmes compteurs."""
    decisions: list[tuple[int, FileStatus, str | None, int]] = []
    etats: list[list[tuple[int, str, str | None, int]]] = []
    comptes: list[tuple[int, int]] = []
    for batch in (1, 3, 1000):
        with Database(tmp_path / f"b{batch}.sqlite") as db:
            scan = db.start_scan("a.csv")
            db.upsert_files([_row(f"f{i}.txt") for i in range(12)], scan)
            fichiers = list(db.iter_files(ordered=False))
            db.set_file_status(fichiers[0].id, FileStatus.DONE)
            db.set_file_status(fichiers[1].id, FileStatus.ERROR, "boum")
            decisions = [
                (f.id, FileStatus.EXCLUDED if i % 3 else FileStatus.PENDING, "zip", 10 + i)
                for i, f in enumerate(fichiers)
            ]
            comptes.append(db.apply_plan(decisions, batch=batch))
            etats.append(
                [
                    (
                        int(r["id"]),
                        str(r["status"]),
                        r["exclusion_reason"],
                        int(r["priority_score"]),
                    )
                    for r in db.query(
                        "SELECT id, status, exclusion_reason, priority_score FROM files ORDER BY id"
                    )
                ]
            )
            # un `done` et un `error` gardent leur statut, mais voient leur score rafraîchi
            assert db.get_file(fichiers[0].id).status == FileStatus.DONE  # type: ignore[union-attr]
            assert db.get_file(fichiers[1].id).status == FileStatus.ERROR  # type: ignore[union-attr]
    assert comptes[0] == comptes[1] == comptes[2]
    assert etats[0] == etats[1] == etats[2]


def test_migration_interrompue_laisse_la_base_intacte(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Une migration qui échoue en cours de route ne laisse pas la base entre deux états.

    `executescript()` validait implicitement chaque instruction : après une coupure,
    les colonnes v6 existaient mais `schema_version` valait toujours 5, et la
    réouverture rejouait la migration → `duplicate column name`, base inouvrable
    définitivement. Ici on interrompt au milieu et on exige que la base reste
    ouvrable, en v5, avec ses données.
    """
    path = tmp_path / "campagne.sqlite"
    _v5_database(path)
    vrai_decoupage = db_module.split_sql_statements

    def decoupage_qui_echoue(script: str) -> list[str]:
        statements = vrai_decoupage(script)
        # Coupure au milieu de la migration, après au moins un ALTER TABLE.
        return [*statements[:2], "SELECT panne_simulee()"]

    monkeypatch.setattr(db_module, "split_sql_statements", decoupage_qui_echoue)
    with pytest.raises(sqlite3.OperationalError) as echec:
        Database(path).close()
    assert "panne_simulee" in str(echec.value), "l'erreur d'origine ne doit pas être masquée"

    monkeypatch.undo()
    with Database(path) as db:  # la base rouvre et se migre normalement
        assert db.schema_version == SCHEMA_VERSION
        assert db.counts()["files"] == 3
        colonnes = {str(r[1]) for r in db.query_values("PRAGMA table_info(files)")}
        assert {"access_key", "write_key"} <= colonnes


def test_bulk_load_ne_masque_pas_l_erreur_et_remet_tous_les_index(tmp_path: Path) -> None:
    """Le `finally` qui reconstruit les index ne doit ni lever ni effacer la vraie cause.

    SQLite retire le `IF NOT EXISTS` du DDL stocké : si un index est déjà revenu
    (une seconde connexion l'a reconstruit), le `CREATE` levait, **remplaçait**
    l'exception du corps et abandonnait la boucle — 4 index sur 11 restaurés, et
    l'utilisateur voyait « index already exists » au lieu de la panne réelle.
    """

    def import_qui_echoue(db: Database) -> None:
        with db.bulk_load(analyze=False):
            db._conn.execute("CREATE INDEX idx_files_fast_hash ON files(fast_hash)")  # noqa: SLF001
            raise RuntimeError("panne réelle de l'import")

    with Database(tmp_path / "x.sqlite") as db:
        attendu = len(FILES_INDEXES)
        with pytest.raises(RuntimeError, match="panne réelle"):
            import_qui_echoue(db)
        noms = {
            str(r[0])
            for r in db.query_values(
                "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='files'"
                " AND sql IS NOT NULL"
            )
        }
        assert noms == set(FILES_INDEXES)
        assert len(noms) == attendu


# ------------------------------------- marqueur d'import (verrou de la seconde connexion)


def _pose_marqueur(path: Path, pid: int, quand: datetime) -> None:
    """Écrit à la main un marqueur `bulk_load` (pid + horodatage) dans `meta`."""
    conn = sqlite3.connect(path)
    conn.execute(
        "INSERT INTO meta(key, value) VALUES(?, ?)"
        " ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (BULK_LOCK_KEY, f"{pid}|{quand.isoformat(timespec='seconds')}"),
    )
    conn.commit()
    conn.close()


def _sans_index(path: Path) -> None:
    """Simule un import interrompu : les index secondaires de `files` ont disparu."""
    conn = sqlite3.connect(path)
    for name in FILES_INDEXES:
        conn.execute(f"DROP INDEX IF EXISTS {name}")
    conn.commit()
    conn.close()


def test_une_seconde_connexion_ne_reconstruit_pas_les_index_pendant_un_import(
    tmp_path: Path,
) -> None:
    """B2 — la fenêtre ouvre une seconde base pendant un import : elle ne doit pas écrire.

    `_ensure_files_indexes` tourne à **chaque** ouverture de `Database`, et un
    `CREATE INDEX` est une écriture. Ouverte pendant qu'un `bulk_load` tient le
    verrou (`gui/lazy.py::LazyScreen._start` le fait dès qu'un écran se rafraîchit),
    cette seconde connexion tuait l'import sur `database is locked` après plusieurs
    minutes de travail.
    """
    path = tmp_path / "x.sqlite"
    reconstruits: list[list[str]] = []
    with Database(path) as db:
        db.upsert_files([_row("a.txt")], db.start_scan("a.csv"))
        with db.bulk_load(analyze=False):
            assert _index_names(db) == set(), "l'import travaille bien sans index"
            with Database(path) as autre:  # la fenêtre, pendant l'import
                reconstruits.append(autre._ensure_files_indexes())  # noqa: SLF001
                assert _index_names(autre) == set(), "aucun index recréé pendant l'import"
            db.upsert_files([_row("b.txt")], db.start_scan("b.csv"))
    assert reconstruits == [[]], "l'ouverture concurrente n'a rien reconstruit"
    with Database(path) as db:
        assert _index_names(db) == set(FILES_INDEXES), "l'import les recrée en sortant"
        assert db.counts()["files"] == 2
        assert db.query("SELECT 1 FROM meta WHERE key=?", (BULK_LOCK_KEY,)) == []


def test_le_marqueur_dun_import_tue_ne_bloque_pas_la_reconstruction(tmp_path: Path) -> None:
    """Processus mort : le marqueur ne vaut plus rien, les index reviennent aussitôt."""
    path = tmp_path / "x.sqlite"
    with Database(path) as db:
        db.upsert_files([_row("a.txt")], db.start_scan("a.csv"))
    _sans_index(path)
    _pose_marqueur(path, 999_999_999, datetime.now(UTC))  # pid qui n'existe pas
    with Database(path) as db:
        assert _index_names(db) == set(FILES_INDEXES)


def test_un_marqueur_perime_ne_bloque_pas_la_reconstruction(tmp_path: Path) -> None:
    """Numéro de processus réattribué : passé le délai, le marqueur n'est plus cru."""
    path = tmp_path / "x.sqlite"
    with Database(path) as db:
        db.upsert_files([_row("a.txt")], db.start_scan("a.csv"))
    _sans_index(path)
    vieux = datetime.now(UTC) - timedelta(seconds=BULK_LOCK_TTL_S + 60)
    _pose_marqueur(path, os.getpid(), vieux)  # pid bien vivant, mais marqueur trop vieux
    with Database(path) as db:
        assert _index_names(db) == set(FILES_INDEXES)


def test_un_marqueur_frais_dun_processus_vivant_fait_abdiquer(tmp_path: Path) -> None:
    """Marqueur frais + pid vivant : la reconstruction est laissée à l'import."""
    path = tmp_path / "x.sqlite"
    with Database(path) as db:
        db.upsert_files([_row("a.txt")], db.start_scan("a.csv"))
    _sans_index(path)
    _pose_marqueur(path, os.getpid(), datetime.now(UTC))
    with Database(path) as db:
        assert _index_names(db) == set(), "un import est en cours : on n'écrit pas"
    # marqueur illisible → il ne protège rien
    conn = sqlite3.connect(path)
    conn.execute("UPDATE meta SET value='n importe quoi' WHERE key=?", (BULK_LOCK_KEY,))
    conn.commit()
    conn.close()
    with Database(path) as db:
        assert _index_names(db) == set(FILES_INDEXES)


# ------------------------------------------- sauvegarde d'avant migration (BDR4)


def test_ouverture_refusee_si_la_sauvegarde_davant_migration_echoue(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Disque plein : refuser d'ouvrir plutôt que migrer sans filet.

    L'échec de la copie était journalisé puis ignoré, et la migration partait
    quand même — précisément dans le cas (plus de place) où elle a le plus de
    chances de casser en route, et sans copie pour revenir en arrière.
    """
    path = tmp_path / "campagne.sqlite"
    _v5_database(path)

    def disque_plein(_self: Database, _target: object) -> None:
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(Database, "backup_to", disque_plein)
    with pytest.raises(MigrationBackupError, match="sauvegarde avant migration impossible"):
        Database(path)
    monkeypatch.undo()

    conn = sqlite3.connect(path)  # la base est restée en v5, intacte et lisible
    assert conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()[0] == "5"
    assert conn.execute("SELECT COUNT(*) FROM files").fetchone()[0] == 3
    colonnes = {str(r[1]) for r in conn.execute("PRAGMA table_info(files)")}
    assert "access_key" not in colonnes, "aucune migration n'a démarré"
    conn.close()

    with Database(path) as db:  # place libérée : l'ouverture suivante migre normalement
        assert db.schema_version == SCHEMA_VERSION
        assert db.counts()["files"] == 3
        assert list(backup_dir_for(path).glob("*.sqlite")), "et la sauvegarde est bien là"


def test_une_ouverture_refusee_ne_laisse_pas_de_connexion_ouverte(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Sous Windows, une connexion fuitée empêcherait de renommer ou supprimer la base."""
    path = tmp_path / "campagne.sqlite"
    _v5_database(path)
    ouvertes: list[sqlite3.Connection] = []
    vrai_connect = sqlite3.connect

    def espion(*args: object, **kwargs: object) -> sqlite3.Connection:
        conn = vrai_connect(*args, **kwargs)  # type: ignore[arg-type]
        ouvertes.append(conn)
        return conn

    monkeypatch.setattr(sqlite3, "connect", espion)
    monkeypatch.setattr(
        Database, "backup_to", lambda _s, _t: (_ for _ in ()).throw(OSError("disque plein"))
    )
    with pytest.raises(MigrationBackupError):
        Database(path)
    for conn in ouvertes:
        with pytest.raises(sqlite3.ProgrammingError, match="[Cc]losed"):
            conn.execute("SELECT 1")


# --------------------------------------------- sélection du run par identifiants (P1)


def _campagne_a_analyser(path: Path, nombre: int) -> Database:
    """Base peuplée de `nombre` fichiers `pending` aux priorités variées."""
    db = Database(path)
    scan = db.start_scan("a.csv")
    db.upsert_files([_row(f"f{i:03d}.txt", size=100 + i) for i in range(nombre)], scan)
    db.apply_plan([(f.id, FileStatus.PENDING, None, i % 7) for i, f in enumerate(db.iter_files())])
    return db


def test_select_pending_ids_puis_files_by_ids_redonnent_select_pending(tmp_path: Path) -> None:
    """P1 — la sélection par identifiants est la même sélection, dans le même ordre.

    Le pipeline gardait 700 797 `FileRow` (1 722 Mo) du début à la fin d'un run de
    plusieurs heures. Il ne garde plus que les identifiants et recharge un lot à la
    fois : à condition que les deux chemins désignent exactement les mêmes fichiers,
    dans le même ordre.
    """
    with _campagne_a_analyser(tmp_path / "x.sqlite", 40) as db:
        complet = db.select_pending(10**9, prompt_hash="p", model="m")
        ids = db.select_pending_ids(10**9, prompt_hash="p", model="m")
        assert ids == [f.id for f in complet]
        assert db.files_by_ids(ids) == complet
        # lot par lot, comme le pipeline
        recharge = [
            row
            for start in range(0, len(ids), 7)
            for row in db.files_by_ids(ids[start : start + 7])
        ]
        assert recharge == complet
        # `limit` : même troncature des deux côtés
        for limite in (0, 1, 13, 40, 999):
            assert db.select_pending_ids(limite, prompt_hash="p", model="m") == [
                f.id for f in db.select_pending(limite, prompt_hash="p", model="m")
            ]


def test_count_pending_compte_ce_que_la_selection_rendrait(tmp_path: Path) -> None:
    with _campagne_a_analyser(tmp_path / "x.sqlite", 25) as db:
        assert db.count_pending(prompt_hash="p", model="m") == 25
        for limite in (0, 1, 10, 25, 999):
            assert db.count_pending(prompt_hash="p", model="m", limit=limite) == len(
                db.select_pending_ids(limite, prompt_hash="p", model="m")
            )
        # un fichier analysé sort de la sélection, comme pour `select_pending`
        premier = db.select_pending_ids(1, prompt_hash="p", model="m")[0]
        db.store_analysis(premier, None, 1, prompt_hash="p", model="m", analysis=_analysis("f"))
        assert db.count_pending(prompt_hash="p", model="m") == 24
        assert db.count_pending(prompt_hash="autre", model="m") == 24, "un `done` reste hors plan"


def test_files_by_ids_respecte_l_ordre_et_ignore_les_inconnus(tmp_path: Path) -> None:
    """L'ordre demandé fait foi (c'est lui qui porte la priorité), les absents s'effacent."""
    with _campagne_a_analyser(tmp_path / "x.sqlite", 1_200) as db:
        ids = db.select_pending_ids(10**9, prompt_hash="p", model="m")
        melange = [ids[7], ids[0], ids[999], ids[3]]
        assert [f.id for f in db.files_by_ids(melange)] == melange
        assert db.files_by_ids([]) == []
        assert [f.id for f in db.files_by_ids([ids[1], 10**9, ids[2]])] == [ids[1], ids[2]]
        # plus de 500 identifiants : le découpage en paquets ne change ni l'ordre ni le nombre
        assert [f.id for f in db.files_by_ids(ids)] == ids


# --------------------------------------- filtres de l'écran Résultats en SQL (P2)

_SEVERITE = {"C3": 0, "C2": 1, "C1": 2, "C0": 3, "N/A": 4}


def _ordre_python(r: Any) -> tuple[int, int, str]:
    """Copie de `gui.tab_results._display_order` — la référence à reproduire en SQL."""
    sec = r["security_classification"]
    status = r["status"] or ""
    if sec:
        return (0, _SEVERITE.get(sec, 5), str(r["name"]).lower())
    return ({"error": 1, "done": 2}.get(status, 3), 0, str(r["name"]).lower())


def _reference_python(
    db: Database,
    *,
    security: str | None = None,
    rgpd: str | None = None,
    review: str | None = None,
    search: str | None = None,
) -> list[sqlite3.Row]:
    """Ce que l'écran calculait en Python : tout relire, filtrer, trier.

    `search` est replié en minuscules comme l'écran le faisait avant de comparer
    (`self.search_var.get().strip().lower()`).
    """
    needle = search.lower() if search else None

    def keep(r: sqlite3.Row) -> bool:
        if security is not None and (r["security_classification"] or "") != security:
            return False
        if rgpd is not None and (r["rgpd_risk_level"] or "") != rgpd:
            return False
        if review is not None and (r["review_status"] or "") != review:
            return False
        haystack = f"{r['path']} {r['resume'] or ''} {r['owner'] or ''}".lower()
        return not (needle and needle not in haystack)

    return sorted((r for r in db.latest_analyses() if keep(r)), key=_ordre_python)


def _campagne_resultats(path: Path) -> Database:
    """Campagne variée : analysées de toutes gravités, erreurs, exclus, revues, accents."""
    db = Database(path)
    scan = db.start_scan("a.csv")
    noms = [
        "Étude C3.txt",
        "etude c2.txt",
        "ANALYSE C1.txt",
        "analyse c0.txt",
        "sans_analyse.txt",
        "erreur 100%.txt",
        "fichier_1.txt",
        "exclu.zip",
        "propriétaire.txt",
    ]
    db.upsert_files([_row(n, fast_hash=f"h{i}") for i, n in enumerate(noms)], scan)
    par_nom = {f.name: f for f in db.iter_files()}
    for nom, sec, risque in (
        ("Étude C3.txt", "C3", "critical"),
        ("etude c2.txt", "C2", "high"),
        ("ANALYSE C1.txt", "C1", "low"),
        ("analyse c0.txt", "C0", "none"),
        ("erreur 100%.txt", "N/A", "none"),
        ("fichier_1.txt", "C2", "medium"),
        ("propriétaire.txt", "C1", "none"),
    ):
        f = par_nom[nom]
        analyse = replace(
            _analysis(nom),
            resume=f"résumé de {nom} 100% sûr",
            security=DomainAnalysis(sec, 80, {"justification": "j"}),
            rgpd=DomainAnalysis(risque, 70, {"data_types": []}),
        )
        db.store_analysis(f.id, None, 1, prompt_hash="p", model="m", analysis=analyse)
    db.set_file_status(par_nom["sans_analyse.txt"].id, FileStatus.ERROR, "boum")
    db.set_file_status(par_nom["exclu.zip"].id, FileStatus.EXCLUDED, "extension exclue")
    db.set_review(par_nom["Étude C3.txt"].id, "validated", reviewer="moi")
    db.set_review(par_nom["etude c2.txt"].id, "corrected", corrected_security="C3")
    db.set_review(par_nom["ANALYSE C1.txt"].id, "to_review")
    return db


_CAS_FILTRES: list[dict[str, str | None]] = [
    {},
    {"security": "C3"},
    {"security": "C2"},
    {"security": "N/A"},
    {"security": ""},
    {"rgpd": "critical"},
    {"rgpd": "none"},
    {"rgpd": ""},
    {"review": ""},
    {"review": "validated"},
    {"review": "to_review"},
    {"review": "corrected"},
    {"search": "etude"},
    {"search": "ANALYSE"},
    {"search": "résumé"},
    {"search": "dossier"},
    {"search": "dom\\x"},
    {"search": "100%"},
    {"search": "fichier_1"},
    {"search": "introuvable"},
    {"security": "C2", "rgpd": "high", "review": "corrected"},
    {"security": "C1", "review": "", "search": "propri"},
]


@pytest.mark.parametrize(
    "filtres", _CAS_FILTRES, ids=lambda f: "-".join(f"{k}={v!r}" for k, v in f.items()) or "aucun"
)
def test_les_filtres_sql_rendent_exactement_ce_que_python_rendait(
    tmp_path: Path, filtres: dict[str, str | None]
) -> None:
    """P2 — filtres, tri et limite descendus en SQL : mêmes lignes, même ordre, même total.

    L'écran relisait les 934 028 lignes d'une campagne (9,3 s, 950 Mo) pour n'en
    afficher 1 000, et recommençait à chaque filtre comme après chaque validation.
    Le SQL doit rendre le même résultat, jokers `%`, `_` et casse compris — le
    filtrage Python comparait des sous-chaînes, pas des motifs.
    """
    with _campagne_resultats(tmp_path / "x.sqlite") as db:
        attendu = _reference_python(db, **filtres)  # type: ignore[arg-type]
        obtenu = sorted(db.latest_analyses(**filtres, display_order=True), key=_ordre_python)  # type: ignore[arg-type]
        assert [dict(r) for r in obtenu] == [dict(r) for r in attendu]
        assert db.count_latest_analyses(**filtres) == len(attendu)  # type: ignore[arg-type]


def test_la_limite_sql_rend_le_debut_exact_de_la_liste_triee(tmp_path: Path) -> None:
    """`limit` prend bien les N premières lignes de l'ordre d'affichage, pas N au hasard."""
    with _campagne_resultats(tmp_path / "x.sqlite") as db:
        complet = _reference_python(db)
        for limite in (1, 2, 5, len(complet), len(complet) + 10):
            page = sorted(db.latest_analyses(limit=limite, display_order=True), key=_ordre_python)
            assert [dict(r) for r in page] == [dict(r) for r in complet[:limite]]
        assert db.count_latest_analyses() == len(complet), "le total ignore la limite"


def test_l_ordre_daffichage_place_les_analyses_par_gravite_puis_erreurs_puis_reste(
    tmp_path: Path,
) -> None:
    """Le tri SQL seul (sans re-tri Python) range déjà les fichiers dans les bons paliers."""
    with _campagne_resultats(tmp_path / "x.sqlite") as db:
        rangs = [_ordre_python(r)[:2] for r in db.latest_analyses(display_order=True)]
        assert rangs == sorted(rangs), f"paliers dans le désordre : {rangs}"
        noms = [str(r["name"]) for r in db.latest_analyses(display_order=True)]
        assert noms[0] == "Étude C3.txt", "le plus sensible d'abord"
        assert noms[-1] == "exclu.zip", "ni analysé, ni en erreur, ni done : bon dernier"
        assert noms[-2] == "sans_analyse.txt", "en erreur : juste après les analysés"


def test_latest_analyses_sans_filtre_reste_trie_par_chemin_pour_les_exports(
    tmp_path: Path,
) -> None:
    """Les exports consomment ce curseur : leur ordre historique ne doit pas bouger."""
    with _campagne_resultats(tmp_path / "x.sqlite") as db:
        chemins = [str(r["path"]) for r in db.latest_analyses()]
        assert chemins == sorted(chemins)
        # une fiche seule reste accessible par identifiant
        premier = next(iter(db.latest_analyses()))
        fiche = list(db.latest_analyses(file_id=int(premier["id"])))
        assert len(fiche) == 1
        assert dict(fiche[0]) == dict(premier)


def test_count_latest_analyses_sans_filtre_compte_tous_les_fichiers(tmp_path: Path) -> None:
    with _campagne_resultats(tmp_path / "x.sqlite") as db:
        assert db.count_latest_analyses() == db.counts()["files"] == 9


def test_le_compromis_sur_les_accents_est_celui_qui_est_documente(tmp_path: Path) -> None:
    """`LOWER()` et `LIKE` de SQLite ne replient que l'ASCII : ce que ça change, exactement.

    Le compromis est assumé (`Database.latest_analyses`) mais il doit rester
    **connu** : une lettre accentuée majuscule ne se replie pas. Casse ASCII et
    accents déjà en minuscules, eux, marchent comme avant.
    """
    with _campagne_resultats(tmp_path / "x.sqlite") as db:

        def noms(**filtres: str) -> set[str]:
            return {str(r["name"]) for r in db.latest_analyses(**filtres)}  # type: ignore[arg-type]

        assert noms(search="ANALYSE") == noms(search="analyse")  # casse ASCII : repliée
        assert "propriétaire.txt" in noms(search="propriétaire")  # accent minuscule : trouvé
        assert "Étude C3.txt" in noms(search="Étude")  # accent majuscule tel quel : trouvé
        assert "Étude C3.txt" not in noms(search="étude"), "limite connue de LOWER() SQLite"
        assert _reference_python(db, search="étude"), "Python, lui, l'aurait trouvé"

        # tri : deux noms que seul un accent sépare ne se rangent pas comme en Python,
        # d'où le re-tri exact des lignes rendues côté écran.
        approche = [str(r["name"]) for r in db.latest_analyses(display_order=True)]
        exact = [str(r["name"]) for r in _reference_python(db)]
        assert sorted(approche) == sorted(exact), "les mêmes lignes, quel que soit l'ordre"
        assert sorted(approche, key=str.lower) == sorted(exact, key=str.lower)
