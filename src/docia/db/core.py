"""Socle de `Database` : connexion, pragmas, transaction, migrations, chargement en masse.

Les opérations par table sont des mixins (`docia.db.files`, `.blocks`, `.analyses`,
`.prompts`, `.stats`) qui n'ont besoin que de ce socle ; `docia.db.database` les assemble.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import sys
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Self

from docia.db.schema import _MIGRATIONS, FILES_INDEXES, SCHEMA_VERSION
from docia.db.sql import (
    _UNIQUE_INDEX_RE,
    split_sql_statements,
)

BACKUP_DIR_SUFFIX = ".backups"
"""Suffixe du dossier de sauvegardes, à côté de la base (`docia.sqlite.backups`)."""

logger = logging.getLogger(__name__)


class MigrationBackupError(OSError):
    """La sauvegarde d'avant migration a échoué : la base n'est **pas** ouverte.

    Migrer sans filet, c'est risquer de perdre une campagne de plusieurs heures
    pour un disque plein. Hérite d'`OSError` : les appelants qui traitent déjà
    les échecs d'accès au fichier de base l'attrapent sans changement.
    """


def backup_dir_for(db_path: Path) -> Path:
    """Dossier de sauvegardes d'une base : `<base>.backups` (à côté du fichier)."""
    return db_path.with_name(db_path.name + BACKUP_DIR_SUFFIX)


CAMPAIGN_NEW = "neuve"
"""Aucune base à ce chemin, ou fichier SQLite vide : `Database` peut la créer."""

CAMPAIGN_DOCIA = "docia"
"""Base docia existante (`meta.schema_version` présent)."""

CAMPAIGN_FOREIGN = "étrangère"
"""Fichier existant qui n'est pas une campagne docia : ne rien y greffer."""


def campaign_kind(target: str | Path) -> str:
    """`neuve`, `docia` ou `étrangère` — **sans rien créer ni modifier**.

    `Database(chemin)` crée le dossier manquant, le fichier manquant, et greffe les
    tables docia dans n'importe quel SQLite ouvrable. Une faute de frappe dans
    `--db` fabriquait donc une base vide, et `docia status` ou `docia report`
    annonçaient « 0 fichier, 0 sensible » en code retour 0, sur une campagne
    inventée : pour un outil dont la sortie justifie des suppressions, c'est le
    pire résultat possible. Les commandes de **lecture** regardent donc avant
    d'ouvrir (`cli._require_existing_campaign`) ; `init`, `ingest` et `scan`
    gardent le droit de créer.

    Même contrôle que `gui.app.campaign_kind`, qui a découvert le danger sur une
    base « contacts » d'un autre logiciel enrichie de douze tables docia pendant
    que le journal affirmait « aucune donnée effacée ».
    """
    path = Path(target)
    try:
        if not path.exists() or path.stat().st_size == 0:
            return CAMPAIGN_NEW
        con = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
    except (OSError, ValueError, sqlite3.Error):
        return CAMPAIGN_FOREIGN
    try:
        names = {
            str(r[0]) for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        if not names:
            return CAMPAIGN_NEW  # fichier SQLite vide : utilisable comme campagne neuve
        if "meta" not in names:
            return CAMPAIGN_FOREIGN
        row = con.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
        return CAMPAIGN_DOCIA if row else CAMPAIGN_FOREIGN
    except sqlite3.Error:
        return CAMPAIGN_FOREIGN  # pas un fichier SQLite (texte, archive, base corrompue)
    finally:
        con.close()


def _process_alive(pid: int) -> bool:
    """Vrai si le processus `pid` tourne encore sur cette machine.

    Sous Windows, `os.kill(pid, 0)` **tue** la cible (`TerminateProcess`) : le
    test passe donc par `OpenProcess` + `GetExitCodeProcess`.
    """
    if pid <= 0:
        return False
    if sys.platform == "win32":  # pragma: no cover - branche Windows
        import ctypes

        kernel32 = ctypes.windll.kernel32
        handle = kernel32.OpenProcess(0x1000, False, pid)  # PROCESS_QUERY_LIMITED_INFORMATION
        if not handle:
            return False
        try:
            code = ctypes.c_ulong()
            if not kernel32.GetExitCodeProcess(handle, ctypes.byref(code)):
                return False
            return int(code.value) == 259  # STILL_ACTIVE
        finally:
            kernel32.CloseHandle(handle)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:  # pragma: no cover - processus d'un autre utilisateur
        return True
    return True


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


BULK_CACHE_PAGES = -262_144
"""Cache SQLite pendant un chargement en masse : 256 Mo (valeur négative = kio)."""

BULK_LOCK_KEY = "bulk_load_owner"
"""Clé `meta` posée par `bulk_load` : `<pid>|<horodatage ISO UTC>`, validée en base.

Une seconde connexion ouverte pendant l'import (la fenêtre rafraîchit un écran
pendant qu'on charge un CSV) voit ce marqueur et **n'écrit pas** : sans lui,
`_ensure_files_indexes` lançait des `CREATE INDEX` — donc une écriture — pendant
que `bulk_load` tenait le verrou, et l'import mourait sur `database is locked`
après plusieurs minutes de travail.
"""

BULK_LOCK_TTL_S = 6 * 3_600
"""Durée au-delà de laquelle un marqueur `bulk_load` n'est plus cru.

Le marqueur est retiré par le `finally` de `bulk_load` : il ne survit qu'à un
processus **tué**, et le test « ce pid tourne-t-il encore ? » suffit alors à
débloquer la reconstruction dès la réouverture suivante. Ce délai n'est que le
dernier filet contre la réutilisation d'un numéro de processus (fréquente sous
Windows) : six heures couvrent très largement le plus long import observé
(quelques minutes pour 934 000 fichiers) sans immobiliser les index une journée.
"""

_TOUCH_FLUSH = 1_000
"""Mises à jour « fichier inchangé » accumulées avant un `executemany`."""

REVIEW_STATUSES = ("to_review", "validated", "corrected")

ITER_FILES_BATCH = 10_000
"""Fichiers lus par aller-retour SQLite dans `iter_files(ordered=False)`."""

APPLY_PLAN_BATCH = 5_000
"""Décisions de plan regroupées par `executemany` dans `apply_plan`."""


class _DatabaseCore:
    """Accès à la base. Utiliser comme gestionnaire de contexte ou appeler `close()`."""

    read_only: bool
    """Vrai quand la base n'a pu être ouverte qu'en **lecture** (voir `_open_pragmas`)."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        try:
            self.read_only = self._open_pragmas()
            if not self.read_only:
                self._migrate()
                self._ensure_files_indexes()
        except BaseException:
            # Ouverture refusée (sauvegarde impossible, migration interrompue) : sans
            # ce `close`, la connexion — et son verrou WAL — survivait à l'exception
            # sans qu'aucun objet ne la référence.
            self._conn.close()
            raise

    _WRITE_PRAGMAS = ("PRAGMA journal_mode=WAL", "PRAGMA synchronous=NORMAL")
    """Réglages qui **écrivent** dans la base : refusés, ils n'interdisent pas de lire."""

    _READ_PRAGMAS = ("PRAGMA foreign_keys=ON", "PRAGMA busy_timeout=5000")
    """Réglages de session : acceptés même sur une base en lecture seule."""

    def _open_pragmas(self) -> bool:
        """Applique les réglages d'ouverture. Rend `True` si la base est en lecture seule.

        `PRAGMA journal_mode=WAL` est une **écriture**. Inconditionnel, il interdisait
        jusqu'à la simple lecture d'une campagne archivée : support en écriture
        protégée, dossier verrouillé, copie déposée sur un partage en lecture. Rendre
        un rapport sur une campagne close est pourtant un besoin ordinaire, et rien
        n'y écrit.

        Le cas normal ne change pas d'un pouce : les deux `PRAGMA` passent, la base
        s'ouvre en écriture, migrations et index compris. Le repli n'est tenté que si
        SQLite refuse, et seulement pour une base docia **déjà au schéma courant** :
        une base neuve ou plus ancienne a besoin d'écrire (création des tables,
        migration), et son refus est relayé tel quel — c'est le message d'erreur en
        une ligne que `cli.main` sait déjà rendre.
        """
        try:
            for pragma in self._WRITE_PRAGMAS:
                self._conn.execute(pragma)
        except sqlite3.OperationalError as refus:
            self._reopen_read_only(refus)
            read_only = True
        else:
            read_only = False
        for pragma in self._READ_PRAGMAS:
            self._conn.execute(pragma)
        return read_only

    def _reopen_read_only(self, refus: sqlite3.OperationalError) -> None:
        """Rouvre en lecture une base qui refuse l'écriture, ou relaie le refus."""
        if not self._can_read():
            # Base déjà en WAL dont le dossier est verrouillé : SQLite exige un
            # fichier `-shm` qu'il ne peut pas créer, et refuse jusqu'au `SELECT`.
            # `immutable=1` est le seul mode qui lise encore — il promet que le
            # fichier ne bougera pas, ce qui est exactement le cas d'une campagne
            # archivée sur un support protégé en écriture.
            try:
                conn = sqlite3.connect(
                    self.path.resolve().as_uri() + "?mode=ro&immutable=1",
                    uri=True,
                    check_same_thread=False,
                )
            except (OSError, ValueError, sqlite3.Error):
                raise refus from None
            conn.row_factory = sqlite3.Row
            self._conn.close()
            self._conn = conn
        version = self._readable_schema_version()
        if version != SCHEMA_VERSION:
            # Ni base docia (0), ni schéma courant : il faudrait écrire pour la créer
            # ou la migrer. Le refus d'origine dit la vraie cause.
            raise refus
        logger.warning(
            "campagne %s ouverte en lecture seule (écriture refusée : %s)", self.path, refus
        )

    def _can_read(self) -> bool:
        try:
            self._conn.execute("SELECT 1 FROM sqlite_master LIMIT 1").fetchone()
        except sqlite3.Error:
            return False
        return True

    def _readable_schema_version(self) -> int:
        """Version de schéma, ou 0 si la table `meta` est absente ou illisible."""
        try:
            return self.schema_version
        except sqlite3.Error:
            return 0

    def _refuse_if_read_only(self, operation: str) -> None:
        """Refuse une écriture d'emblée sur une base ouverte en lecture seule."""
        if self.read_only:
            raise sqlite3.OperationalError(
                f"{operation} impossible : {self.path} est ouverte en lecture seule"
            )

    # ------------------------------------------------------------------ infra
    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def close(self) -> None:
        """Ferme la connexion (idempotent via `with`)."""
        self._conn.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """`BEGIN` … `COMMIT`, `ROLLBACK` sur exception — sans jamais masquer l'erreur d'origine."""
        try:
            self._conn.execute("BEGIN")
            yield self._conn
            self._conn.execute("COMMIT")
        except BaseException:
            # Le `ROLLBACK` peut lui-même échouer (plus de transaction active si le
            # corps a validé, base verrouillée…). Son échec ne doit jamais remplacer
            # l'exception d'origine : c'est elle qui dit ce qui s'est réellement passé.
            with suppress(sqlite3.Error):
                self._conn.execute("ROLLBACK")
            raise

    def query(self, sql: str, params: tuple[object, ...] = ()) -> list[sqlite3.Row]:
        """Exécute un `SELECT` et rend les lignes (accès lecture seule pour `views.py`)."""
        return list(self._conn.execute(sql, params))

    def query_values(self, sql: str, params: tuple[object, ...] = ()) -> list[tuple[Any, ...]]:
        """Comme `query`, mais rend des tuples bruts, sans `sqlite3.Row`.

        Réservé aux agrégations qui parcourent des centaines de milliers de
        lignes (répartition par répertoire) : un objet de moins par ligne.
        """
        cursor = self._conn.cursor()
        cursor.row_factory = None
        try:
            rows: list[tuple[Any, ...]] = cursor.execute(sql, params).fetchall()
            return rows
        finally:
            cursor.close()

    def backup_to(self, path: str | Path) -> None:
        """Copie cohérente de la base vers `path` (API `sqlite3.Connection.backup`).

        Utilisable pendant un run : SQLite garantit un instantané cohérent.
        """
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        dest = sqlite3.connect(str(target))
        try:
            self._conn.backup(dest)
        finally:
            dest.close()

    @property
    def schema_version(self) -> int:
        """Version de schéma lue dans `meta` (0 si la table est absente)."""
        row = self._conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
        return int(row["value"]) if row else 0

    def _backup_before_migration(self) -> None:
        """Copie la base telle quelle avant d'appliquer une migration de schéma.

        Ne fait rien pour une base neuve (aucune table) ni pour une base déjà à
        jour. Une copie impossible (disque plein, droits) **interrompt l'ouverture**
        (`MigrationBackupError`) : une migration change le schéma d'une campagne de
        plusieurs heures, et le seul cas où la sauvegarde échoue — le disque plein —
        est précisément celui où la migration a le plus de chances de casser en
        route. Mieux vaut un message clair (« libérez de la place ») qu'une base à
        moitié migrée sans copie de secours.

        Le nom est **horodaté** et n'écrase jamais un fichier existant : une
        migration interrompue laissait autrefois une base à moitié migrée, et
        chaque nouvelle tentative d'ouverture — le réflexe de l'utilisateur —
        recopiait cette base cassée par-dessus la seule sauvegarde saine.
        """
        names = {
            str(r[0])
            for r in self._conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        if not names:
            return  # base neuve : rien à sauvegarder
        current = self.schema_version if "meta" in names else 0
        if current >= SCHEMA_VERSION:
            return
        stamp = datetime.now().strftime("%Y%m%dT%H%M%S")
        base = f"{self.path.stem}_avant_migration_v{SCHEMA_VERSION}_{stamp}"
        target = backup_dir_for(self.path) / f"{base}.sqlite"
        suffix = 1
        while target.exists():  # jamais deux sauvegardes dans le même fichier
            target = backup_dir_for(self.path) / f"{base}_{suffix}.sqlite"
            suffix += 1
        try:
            # `backup_to` passe par l'API `sqlite3.Connection.backup`, qui inclut ce
            # qui n'est encore que dans le journal WAL. Une simple copie du fichier
            # principal perdait tout ce qui était validé mais pas encore reporté —
            # c'est-à-dire précisément ce qu'une sauvegarde d'avant-migration doit
            # protéger après un arrêt brutal.
            self.backup_to(target)
        except (OSError, sqlite3.Error) as exc:
            with suppress(OSError):  # copie partielle : elle ne protège rien
                target.unlink(missing_ok=True)
            raise MigrationBackupError(
                f"sauvegarde avant migration impossible ({target}) : {exc}. "
                f"La base n'a pas été migrée en v{SCHEMA_VERSION} et reste utilisable "
                "par la version précédente : libérez de la place (ou corrigez les droits) "
                "sur ce dossier, puis rouvrez la campagne."
            ) from exc
        logger.info("sauvegarde avant migration v%s → %s", SCHEMA_VERSION, target)

    def _migrate(self) -> None:
        """Applique les migrations manquantes, **une transaction par version**.

        `executescript()` valide implicitement la transaction en cours avant de
        s'exécuter : encadré par `transaction()`, il n'apportait donc aucune
        atomicité. Une interruption (coupure, disque plein) laissait la base à
        moitié migrée — colonnes créées mais `schema_version` inchangé — et la
        réouverture rejouait la migration depuis le début : `duplicate column
        name`, base inouvrable, définitivement. Les instructions sont désormais
        jouées une par une dans une vraie transaction : soit la version passe en
        entier, soit la base reste exactement dans son état d'avant.
        """
        self._backup_before_migration()
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        current = self.schema_version
        for version, sql in _MIGRATIONS:
            if version > current:
                with self.transaction() as conn:
                    for statement in split_sql_statements(sql):
                        conn.execute(statement)
                    conn.execute(
                        "INSERT INTO meta(key, value) VALUES('schema_version', ?) "
                        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                        (str(version),),
                    )
                current = version

    # --------------------------------------------------------- chargement en masse
    def _files_index_names(self) -> set[str]:
        """Noms des index présents sur `files` (index UNIQUE implicites compris)."""
        return {
            str(r[0])
            for r in self._conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='files'"
            )
        }

    def _bulk_load_owner(self) -> int | None:
        """Pid du chargement en masse en cours, ou None (aucun, périmé, ou mort).

        Le marqueur `meta[BULK_LOCK_KEY]` vaut `<pid>|<horodatage ISO>`. Il n'est
        cru que si les deux conditions tiennent : le processus qui l'a posé tourne
        encore **et** le marqueur a moins de `BULK_LOCK_TTL_S` (garde-fou contre un
        numéro de processus réattribué après un arrêt brutal).
        """
        row = self._conn.execute("SELECT value FROM meta WHERE key=?", (BULK_LOCK_KEY,)).fetchone()
        if row is None:
            return None
        pid_text, _, stamp = str(row[0]).partition("|")
        try:
            pid = int(pid_text)
            posed_at = datetime.fromisoformat(stamp)
        except ValueError:
            return None
        if abs((datetime.now(UTC) - posed_at).total_seconds()) > BULK_LOCK_TTL_S:
            return None
        return pid if _process_alive(pid) else None

    def _ensure_files_indexes(self) -> list[str]:
        """Filet à l'ouverture : recrée les index secondaires de `files` qui manquent.

        Un chargement en masse (`bulk_load`) travaille index supprimés. Son `finally`
        les recrée, mais un processus tué (ou une coupure de courant) laisserait la
        base sans eux : toutes les vues repasseraient en balayage complet sans que
        personne ne le voie. La vérification coûte une lecture de `sqlite_master`
        (moins d'une milliseconde) à chaque ouverture ; la reconstruction, elle, ne
        se déclenche que si un index manque vraiment.

        Elle **abdique** tant qu'un `bulk_load` est en cours (marqueur `meta`
        vivant, cf. `_bulk_load_owner`). Ce n'est pas une optimisation : ouvrir une
        seconde `Database` pendant un import n'a rien d'exceptionnel — la fenêtre le
        fait d'elle-même quand un écran se rafraîchit (`gui.lazy.LazyScreen._start`)
        — et un `CREATE INDEX` est une **écriture** : lancé pendant que l'import
        tient le verrou, il faisait mourir sur `database is locked` un chargement de
        plusieurs minutes. L'import recrée ses index lui-même en sortant ; si son
        processus est tué, le marqueur ne survit ni à la mort du pid ni au délai, et
        la reconstruction reprend à l'ouverture suivante.

        Returns:
            Les index recréés (vide dans le cas normal, et en cas d'abdication).
        """
        present = self._files_index_names()
        missing = [name for name in FILES_INDEXES if name not in present]
        if not missing:
            return []
        owner = self._bulk_load_owner()
        if owner is not None:
            logger.info(
                "index manquants sur `files` : chargement en masse en cours (pid %s) —"
                " reconstruction laissée à l'import",
                owner,
            )
            return []
        logger.warning(
            "index manquants sur `files` (import interrompu ?) : %s — reconstruction",
            ", ".join(missing),
        )
        for name in missing:
            self._conn.execute(FILES_INDEXES[name])
        self._conn.commit()
        return missing

    @contextmanager
    def bulk_load(self, *, analyze: bool = True) -> Iterator[None]:
        """Charge `files` en masse : index secondaires retirés le temps de l'écriture.

        Maintenir onze index à chaque ligne insérée coûte plus cher que les
        reconstruire d'un bloc à la fin (mesuré : ×5 sur un CSV de 250 Mo). L'index
        UNIQUE implicite de `path_key` n'est **pas** touché : c'est lui qui rend le
        `SELECT … WHERE path_key=?` de `upsert_files` immédiat.

        Aucun index **UNIQUE** n'est retiré, implicite ou déclaré : un index unique
        n'accélère pas, il *interdit*. Le supprimer le temps d'un import laisserait
        entrer les doublons qu'il refuse, et le `CREATE UNIQUE INDEX` du `finally`
        échouerait alors sur ces doublons — la contrainte disparue pour de bon, sans
        que rien ne la réclame. Le filtre porte sur la définition SQL relue, donc il
        couvre aussi un index unique qu'une migration future ajouterait.

        Les définitions sont relues dans `sqlite_master` avant la suppression, donc
        recréées à l'identique (y compris un index ajouté par une migration future).
        `PRAGMA cache_size` et `temp_store` sont élargis pendant l'opération puis
        remis à leur valeur d'origine.

        Un marqueur `meta[BULK_LOCK_KEY]` (pid + horodatage) est **validé en base**
        avant la suppression des index et retiré à la fin : toute autre connexion
        ouverte pendant l'import le voit et renonce à reconstruire les index
        (`_ensure_files_indexes`), au lieu d'écrire pendant que l'import tient le
        verrou et de le tuer sur `database is locked`.

        Le `finally` recrée les index même si le corps échoue ; un processus tué en
        plein import échappe forcément à ce `finally`, d'où le filet de
        `_ensure_files_indexes` à la réouverture de la base.

        Compromis : pendant le chargement, toute lecture de `files` (vues,
        statistiques) balaie la table. C'est sans conséquence — une campagne est
        mono-poste et l'utilisateur attend la fin de son import.

        Args:
            analyze: rejoue `ANALYZE` après la reconstruction (statistiques du
                planificateur). Inutile si l'appelant enchaîne sur `finish_scan`,
                qui le fait déjà.

        Raises:
            sqlite3.OperationalError: base ouverte en lecture seule. Le refus arrive
                **avant** la suppression du premier index : sans lui, `bulk_load`
                échouait au milieu de son travail sur une base non écrivable.
        """
        self._refuse_if_read_only("chargement en masse")
        defs: list[tuple[str, str]] = []
        for r in self._conn.execute(
            "SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name='files'"
            " AND sql IS NOT NULL ORDER BY name"
        ):
            name, sql = str(r["name"]), str(r["sql"])
            if _UNIQUE_INDEX_RE.match(sql):
                continue  # contrainte de données : jamais retirée (voir la docstring)
            defs.append((name, sql))
        previous_cache = int(self._conn.execute("PRAGMA cache_size").fetchone()[0])
        previous_temp = int(self._conn.execute("PRAGMA temp_store").fetchone()[0])
        self._conn.execute(
            "INSERT INTO meta(key, value) VALUES(?, ?)"
            " ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (BULK_LOCK_KEY, f"{os.getpid()}|{_now()}"),
        )
        self._conn.commit()  # visible des autres connexions AVANT la première écriture
        for name, _sql in defs:
            self._conn.execute(f'DROP INDEX IF EXISTS "{name}"')
        self._conn.execute(f"PRAGMA cache_size={BULK_CACHE_PAGES}")
        self._conn.execute("PRAGMA temp_store=MEMORY")
        self._conn.commit()
        try:
            yield
        finally:
            # SQLite retire le `IF NOT EXISTS` du DDL qu'il stocke : si un index est
            # déjà revenu (une autre connexion l'a reconstruit entre-temps), le
            # `CREATE` lève. Sans les gardes ci-dessous, cette exception **remplaçait**
            # celle du corps — la vraie cause de l'échec d'import disparaissait — et
            # la boucle s'arrêtait, laissant 4 index sur 11.
            present = self._files_index_names()
            for name, sql in defs:
                if name in present:
                    continue
                try:
                    self._conn.execute(sql)
                except sqlite3.Error:
                    logger.exception("reconstruction de l'index %s", name)
            try:
                if analyze:
                    self._conn.execute("ANALYZE")
                self._conn.commit()
            except sqlite3.Error:
                logger.exception("fin de chargement en masse")
            # Marqueur retiré une fois les index revenus, quoi qu'ait donné `ANALYZE` :
            # une autre connexion qui ouvre ensuite la base n'a plus rien à
            # reconstruire, et le marqueur ne survit qu'à un processus tué.
            try:
                self._conn.execute("DELETE FROM meta WHERE key=?", (BULK_LOCK_KEY,))
                self._conn.commit()
            except sqlite3.Error:
                logger.exception("retrait du marqueur de chargement en masse")
            with suppress(sqlite3.Error):
                self._conn.execute(f"PRAGMA cache_size={previous_cache}")
                self._conn.execute(f"PRAGMA temp_store={previous_temp}")
