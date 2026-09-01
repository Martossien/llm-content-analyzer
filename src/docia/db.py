"""Base SQLite : schéma versionné, accès aux fichiers, blocs et analyses.

Une seule connexion par `Database` (mode WAL, `check_same_thread=False` car
le pipeline est asynchrone mais mono-thread). Toutes les écritures passent par
des méthodes explicites ; aucun `ALTER` implicite hors `_MIGRATIONS`.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
import sys
from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager, suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from docia.models import (
    BlockFile,
    BlockSpec,
    BlockStatus,
    FileAnalysis,
    FileRow,
    FileStatus,
    LLMUsage,
    SmbeagleRow,
    path_key,
)

SCHEMA_VERSION = 7

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


def _stamp() -> str:
    """Horodatage local `AAAAMMJJTHHMMSS` pour nommer une sauvegarde."""
    return datetime.now().strftime("%Y%m%dT%H%M%S")  # noqa: DTZ005 - nom de fichier, heure locale


def _free_path(directory: Path, base: str, suffix: str = ".sqlite") -> Path:
    """Chemin libre `<directory>/<base>[_n]<suffix>` : n'écrase jamais un fichier."""
    candidate = directory / f"{base}{suffix}"
    counter = 2
    while candidate.exists():
        candidate = directory / f"{base}_{counter}{suffix}"
        counter += 1
    return candidate


def _split_sql_statements(script: str) -> list[str]:
    """Découpe un script SQL en instructions, sur les `;` **hors littéraux**.

    `sqlite3.Connection.executescript` ne convient pas aux migrations : il valide
    implicitement la transaction en cours (comportement documenté de CPython),
    donc chaque `ALTER`/`UPDATE`/`CREATE INDEX` serait validé séparément et une
    interruption laisserait la base à mi-chemin. Les instructions sont donc
    jouées une à une dans une transaction explicite, ce qui suppose de savoir
    découper : un `;` dans une chaîne (`'a;b'`), un identifiant entre guillemets
    ou un commentaire ne sépare rien.
    """
    statements: list[str] = []
    current: list[str] = []
    closing: str | None = None  # délimiteur de fin du littéral / identifiant courant
    comment: str | None = None  # "--" (jusqu'à la fin de ligne) ou "/*"
    index = 0
    size = len(script)
    while index < size:
        char = script[index]
        following = script[index + 1] if index + 1 < size else ""
        if comment == "--":
            if char == "\n":
                comment = None
                current.append(char)
            index += 1
            continue
        if comment == "/*":
            if char == "*" and following == "/":
                comment = None
                index += 2
                continue
            index += 1
            continue
        if closing is not None:
            current.append(char)
            index += 1
            if char == closing:
                if closing != "]" and following == closing:  # '' ou "" échappé
                    current.append(following)
                    index += 1
                    continue
                closing = None
            continue
        if char == "-" and following == "-":
            comment = "--"
            index += 2
            continue
        if char == "/" and following == "*":
            comment = "/*"
            index += 2
            continue
        if char in "'\"`":
            closing = char
        elif char == "[":
            closing = "]"
        elif char == ";":
            statements.append("".join(current))
            current = []
            index += 1
            continue
        current.append(char)
        index += 1
    statements.append("".join(current))
    return [stripped for statement in statements if (stripped := statement.strip())]


_ANALYZE_RE = re.compile(r"^ANALYZE\b", re.IGNORECASE)
"""`ANALYZE` reconstruit `sqlite_stat1` : hors transaction de migration (long, et
purement statistique — le rejouer plus tard ne change aucune donnée)."""

_CREATE_INDEX_RE = re.compile(r"^\s*CREATE\s+INDEX\s+(?!IF\s+NOT\s+EXISTS\b)", re.IGNORECASE)


def _idempotent_create_index(sql: str) -> str:
    """Ajoute `IF NOT EXISTS` à un `CREATE INDEX` qui n'en a pas.

    `sqlite_master.sql` conserve le texte d'origine **sans** le `IF NOT EXISTS` :
    rejouer tel quel un index déjà présent lèverait `index … already exists`.
    """
    return _CREATE_INDEX_RE.sub("CREATE INDEX IF NOT EXISTS ", sql, count=1)


_UNIQUE_INDEX_RE = re.compile(r"^\s*CREATE\s+UNIQUE\s+INDEX\b", re.IGNORECASE)
"""Un index **UNIQUE** est une contrainte de données, pas une aide au planificateur."""

_IF_NOT_EXISTS_RE = re.compile(r"\bIF\s+NOT\s+EXISTS\s+", re.IGNORECASE)


def normalize_index_sql(sql: str) -> str:
    """Forme canonique d'un `CREATE INDEX`, pour comparer deux définitions.

    `sqlite_master.sql` conserve le texte d'origine moins le `IF NOT EXISTS`, avec
    ses espaces et sa casse : comparer les chaînes brutes est impossible, comparer
    les seuls **noms** ne prouve rien (un index déclaré sur les mauvaises colonnes
    porte le même nom). On ramène donc les deux formes à la même chaîne : casse
    unifiée, `IF NOT EXISTS` retiré, espaces réduits, ponctuation resserrée.
    """
    text = _IF_NOT_EXISTS_RE.sub("", sql.strip().rstrip(";")).lower()
    text = re.sub(r"\s+", " ", text)
    return re.sub(r"\s*([(),])\s*", r"\1", text)


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


def date_key_sql(column: str) -> str:
    """Expression SQL rendant `yyyymmdd` (ou `''`) pour une date SMBeagle ou ISO.

    Miroir exact de `date_key` : les deux doivent rendre la même chaîne pour
    toute valeur (vérifié par `tests/test_db.py`). Sert à remplir `files.access_key`
    et `files.write_key` (schéma v6) et à les rétro-remplir à la migration.
    """
    return (
        f"CASE WHEN length({column})>=10 AND substr({column},3,1)='/' AND substr({column},6,1)='/'"
        f" THEN substr({column},7,4)||substr({column},4,2)||substr({column},1,2)"
        f" WHEN length({column})>=10 AND substr({column},5,1)='-'"
        f" THEN substr({column},1,4)||substr({column},6,2)||substr({column},9,2)"
        f" ELSE '' END"
    )


def date_key(value: str) -> str:
    """`yyyymmdd` d'une date SMBeagle (`dd/MM/yyyy…`) ou ISO, `''` si illisible.

    Clé comparable lexicographiquement : c'est elle qui est stockée dans
    `files.access_key` / `files.write_key` pour que les vues d'ancienneté
    s'appuient sur un index au lieu de reformater chaque ligne.

    Miroir exact de `date_key_sql` — y compris sur l'octet NUL, où `length()` de
    SQLite s'arrête alors que `len()` de Python compte tout. Sans cette
    précaution, une base **importée** et une base **migrée** portant les mêmes
    données n'auraient pas les mêmes clés (un CSV corrompu suffit : l'import lit
    en `errors="replace"`), et les statistiques d'ancienneté divergeraient en
    silence.
    """
    if len(value.partition("\x00")[0]) >= 10:
        if value[2] == "/" and value[5] == "/":
            return value[6:10] + value[3:5] + value[0:2]
        if value[4] == "-":
            return value[0:4] + value[5:7] + value[8:10]
    return ""


def first_access_sql(prefix: str = "") -> str:
    """Date d'accès retenue pour l'ancienneté : la première observée (schéma v5).

    Le hachage et l'extraction de l'audit lisent les fichiers et peuvent
    rafraîchir la date d'accès NTFS : la statistique « non accédé depuis N ans »
    s'appuie donc sur `access_time_first`, et ne retombe sur `access_time` que
    si cette première observation manque.
    """
    return f"COALESCE(NULLIF({prefix}access_time_first, ''), {prefix}access_time)"


def split_sql_statements(script: str) -> list[str]:
    """Découpe un script SQL en instructions, en respectant les littéraux `'…'`.

    Sert aux migrations : elles doivent être jouées instruction par instruction
    dans une transaction explicite, `executescript()` validant implicitement au
    passage (donc sans aucune atomicité). Les commentaires `--` sont ignorés ;
    un `;` à l'intérieur d'une chaîne ne coupe pas.
    """
    statements: list[str] = []
    current: list[str] = []
    in_string = False
    in_comment = False
    index = 0
    while index < len(script):
        char = script[index]
        if in_comment:
            if char == "\n":
                in_comment = False
                current.append(char)
            index += 1
            continue
        if in_string:
            current.append(char)
            if char == "'":
                # `''` à l'intérieur d'une chaîne est un apostrophe littéral.
                if index + 1 < len(script) and script[index + 1] == "'":
                    current.append("'")
                    index += 2
                    continue
                in_string = False
            index += 1
            continue
        if char == "'":
            in_string = True
            current.append(char)
        elif char == "-" and script[index : index + 2] == "--":
            in_comment = True
            index += 2
            continue
        elif char == ";":
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
        else:
            current.append(char)
        index += 1
    last = "".join(current).strip()
    if last:
        statements.append(last)
    return statements


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


_SCHEMA_V1 = """
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);

CREATE TABLE IF NOT EXISTS scans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    csv_path TEXT NOT NULL,
    imported_at TEXT NOT NULL,
    rows_total INTEGER NOT NULL DEFAULT 0,
    rows_new INTEGER NOT NULL DEFAULT 0,
    rows_updated INTEGER NOT NULL DEFAULT 0,
    rows_unchanged INTEGER NOT NULL DEFAULT 0,
    rows_invalid INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    path_key TEXT NOT NULL UNIQUE,
    path TEXT NOT NULL,
    name TEXT NOT NULL,
    extension TEXT NOT NULL DEFAULT '',
    host TEXT NOT NULL DEFAULT '',
    hostname TEXT NOT NULL DEFAULT '',
    username TEXT NOT NULL DEFAULT '',
    unc_directory TEXT NOT NULL DEFAULT '',
    base TEXT NOT NULL DEFAULT '',
    directory_type TEXT NOT NULL DEFAULT '',
    size_bytes INTEGER NOT NULL DEFAULT 0,
    creation_time TEXT NOT NULL DEFAULT '',
    last_write_time TEXT NOT NULL DEFAULT '',
    access_time TEXT NOT NULL DEFAULT '',
    file_attributes TEXT NOT NULL DEFAULT '',
    owner TEXT NOT NULL DEFAULT '',
    fast_hash TEXT NOT NULL DEFAULT '',
    file_signature TEXT NOT NULL DEFAULT '',
    readable INTEGER NOT NULL DEFAULT 1,
    writeable INTEGER NOT NULL DEFAULT 0,
    deletable INTEGER NOT NULL DEFAULT 0,
    first_seen_scan_id INTEGER NOT NULL,
    last_seen_scan_id INTEGER NOT NULL,
    content_version INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL DEFAULT 'pending',
    exclusion_reason TEXT,
    priority_score INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_files_status ON files(status);
CREATE INDEX IF NOT EXISTS idx_files_fast_hash ON files(fast_hash);
CREATE INDEX IF NOT EXISTS idx_files_extension ON files(extension);
CREATE INDEX IF NOT EXISTS idx_files_priority ON files(priority_score DESC, path);

CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL DEFAULT 'running',
    model TEXT NOT NULL,
    prompt_hash TEXT NOT NULL,
    config_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS blocks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL REFERENCES runs(id),
    path TEXT NOT NULL,
    tokens_estimated INTEGER NOT NULL DEFAULT 0,
    tokens_with_margin INTEGER NOT NULL DEFAULT 0,
    file_count INTEGER NOT NULL DEFAULT 0,
    oversized INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'built',
    attempts INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    prompt_hash TEXT NOT NULL,
    model TEXT NOT NULL,
    usage_prompt_tokens INTEGER,
    usage_completion_tokens INTEGER,
    latency_ms INTEGER,
    created_at TEXT NOT NULL,
    sent_at TEXT,
    completed_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_blocks_status ON blocks(status);

CREATE TABLE IF NOT EXISTS block_files (
    block_id INTEGER NOT NULL REFERENCES blocks(id),
    file_id INTEGER NOT NULL REFERENCES files(id),
    file_ref TEXT NOT NULL,
    content_version INTEGER NOT NULL,
    oversized INTEGER NOT NULL DEFAULT 0,
    outcome TEXT,
    PRIMARY KEY (block_id, file_id)
);
CREATE INDEX IF NOT EXISTS idx_block_files_file ON block_files(file_id);

CREATE TABLE IF NOT EXISTS analyses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id INTEGER NOT NULL REFERENCES files(id),
    block_id INTEGER REFERENCES blocks(id),
    content_version INTEGER NOT NULL,
    prompt_hash TEXT NOT NULL,
    model TEXT NOT NULL,
    resume TEXT NOT NULL DEFAULT '',
    security_classification TEXT NOT NULL,
    security_confidence INTEGER NOT NULL,
    security_justification TEXT NOT NULL DEFAULT '',
    rgpd_risk_level TEXT NOT NULL,
    rgpd_data_types TEXT NOT NULL DEFAULT '[]',
    rgpd_confidence INTEGER NOT NULL,
    finance_document_type TEXT NOT NULL,
    finance_amounts TEXT NOT NULL DEFAULT '[]',
    finance_confidence INTEGER NOT NULL,
    legal_contract_type TEXT NOT NULL,
    legal_parties TEXT NOT NULL DEFAULT '[]',
    legal_confidence INTEGER NOT NULL,
    raw_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (file_id, content_version, prompt_hash, model)
);
CREATE INDEX IF NOT EXISTS idx_analyses_file ON analyses(file_id);
CREATE INDEX IF NOT EXISTS idx_analyses_security ON analyses(security_classification);
CREATE INDEX IF NOT EXISTS idx_analyses_rgpd ON analyses(rgpd_risk_level);
"""

_SCHEMA_V2 = """
ALTER TABLE block_files ADD COLUMN segment_index INTEGER NOT NULL DEFAULT 0;
ALTER TABLE block_files ADD COLUMN segment_count INTEGER NOT NULL DEFAULT 1;
ALTER TABLE analyses ADD COLUMN segments INTEGER NOT NULL DEFAULT 1;

CREATE TABLE IF NOT EXISTS segment_analyses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id INTEGER NOT NULL REFERENCES files(id),
    block_id INTEGER REFERENCES blocks(id),
    content_version INTEGER NOT NULL,
    prompt_hash TEXT NOT NULL,
    model TEXT NOT NULL,
    segment_index INTEGER NOT NULL,
    segment_count INTEGER NOT NULL,
    raw_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (file_id, content_version, prompt_hash, model, segment_index)
);
CREATE INDEX IF NOT EXISTS idx_segment_analyses_file ON segment_analyses(file_id);
"""

_SCHEMA_V3 = """
ALTER TABLE analyses ADD COLUMN retention_required INTEGER NOT NULL DEFAULT 0;
ALTER TABLE analyses ADD COLUMN retention_years INTEGER NOT NULL DEFAULT 0;
ALTER TABLE analyses ADD COLUMN retention_basis TEXT NOT NULL DEFAULT 'none';
ALTER TABLE analyses ADD COLUMN retention_justification TEXT NOT NULL DEFAULT '';
ALTER TABLE analyses ADD COLUMN retention_confidence INTEGER NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS prompts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    text TEXT NOT NULL,
    hash TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS reviews (
    file_id INTEGER PRIMARY KEY REFERENCES files(id),
    status TEXT NOT NULL DEFAULT 'to_review',
    comment TEXT NOT NULL DEFAULT '',
    corrected_security TEXT,
    corrected_rgpd TEXT,
    corrected_retention_years INTEGER,
    reviewer TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL
);
"""

_SCHEMA_V4 = """
CREATE INDEX IF NOT EXISTS idx_files_hash_size ON files(fast_hash, size_bytes);
CREATE INDEX IF NOT EXISTS idx_files_owner ON files(owner);
CREATE INDEX IF NOT EXISTS idx_files_access_time ON files(access_time);
CREATE INDEX IF NOT EXISTS idx_files_last_write ON files(last_write_time);
CREATE INDEX IF NOT EXISTS idx_files_base ON files(base);
CREATE INDEX IF NOT EXISTS idx_analyses_retention ON analyses(retention_required);
CREATE INDEX IF NOT EXISTS idx_reviews_status ON reviews(status);
"""
"""v4 : index de restitution (`views.py`) — doublons, ancienneté, propriétaire, partage."""

_SCHEMA_V5 = """
ALTER TABLE files ADD COLUMN access_time_first TEXT NOT NULL DEFAULT '';
UPDATE files SET access_time_first = access_time;
ALTER TABLE scans ADD COLUMN kind TEXT NOT NULL DEFAULT 'import';
ALTER TABLE scans ADD COLUMN manifest_json TEXT NOT NULL DEFAULT '';
ALTER TABLE scans ADD COLUMN scanner_elapsed_s REAL NOT NULL DEFAULT 0;
"""
"""v5 : `access_time_first` = date d'accès observée au premier scan (ou au dernier
changement de contenu). L'audit lui-même lit les fichiers (hachage, signature,
extraction) et peut mettre à jour la date d'accès NTFS : les statistiques
« non accédé depuis N ans » s'appuient sur cette première observation, jamais
sur une date rafraîchie par un rescan d'un fichier inchangé. `scans.kind` =
`scan` (SMBeagle piloté par docia, manifeste conservé) ou `import` (CSV fourni)."""

_SCHEMA_V6 = f"""
ALTER TABLE files ADD COLUMN access_key TEXT NOT NULL DEFAULT '';
ALTER TABLE files ADD COLUMN write_key TEXT NOT NULL DEFAULT '';
UPDATE files SET access_key = {date_key_sql(first_access_sql())},
                 write_key = {date_key_sql("last_write_time")};

CREATE INDEX IF NOT EXISTS idx_files_access_key ON files(access_key, size_bytes);
CREATE INDEX IF NOT EXISTS idx_files_write_key ON files(write_key, size_bytes);
CREATE INDEX IF NOT EXISTS idx_files_extension_size ON files(extension, size_bytes);
CREATE INDEX IF NOT EXISTS idx_files_owner_size ON files(owner, size_bytes);
CREATE INDEX IF NOT EXISTS idx_files_share_size ON files(base, unc_directory, size_bytes);
CREATE INDEX IF NOT EXISTS idx_files_status_size ON files(status, size_bytes);
CREATE INDEX IF NOT EXISTS idx_analyses_file_latest ON analyses(file_id, created_at, id);

DROP INDEX IF EXISTS idx_files_extension;
DROP INDEX IF EXISTS idx_files_owner;
DROP INDEX IF EXISTS idx_files_base;
DROP INDEX IF EXISTS idx_files_status;

ANALYZE;
"""
"""v6 : `access_key` / `write_key` = dates d'accès et d'écriture normalisées en
`yyyymmdd` (`''` si absente ou illisible), remplies à l'insertion comme à la mise
à jour par `upsert_files` et rétro-remplies ici. Les vues d'ancienneté comparaient
jusqu'ici des `substr()` calculés ligne par ligne : aucun index n'était utilisable
et chaque seuil relançait un balayage complet. Les index sont couvrants (la taille
suit la clé) pour que les totaux se lisent sans toucher la table ; les index d'une
seule colonne qu'ils remplacent (préfixe identique) sont supprimés. `ANALYZE` donne
au planificateur les cardinalités réelles dès la migration ; il est rejoué à la fin
de chaque scan (`finish_scan`)."""

_SCHEMA_V7 = """
ALTER TABLE scans ADD COLUMN complete INTEGER NOT NULL DEFAULT 1;
ALTER TABLE scans ADD COLUMN skipped_json TEXT NOT NULL DEFAULT '';
ALTER TABLE scans ADD COLUMN cancelled INTEGER NOT NULL DEFAULT 0;
ALTER TABLE scans ADD COLUMN exit_code INTEGER NOT NULL DEFAULT 0;
ALTER TABLE scans ADD COLUMN expected_files INTEGER NOT NULL DEFAULT -1;
"""
"""v7 : le **périmètre** du scan, conservé avec la campagne.

Un audit sert à décider de suppressions : « cette campagne porte-t-elle sur tout
ce qu'on a demandé ? » doit trouver sa réponse en base des mois plus tard, sans
le manifeste ni le CSV sous la main. Jusqu'ici la table `scans` ne portait que ce
qui avait été importé, jamais ce qui manquait : un partage refusé par une ACL,
un scan arrêté par l'utilisateur et un scan complet y étaient identiques.

- `complete` : 0 dès qu'une cible a été écartée, que le scan a été arrêté ou que
  le CSV est plus court que le compte annoncé. **1 par défaut** — les scans déjà
  en base, et ceux d'un `SMBeagle.exe` antérieur qui rend 0 et n'écrit aucun
  `skipped`, restent réputés complets : la migration n'invente pas de faux
  positif « périmètre incomplet ».
- `skipped_json` : liste JSON des cibles demandées mais non scannées (`''` = aucune).
- `cancelled` : l'utilisateur a arrêté le scan (attendu) — à ne pas confondre
  avec un scanner mort en écrivant, que `scan.run_scan` refuse d'importer.
- `exit_code` : code de retour du scanner (0 par défaut ; 4 = périmètre amputé).
- `expected_files` : nombre de fichiers annoncé par le scanner, à comparer à
  `rows_total`. **-1 = inconnu** (import d'un CSV fourni, scan d'avant la v7) :
  sans cette valeur distincte de 0, un import ordinaire aurait paru tronqué.
"""

_MIGRATIONS: list[tuple[int, str]] = [
    (1, _SCHEMA_V1),
    (2, _SCHEMA_V2),
    (3, _SCHEMA_V3),
    (4, _SCHEMA_V4),
    (5, _SCHEMA_V5),
    (6, _SCHEMA_V6),
    (7, _SCHEMA_V7),
]

FILES_INDEXES: dict[str, str] = {
    "idx_files_fast_hash": "CREATE INDEX IF NOT EXISTS idx_files_fast_hash ON files(fast_hash)",
    "idx_files_priority": (
        "CREATE INDEX IF NOT EXISTS idx_files_priority ON files(priority_score DESC, path)"
    ),
    "idx_files_hash_size": (
        "CREATE INDEX IF NOT EXISTS idx_files_hash_size ON files(fast_hash, size_bytes)"
    ),
    "idx_files_access_time": (
        "CREATE INDEX IF NOT EXISTS idx_files_access_time ON files(access_time)"
    ),
    "idx_files_last_write": (
        "CREATE INDEX IF NOT EXISTS idx_files_last_write ON files(last_write_time)"
    ),
    "idx_files_access_key": (
        "CREATE INDEX IF NOT EXISTS idx_files_access_key ON files(access_key, size_bytes)"
    ),
    "idx_files_write_key": (
        "CREATE INDEX IF NOT EXISTS idx_files_write_key ON files(write_key, size_bytes)"
    ),
    "idx_files_extension_size": (
        "CREATE INDEX IF NOT EXISTS idx_files_extension_size ON files(extension, size_bytes)"
    ),
    "idx_files_owner_size": (
        "CREATE INDEX IF NOT EXISTS idx_files_owner_size ON files(owner, size_bytes)"
    ),
    "idx_files_share_size": (
        "CREATE INDEX IF NOT EXISTS idx_files_share_size ON files(base, unc_directory, size_bytes)"
    ),
    "idx_files_status_size": (
        "CREATE INDEX IF NOT EXISTS idx_files_status_size ON files(status, size_bytes)"
    ),
}
"""Index secondaires de `files` attendus au schéma courant, avec leur définition.

Miroir de ce que produisent les migrations (v1 + v4 + v6, moins ceux que la v6
supprime) : `tests/test_db.py` compare ce dictionnaire aux index réellement
présents dans une base neuve, il ne peut donc pas dériver en silence. Il sert de
filet à l'ouverture (`_ensure_files_indexes`) quand un chargement en masse
(`bulk_load`) a été interrompu avant d'avoir recréé les index.
"""

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

_TOUCH_SQL = (
    "UPDATE files SET last_seen_scan_id=?, access_time=?, access_key=?, updated_at=? WHERE id=?"
)
"""Mise à jour d'un fichier revu inchangé : il n'a été *vu*, rien de son contenu ne change."""

REVIEW_STATUSES = ("to_review", "validated", "corrected")

ITER_FILES_BATCH = 10_000
"""Fichiers lus par aller-retour SQLite dans `iter_files(ordered=False)`."""

APPLY_PLAN_BATCH = 5_000
"""Décisions de plan regroupées par `executemany` dans `apply_plan`."""

_PLAN_EXCLUDE_SQL = (
    "UPDATE files SET status='excluded', exclusion_reason=?, priority_score=?, updated_at=?"
    " WHERE id=? AND status IN ('pending','excluded','queued')"
)
"""Décision « exclu » : ne rétrograde jamais un fichier `done` ou `error`."""

_PENDING_WHERE = """
 WHERE f.status='pending'
   AND NOT EXISTS (SELECT 1 FROM analyses a WHERE a.file_id=f.id
                   AND a.content_version=f.content_version
                   AND a.prompt_hash=? AND a.model=?)"""
"""Critère unique de « fichier à analyser », partagé par `select_pending`,
`select_pending_ids` et `count_pending` : trois formulations, une seule définition —
elles ne peuvent plus diverger. Attend deux paramètres : `prompt_hash`, `model`."""

_LATEST_SELECT = """SELECT f.path, f.name, f.extension, f.size_bytes, f.owner, f.host, f.status,
       f.exclusion_reason, f.content_version, a.model, a.prompt_hash, a.resume,
       a.security_classification, a.security_confidence, a.security_justification,
       a.rgpd_risk_level, a.rgpd_data_types, a.rgpd_confidence,
       a.finance_document_type, a.finance_amounts, a.finance_confidence,
       a.legal_contract_type, a.legal_parties, a.legal_confidence, a.created_at,
       a.segments, a.retention_required, a.retention_years, a.retention_basis,
       a.retention_justification, a.retention_confidence,
       r.status AS review_status, r.comment AS review_comment,
       r.corrected_security, r.corrected_rgpd, r.corrected_retention_years,
       r.reviewer, r.updated_at AS reviewed_at, f.id AS id"""
"""Colonnes rendues par `latest_analyses` : le fichier, sa dernière analyse, sa revue."""

_REVIEWS_JOIN = " LEFT JOIN reviews r ON r.file_id = f.id"

_LATEST_JOINS = (
    " LEFT JOIN analyses a ON a.id = (SELECT id FROM analyses WHERE file_id=f.id"
    " ORDER BY created_at DESC, id DESC LIMIT 1)" + _REVIEWS_JOIN
)
"""Dernière analyse d'un fichier + sa revue. La sous-requête corrélée s'appuie sur
`idx_analyses_file_latest (file_id, created_at, id)`."""

_LATEST_FROM = " FROM files f" + _LATEST_JOINS

_IS_LATEST = (
    "a.id = (SELECT id FROM analyses WHERE file_id = a.file_id"
    " ORDER BY created_at DESC, id DESC LIMIT 1)"
)
"""Même règle que `_LATEST_JOINS`, mais en partant des analyses (`analyses a`).

Sert aux compteurs de `counts` et `classification_summary`, qui n'ont pas besoin
de la table `files`. Copie textuelle de `views.latest_analysis_sql("a.file_id")` —
`docia.db` ne peut pas importer `docia.views` (le cycle est dans l'autre sens) ;
`tests/test_views.py` compare les deux mot à mot."""

_DISPLAY_ORDER_SQL = """
    CASE WHEN COALESCE(a.security_classification,'') <> '' THEN 0
         WHEN f.status='error' THEN 1
         WHEN f.status='done' THEN 2
         ELSE 3 END,
    CASE COALESCE(a.security_classification,'')
         WHEN '' THEN 0 WHEN 'C3' THEN 0 WHEN 'C2' THEN 1 WHEN 'C1' THEN 2
         WHEN 'C0' THEN 3 WHEN 'N/A' THEN 4 ELSE 5 END,
    LOWER(f.name)"""
"""Ordre d'affichage de l'écran Résultats, en SQL — miroir de `gui.tab_results._display_order`.
Approché sur le dernier critère : `LOWER()` de SQLite ignore les accents (voir
`latest_analyses`). `''` en second rang vaut 0 : un fichier sans classification est
déjà départagé par le premier rang, comme en Python."""


def _like_escape(text: str) -> str:
    """Rend littéraux `%`, `_` et `\\` dans un motif `LIKE … ESCAPE '\\'`.

    Sans cela, chercher « 100% » ou « fichier_1 » dans l'écran Résultats ne
    cherchait plus une sous-chaîne mais un motif : « % » ramenait la campagne
    entière. Le filtrage Python qu'on remplace comparait, lui, des sous-chaînes.
    """
    for char in ("\\", "%", "_"):
        text = text.replace(char, "\\" + char)
    return text


def _needs_analysis(security: str | None, rgpd: str | None, search: str | None) -> bool:
    """Vrai si les filtres demandés lisent la dernière analyse (jointure coûteuse)."""
    return security is not None or rgpd is not None or bool(search)


def _latest_filters(
    security: str | None, rgpd: str | None, review: str | None, search: str | None
) -> tuple[str, list[object]]:
    """(clause `WHERE`, paramètres) des filtres de l'écran Résultats.

    Chaque filtre reproduit à l'identique le test Python qu'il remplace, `None`
    valant « pas de filtre » et `''` un filtre sur la valeur vide (« non vérifié »).
    """
    clauses: list[str] = []
    params: list[object] = []
    if security is not None:
        clauses.append("COALESCE(a.security_classification,'') = ?")
        params.append(security)
    if rgpd is not None:
        clauses.append("COALESCE(a.rgpd_risk_level,'') = ?")
        params.append(rgpd)
    if review is not None:
        clauses.append("COALESCE(r.status,'') = ?")
        params.append(review)
    if search:
        # Même botte de foin qu'en Python : chemin, résumé, propriétaire, séparés par
        # une espace. `LIKE` replie déjà la casse — mais **l'ASCII seulement**, des
        # deux côtés : le motif n'est donc pas replié en Python avant d'arriver ici,
        # sans quoi « Étude » deviendrait « étude » et ne retrouverait plus « Étude »
        # dans une botte de foin que SQLite, lui, n'a pas repliée.
        clauses.append(
            "f.path || ' ' || COALESCE(a.resume,'') || ' ' || COALESCE(f.owner,'')"
            " LIKE ? ESCAPE '\\'"
        )
        params.append(f"%{_like_escape(search)}%")
    return (" WHERE " + " AND ".join(clauses)) if clauses else "", params


_PLAN_KEEP_SQL = (
    "UPDATE files SET"
    " status=CASE WHEN status IN ('excluded','queued') THEN 'pending' ELSE status END,"
    " exclusion_reason=CASE WHEN status='excluded' THEN NULL ELSE exclusion_reason END,"
    " priority_score=?, updated_at=? WHERE id=?"
)
"""Décision « à analyser » : un fichier `done` ou `error` garde son statut, son score est rafraîchi."""


class Database:
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
    def __enter__(self) -> Database:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def close(self) -> None:
        self._conn.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
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
        tronque = expected_files >= 0 and expected_files > rows_total
        complete = not skipped and not cancelled and not tronque
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

    # ------------------------------------------------------------------ runs
    def start_run(self, *, model: str, prompt_hash: str, config_json: str) -> int:
        cur = self._conn.execute(
            "INSERT INTO runs(started_at, model, prompt_hash, config_json) VALUES(?,?,?,?)",
            (_now(), model, prompt_hash, config_json),
        )
        self._conn.commit()
        return int(cur.lastrowid or 0)

    def finish_run(self, run_id: int, status: str = "done") -> None:
        self._conn.execute(
            "UPDATE runs SET finished_at=?, status=? WHERE id=?", (_now(), status, run_id)
        )
        self._conn.commit()

    # ------------------------------------------------------------------ blocks
    def create_block(self, run_id: int, spec: BlockSpec, *, prompt_hash: str, model: str) -> int:
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
        self._conn.execute(
            "UPDATE blocks SET status='sent', attempts=attempts+1, sent_at=? WHERE id=?",
            (_now(), block_id),
        )
        self._conn.commit()

    def mark_block_done(self, block_id: int, usage: LLMUsage | None) -> None:
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
        self._conn.execute(
            "UPDATE blocks SET status='error', completed_at=?, error=? WHERE id=?",
            (_now(), error[:2000], block_id),
        )
        self._conn.commit()

    def block_files(self, block_id: int) -> list[BlockFile]:
        rows = self._conn.execute(
            "SELECT file_id, file_ref, content_version, oversized, segment_index, segment_count"
            " FROM block_files WHERE block_id=? ORDER BY rowid",
            (block_id,),
        ).fetchall()
        return [
            BlockFile(
                int(r["file_id"]),
                r["file_ref"],
                int(r["content_version"]),
                bool(r["oversized"]),
                int(r["segment_index"]),
                int(r["segment_count"]),
            )
            for r in rows
        ]

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
        self._conn.execute(
            "UPDATE block_files SET outcome=? WHERE block_id=? AND file_id=?",
            (outcome, block_id, file_id),
        )
        self._conn.commit()

    def pending_blocks(self, *, prompt_hash: str, model: str) -> list[BlockSpec]:
        """Blocs `built`/`sent` d'un run précédent, à (re)envoyer — reprise."""
        rows = self._conn.execute(
            "SELECT * FROM blocks WHERE status IN ('built','sent') AND prompt_hash=? AND model=? ORDER BY id",
            (prompt_hash, model),
        ).fetchall()
        specs: list[BlockSpec] = []
        for r in rows:
            specs.append(
                BlockSpec(
                    path=Path(r["path"]),
                    files=self.block_files(int(r["id"])),
                    tokens_estimated=int(r["tokens_estimated"]),
                    tokens_with_margin=int(r["tokens_with_margin"]),
                    oversized=bool(r["oversized"]),
                    block_id=int(r["id"]),
                )
            )
        return specs

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
        """Nombre de fichiers ayant au moins une analyse (miroir de `views._analyzed_files`)."""
        return int(self._conn.execute("SELECT COUNT(DISTINCT file_id) FROM analyses").fetchone()[0])

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
            f" WHERE {_IS_LATEST} GROUP BY 1, 2, 3, 4"
        ):
            number = int(row[len(self._SUMMARY_COLUMNS)])
            for position, (_, key) in enumerate(self._SUMMARY_COLUMNS):
                bucket = out[key]
                bucket[row[position]] = bucket.get(row[position], 0) + number
        return out
