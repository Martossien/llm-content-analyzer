"""Fragments SQL partagés et petites fonctions de traduction SQL ↔ Python.

Aucune connexion ici : ce module ne fait que produire du texte SQL (règles de
« dernière analyse qui fait foi », clés de dates, filtres de l'écran Résultats)
et découper les scripts de migration. `docia.db.database` et `docia.views` s'en
servent ; la règle vit **ici, une seule fois** (voir `latest_analysis_sql`).
"""

from __future__ import annotations

import re


def split_sql_statements(script: str) -> list[str]:
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


def latest_analysis_sql(file_id: str, *, alias: str = "a", file_alias: str = "f") -> str:
    """Condition SQL « `alias` est l'analyse qui **fait foi** pour le fichier `file_id` ».

    Deux exigences, indissociables, et c'est tout l'objet de cette fonction :

    1. **la plus récente** — par `created_at`, départagée par `id` décroissant quand
       deux analyses portent le même horodatage (réanalyse dans la même seconde) ;
    2. **portant sur le contenu actuel** — `content_version` de l'analyse égale celle
       du fichier. Un fichier modifié depuis son analyse repasse `pending` avec
       `content_version + 1` : sa classification ne décrit plus rien.

    La seconde condition est **dans** la sous-requête, pas après elle : « la plus
    récente parmi celles du contenu actuel », et non « la plus récente, si par chance
    elle porte sur le contenu actuel ». Écrite en second filtre, elle faisait
    disparaître de toutes les vues un fichier dont une analyse valide existait mais
    qu'une analyse plus récente portant sur une autre version masquait.

    Les séparer a coûté cher. La règle vivait en **cinq exemplaires** — `views`,
    `db._LATEST_JOINS` (écran Résultats, exports CSV/JSON), `db._IS_LATEST`
    (`classification_summary`, `docia status`), `db.count_analyzed_files` et
    `report.powerbi` — et la seconde exigence n'a d'abord été ajoutée qu'à un seul.
    Le test qui prétendait les comparer confrontait un fragment de texte qui,
    justement, ne contenait pas `content_version` : il passait pendant que les
    chemins divergeaient. Le rapport disait 0 candidat au nettoyage là où l'export
    Power BI et le classeur en annonçaient un, avec la **nouvelle** taille du fichier
    et son **ancienne** classe de sécurité.

    La fonction vit ici, dans `docia.db`, et non dans `docia.views` : le cycle
    d'imports va de `views` vers `db`, donc `db` ne pouvait pas importer sa propre
    règle et en gardait une copie textuelle. C'est cette copie qui a divergé.

    Args:
        file_id: expression SQL désignant l'identifiant du fichier (`f.id`,
            `a.file_id`…), selon la table par laquelle la requête entre.
        alias: alias de la table `analyses`.
        file_alias: alias de la table `files`. La requête **doit** la joindre :
            sans elle, `content_version` n'a rien à quoi se comparer.
    """
    return (
        f"{alias}.id = (SELECT id FROM analyses WHERE file_id = {file_id}"
        f" AND content_version = {file_alias}.content_version"
        " ORDER BY created_at DESC, id DESC LIMIT 1)"
    )


_TOUCH_SQL = (
    "UPDATE files SET last_seen_scan_id=?, access_time=?, access_key=?, updated_at=? WHERE id=?"
)
"""Mise à jour d'un fichier revu inchangé : il n'a été *vu*, rien de son contenu ne change."""

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

_LATEST_JOINS = f" LEFT JOIN analyses a ON {latest_analysis_sql('f.id')}" + _REVIEWS_JOIN
"""Analyse faisant foi pour un fichier + sa revue. La sous-requête corrélée s'appuie
sur `idx_analyses_file_latest (file_id, created_at, id)`.

`LEFT JOIN` : un fichier dont l'analyse ne porte plus sur le contenu actuel reste
**listé**, avec des colonnes d'analyse vides et son statut `pending` — il n'est ni
masqué, ni décoré d'une classification périmée."""

_LATEST_FROM = " FROM files f" + _LATEST_JOINS

_IS_LATEST = latest_analysis_sql("a.file_id")
"""Même règle que `_LATEST_JOINS`, mais en partant des analyses (`analyses a`).

Sert aux compteurs de `counts` et à `classification_summary` — qui doivent donc
**joindre `files`** (alias `f`) : ils s'en passaient, et comptaient de ce fait les
analyses devenues caduques. `docia status` annonçait alors une classification pour
des fichiers que la base sait pourtant `pending`."""

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
