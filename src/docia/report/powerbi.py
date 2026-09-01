"""Export Power BI Report Server : dossier de CSV au schéma stable (étoile simple).

Contrat volontairement figé, pensé pour une **réplication programmée** : noms de
fichiers constants, colonnes constantes, écrasement à chaque export. Les CSV sont
en UTF-8 avec BOM, séparateur `;`, décimale `.`, dates ISO `yyyy-MM-dd`. La clé de
jointure est `file_id` (table de faits `files`).

`POWERBI_COLUMNS` est la seule description du schéma : les CSV et le
`README_powerbi.md` en sont dérivés, ils ne peuvent donc pas diverger.
"""

from __future__ import annotations

import csv
import json
import sqlite3
from collections.abc import Iterable, Iterator
from datetime import date
from pathlib import Path
from typing import Any

from docia import views
from docia.db import Database
from docia.ingest.smbeagle_csv import parse_smbeagle_datetime
from docia.report import tabular

DELIMITER = ";"
ENCODING = "utf-8-sig"
"""UTF-8 avec BOM : Excel et Report Server détectent l'encodage sans réglage."""

PAGE_ROWS = 20_000
"""Lignes lues par aller-retour SQLite dans les tables volumineuses (`files`, `analyses`).

Les CSV sont écrits en flux : la campagne entière de fichiers n'est à aucun
moment en mémoire (voir `export_powerbi` pour ce que coûte `duplicates.csv`).
"""

Column = tuple[str, str, str]
"""(nom, type Power BI, description)."""

POWERBI_COLUMNS: dict[str, tuple[Column, ...]] = {
    "files.csv": (
        (
            "file_id",
            "Entier (clé)",
            "Identifiant du fichier — clé de jointure de toutes les tables",
        ),
        ("path", "Texte", "Chemin UNC complet"),
        ("name", "Texte", "Nom du fichier"),
        ("extension", "Texte", "Extension en minuscules, sans point"),
        ("host", "Texte", "Hôte SMBeagle"),
        ("hostname", "Texte", "Nom DNS de l'hôte"),
        ("username", "Texte", "Compte ayant réalisé le scan"),
        ("unc_directory", "Texte", "Répertoire UNC du fichier"),
        ("share", "Texte", "Partage déduit (`base`, sinon `\\\\serveur\\partage`)"),
        ("directory_type", "Texte", "Type de répertoire SMBeagle"),
        ("size_bytes", "Entier", "Taille en octets"),
        ("creation_date", "Date ISO", "Date de création (yyyy-MM-dd, vide si illisible)"),
        ("last_write_date", "Date ISO", "Date de dernière modification"),
        ("access_date", "Date ISO", "Date de dernier accès"),
        ("file_attributes", "Texte", "Attributs Windows"),
        ("owner", "Texte", "Propriétaire du fichier"),
        ("fast_hash", "Texte", "Empreinte rapide SMBeagle (clé de famille de doublons)"),
        ("file_signature", "Texte", "Signature de type détectée"),
        ("readable", "Booléen 0/1", "Lisible par le compte de scan"),
        ("writeable", "Booléen 0/1", "Modifiable par le compte de scan"),
        ("deletable", "Booléen 0/1", "Supprimable par le compte de scan"),
        ("content_version", "Entier", "Version de contenu (incrémentée à chaque modification)"),
        ("status", "Texte", "pending | excluded | queued | done | error"),
        ("exclusion_reason", "Texte", "Raison d'exclusion ou d'erreur, vide sinon"),
        ("priority_score", "Entier", "Score de priorité d'analyse"),
        ("age_days_access", "Entier", "Jours depuis le dernier accès (vide si date illisible)"),
        ("age_days_write", "Entier", "Jours depuis la dernière modification"),
    ),
    "analyses.csv": (
        ("file_id", "Entier (clé)", "Fichier analysé — jointure vers files.csv"),
        ("content_version", "Entier", "Version de contenu analysée"),
        ("model", "Texte", "Modèle de langage utilisé"),
        ("prompt_hash", "Texte", "Empreinte du prompt système (16 caractères)"),
        ("analyzed_at", "Date ISO", "Date de l'analyse"),
        ("segments", "Entier", "Nombre de segments agrégés (1 = fichier analysé d'un bloc)"),
        ("resume", "Texte", "Résumé du contenu"),
        ("security_classification", "Texte", "C0 | C1 | C2 | C3 | N/A"),
        ("security_confidence", "Entier", "Confiance sécurité (0–100)"),
        ("security_justification", "Texte", "Justification de la classification"),
        ("rgpd_risk_level", "Texte", "none | low | medium | high | critical | N/A"),
        ("rgpd_data_types", "Texte", "Types de données personnelles, séparés par `|`"),
        ("rgpd_confidence", "Entier", "Confiance RGPD (0–100)"),
        ("finance_document_type", "Texte", "Type de document financier"),
        ("finance_amounts", "Texte", "Montants détectés, séparés par `|`"),
        ("finance_confidence", "Entier", "Confiance finance (0–100)"),
        ("legal_contract_type", "Texte", "Type de document juridique"),
        ("legal_parties", "Texte", "Parties détectées, séparées par `|`"),
        ("legal_confidence", "Entier", "Confiance juridique (0–100)"),
        ("retention_required", "Booléen 0/1", "Le fichier doit-il être conservé"),
        ("retention_years", "Entier", "Durée de conservation en années"),
        ("retention_basis", "Texte", "none | proof | legal | fiscal | rh | contractual | N/A"),
        ("retention_justification", "Texte", "Fondement de la durée de conservation"),
        ("retention_confidence", "Entier", "Confiance conservation (0–100)"),
        ("retention_end_date", "Date ISO", "Dernière écriture + durée (vide si indéterminée)"),
    ),
    "reviews.csv": (
        ("file_id", "Entier (clé)", "Fichier vérifié — jointure vers files.csv"),
        ("status", "Texte", "to_review | validated | corrected"),
        ("comment", "Texte", "Commentaire du vérificateur"),
        ("corrected_security", "Texte", "Classe de sécurité corrigée, vide si non corrigée"),
        ("corrected_rgpd", "Texte", "Niveau RGPD corrigé, vide si non corrigé"),
        ("corrected_retention_years", "Entier", "Durée de conservation corrigée, vide sinon"),
        ("reviewer", "Texte", "Auteur de la vérification"),
        ("reviewed_at", "Date ISO", "Date de la dernière vérification"),
    ),
    "duplicates.csv": (
        ("family_id", "Texte (clé)", "Famille de doublons : empreinte + taille"),
        ("file_id", "Entier (clé)", "Exemplaire — jointure vers files.csv"),
        ("path", "Texte", "Chemin de l'exemplaire"),
        ("fast_hash", "Texte", "Empreinte commune à la famille"),
        ("size_bytes", "Entier", "Taille unitaire en octets"),
        ("copies", "Entier", "Nombre d'exemplaires de la famille"),
        (
            "reclaimable_bytes",
            "Entier",
            "Octets récupérables pour la famille (taille × (copies − 1)) — à sommer par famille, jamais par ligne",
        ),
    ),
    "runs.csv": (
        ("run_id", "Entier (clé)", "Identifiant du run d'analyse"),
        ("started_at", "Date ISO", "Début du run"),
        ("finished_at", "Date ISO", "Fin du run, vide si interrompu"),
        ("status", "Texte", "running | done | error"),
        ("model", "Texte", "Modèle utilisé"),
        ("prompt_hash", "Texte", "Empreinte du prompt système"),
        ("blocks", "Entier", "Blocs construits"),
        ("blocks_done", "Entier", "Blocs traités avec succès"),
        ("blocks_error", "Entier", "Blocs en erreur"),
        ("files", "Entier", "Fichiers placés dans des blocs"),
        ("prompt_tokens", "Entier", "Tokens d'entrée consommés"),
        ("completion_tokens", "Entier", "Tokens de sortie produits"),
        ("duration_s", "Décimal", "Durée du run en secondes"),
        ("avg_latency_ms", "Décimal", "Latence moyenne par bloc, en millisecondes"),
        ("tokens_per_file", "Décimal", "Coût moyen en tokens par fichier"),
    ),
}
"""Schéma stable : nom de fichier → colonnes (nom, type, description)."""

README_NAME = "README_powerbi.md"


def _iso(text: str) -> str:
    """Date SMBeagle ou ISO → `yyyy-MM-dd`, vide si illisible."""
    parsed = parse_smbeagle_datetime(text)
    return parsed.strftime("%Y-%m-%d") if parsed else ""


def _age_days(text: str, reference: date) -> str:
    parsed = parse_smbeagle_datetime(text)
    return str((reference - parsed.date()).days) if parsed else ""


def _readable(value: object) -> str:
    """Valeur JSON rendue en texte, jamais en `repr` Python.

    `null` → vide, `true`/`false` → `oui`/`non`, objet → `clé=valeur` séparés par
    des espaces, liste imbriquée → valeurs séparées par `, `.
    """
    if value is None:
        return ""
    if isinstance(value, bool):
        return "oui" if value else "non"
    if isinstance(value, dict):
        return " ".join(f"{key}={_readable(item)}" for key, item in value.items())
    if isinstance(value, list):
        return ", ".join(_readable(item) for item in value)
    return str(value)


def _flat(value: object) -> str:
    """Liste JSON stockée en TEXT → valeurs séparées par `|`, en texte présentable.

    Les colonnes concernées (`rgpd_data_types`, `finance_amounts`,
    `legal_parties`) sont lues telles quelles dans Power BI, le plus souvent en
    segment : un `None`, un `True` ou un `{'x': 1}` Python n'y a rien à faire.
    Les booléens sont rendus `oui`/`non` — c'est du texte libre français affiché
    à l'écran, pas une valeur logique : le schéma a pour cela des colonnes
    `Booléen 0/1` distinctes (`retention_required`, `readable`…), qui restent
    numériques. Les entrées vides (`null`) sont omises plutôt que de laisser un
    séparateur orphelin en tête de cellule.
    """
    if value in (None, "", "[]"):
        return ""
    try:
        data = json.loads(str(value))
    except ValueError:
        return str(value)
    if isinstance(data, list):
        return "|".join(part for part in (_readable(item) for item in data) if part)
    return _readable(data)


def _write_csv(path: Path, columns: tuple[Column, ...], rows: Iterable[list[Any]]) -> int:
    """Écrit le CSV ligne à ligne et rend le nombre de lignes de données.

    Chaque valeur passe par `tabular.csv_cell` : ces CSV sont en UTF-8 avec BOM
    et séparés par `;` **précisément pour qu'Excel les ouvre d'un double-clic**,
    et Excel y voit une formule dans toute cellule texte commençant par `=`, `+`,
    `-` ou `@` — d'où `#NOM ?` à la place de `- copie.docx` ou de
    `+33 1 23 45 67 89.pdf`, et une formule DDE à partir d'un résumé de la LLM.
    Les nombres et les dates ne sont pas touchés.
    """
    count = 0
    with path.open("w", newline="", encoding=ENCODING) as handle:
        writer = csv.writer(handle, delimiter=DELIMITER, lineterminator="\r\n")
        writer.writerow([name for name, _, _ in columns])
        for row in rows:
            writer.writerow([tabular.csv_cell(value) for value in row])
            count += 1
    return count


def _pages(db: Database, sql: str, key: str, *, page: int = PAGE_ROWS) -> Iterator[sqlite3.Row]:
    """Parcourt une requête volumineuse par tranches de `page` lignes.

    `sql` doit filtrer sur `<clé> > ?`, trier par cette clé croissante et finir
    par `LIMIT ?`. La pagination par clé (et non par `OFFSET`) garde un coût
    constant par tranche, et la mémoire bornée à une tranche quelle que soit la
    taille de la campagne.
    """
    last = -1
    while True:
        rows = db.query(sql, (last, page))
        if not rows:
            return
        yield from rows
        last = int(rows[-1][key])
        if len(rows) < page:
            return


def _files_rows(db: Database, reference: date) -> Iterator[list[Any]]:
    rows = _pages(db, "SELECT * FROM files WHERE id > ? ORDER BY id LIMIT ?", "id")
    return (
        [
            int(r["id"]),
            r["path"],
            r["name"],
            r["extension"],
            r["host"],
            r["hostname"],
            r["username"],
            r["unc_directory"],
            views.share_label(str(r["base"]), str(r["unc_directory"])),
            r["directory_type"],
            int(r["size_bytes"]),
            _iso(str(r["creation_time"])),
            _iso(str(r["last_write_time"])),
            _iso(str(r["access_time"])),
            r["file_attributes"],
            r["owner"],
            r["fast_hash"],
            r["file_signature"],
            int(r["readable"]),
            int(r["writeable"]),
            int(r["deletable"]),
            int(r["content_version"]),
            r["status"],
            r["exclusion_reason"] or "",
            int(r["priority_score"]),
            _age_days(str(r["access_time"]), reference),
            _age_days(str(r["last_write_time"]), reference),
        ]
        for r in rows
    )


_LATEST_ANALYSIS_JOIN = f" FROM files f JOIN analyses a ON {views.latest_analysis_sql('f.id')}"
"""Jointure « fichier + sa dernière analyse » — même règle que partout ailleurs.

Cette condition était réécrite ici à la main, alors qu'elle existe déjà dans
`docia.views` et dans `docia.db` : trois copies de la règle qui décide quelle
analyse fait foi. Elle vient maintenant de `views.latest_analysis_sql`."""


def _analyses_rows(db: Database) -> Iterator[list[Any]]:
    rows = _pages(
        db,
        "SELECT f.id AS file_id, f.last_write_time AS lwt, a.content_version AS cv, a.model AS model,"
        " a.prompt_hash AS ph, a.created_at AS created, a.segments AS segments, a.resume AS resume,"
        " a.security_classification AS sec, a.security_confidence AS secc,"
        " a.security_justification AS secj, a.rgpd_risk_level AS rgpd, a.rgpd_data_types AS rgpdt,"
        " a.rgpd_confidence AS rgpdc, a.finance_document_type AS fin, a.finance_amounts AS fina,"
        " a.finance_confidence AS finc, a.legal_contract_type AS leg, a.legal_parties AS legp,"
        " a.legal_confidence AS legc, a.retention_required AS rr, a.retention_years AS ry,"
        " a.retention_basis AS rb, a.retention_justification AS rj, a.retention_confidence AS rc"
        f"{_LATEST_ANALYSIS_JOIN}"
        " WHERE f.id > ? ORDER BY f.id LIMIT ?",
        "file_id",
    )
    for r in rows:
        written = parse_smbeagle_datetime(str(r["lwt"]))
        years = int(r["ry"] or 0)
        end = ""
        # `years == 0` avec `required=1` : le modèle exige la conservation sans en
        # donner la durée. La colonne reste **vide** (« indéterminée », comme
        # l'annonce le schéma) au lieu de rendre la date d'écriture elle-même,
        # c'est-à-dire une échéance déjà passée. Voir `views.retention_plan`.
        if written is not None and int(r["rr"] or 0) and years > 0:
            shifted = views.shift_years(written.date(), years)
            end = shifted.strftime("%Y-%m-%d")
        yield (
            [
                int(r["file_id"]),
                int(r["cv"]),
                r["model"],
                r["ph"],
                _iso(str(r["created"])),
                int(r["segments"]),
                r["resume"],
                r["sec"],
                int(r["secc"]),
                r["secj"],
                r["rgpd"],
                _flat(r["rgpdt"]),
                int(r["rgpdc"]),
                r["fin"],
                _flat(r["fina"]),
                int(r["finc"]),
                r["leg"],
                _flat(r["legp"]),
                int(r["legc"]),
                int(r["rr"] or 0),
                years,
                r["rb"],
                r["rj"],
                int(r["rc"] or 0),
                end,
            ]
        )


def _reviews_rows(db: Database) -> Iterator[list[Any]]:
    return (
        [
            int(r["file_id"]),
            r["status"],
            r["comment"],
            r["corrected_security"] or "",
            r["corrected_rgpd"] or "",
            r["corrected_retention_years"] if r["corrected_retention_years"] is not None else "",
            r["reviewer"],
            _iso(str(r["updated_at"])),
        ]
        for r in _pages(
            db, "SELECT * FROM reviews WHERE file_id > ? ORDER BY file_id LIMIT ?", "file_id"
        )
    )


def _duplicates_rows(db: Database) -> Iterator[list[Any]]:
    """Un exemplaire par ligne, famille par famille, sans jamais toutes les tenir.

    `views.duplicates(db)` — sans limite — construisait ici le rapport **entier** :
    une requête par famille (155 673 sur un parc de 934 028 fichiers) et la liste
    des chemins de tous les doublons de la campagne en mémoire, exactement ce que
    le docstring du module promet de ne pas faire. `iter_duplicate_families` lit
    les familles par paquets et les rend une par une.
    """
    for family in views.iter_duplicate_families(db):
        for file_id, path in zip(family.file_ids, family.paths, strict=True):
            yield [
                family.family_id,
                file_id,
                path,
                family.fast_hash,
                family.size_bytes,
                family.copies,
                family.reclaimable_bytes,
            ]


def _runs_rows(db: Database) -> Iterator[list[Any]]:
    return (
        [
            r.run_id,
            _iso(r.started_at),
            _iso(r.finished_at),
            r.status,
            r.model,
            r.prompt_hash,
            r.blocks,
            r.blocks_done,
            r.blocks_error,
            r.files,
            r.prompt_tokens,
            r.completion_tokens,
            r.duration_s,
            r.avg_latency_ms,
            r.tokens_per_file,
        ]
        for r in views.runs_summary(db)
    )


def _readme(generated: date, counts: dict[str, int]) -> str:
    lines = [
        "# Export Power BI — Doc-IA analyzer",
        "",
        f"Généré le {generated.strftime('%d/%m/%Y')}.",
        "",
        "## Format",
        "",
        "- CSV **UTF-8 avec BOM**, séparateur `;`, séparateur décimal `.`, fins de ligne CRLF.",
        "- Dates au format ISO `yyyy-MM-dd` ; les valeurs illisibles sont laissées vides.",
        "- Listes (types de données, montants, parties) aplaties, valeurs séparées par `|` ;",
        "  `null` est omis et les booléens y sont écrits `oui` / `non` (ce sont des colonnes de",
        "  texte ; les vraies valeurs logiques ont leurs propres colonnes `0/1`).",
        "- **Protection contre l'injection de formule** : ces CSV sont faits pour être ouverts",
        "  aussi d'un double-clic dans Excel, qui interprète comme une formule toute cellule de",
        "  texte commençant par `=`, `+`, `-` ou `@` (`- copie.docx` s'affiche alors `#NOM ?`, et",
        "  un résumé rendu par le modèle pourrait y glisser un `=HYPERLINK(...)` ou un appel DDE).",
        "  Ces valeurs **texte** sont donc préfixées d'une apostrophe `'` à l'écriture. Les",
        "  nombres et les dates ne sont pas touchés et restent exploitables tels quels ; dans",
        "  Power BI, retirer au besoin l'apostrophe de tête sur les colonnes concernées",
        '  (`Texte.Supprimer début` / `Text.TrimStart(_, "\'")`).',
        "",
        "## Modèle (étoile simple)",
        "",
        "`files.csv` est la table de faits ; `analyses.csv`, `reviews.csv` et `duplicates.csv`",
        "s'y rattachent par **`file_id`** (relation 1 → 1 pour les analyses et les revues,",
        "1 → n pour les doublons). `runs.csv` est indépendante (suivi d'exécution).",
        "",
        "## Rafraîchissement",
        "",
        "Les noms de fichiers et les colonnes sont **stables** : relancer",
        "`docia export --format powerbi --out <dossier>` écrase les fichiers en place.",
        "Planifier l'export puis une réplication du dossier vers Report Server ;",
        "un rafraîchissement quotidien suffit (les données proviennent d'un scan SMBeagle).",
        "",
        "## Tables et colonnes",
        "",
    ]
    for name, columns in POWERBI_COLUMNS.items():
        lines += [
            f"### `{name}` ({counts.get(name, 0)} ligne(s))",
            "",
            "| Colonne | Type | Description |",
            "|---|---|---|",
        ]
        lines += [f"| `{col}` | {kind} | {desc} |" for col, kind, desc in columns]
        lines.append("")
    return "\n".join(lines)


def export_powerbi(db: Database, directory: Path, *, today: date | None = None) -> list[Path]:
    """Écrit les CSV et le README dans `directory` ; rend la liste des fichiers écrits.

    `files.csv`, `analyses.csv` et `reviews.csv` sont lues par tranches
    (`PAGE_ROWS`) et écrites au fil de l'eau : elles ne coûtent que la tranche
    courante. `duplicates.csv` coûte en plus **un résumé par famille de doublons**
    — empreinte, taille, nombre d'exemplaires, octets récupérables — le temps de
    les ordonner par gain décroissant ; les chemins, eux, sont lus par paquets de
    familles. `runs.csv` tient un enregistrement par run.

    Autrement dit : la campagne entière de *fichiers* n'est jamais en mémoire,
    mais l'export n'est pas à mémoire constante pour autant. La docstring
    précédente affirmait « jamais la campagne entière » alors que
    `_duplicates_rows` construisait le rapport complet des doublons, chemins
    compris.
    """
    reference = today if today is not None else date.today()
    directory.mkdir(parents=True, exist_ok=True)
    tables: tuple[tuple[str, Iterable[list[Any]]], ...] = (
        ("files.csv", _files_rows(db, reference)),
        ("analyses.csv", _analyses_rows(db)),
        ("reviews.csv", _reviews_rows(db)),
        ("duplicates.csv", _duplicates_rows(db)),
        ("runs.csv", _runs_rows(db)),
    )
    written: list[Path] = []
    counts: dict[str, int] = {}
    for name, rows in tables:
        path = directory / name
        counts[name] = _write_csv(path, POWERBI_COLUMNS[name], rows)
        written.append(path)
    readme = directory / README_NAME
    readme.write_text(_readme(reference, counts), encoding="utf-8")
    written.append(readme)
    return written
