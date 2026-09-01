"""Rapport Markdown : mêmes chiffres que le HTML, collable dans un mail ou un ticket.

Mêmes *chiffres* — les deux rendus lisent le même `ReportData` —, pas la même
*forme* : ce Markdown n'a pas la section « Répartition RGPD par partage » du HTML,
et ses numéros de sous-sections décalent donc d'une unité à partir de 3.3. C'est
délibéré (voir `docia.report.data`).
"""

from __future__ import annotations

from datetime import date

from docia import views
from docia.db import Database
from docia.report.data import ReportData, collect, listing_of


def _n(value: int) -> str:
    return views.format_int(value)


def _b(value: int) -> str:
    return views.format_bytes(value)


def _cell(value: object) -> str:
    """Contenu sûr d'une cellule de tableau GFM.

    Les barres verticales et les retours à la ligne casseraient le tableau. `&`,
    `<` et `>` sont échappés comme le fait déjà `html._esc` : ce Markdown est
    destiné « à un wiki ou à un mail », c'est-à-dire à des rendus qui laissent
    passer le HTML brut (GitLab, Confluence, courrier au format HTML), et les
    cellules contiennent des chemins du partage ainsi que le `resume` et les
    justifications rendus par le modèle, sans contrainte de caractères. Les
    accents graves sont neutralisés pour qu'un résumé n'ouvre pas de code en
    ligne au milieu du tableau.
    """
    text = str(value).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return text.replace("`", "\\`").replace("|", "\\|").replace("\n", " ")


def _table(headers: list[str], rows: list[list[str]], *, empty: str) -> list[str]:
    if not rows:
        return [f"*{empty}*", ""]
    out = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    out += ["| " + " | ".join(_cell(c) for c in row) + " |" for row in rows]
    out.append("")
    return out


def _cut(data: ReportData, name: str, shown: int) -> list[str]:
    """Note « ce tableau est coupé », ou rien s'il ne l'est pas (voir `html._cut`)."""
    hidden = data.hidden(name, shown)
    if not hidden:
        return []
    return [
        f"*Les {_n(shown)} premières lignes sur {_n(shown + hidden)} — "
        f"les {_n(hidden)} autres sont dans {listing_of(name)}.*",
        "",
    ]


def render_markdown(
    db: Database, *, today: date | None = None, data: ReportData | None = None
) -> str:
    """Rapport complet en Markdown (tableaux GFM)."""
    report = data if data is not None else collect(db, today=today)
    o = report.overview
    lines: list[str] = [
        "# Doc-IA — rapport d'analyse",
        "",
        f"- **Généré le** : {o.generated_at.strftime('%d/%m/%Y')}",
        f"- **Base** : `{o.db_path}`",
        f"- **Modèle** : {o.model or '—'}",
        f"- **Prompt** : {o.prompt_name} `{o.prompt_hash}`",
        "",
        "## 1. Synthèse",
        "",
    ]
    lines += _table(
        ["Indicateur", "Valeur", "Détail"],
        [
            ["Fichiers inventoriés", _n(o.total_files), _b(o.total_bytes)],
            [
                "Analysés par la LLM",
                _n(o.analyzed),
                f"{views.percent(o.analyzed, o.total_files)} %",
            ],
            ["À analyser", _n(o.pending), f"exclus {_n(o.excluded)}, erreurs {_n(o.errors)}"],
            [
                "Espace récupérable (doublons)",
                _b(o.duplicate_reclaimable_bytes),
                f"{_n(o.duplicate_families)} famille(s)",
            ],
            [
                f"Non accédés depuis {o.stale_years} ans",
                _b(o.stale_bytes),
                f"{_n(o.stale_files)} fichier(s)",
            ],
            ["Fichiers sensibles (C2/C3)", _n(o.sensitive_files), "classification LLM"],
            ["RGPD élevé ou critique", _n(o.rgpd_at_risk), "à traiter en priorité"],
            ["À conserver", _n(o.retention_files), "plan de conservation"],
            ["Candidats au nettoyage", _b(o.cleanup_bytes), f"{_n(o.cleanup_files)} fichier(s)"],
            ["Vérifiés par un humain", _n(o.reviewed), f"sur {_n(o.analyzed)} analysés"],
        ],
        empty="Base vide.",
    )

    dup = report.duplicates
    lines += [
        "## 2. Hygiène du stockage",
        "",
        "### 2.1 Doublons",
        "",
        f"{_n(dup.total_families)} famille(s), {_n(dup.total_copies)} exemplaire(s), "
        f"**{_b(dup.total_reclaimable_bytes)} récupérables**.",
        "",
    ]
    lines += _table(
        ["Exemplaire de référence", "Copies", "Taille unitaire", "Récupérable", "Empreinte"],
        [
            [
                f.paths[0] if f.paths else f.family_id,
                _n(f.copies),
                _b(f.size_bytes),
                _b(f.reclaimable_bytes),
                f.fast_hash[:16],
            ]
            for f in dup.families
        ],
        empty="Aucun doublon détecté.",
    )
    lines += _cut(report, "duplicates", len(dup.families))
    lines += ["### 2.2 Ancienneté", ""]
    lines += _table(
        ["Seuil", "Antérieur au", "Non accédés", "Volume", "Non modifiés", "Volume"],
        [
            [
                f"{b.years} an(s)",
                b.cutoff.strftime("%d/%m/%Y"),
                _n(b.not_accessed_files),
                _b(b.not_accessed_bytes),
                _n(b.not_modified_files),
                _b(b.not_modified_bytes),
            ]
            for b in report.stale
        ],
        empty="Aucune date exploitable.",
    )
    lines += ["### 2.3 Tailles", ""]
    lines += _table(
        ["Tranche", "Fichiers", "Volume", "% fichiers"],
        [[g.label, _n(g.files), _b(g.bytes), f"{g.percent_files}"] for g in report.sizes],
        empty="Aucun fichier.",
    )
    lines += [
        f"Fichiers d'au plus {_n(report.tiny.max_bytes)} octets : {_n(report.tiny.files)} "
        f"(dont {_n(report.tiny.empty_files)} vides), {_b(report.tiny.bytes)}.",
        "",
        "### 2.4 Extensions",
        "",
    ]
    lines += _table(
        ["Extension", "Fichiers", "Volume", "% volume"],
        [[g.label, _n(g.files), _b(g.bytes), f"{g.percent_bytes}"] for g in report.extensions],
        empty="Aucun fichier.",
    )
    lines += _cut(report, "extensions", len(report.extensions))
    lines += ["### 2.5 Propriétaires", ""]
    lines += _table(
        ["Propriétaire", "Fichiers", "Volume", "% volume"],
        [[g.label, _n(g.files), _b(g.bytes), f"{g.percent_bytes}"] for g in report.owners],
        empty="Aucun fichier.",
    )
    lines += _cut(report, "owners", len(report.owners))
    lines += ["### 2.6 Partages", ""]
    lines += _table(
        ["Partage", "Fichiers", "Volume", "% volume"],
        [[g.label, _n(g.files), _b(g.bytes), f"{g.percent_bytes}"] for g in report.shares],
        empty="Aucun partage.",
    )
    lines += ["### 2.7 Répertoires", ""]
    lines += _table(
        ["Répertoire (2 niveaux)", "Fichiers", "Volume", "Analysés", "C2 + C3"],
        [
            [r.label, _n(r.files), _b(r.bytes), _n(r.analyzed), _n(r.sensitive)]
            for r in report.directories
        ],
        empty="Aucun répertoire.",
    )
    lines += _cut(report, "directories", len(report.directories))

    matrix_headers = ["Partage", "Analysés", *views.SECURITY_CLASSES, "RGPD élevé/critique"]
    lines += ["## 3. Risque et conformité", "", "### 3.1 Classification par partage", ""]
    lines += _table(
        matrix_headers,
        [
            [r.label, _n(r.analyzed)]
            + [_n(r.security.get(k, 0)) for k in views.SECURITY_CLASSES]
            + [_n(r.rgpd.get("high", 0) + r.rgpd.get("critical", 0))]
            for r in report.by_share
        ],
        empty="Aucune analyse.",
    )
    lines += ["### 3.2 Classification par propriétaire", ""]
    lines += _table(
        ["Propriétaire", "Analysés", *views.SECURITY_CLASSES, "RGPD élevé/critique"],
        [
            [r.label, _n(r.analyzed)]
            + [_n(r.security.get(k, 0)) for k in views.SECURITY_CLASSES]
            + [_n(r.rgpd.get("high", 0) + r.rgpd.get("critical", 0))]
            for r in report.by_owner
        ],
        empty="Aucune analyse.",
    )
    lines += _cut(report, "by_owner", len(report.by_owner))
    lines += ["### 3.3 Top des fichiers sensibles", ""]
    lines += _table(
        ["Sécurité", "RGPD", "Chemin", "Propriétaire", "Taille", "Résumé", "Revue"],
        [
            [
                f.security,
                f.rgpd,
                f.path,
                f.owner,
                _b(f.size_bytes),
                f.resume[:160],
                f.review_status or "—",
            ]
            for f in report.sensitive
        ],
        empty="Aucun fichier sensible identifié.",
    )
    lines += _cut(report, "sensitive", len(report.sensitive))
    plan = report.retention
    lines += [
        "### 3.4 Plan de conservation",
        "",
        f"{_n(plan.total_files)} fichier(s) à conserver ({_b(plan.total_bytes)}), "
        f"dont {_n(plan.expired_files)} dont la durée est échue.",
        "",
    ]
    if plan.undetermined_files:
        lines += [
            f"{_n(plan.undetermined_files)} fichier(s) sont déclarés à conserver **sans durée** "
            "(0 an) : réponse incohérente du modèle, aucune fin de conservation n'est calculée "
            "et ils ne sont **jamais** comptés comme échus. À trancher par un humain.",
            "",
        ]
    lines += _table(
        ["Fondement", "Fichiers", "Volume"],
        [[g.label, _n(g.files), _b(g.bytes)] for g in plan.by_basis],
        empty="Aucune obligation de conservation identifiée.",
    )
    lines += _table(
        ["Fin de conservation", "Fondement", "Durée (ans)", "Chemin", "Taille", "Échu"],
        [
            [
                r.end_date.strftime("%d/%m/%Y") if r.end_date else "—",
                views.RETENTION_BASIS_LABELS.get(r.basis, r.basis),
                views.RETENTION_UNDETERMINED if r.undetermined else _n(r.years),
                r.path,
                _b(r.size_bytes),
                "oui" if r.expired else "non",
            ]
            for r in plan.rows
        ],
        empty="Aucun fichier à conserver.",
    )
    lines += _cut(report, "retention", len(plan.rows))
    cleanup = report.cleanup
    lines += [
        "### 3.5 Candidats au nettoyage",
        "",
        f"Sans obligation de conservation, C0/C1, non accédés depuis {cleanup.years} ans "
        f"(avant le {cleanup.cutoff.strftime('%d/%m/%Y')}) : {_n(cleanup.total_files)} fichier(s), "
        f"**{_b(cleanup.total_bytes)} libérables**.",
        "",
    ]
    lines += _table(
        ["Chemin", "Propriétaire", "Taille", "Dernier accès", "Sécurité"],
        [[r.path, r.owner, _b(r.size_bytes), r.access_time, r.security] for r in cleanup.rows],
        empty="Aucun candidat.",
    )
    lines += _cut(report, "cleanup", len(cleanup.rows))

    rev = report.reviews
    lines += [
        "## 4. Vérification humaine",
        "",
        f"À vérifier {_n(rev.to_review)} — validés {_n(rev.validated)} — "
        f"corrigés {_n(rev.corrected)} — non revus {_n(rev.not_reviewed)} "
        f"(avancement {rev.percent_reviewed} %).",
        "",
    ]
    lines += _table(
        ["Chemin", "Sécurité LLM", "Sécurité corrigée", "RGPD LLM", "RGPD corrigé"],
        [
            [
                d.path,
                d.llm_security,
                d.corrected_security or "—",
                d.llm_rgpd,
                d.corrected_rgpd or "—",
            ]
            for d in rev.discrepancies
        ],
        empty="Aucun écart entre la LLM et les corrections humaines.",
    )
    lines += _cut(report, "discrepancies", len(rev.discrepancies))

    lines += ["## 5. Exécution", "", "### 5.1 Statuts des fichiers", ""]
    lines += _table(
        ["Statut", "Fichiers", "Volume"],
        [
            [k, _n(v), _b(report.status.bytes.get(k, 0))]
            for k, v in sorted(report.status.counts.items(), key=lambda kv: -kv[1])
        ],
        empty="Base vide.",
    )
    lines += ["### 5.2 Exclusions et erreurs", ""]
    lines += _table(
        ["Raison", "Fichiers", "Volume"],
        [[g.label, _n(g.files), _b(g.bytes)] for g in report.status.reasons],
        empty="Aucune exclusion ni erreur enregistrée.",
    )
    lines += ["### 5.3 Runs", ""]
    lines += _table(
        [
            "Run",
            "Début",
            "Statut",
            "Modèle",
            "Prompt",
            "Blocs",
            "Blocs en erreur",
            "Fichiers",
            "Tokens prompt",
            "Tokens sortie",
            "Tokens / fichier",
            "Durée (s)",
        ],
        [
            [
                _n(r.run_id),
                r.started_at,
                r.status,
                r.model,
                r.prompt_hash,
                _n(r.blocks),
                _n(r.blocks_error),
                _n(r.files),
                _n(r.prompt_tokens),
                _n(r.completion_tokens),
                f"{r.tokens_per_file}",
                f"{r.duration_s:.0f}",
            ]
            for r in report.runs
        ],
        empty="Aucun run enregistré.",
    )
    lines += [
        "---",
        "",
        "*Les classifications sont proposées par un modèle de langage et doivent être "
        "vérifiées avant toute décision de suppression.*",
        "",
    ]
    return "\n".join(lines)
