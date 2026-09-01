"""Rapport HTML autonome : un seul fichier, CSS en ligne, aucun JavaScript.

Les graphiques sont des barres SVG écrites directement en Python (aucune
bibliothèque, aucun CDN) ; la page est lisible à l'écran comme à l'impression.
"""

from __future__ import annotations

from datetime import date
from html import escape

from docia import views
from docia.db import Database
from docia.report.data import ReportData, collect, listing_of

_SECURITY_COLORS: dict[str, str] = {
    "C0": "#9aa5b1",
    "C1": "#3d7ea6",
    "C2": "#d18f2a",
    "C3": "#b4453c",
    "N/A": "#cbd2d9",
}
_RGPD_COLORS: dict[str, str] = {
    "none": "#9aa5b1",
    "low": "#5b9279",
    "medium": "#d18f2a",
    "high": "#c96f3f",
    "critical": "#b4453c",
    "N/A": "#cbd2d9",
}

_CSS = """
:root { color-scheme: light; }
* { box-sizing: border-box; }
body { margin: 0; padding: 0 0 4rem; background: #f4f6f8; color: #1f2933;
  font-family: "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif; font-size: 14px; line-height: 1.5; }
.wrap { max-width: 1180px; margin: 0 auto; padding: 0 24px; }
header.page { background: #23384d; color: #fff; padding: 28px 0 22px; margin-bottom: 24px; }
header.page h1 { margin: 0 0 6px; font-size: 26px; font-weight: 600; }
header.page .meta { font-size: 13px; color: #c9d6e2; }
header.page .meta span { margin-right: 18px; white-space: nowrap; }
nav.sommaire { margin: 0 0 26px; font-size: 13px; }
nav.sommaire a { color: #23384d; text-decoration: none; border: 1px solid #d9e2ec; background: #fff;
  border-radius: 4px; padding: 5px 10px; margin: 0 6px 6px 0; display: inline-block; }
section { margin: 0 0 34px; }
h2 { font-size: 19px; margin: 30px 0 4px; padding-bottom: 6px; border-bottom: 2px solid #23384d; }
h3 { font-size: 15px; margin: 22px 0 8px; color: #33475b; }
p.note { color: #52606d; font-size: 13px; margin: 4px 0 12px; }
.tiles { display: flex; flex-wrap: wrap; gap: 12px; margin: 16px 0 8px; }
.tile { flex: 1 1 190px; background: #fff; border: 1px solid #d9e2ec; border-left: 4px solid #3d7ea6;
  border-radius: 4px; padding: 12px 14px; }
.tile .k { font-size: 12px; text-transform: uppercase; letter-spacing: .04em; color: #52606d; }
.tile .v { font-size: 22px; font-weight: 600; margin: 2px 0; }
.tile .s { font-size: 12px; color: #616e7c; }
.tile.alert { border-left-color: #b4453c; }
.tile.gain { border-left-color: #5b9279; }
table { border-collapse: collapse; width: 100%; background: #fff; margin: 6px 0 10px;
  border: 1px solid #d9e2ec; font-size: 13px; }
thead th { background: #eef2f6; text-align: left; padding: 7px 9px; border-bottom: 2px solid #cbd6e2;
  font-weight: 600; white-space: nowrap; }
tbody td { padding: 6px 9px; border-bottom: 1px solid #eaeff4; vertical-align: top; }
tbody tr:nth-child(even) td { background: #fafbfc; }
td.num, th.num { text-align: right; white-space: nowrap; font-variant-numeric: tabular-nums; }
.path, td.path { font-family: Consolas, "DejaVu Sans Mono", monospace; font-size: 12px; word-break: break-all; }
.badge { display: inline-block; border-radius: 3px; padding: 1px 6px; color: #fff; font-size: 12px; }
.legend { font-size: 12px; color: #52606d; margin: 2px 0 10px; }
.legend i { display: inline-block; width: 10px; height: 10px; border-radius: 2px; margin: 0 4px 0 12px; }
.empty { color: #7b8794; font-style: italic; }
.perimetre { background: #fdecea; border: 2px solid #b4453c; border-left-width: 8px;
  border-radius: 4px; padding: 14px 18px; margin: 0 0 22px; color: #6b1f18; }
.perimetre h2 { margin: 0 0 6px; font-size: 18px; border: 0; padding: 0; color: #8c2f26; }
.perimetre p { margin: 4px 0; font-size: 14px; }
.perimetre ul { margin: 6px 0 0; padding-left: 20px; font-size: 13px; }
.perimetre li { margin: 2px 0; }
.perimetre code { font-family: Consolas, "DejaVu Sans Mono", monospace; word-break: break-all; }
footer { color: #7b8794; font-size: 12px; border-top: 1px solid #d9e2ec; padding-top: 10px; margin-top: 30px; }
@media print {
  body { background: #fff; font-size: 11px; }
  header.page { background: #fff; color: #1f2933; border-bottom: 2px solid #23384d; }
  header.page .meta { color: #52606d; }
  nav.sommaire { display: none; }
  .perimetre { break-inside: avoid; break-after: avoid; }
  section { break-inside: avoid; }
  h2 { break-after: avoid; }
  table { break-inside: auto; }
  tr { break-inside: avoid; }
}
"""


def _esc(value: object) -> str:
    return escape(str(value), quote=True)


def _n(value: int) -> str:
    return views.format_int(value)


def _b(value: int) -> str:
    return views.format_bytes(value)


def _tile(key: str, value: str, sub: str = "", kind: str = "") -> str:
    classes = "tile" + (f" {kind}" if kind else "")
    sub_html = f'<div class="s">{_esc(sub)}</div>' if sub else ""
    return (
        f'<div class="{classes}"><div class="k">{_esc(key)}</div>'
        f'<div class="v">{_esc(value)}</div>{sub_html}</div>'
    )


def _table(headers: list[tuple[str, bool]], rows: list[list[str]], *, empty: str) -> str:
    """Tableau HTML ; `headers` = (libellé, aligné à droite). Cellules déjà échappées."""
    if not rows:
        return f'<p class="empty">{_esc(empty)}</p>'
    head = "".join(
        f'<th class="num">{_esc(label)}</th>' if num else f"<th>{_esc(label)}</th>"
        for label, num in headers
    )
    body: list[str] = []
    for row in rows:
        cells = "".join(
            f'<td class="num">{cell}</td>' if headers[i][1] else f"<td>{cell}</td>"
            for i, cell in enumerate(row)
        )
        body.append(f"<tr>{cells}</tr>")
    return f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(body)}</tbody></table>"


def _bars(items: list[tuple[str, int, str]], *, color: str = "#3d7ea6", unit: str = "") -> str:
    """Barres horizontales en SVG : (libellé, valeur, texte affiché)."""
    if not items:
        return f'<p class="empty">Aucune donnée{_esc(unit)}</p>'
    row_h, label_w, bar_w, value_w = 22, 250, 460, 110
    width = label_w + bar_w + value_w
    height = row_h * len(items) + 6
    peak = max((value for _, value, _ in items), default=0) or 1
    parts = [
        f'<svg viewBox="0 0 {width} {height}" width="100%" height="{height}" '
        f'role="img" xmlns="http://www.w3.org/2000/svg">'
    ]
    for index, (label, value, text) in enumerate(items):
        y = index * row_h + 4
        length = max(1, round(bar_w * value / peak))
        parts.append(
            f'<text x="0" y="{y + 12}" font-size="12" fill="#33475b">{_esc(label[:44])}</text>'
            f'<rect x="{label_w}" y="{y + 2}" width="{length}" height="13" rx="2" fill="{color}"/>'
            f'<text x="{label_w + bar_w + 8}" y="{y + 12}" font-size="12" fill="#52606d">{_esc(text)}</text>'
        )
    parts.append("</svg>")
    return "".join(parts)


def _stacked(
    rows: list[views.AxisRow],
    colors: dict[str, str],
    keys: tuple[str, ...],
    *,
    domain: str = "security",
) -> str:
    """Barres empilées : répartition des classes par valeur d'axe."""
    usable = [r for r in rows if r.analyzed > 0]
    if not usable:
        return '<p class="empty">Aucune analyse disponible pour cet axe.</p>'
    row_h, label_w, bar_w, value_w = 22, 250, 460, 110
    width = label_w + bar_w + value_w
    height = row_h * len(usable) + 6
    parts = [
        f'<svg viewBox="0 0 {width} {height}" width="100%" height="{height}" '
        f'role="img" xmlns="http://www.w3.org/2000/svg">'
    ]
    for index, row in enumerate(usable):
        y = index * row_h + 4
        offset = float(label_w)
        source = row.security if domain == "security" else row.rgpd
        for key in keys:
            count = source.get(key, 0)
            if not count:
                continue
            length = bar_w * count / row.analyzed
            parts.append(
                f'<rect x="{offset:.1f}" y="{y + 2}" width="{length:.1f}" height="13" '
                f'fill="{colors.get(key, "#cbd2d9")}"/>'
            )
            offset += length
        parts.append(
            f'<text x="0" y="{y + 12}" font-size="12" fill="#33475b">{_esc(row.label[-44:])}</text>'
            f'<text x="{label_w + bar_w + 8}" y="{y + 12}" font-size="12" fill="#52606d">'
            f"{_esc(_n(row.analyzed))} analysés</text>"
        )
    parts.append("</svg>")
    return "".join(parts)


def _legend(colors: dict[str, str], keys: tuple[str, ...]) -> str:
    items = "".join(f'<i style="background:{colors.get(k, "#cbd2d9")}"></i>{_esc(k)}' for k in keys)
    return f'<p class="legend">Légende :{items}</p>'


def _badge(value: str, colors: dict[str, str]) -> str:
    return f'<span class="badge" style="background:{colors.get(value, "#9aa5b1")}">{_esc(value)}</span>'


def _cut(data: ReportData, name: str, shown: int) -> str:
    """Note « ce tableau est coupé », ou rien du tout s'il ne l'est pas.

    Un rapport de direction a le droit d'être court ; il n'a pas le droit
    d'annoncer « 182 346 fichiers, 7,5 To libérables » puis d'en montrer
    cinquante sans le dire.
    """
    hidden = data.hidden(name, shown)
    if not hidden:
        return ""
    total = shown + hidden
    return (
        f'<p class="note">Les {_n(shown)} premières lignes sur {_n(total)} — '
        f"les {_n(hidden)} autres sont dans {_esc(listing_of(name))}.</p>"
    )


def _undetermined_note(plan: views.RetentionPlan) -> str:
    """Fichiers « à conserver » dont le modèle n'a pas donné de durée."""
    if not plan.undetermined_files:
        return ""
    return (
        f'<p class="note">{_n(plan.undetermined_files)} fichier(s) sont déclarés à conserver '
        "<strong>sans durée</strong> (0 an) : la réponse du modèle est incohérente, aucune fin "
        "de conservation n'est calculée et ces fichiers ne sont <strong>jamais</strong> comptés "
        "comme échus. À trancher par un humain.</p>"
    )


# ----------------------------------------------------------------- sections


def _bandeau_perimetre(data: ReportData) -> str:
    """Bandeau « inventaire incomplet », **avant** le sommaire et la synthèse.

    Le rapport HTML est remis à la direction et sert à justifier des suppressions :
    un périmètre amputé ne peut pas être une note en pied de page. Chaîne vide
    quand le périmètre est entier — un rapport normal ne montre rien.
    """
    scope = data.scope
    if not scope.incomplete:
        return ""
    cibles = scope.skipped_targets
    liste = (
        "<ul>" + "".join(f"<li><code>{_esc(c)}</code></li>" for c in cibles) + "</ul>"
        if cibles
        else ""
    )
    quoi_faire = "".join(f"<p>{_esc(message)}</p>" for message in scope.warnings)
    return (
        '<div class="perimetre" id="perimetre"><h2>Inventaire incomplet</h2>'
        f"<p>{_esc(scope.headline())}</p>"
        f"{liste}{quoi_faire}</div>"
    )


def _section_synthese(data: ReportData) -> str:
    o = data.overview
    tiles = [
        _tile("Fichiers inventoriés", _n(o.total_files), _b(o.total_bytes)),
        _tile(
            "Analysés par la LLM",
            _n(o.analyzed),
            f"{views.percent(o.analyzed, o.total_files)} %".replace(".", ","),
        ),
        _tile("À analyser", _n(o.pending), f"exclus : {_n(o.excluded)} — erreurs : {_n(o.errors)}"),
        _tile(
            "Espace récupérable (doublons)",
            _b(o.duplicate_reclaimable_bytes),
            f"{_n(o.duplicate_families)} famille(s)",
            kind="gain",
        ),
        _tile(
            f"Non accédés depuis {o.stale_years} ans",
            _b(o.stale_bytes),
            f"{_n(o.stale_files)} fichier(s)",
            kind="gain",
        ),
        _tile(
            "Fichiers sensibles (C2/C3)", _n(o.sensitive_files), "classification LLM", kind="alert"
        ),
        _tile("RGPD élevé ou critique", _n(o.rgpd_at_risk), "à traiter en priorité", kind="alert"),
        _tile("À conserver", _n(o.retention_files), "plan de conservation"),
        _tile(
            "Candidats au nettoyage",
            _b(o.cleanup_bytes),
            f"{_n(o.cleanup_files)} fichier(s)",
            kind="gain",
        ),
        _tile("Vérifiés par un humain", _n(o.reviewed), f"sur {_n(o.analyzed)} analysés"),
    ]
    return (
        '<section id="synthese"><h2>1. Synthèse</h2>'
        '<p class="note">Chiffres clés de la base au jour du rapport. Les gains d\'espace '
        "cumulent des périmètres qui peuvent se recouper (un doublon peut aussi être ancien).</p>"
        f'<div class="tiles">{"".join(tiles)}</div></section>'
    )


def _section_hygiene(data: ReportData) -> str:
    dup = data.duplicates
    dup_rows = [
        [
            _esc(f.paths[0] if f.paths else f.family_id),
            _n(f.copies),
            _b(f.size_bytes),
            _b(f.reclaimable_bytes),
            _esc(f.fast_hash[:16]),
        ]
        for f in dup.families
    ]
    stale_rows = [
        [
            f"{_esc(b.years)} an(s)",
            _esc(b.cutoff.strftime("%d/%m/%Y")),
            _n(b.not_accessed_files),
            _b(b.not_accessed_bytes),
            _n(b.not_modified_files),
            _b(b.not_modified_bytes),
        ]
        for b in data.stale
    ]
    ext_rows = [
        [_esc(g.label), _n(g.files), _b(g.bytes), f"{g.percent_bytes:.1f}".replace(".", ",")]
        for g in data.extensions
    ]
    owner_rows = [
        [_esc(g.label), _n(g.files), _b(g.bytes), f"{g.percent_bytes:.1f}".replace(".", ",")]
        for g in data.owners
    ]
    share_rows = [
        [_esc(g.label), _n(g.files), _b(g.bytes), f"{g.percent_bytes:.1f}".replace(".", ",")]
        for g in data.shares
    ]
    dir_rows = [
        [_esc(r.label), _n(r.files), _b(r.bytes), _n(r.analyzed), _n(r.sensitive)]
        for r in data.directories
    ]
    size_rows = [
        [_esc(g.label), _n(g.files), _b(g.bytes), f"{g.percent_files:.1f}".replace(".", ",")]
        for g in data.sizes
    ]
    return "".join(
        [
            '<section id="hygiene"><h2>2. Hygiène du stockage</h2>',
            '<p class="note">Ces indicateurs ne dépendent pas de la LLM : ils sont disponibles '
            "dès l'import du scan SMBeagle.</p>",
            "<h3>2.1 Doublons — espace récupérable</h3>",
            f'<p class="note">{_n(dup.total_families)} famille(s) de fichiers identiques '
            f"(même empreinte et même taille), {_n(dup.total_copies)} exemplaire(s) au total, "
            f"<strong>{_esc(_b(dup.total_reclaimable_bytes))} récupérables</strong> en ne gardant "
            "qu'un exemplaire par famille.</p>",
            _bars(
                [
                    (
                        f.paths[0].rsplit("\\", 1)[-1] if f.paths else f.family_id,
                        f.reclaimable_bytes,
                        _b(f.reclaimable_bytes),
                    )
                    for f in dup.families[:12]
                ],
                color="#5b9279",
            ),
            _table(
                [
                    ("Exemplaire de référence", False),
                    ("Copies", True),
                    ("Taille unitaire", True),
                    ("Récupérable", True),
                    ("Empreinte", False),
                ],
                dup_rows,
                empty="Aucun doublon détecté.",
            ),
            _cut(data, "duplicates", len(dup.families)),
            "<h3>2.2 Ancienneté</h3>",
            '<p class="note">Fichiers dont la date de dernier accès (ou de dernière '
            "modification) est antérieure au seuil.</p>",
            _table(
                [
                    ("Seuil", False),
                    ("Antérieur au", False),
                    ("Non accédés", True),
                    ("Volume", True),
                    ("Non modifiés", True),
                    ("Volume", True),
                ],
                stale_rows,
                empty="Aucune date exploitable.",
            ),
            _bars(
                [
                    (f"{b.years} an(s)", b.not_accessed_bytes, _b(b.not_accessed_bytes))
                    for b in data.stale
                ],
                color="#3d7ea6",
            ),
            "<h3>2.3 Tailles</h3>",
            _table(
                [("Tranche", False), ("Fichiers", True), ("Volume", True), ("% fichiers", True)],
                size_rows,
                empty="Aucun fichier.",
            ),
            f'<p class="note">Fichiers d\'au plus {_n(data.tiny.max_bytes)} octets : '
            f"{_n(data.tiny.files)} (dont {_n(data.tiny.empty_files)} vides), "
            f"{_esc(_b(data.tiny.bytes))}.</p>",
            "<h3>2.4 Extensions</h3>",
            _bars([(g.label, g.bytes, _b(g.bytes)) for g in data.extensions[:12]]),
            _table(
                [("Extension", False), ("Fichiers", True), ("Volume", True), ("% volume", True)],
                ext_rows,
                empty="Aucun fichier.",
            ),
            _cut(data, "extensions", len(data.extensions)),
            "<h3>2.5 Propriétaires</h3>",
            _bars([(g.label, g.bytes, _b(g.bytes)) for g in data.owners[:12]], color="#7d6b91"),
            _table(
                [("Propriétaire", False), ("Fichiers", True), ("Volume", True), ("% volume", True)],
                owner_rows,
                empty="Aucun fichier.",
            ),
            _cut(data, "owners", len(data.owners)),
            "<h3>2.6 Partages</h3>",
            _table(
                [("Partage", False), ("Fichiers", True), ("Volume", True), ("% volume", True)],
                share_rows,
                empty="Aucun partage.",
            ),
            "<h3>2.7 Répertoires</h3>",
            _table(
                [
                    ("Répertoire (2 niveaux)", False),
                    ("Fichiers", True),
                    ("Volume", True),
                    ("Analysés", True),
                    ("C2 + C3", True),
                ],
                dir_rows,
                empty="Aucun répertoire.",
            ),
            _cut(data, "directories", len(data.directories)),
            "</section>",
        ]
    )


def _section_risque(data: ReportData) -> str:
    sec_headers: list[tuple[str, bool]] = [("Partage", False), ("Analysés", True)]
    sec_headers += [(k, True) for k in views.SECURITY_CLASSES]
    sec_headers += [("RGPD élevé/critique", True)]
    share_rows = [
        [_esc(r.label), _n(r.analyzed)]
        + [_n(r.security.get(k, 0)) for k in views.SECURITY_CLASSES]
        + [_n(r.rgpd.get("high", 0) + r.rgpd.get("critical", 0))]
        for r in data.by_share
    ]
    owner_rows = [
        [_esc(r.label), _n(r.analyzed)]
        + [_n(r.security.get(k, 0)) for k in views.SECURITY_CLASSES]
        + [_n(r.rgpd.get("high", 0) + r.rgpd.get("critical", 0))]
        for r in data.by_owner
    ]
    top_rows = [
        [
            _badge(f.security, _SECURITY_COLORS),
            _badge(f.rgpd, _RGPD_COLORS),
            f'<span class="path">{_esc(f.path)}</span>',
            _esc(f.owner),
            _b(f.size_bytes),
            _esc(f.resume[:220]),
            _esc(f.justification[:180]),
            _esc(f.review_status or "—"),
        ]
        for f in data.sensitive
    ]
    plan = data.retention
    plan_rows = [
        [
            _esc(r.end_date.strftime("%d/%m/%Y") if r.end_date else "—"),
            _esc(views.RETENTION_BASIS_LABELS.get(r.basis, r.basis)),
            _esc(views.RETENTION_UNDETERMINED) if r.undetermined else _n(r.years),
            f'<span class="path">{_esc(r.path)}</span>',
            _esc(r.owner),
            _b(r.size_bytes),
            "oui" if r.expired else "non",
        ]
        for r in plan.rows
    ]
    basis_rows = [[_esc(g.label), _n(g.files), _b(g.bytes)] for g in plan.by_basis]
    cleanup = data.cleanup
    cleanup_rows = [
        [
            f'<span class="path">{_esc(r.path)}</span>',
            _esc(r.owner),
            _b(r.size_bytes),
            _esc(r.access_time),
            _esc(r.security),
        ]
        for r in cleanup.rows
    ]
    return "".join(
        [
            '<section id="risque"><h2>3. Risque et conformité</h2>',
            "<h3>3.1 Classification par partage</h3>",
            _legend(_SECURITY_COLORS, views.SECURITY_CLASSES),
            _stacked(data.by_share, _SECURITY_COLORS, views.SECURITY_CLASSES),
            _table(sec_headers, share_rows, empty="Aucune analyse."),
            "<h3>3.2 Classification par propriétaire</h3>",
            _stacked(data.by_owner, _SECURITY_COLORS, views.SECURITY_CLASSES),
            _table(sec_headers, owner_rows, empty="Aucune analyse."),
            _cut(data, "by_owner", len(data.by_owner)),
            "<h3>3.3 Répartition RGPD par partage</h3>",
            _legend(_RGPD_COLORS, views.RGPD_LEVELS),
            _stacked(data.by_share, _RGPD_COLORS, views.RGPD_LEVELS, domain="rgpd"),
            "<h3>3.4 Top des fichiers sensibles</h3>",
            '<p class="note">Fichiers classés C2 ou C3, ou dont le risque RGPD est élevé '
            "ou critique, du plus sensible au moins sensible.</p>",
            _table(
                [
                    ("Sécurité", False),
                    ("RGPD", False),
                    ("Chemin", False),
                    ("Propriétaire", False),
                    ("Taille", True),
                    ("Résumé", False),
                    ("Justification", False),
                    ("Revue", False),
                ],
                top_rows,
                empty="Aucun fichier sensible identifié.",
            ),
            _cut(data, "sensitive", len(data.sensitive)),
            "<h3>3.5 Plan de conservation</h3>",
            f'<p class="note">{_n(plan.total_files)} fichier(s) à conserver '
            f"({_esc(_b(plan.total_bytes))}), dont {_n(plan.expired_files)} dont la durée est "
            "échue. La date de fin vaut « dernière écriture + durée ».</p>",
            _undetermined_note(plan),
            _table(
                [("Fondement", False), ("Fichiers", True), ("Volume", True)],
                basis_rows,
                empty="Aucune obligation de conservation identifiée.",
            ),
            _table(
                [
                    ("Fin de conservation", False),
                    ("Fondement", False),
                    ("Durée (ans)", True),
                    ("Chemin", False),
                    ("Propriétaire", False),
                    ("Taille", True),
                    ("Échu", False),
                ],
                plan_rows,
                empty="Aucun fichier à conserver.",
            ),
            _cut(data, "retention", len(plan.rows)),
            "<h3>3.6 Candidats au nettoyage</h3>",
            f'<p class="note">Fichiers sans obligation de conservation, classés C0 ou C1, '
            f"non accédés depuis {cleanup.years} ans (avant le "
            f"{_esc(cleanup.cutoff.strftime('%d/%m/%Y'))}) : {_n(cleanup.total_files)} fichier(s), "
            f"<strong>{_esc(_b(cleanup.total_bytes))} libérables</strong>.</p>",
            _table(
                [
                    ("Chemin", False),
                    ("Propriétaire", False),
                    ("Taille", True),
                    ("Dernier accès", False),
                    ("Sécurité", False),
                ],
                cleanup_rows,
                empty="Aucun candidat.",
            ),
            _cut(data, "cleanup", len(cleanup.rows)),
            "</section>",
        ]
    )


def _section_verification(data: ReportData) -> str:
    r = data.reviews
    gap_rows = [
        [
            f'<span class="path">{_esc(d.path)}</span>',
            _esc(d.llm_security),
            _esc(d.corrected_security or "—"),
            _esc(d.llm_rgpd),
            _esc(d.corrected_rgpd or "—"),
        ]
        for d in r.discrepancies
    ]
    tiles = [
        _tile("À vérifier", _n(r.to_review)),
        _tile("Validés", _n(r.validated)),
        _tile("Corrigés", _n(r.corrected)),
        _tile("Non revus", _n(r.not_reviewed), f"sur {_n(r.analyzed)} analysés"),
        _tile("Avancement", f"{r.percent_reviewed:.1f} %".replace(".", ","), "validés + corrigés"),
    ]
    return "".join(
        [
            '<section id="verification"><h2>4. Vérification humaine</h2>',
            f'<div class="tiles">{"".join(tiles)}</div>',
            "<h3>4.1 Écarts entre la LLM et la correction humaine</h3>",
            _table(
                [
                    ("Chemin", False),
                    ("Sécurité LLM", False),
                    ("Sécurité corrigée", False),
                    ("RGPD LLM", False),
                    ("RGPD corrigé", False),
                ],
                gap_rows,
                empty="Aucun écart : aucune classe corrigée ne diffère de la classe rendue.",
            ),
            _cut(data, "discrepancies", len(r.discrepancies)),
            "</section>",
        ]
    )


def _section_execution(data: ReportData) -> str:
    run_rows = [
        [
            _n(r.run_id),
            _esc(r.started_at),
            _esc(r.status),
            _esc(r.model),
            _esc(r.prompt_hash),
            _n(r.blocks),
            _n(r.blocks_error),
            _n(r.files),
            _n(r.prompt_tokens),
            _n(r.completion_tokens),
            f"{r.tokens_per_file:.1f}".replace(".", ","),
            f"{r.duration_s:.0f}",
        ]
        for r in data.runs
    ]
    status_rows = [
        [_esc(k), _n(v), _b(data.status.bytes.get(k, 0))]
        for k, v in sorted(data.status.counts.items(), key=lambda kv: -kv[1])
    ]
    reason_rows = [[_esc(g.label), _n(g.files), _b(g.bytes)] for g in data.status.reasons]
    return "".join(
        [
            '<section id="execution"><h2>5. Exécution</h2>',
            "<h3>5.1 Statuts des fichiers</h3>",
            _table(
                [("Statut", False), ("Fichiers", True), ("Volume", True)],
                status_rows,
                empty="Base vide.",
            ),
            "<h3>5.2 Exclusions et erreurs</h3>",
            _table(
                [("Raison", False), ("Fichiers", True), ("Volume", True)],
                reason_rows,
                empty="Aucune exclusion ni erreur enregistrée.",
            ),
            "<h3>5.3 Runs</h3>",
            _table(
                [
                    ("Run", True),
                    ("Début", False),
                    ("Statut", False),
                    ("Modèle", False),
                    ("Prompt", False),
                    ("Blocs", True),
                    ("Blocs en erreur", True),
                    ("Fichiers", True),
                    ("Tokens prompt", True),
                    ("Tokens sortie", True),
                    ("Tokens / fichier", True),
                    ("Durée (s)", True),
                ],
                run_rows,
                empty="Aucun run enregistré.",
            ),
            "</section>",
        ]
    )


def render_html(db: Database, *, today: date | None = None, data: ReportData | None = None) -> str:
    """Rapport complet en un seul fichier HTML autonome."""
    report = data if data is not None else collect(db, today=today)
    o = report.overview
    nav = "".join(
        f'<a href="#{anchor}">{_esc(label)}</a>'
        for anchor, label in (
            ("synthese", "1. Synthèse"),
            ("hygiene", "2. Hygiène"),
            ("risque", "3. Risque"),
            ("verification", "4. Vérification"),
            ("execution", "5. Exécution"),
        )
    )
    meta = (
        f"<span>Généré le {_esc(o.generated_at.strftime('%d/%m/%Y'))}</span>"
        f"<span>Base : {_esc(o.db_path)}</span>"
        f"<span>Modèle : {_esc(o.model or '—')}</span>"
        f"<span>Prompt : {_esc(o.prompt_name)} {_esc(o.prompt_hash)}</span>"
        + ("<span>⚠ Périmètre incomplet</span>" if report.scope.incomplete else "")
    )
    body = "".join(
        [
            _section_synthese(report),
            _section_hygiene(report),
            _section_risque(report),
            _section_verification(report),
            _section_execution(report),
        ]
    )
    return (
        "<!DOCTYPE html>\n"
        '<html lang="fr"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1">'
        "<title>Doc-IA — rapport d'analyse</title>"
        f"<style>{_CSS}</style></head><body>"
        f'<header class="page"><div class="wrap"><h1>Doc-IA — rapport d\'analyse</h1>'
        f'<div class="meta">{meta}</div></div></header>'
        f'<div class="wrap">{_bandeau_perimetre(report)}<nav class="sommaire">{nav}</nav>{body}'
        "<footer>Rapport produit par Doc-IA analyzer — les classifications sont proposées "
        "par un modèle de langage et doivent être vérifiées avant toute décision "
        "de suppression.</footer></div></body></html>"
    )
