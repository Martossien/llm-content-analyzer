"""Écran Statistiques : quatre sous-onglets — Hygiène (doublons, ancienneté, répartitions),
Risque (classification, sensibles, matrice), Conservation (plan), Vérification (avancement).
Chaque sous-onglet : tuiles chiffrées, graphique en barres, tableau détaillé.

Les chiffres sont calculés **hors du thread Tk** (`app.run_background`) et **uniquement
pour le sous-onglet affiché** : sur une campagne de 200 000 fichiers, tout recalculer dans
la fenêtre gelait l'interface une trentaine de secondes à chaque rafraîchissement.
Les fonctions `compute_*` ne touchent pas à Tk : elles rendent une `SectionData`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from docia.gui.theme import (
    ACCENT,
    ACCENT_OK,
    FONT_FAMILY,
    FONT_SIZE_SMALL,
    format_bytes,
    format_int,
    severity_color,
)
from docia.gui.widgets import BarChart, KpiTile, Table, rows_from_records

TAB_NAME = "Statistiques"

_HYGIENE_VIEWS = (
    "Doublons (espace récupérable)",
    "Ancienneté (non accédés / non modifiés)",
    "Candidats au nettoyage",
    "Extensions",
    "Propriétaires",
    "Partages",
    "Répertoires",
    "Tailles",
)
_RISK_VIEWS = (
    "Fichiers sensibles",
    "Classification × partage",
    "Classification × propriétaire",
    "Classification × répertoire",
)
_RETENTION_VIEWS = ("Plan de conservation",)
_REVIEW_VIEWS = ("Écarts LLM / humain", "Runs")

_WAIT = "calcul en cours…"


@dataclass
class SectionData:
    """Ce qu'un sous-onglet affiche — calculé hors du thread Tk, appliqué dedans."""

    tiles: dict[str, str] = field(default_factory=dict)
    chart: list[tuple[str, float, str | None]] = field(default_factory=list)
    chart_title: str = ""
    chart_unit: str = ""
    cols: list[str] = field(default_factory=list)
    rows: list[list[str]] = field(default_factory=list)
    summary: str = ""
    tags: list[str] | None = None


class _Section:
    """Un sous-onglet : tuiles + graphique + sélecteur de vue + tableau."""

    def __init__(
        self,
        ctk: Any,
        parent: Any,
        *,
        tiles: list[tuple[str, str, str]],
        views: tuple[str, ...],
        on_view: Any,
    ) -> None:
        self.ctk = ctk
        row = ctk.CTkFrame(parent, fg_color="transparent")
        row.pack(fill="x", padx=6, pady=(6, 2))
        self.tiles: dict[str, KpiTile] = {}
        for key, label, color in tiles:
            tile = KpiTile(ctk, row, label=label, color=color, width=200)
            tile.pack(side="left", padx=4)
            self.tiles[key] = tile
        self.chart = BarChart(ctk, row, width=560, height=100)
        self.chart.pack(side="left", padx=10, fill="x", expand=True)
        sel = ctk.CTkFrame(parent, fg_color="transparent")
        sel.pack(fill="x", padx=6, pady=(4, 2))
        ctk.CTkLabel(sel, text="Vue").pack(side="left")
        self.view_var = ctk.StringVar(value=views[0])
        ctk.CTkOptionMenu(
            sel, variable=self.view_var, values=list(views), width=300, command=lambda _v: on_view()
        ).pack(side="left", padx=6)
        self.summary = ctk.CTkLabel(
            sel, text="", anchor="w", font=ctk.CTkFont(family=FONT_FAMILY, size=FONT_SIZE_SMALL)
        )
        self.summary.pack(side="left", padx=10, fill="x", expand=True)
        self.table = Table(ctk, parent, columns=[], height=360)
        self.table.pack(fill="both", expand=True, padx=6, pady=(0, 6))

    def set_table(
        self, cols: list[str], rows: list[list[str]], summary: str, tags: list[str] | None = None
    ) -> None:
        self.table.columns = cols
        self.table.set_rows(rows, tags)
        self.summary.configure(text=summary)

    def apply(self, data: SectionData) -> None:
        """Affiche une `SectionData` (thread Tk)."""
        for key, tile in self.tiles.items():
            tile.set(data.tiles.get(key, "—"))
        self.chart.draw(data.chart, title=data.chart_title, unit=data.chart_unit)
        self.set_table(data.cols, data.rows, data.summary, data.tags)

    def waiting(self) -> None:
        """Indique que le calcul est lancé — la fenêtre reste utilisable."""
        self.summary.configure(text=_WAIT)


class StatsTab:
    def __init__(self, app: Any, parent: Any) -> None:
        self.app = app
        self.parent = parent
        self.ctk = app.ctk
        self._dirty = True
        self._shown_key: tuple[str, str, int] | None = None
        self._token = 0

    def build(self) -> None:
        ctk, p = self.ctk, self.parent
        top = ctk.CTkFrame(p, fg_color="transparent")
        top.pack(fill="x", padx=8, pady=(6, 0))
        ctk.CTkLabel(top, text="Seuil d'ancienneté (ans)").pack(side="left")
        self.years_var = ctk.StringVar(value="5")
        ctk.CTkEntry(top, textvariable=self.years_var, width=50).pack(side="left", padx=(4, 8))
        ctk.CTkButton(top, text="Actualiser", width=100, command=self.refresh).pack(side="left")
        ctk.CTkButton(
            top,
            text="Rapport HTML…",
            width=130,
            fg_color="#6b7280",
            command=self._html_report,
        ).pack(side="right")

        self.sub = ctk.CTkTabview(p, anchor="w", command=self.refresh_if_needed)
        self.sub.pack(fill="both", expand=True, padx=8, pady=(0, 6))
        self.sections: dict[str, _Section] = {}
        self.hygiene = self.sections["Hygiène"] = _Section(
            ctk,
            self.sub.add("Hygiène"),
            tiles=[
                ("dup", "récupérables (doublons)", severity_color("C2")),
                ("stale", "non accédés (seuil)", severity_color("C1")),
                ("cleanup", "libérables (nettoyage)", ACCENT),
            ],
            views=_HYGIENE_VIEWS,
            on_view=self.refresh,
        )
        self.risk = self.sections["Risque"] = _Section(
            ctk,
            self.sub.add("Risque"),
            tiles=[
                ("c3", "C3 secret", severity_color("C3")),
                ("c2", "C2 confidentiel", severity_color("C2")),
                ("rgpd", "RGPD élevé / critique", severity_color("critical")),
            ],
            views=_RISK_VIEWS,
            on_view=self.refresh,
        )
        self.retention = self.sections["Conservation"] = _Section(
            ctk,
            self.sub.add("Conservation"),
            tiles=[
                ("keep", "fichiers à conserver", ACCENT),
                ("expired", "durée échue", severity_color("C1")),
            ],
            views=_RETENTION_VIEWS,
            on_view=self.refresh,
        )
        self.review = self.sections["Vérification"] = _Section(
            ctk,
            self.sub.add("Vérification"),
            tiles=[
                ("reviewed", "vérifiés", ACCENT_OK),
                ("corrected", "corrigés", severity_color("C2")),
                ("pct", "% vérifiés", ACCENT),
            ],
            views=_REVIEW_VIEWS,
            on_view=self.refresh,
        )
        self.app.on_refresh(self.refresh)

    # ------------------------------------------------------------------ état
    def _years(self) -> int:
        raw = self.years_var.get().strip()
        return int(raw) if raw.isdigit() and int(raw) > 0 else 5

    def _section_name(self) -> str:
        name = str(self.sub.get())
        return name if name in self.sections else "Hygiène"

    def refresh(self) -> None:
        """Marque l'écran à recalculer ; le calcul n'a lieu que s'il est visible."""
        self._dirty = True
        self.refresh_if_needed()

    def refresh_if_needed(self) -> None:
        """Recalcule le sous-onglet affiché si nécessaire (changement d'onglet, de vue…)."""
        if self.app.current_tab() != TAB_NAME:
            return
        section = self._section_name()
        key = (section, self.sections[section].view_var.get(), self._years())
        if not self._dirty and key == self._shown_key:
            return
        self._compute(section, key)

    def _compute(self, section: str, key: tuple[str, str, int]) -> None:
        if not self.app.db_path().exists():
            for s in self.sections.values():
                s.apply(SectionData(summary="aucune campagne ouverte"))
            self._dirty = False
            self._shown_key = None
            return
        self.sections[section].waiting()
        self._token += 1
        token = self._token
        app, view_name, years = self.app, key[1], key[2]

        def compute() -> SectionData:
            from docia import views

            with app.open_db() as db:
                return _COMPUTE[section](views, db, view_name, years)

        def apply(data: SectionData) -> None:
            if token != self._token:  # un calcul plus récent a été demandé
                return
            self.sections[section].apply(data)
            self._dirty = False
            self._shown_key = key

        app.run_background(compute, apply, name="statistiques")

    def _html_report(self) -> None:
        """Produit le rapport HTML (mêmes chiffres que cet écran) et l'ouvre."""
        from docia.gui.service_shim import produce_document

        produce_document(self.app, "html", "report")


# --------------------------------------------------------------- calculs (hors Tk)


def compute_hygiene(views: Any, db: Any, view_name: str, years: int) -> SectionData:
    """Doublons, ancienneté, nettoyage et répartitions — aucun appel Tk."""
    ov = views.overview(db, stale_years=years)
    data = SectionData(
        tiles={
            "dup": format_bytes(ov.duplicate_reclaimable_bytes),
            "stale": format_int(ov.stale_files),
            "cleanup": format_bytes(ov.cleanup_bytes),
        },
        chart=[(g.label, g.bytes / 1e6, None) for g in views.by_extension(db, limit=8)],
        chart_title="volume par extension",
        chart_unit="Mo",
    )
    if view_name.startswith("Doublons"):
        rep = views.duplicates(db, limit=500)
        data.cols = ["copies", "taille", "récupérable", "chemins"]
        data.rows = [
            [
                str(f.copies),
                format_bytes(f.size_bytes),
                format_bytes(f.reclaimable_bytes),
                " | ".join(f.paths[:4]),
            ]
            for f in rep.families
        ]
        data.summary = (
            f"{rep.total_families} familles, {rep.total_copies} exemplaires — "
            f"récupérable : {format_bytes(rep.total_reclaimable_bytes)}"
        )
    elif view_name.startswith("Ancienneté"):
        data.cols = ["depuis (ans)", "non accédés", "volume", "non modifiés", "volume "]
        data.rows = [
            [
                str(b.years),
                format_int(b.not_accessed_files),
                format_bytes(b.not_accessed_bytes),
                format_int(b.not_modified_files),
                format_bytes(b.not_modified_bytes),
            ]
            for b in views.stale_files(db)
        ]
        data.summary = "d'après les dates du scan SMBeagle"
    elif view_name.startswith("Candidats"):
        rep = views.cleanup_candidates(db, years=years)
        data.cols = ["dernier accès", "taille", "sécu", "propriétaire", "chemin"]
        data.rows = [
            [r.access_time, format_bytes(r.size_bytes), r.security, r.owner, r.path]
            for r in rep.rows
        ]
        data.summary = (
            f"{rep.total_files} fichiers non requis, non sensibles, non accédés depuis "
            f"{years} ans — {format_bytes(rep.total_bytes)} libérables"
        )
    elif view_name == "Répertoires":
        data.cols, data.rows, data.summary = _axis(views.by_directory(db, limit=300), "répertoire")
    else:
        fn = {
            "Extensions": views.by_extension,
            "Propriétaires": views.by_owner,
            "Partages": views.by_share,
        }.get(view_name)
        stats = fn(db, limit=200) if fn else views.size_buckets(db)
        label = view_name.lower()[:-1] if view_name.endswith("s") else view_name.lower()
        data.cols, data.rows, data.summary = _group(stats, label)
    return data


def compute_risk(views: Any, db: Any, view_name: str, _years: int) -> SectionData:
    """Classification de sécurité, fichiers sensibles et matrices — aucun appel Tk."""
    classes = db.classification_summary()
    sec = classes.get("security") or {}
    rg = classes.get("rgpd") or {}
    data = SectionData(
        tiles={
            "c3": format_int(sec.get("C3", 0)),
            "c2": format_int(sec.get("C2", 0)),
            "rgpd": format_int(rg.get("high", 0) + rg.get("critical", 0)),
        },
        chart=[(k, float(sec.get(k, 0)), k) for k in ("C3", "C2", "C1", "C0", "N/A")],
        chart_title="fichiers par classe de sécurité",
    )
    if view_name == "Fichiers sensibles":
        rows_s = views.top_sensitive(db, limit=300)
        data.cols = ["sécu", "RGPD", "propriétaire", "revue", "chemin", "justification"]
        data.rows = [
            [x.security, x.rgpd, x.owner, x.review_status or "", x.path, x.justification]
            for x in rows_s
        ]
        data.tags = [x.security if x.security in ("C3", "C2", "C1") else "ok" for x in rows_s]
        data.summary = f"{len(rows_s)} fichiers C3 ou RGPD élevé/critique"
    else:
        label = view_name.split("× ")[1]
        axis = {"partage": "share", "propriétaire": "owner", "répertoire": "directory"}[label]
        data.cols, data.rows, data.summary = _axis(
            views.classification_matrix(db, axis=axis), label
        )
    return data


def compute_retention(views: Any, db: Any, _view_name: str, _years: int) -> SectionData:
    """Plan de conservation : fondement, durée, échéance — aucun appel Tk."""
    plan = views.retention_plan(db)
    by_basis: dict[str, int] = {}
    for r in plan.rows:
        by_basis[r.basis or "?"] = by_basis.get(r.basis or "?", 0) + 1
    top_basis = sorted(by_basis.items(), key=lambda kv: -kv[1])[:8]
    chart: list[tuple[str, float, str | None]] = [(k, float(v), None) for k, v in top_basis]
    return SectionData(
        tiles={
            "keep": format_int(plan.total_files),
            "expired": format_int(plan.expired_files),
        },
        chart=chart,
        chart_title="fichiers par fondement",
        cols=["fin", "ans", "fondement", "échu", "propriétaire", "chemin", "justification"],
        rows=[
            [
                str(r.end_date or ""),
                str(r.years),
                r.basis,
                "oui" if r.expired else "",
                r.owner,
                r.path,
                r.justification,
            ]
            for r in plan.rows
        ],
        summary=f"{plan.total_files} fichiers à conserver — {plan.expired_files} échus",
        tags=["C1" if r.expired else "ok" for r in plan.rows],
    )


def compute_review(views: Any, db: Any, view_name: str, _years: int) -> SectionData:
    """Avancement de la vérification humaine et écarts avec la LLM — aucun appel Tk."""
    pr = views.review_progress(db)
    data = SectionData(
        tiles={
            "reviewed": format_int(pr.reviewed),
            "corrected": format_int(pr.corrected),
            "pct": f"{pr.percent_reviewed:.0f} %",
        },
        chart=[
            ("validés", float(pr.validated), "done"),
            ("corrigés", float(pr.corrected), "C2"),
            ("à vérifier", float(pr.to_review), "C1"),
            ("non vérifiés", float(pr.not_reviewed), "N/A"),
        ],
        chart_title="avancement de la vérification",
    )
    if view_name == "Runs":
        data.cols, data.rows = rows_from_records(views.runs_summary(db))
        data.summary = "historique des runs (modèle, prompt, tokens, durée)"
    else:
        data.cols = ["chemin", "sécu LLM", "sécu corrigée", "RGPD LLM", "RGPD corrigé"]
        data.rows = [
            [d.path, d.llm_security, d.corrected_security, d.llm_rgpd, d.corrected_rgpd]
            for d in pr.discrepancies
        ]
        data.summary = (
            f"{pr.analyzed} analysés — {pr.to_review} à vérifier, {pr.validated} validés, "
            f"{pr.corrected} corrigés, {pr.not_reviewed} non vérifiés"
        )
    return data


_COMPUTE = {
    "Hygiène": compute_hygiene,
    "Risque": compute_risk,
    "Conservation": compute_retention,
    "Vérification": compute_review,
}


def _group(stats: list[Any], label: str) -> tuple[list[str], list[list[str]], str]:
    cols = [label, "fichiers", "% fichiers", "volume", "% volume"]
    rows = [
        [
            g.label,
            format_int(g.files),
            f"{g.percent_files:.1f}",
            format_bytes(g.bytes),
            f"{g.percent_bytes:.1f}",
        ]
        for g in stats
    ]
    return cols, rows, f"{len(stats)} lignes"


def _axis(stats: list[Any], label: str) -> tuple[list[str], list[list[str]], str]:
    cols = [label, "fichiers", "volume", "analysés", "C0", "C1", "C2", "C3", "N/A", "RGPD élevé+"]
    rows = []
    for a in stats:
        sec = a.security or {}
        rg = a.rgpd or {}
        rows.append(
            [
                a.label,
                format_int(a.files),
                format_bytes(a.bytes),
                format_int(a.analyzed),
                *[str(sec.get(c, 0)) for c in ("C0", "C1", "C2", "C3", "N/A")],
                str(rg.get("high", 0) + rg.get("critical", 0)),
            ]
        )
    return cols, rows, f"{len(stats)} lignes"
