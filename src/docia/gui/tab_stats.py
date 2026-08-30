"""Écran Statistiques : quatre sous-onglets — Hygiène (doublons, ancienneté, répartitions),
Risque (classification, sensibles, matrice), Conservation (plan), Vérification (avancement).
Chaque sous-onglet : tuiles chiffrées, graphique en barres, tableau détaillé."""

from __future__ import annotations

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


class StatsTab:
    def __init__(self, app: Any, parent: Any) -> None:
        self.app = app
        self.parent = parent
        self.ctk = app.ctk

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
            command=lambda: self.app.show_tab("Rapports"),
        ).pack(side="right")

        self.sub = ctk.CTkTabview(p, anchor="w")
        self.sub.pack(fill="both", expand=True, padx=8, pady=(0, 6))
        self.hygiene = _Section(
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
        self.risk = _Section(
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
        self.retention = _Section(
            ctk,
            self.sub.add("Conservation"),
            tiles=[
                ("keep", "fichiers à conserver", ACCENT),
                ("expired", "durée échue", severity_color("C1")),
            ],
            views=("Plan de conservation",),
            on_view=self.refresh,
        )
        self.review = _Section(
            ctk,
            self.sub.add("Vérification"),
            tiles=[
                ("reviewed", "vérifiés", ACCENT_OK),
                ("corrected", "corrigés", severity_color("C2")),
                ("pct", "% vérifiés", ACCENT),
            ],
            views=("Écarts LLM / humain", "Runs"),
            on_view=self.refresh,
        )
        self.app.on_refresh(self.refresh)

    def _years(self) -> int:
        raw = self.years_var.get().strip()
        return int(raw) if raw.isdigit() and int(raw) > 0 else 5

    def refresh(self) -> None:
        if not self.app.db_path().exists():
            for s in (self.hygiene, self.risk, self.retention, self.review):
                s.set_table([], [], "aucune campagne ouverte")
                for tile in s.tiles.values():
                    tile.set("—")
                s.chart.draw([])
            return
        from docia import views

        years = self._years()
        try:
            with self.app.open_db() as db:
                self._refresh_hygiene(views, db, years)
                self._refresh_risk(views, db)
                self._refresh_retention(views, db)
                self._refresh_review(views, db)
        except Exception as exc:  # noqa: BLE001
            self.app.log(f"statistiques indisponibles : {exc}")

    # ---- hygiène
    def _refresh_hygiene(self, views: Any, db: Any, years: int) -> None:
        s = self.hygiene
        ov = views.overview(db, stale_years=years)
        s.tiles["dup"].set(format_bytes(ov.duplicate_reclaimable_bytes))
        s.tiles["stale"].set(format_int(ov.stale_files))
        s.tiles["cleanup"].set(format_bytes(ov.cleanup_bytes))
        s.chart.draw(
            [(g.label, g.bytes / 1e6, None) for g in views.by_extension(db, limit=8)],
            title="volume par extension",
            unit="Mo",
        )
        name = s.view_var.get()
        if name.startswith("Doublons"):
            rep = views.duplicates(db, limit=500)
            rows = [
                [
                    str(f.copies),
                    format_bytes(f.size_bytes),
                    format_bytes(f.reclaimable_bytes),
                    " | ".join(f.paths[:4]),
                ]
                for f in rep.families
            ]
            s.set_table(
                ["copies", "taille", "récupérable", "chemins"],
                rows,
                f"{rep.total_families} familles, {rep.total_copies} exemplaires — récupérable : {format_bytes(rep.total_reclaimable_bytes)}",
            )
        elif name.startswith("Ancienneté"):
            rows = [
                [
                    str(b.years),
                    format_int(b.not_accessed_files),
                    format_bytes(b.not_accessed_bytes),
                    format_int(b.not_modified_files),
                    format_bytes(b.not_modified_bytes),
                ]
                for b in views.stale_files(db)
            ]
            s.set_table(
                ["depuis (ans)", "non accédés", "volume", "non modifiés", "volume "],
                rows,
                "d'après les dates du scan SMBeagle",
            )
        elif name.startswith("Candidats"):
            rep = views.cleanup_candidates(db, years=years)
            rows = [
                [r.access_time, format_bytes(r.size_bytes), r.security, r.owner, r.path]
                for r in rep.rows
            ]
            s.set_table(
                ["dernier accès", "taille", "sécu", "propriétaire", "chemin"],
                rows,
                f"{rep.total_files} fichiers non requis, non sensibles, non accédés depuis {years} ans — {format_bytes(rep.total_bytes)} libérables",
            )
        elif name == "Répertoires":
            s.set_table(*_axis(views.by_directory(db, limit=300), "répertoire"))
        else:
            fn = {
                "Extensions": views.by_extension,
                "Propriétaires": views.by_owner,
                "Partages": views.by_share,
            }.get(name)
            stats = fn(db, limit=200) if fn else views.size_buckets(db)
            s.set_table(*_group(stats, name.lower()[:-1] if name.endswith("s") else name.lower()))

    # ---- risque
    def _refresh_risk(self, views: Any, db: Any) -> None:
        s = self.risk
        classes = db.classification_summary()
        sec = classes.get("security") or {}
        rg = classes.get("rgpd") or {}
        s.tiles["c3"].set(format_int(sec.get("C3", 0)))
        s.tiles["c2"].set(format_int(sec.get("C2", 0)))
        s.tiles["rgpd"].set(format_int(rg.get("high", 0) + rg.get("critical", 0)))
        s.chart.draw(
            [(f"{k}", float(sec.get(k, 0)), k) for k in ("C3", "C2", "C1", "C0", "N/A")],
            title="fichiers par classe de sécurité",
        )
        name = s.view_var.get()
        if name == "Fichiers sensibles":
            rows_s = views.top_sensitive(db, limit=300)
            rows = [
                [x.security, x.rgpd, x.owner, x.review_status or "", x.path, x.justification]
                for x in rows_s
            ]
            tags = [x.security if x.security in ("C3", "C2", "C1") else "ok" for x in rows_s]
            s.set_table(
                ["sécu", "RGPD", "propriétaire", "revue", "chemin", "justification"],
                rows,
                f"{len(rows_s)} fichiers C3 ou RGPD élevé/critique",
                tags,
            )
        else:
            label = name.split("× ")[1]
            axis = {"partage": "share", "propriétaire": "owner", "répertoire": "directory"}[label]
            s.set_table(*_axis(views.classification_matrix(db, axis=axis), label))

    # ---- conservation
    def _refresh_retention(self, views: Any, db: Any) -> None:
        s = self.retention
        plan = views.retention_plan(db)
        s.tiles["keep"].set(format_int(plan.total_files))
        s.tiles["expired"].set(format_int(plan.expired_files))
        by_basis: dict[str, int] = {}
        for r in plan.rows:
            by_basis[r.basis or "?"] = by_basis.get(r.basis or "?", 0) + 1
        s.chart.draw(
            sorted(((k, float(v), None) for k, v in by_basis.items()), key=lambda t: -t[1])[:8],
            title="fichiers par fondement",
        )
        rows = [
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
        ]
        tags = ["C1" if r.expired else "ok" for r in plan.rows]
        s.set_table(
            ["fin", "ans", "fondement", "échu", "propriétaire", "chemin", "justification"],
            rows,
            f"{plan.total_files} fichiers à conserver — {plan.expired_files} échus",
            tags,
        )

    # ---- vérification
    def _refresh_review(self, views: Any, db: Any) -> None:
        s = self.review
        pr = views.review_progress(db)
        s.tiles["reviewed"].set(format_int(pr.reviewed))
        s.tiles["corrected"].set(format_int(pr.corrected))
        s.tiles["pct"].set(f"{pr.percent_reviewed:.0f} %")
        s.chart.draw(
            [
                ("validés", float(pr.validated), "done"),
                ("corrigés", float(pr.corrected), "C2"),
                ("à vérifier", float(pr.to_review), "C1"),
                ("non vérifiés", float(pr.not_reviewed), "N/A"),
            ],
            title="avancement de la vérification",
        )
        if s.view_var.get() == "Runs":
            cols, rows = rows_from_records(views.runs_summary(db))
            s.set_table(cols, rows, "historique des runs (modèle, prompt, tokens, durée)")
        else:
            rows = [
                [d.path, d.llm_security, d.corrected_security, d.llm_rgpd, d.corrected_rgpd]
                for d in pr.discrepancies
            ]
            s.set_table(
                ["chemin", "sécu LLM", "sécu corrigée", "RGPD LLM", "RGPD corrigé"],
                rows,
                f"{pr.analyzed} analysés — {pr.to_review} à vérifier, {pr.validated} validés, {pr.corrected} corrigés, {pr.not_reviewed} non vérifiés",
            )


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
