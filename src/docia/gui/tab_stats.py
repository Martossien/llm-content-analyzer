"""Onglet Statistiques : hygiène du partage (doublons, ancienneté, tailles, répartitions)
et risque (matrice, sensibles, conservation, nettoyage, revues) — mêmes vues que le rapport."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from docia.gui.widgets import Table, rows_from_records

_VIEWS = (
    "Synthèse",
    "Doublons (espace récupérable)",
    "Ancienneté (non accédés / non modifiés)",
    "Tailles",
    "Extensions",
    "Propriétaires",
    "Partages",
    "Répertoires",
    "Classification × partage",
    "Classification × propriétaire",
    "Classification × répertoire",
    "Fichiers sensibles",
    "Plan de conservation",
    "Candidats au nettoyage",
    "Vérification",
    "Runs",
)


class StatsTab:
    def __init__(self, app: Any, parent: Any) -> None:
        self.app = app
        self.parent = parent
        self.ctk = app.ctk

    def build(self) -> None:
        ctk, p = self.ctk, self.parent
        top = ctk.CTkFrame(p, fg_color="transparent")
        top.pack(fill="x", padx=10, pady=(10, 4))
        ctk.CTkLabel(top, text="Vue").pack(side="left")
        self.view_var = ctk.StringVar(value=_VIEWS[0])
        ctk.CTkOptionMenu(
            top,
            variable=self.view_var,
            values=list(_VIEWS),
            width=300,
            command=lambda _v: self.refresh(),
        ).pack(side="left", padx=6)
        ctk.CTkLabel(top, text="Seuil nettoyage (ans)").pack(side="left", padx=(12, 2))
        self.years_var = ctk.StringVar(value="5")
        ctk.CTkEntry(top, textvariable=self.years_var, width=50).pack(side="left")
        ctk.CTkButton(top, text="Rafraîchir", width=90, command=self.refresh).pack(
            side="left", padx=8
        )
        ctk.CTkButton(top, text="Rapport HTML…", command=lambda: self._report("html")).pack(
            side="right", padx=4
        )
        ctk.CTkButton(top, text="Rapport Markdown…", command=lambda: self._report("md")).pack(
            side="right", padx=4
        )

        self.summary = ctk.CTkLabel(p, text="", justify="left", anchor="w")
        self.summary.pack(fill="x", padx=10, pady=(4, 4))
        self.table = Table(ctk, p, columns=[], height=420)
        self.table.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        self.app.on_refresh(self.refresh)

    def refresh(self) -> None:
        if not self.app.db_path().exists():
            self.summary.configure(text="base absente")
            self.table.columns, self.table.rows = [], []
            self.table.set_rows([])
            return
        from docia import views

        name = self.view_var.get()
        years = int(self.years_var.get() or 5) if self.years_var.get().strip().isdigit() else 5
        try:
            with self.app.open_db() as db:
                cols, rows, summary = self._compute(views, db, name, years)
        except Exception as exc:  # noqa: BLE001
            self.summary.configure(text=f"vue indisponible : {exc}")
            return
        self.summary.configure(text=summary)
        self.table.columns = cols
        self.table.set_rows(rows)

    def _compute(
        self, views: Any, db: Any, name: str, years: int
    ) -> tuple[list[str], list[list[str]], str]:
        if name == "Synthèse":
            ov = views.overview(db, stale_years=years)
            data = asdict(ov)
            cols = ["indicateur", "valeur"]
            rows = [[k, _fmt(v)] for k, v in data.items() if not isinstance(v, list | dict)]
            return cols, rows, "Chiffres clés de la base (hygiène et risque)."
        if name.startswith("Doublons"):
            rep = views.duplicates(db, limit=500)
            cols = ["copies", "taille", "récupérable", "chemins"]
            rows = [
                [
                    str(f.copies),
                    views.format_bytes(f.size_bytes),
                    views.format_bytes(f.reclaimable_bytes),
                    " | ".join(f.paths[:4]),
                ]
                for f in rep.families
            ]
            return (
                cols,
                rows,
                f"{rep.total_families} familles, {rep.total_copies} exemplaires — espace récupérable : {views.format_bytes(rep.total_reclaimable_bytes)}",
            )
        if name.startswith("Ancienneté"):
            buckets = views.stale_files(db)
            cols = ["depuis (ans)", "non accédés", "volume", "non modifiés", "volume "]
            rows = [
                [
                    str(b.years),
                    str(b.not_accessed_files),
                    views.format_bytes(b.not_accessed_bytes),
                    str(b.not_modified_files),
                    views.format_bytes(b.not_modified_bytes),
                ]
                for b in buckets
            ]
            return cols, rows, "Fichiers non accédés / non modifiés depuis N ans (dates SMBeagle)."
        if name == "Tailles":
            return _group(views, views.size_buckets(db), "tranche")
        if name == "Extensions":
            return _group(views, views.by_extension(db, limit=200), "extension")
        if name == "Propriétaires":
            return _group(views, views.by_owner(db, limit=200), "propriétaire")
        if name == "Partages":
            return _group(views, views.by_share(db, limit=200), "partage")
        if name == "Répertoires":
            return _axis(views, views.by_directory(db, limit=300), "répertoire")
        if name.startswith("Classification ×"):
            axis = {"partage": "share", "propriétaire": "owner", "répertoire": "directory"}[
                name.split("× ")[1]
            ]
            return _axis(views, views.classification_matrix(db, axis=axis), name.split("× ")[1])
        if name == "Fichiers sensibles":
            rows_s = views.top_sensitive(db, limit=300)
            cols = ["sécu", "RGPD", "propriétaire", "revue", "chemin", "justification"]
            rows = [
                [s.security, s.rgpd, s.owner, s.review_status or "", s.path, s.justification]
                for s in rows_s
            ]
            return cols, rows, f"{len(rows_s)} fichier(s) C3 ou RGPD critical/high."
        if name == "Plan de conservation":
            plan = views.retention_plan(db)
            cols = ["fin", "ans", "fondement", "expiré", "propriétaire", "chemin", "justification"]
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
            return (
                cols,
                rows,
                f"{plan.total_files} fichier(s) à conserver — {plan.expired_files} échu(s).",
            )
        if name == "Candidats au nettoyage":
            rep = views.cleanup_candidates(db, years=years)
            cols = ["dernier accès", "taille", "sécu", "propriétaire", "chemin"]
            rows = [
                [r.access_time, views.format_bytes(r.size_bytes), r.security, r.owner, r.path]
                for r in rep.rows
            ]
            return (
                cols,
                rows,
                f"{rep.total_files} fichier(s) non requis, non sensibles, non accédés depuis {years} ans — {views.format_bytes(rep.total_bytes)} libérables.",
            )
        if name == "Vérification":
            pr = views.review_progress(db)
            cols = ["chemin", "sécu LLM", "sécu corrigée", "RGPD LLM", "RGPD corrigé"]
            rows = [
                [d.path, d.llm_security, d.corrected_security, d.llm_rgpd, d.corrected_rgpd]
                for d in pr.discrepancies
            ]
            return (
                cols,
                rows,
                f"analysés {pr.analyzed} — à vérifier {pr.to_review}, validés {pr.validated}, corrigés {pr.corrected}, non revus {pr.not_reviewed}.",
            )
        cols, rows = rows_from_records(views.runs_summary(db))
        return cols, rows, "Historique des runs (modèle, prompt, tokens, durée)."

    def _report(self, fmt: str) -> None:
        from tkinter import filedialog

        path = filedialog.asksaveasfilename(
            defaultextension=f".{fmt}", filetypes=[(fmt.upper(), f"*.{fmt}")]
        )
        if not path:
            return
        app = self.app
        db_path = str(app.db_path())

        def work() -> None:
            from docia.cli import main as cli_main

            code = cli_main(["--db", db_path, "report", "--format", fmt, "--out", path])
            app.log(f"rapport {fmt} → {path} ({'OK' if code == 0 else 'échec'})")

        app.run_in_thread(work, "rapport")


def _group(views: Any, stats: list[Any], label: str) -> tuple[list[str], list[list[str]], str]:
    cols = [label, "fichiers", "% fichiers", "volume", "% volume"]
    rows = [
        [
            g.label,
            str(g.files),
            f"{g.percent_files:.1f}",
            views.format_bytes(g.bytes),
            f"{g.percent_bytes:.1f}",
        ]
        for g in stats
    ]
    return cols, rows, f"{len(stats)} ligne(s)."


def _axis(views: Any, stats: list[Any], label: str) -> tuple[list[str], list[list[str]], str]:
    cols = [label, "fichiers", "volume", "analysés", "C0", "C1", "C2", "C3", "N/A", "RGPD high+"]
    rows = []
    for a in stats:
        sec = a.security or {}
        rg = a.rgpd or {}
        rows.append(
            [
                a.label,
                str(a.files),
                views.format_bytes(a.bytes),
                str(a.analyzed),
                *[str(sec.get(c, 0)) for c in ("C0", "C1", "C2", "C3", "N/A")],
                str(rg.get("high", 0) + rg.get("critical", 0)),
            ]
        )
    return cols, rows, f"{len(stats)} ligne(s)."


def _fmt(v: object) -> str:
    if isinstance(v, float):
        return f"{v:,.1f}".replace(",", " ")
    if isinstance(v, int) and not isinstance(v, bool):
        return f"{v:,}".replace(",", " ")
    return str(v)
