"""Onglet Résultats & vérification : table filtrable, fiche d'un fichier, revue humaine, exports."""

from __future__ import annotations

from typing import Any

from docia.db import REVIEW_STATUSES
from docia.gui.helpers import result_rows_v31
from docia.gui.widgets import ReadOnlyText, Table

_COLUMNS = ("id", "nom", "sécu", "rgpd", "finance", "juridique", "conservation", "revue", "résumé")
_ALL = "(tous)"


class ResultsTab:
    def __init__(self, app: Any, parent: Any) -> None:
        self.app = app
        self.parent = parent
        self.ctk = app.ctk
        self._rows: list[dict[str, str]] = []
        self._selected_id: int | None = None

    def build(self) -> None:
        ctk, p = self.ctk, self.parent
        filters = ctk.CTkFrame(p, fg_color="transparent")
        filters.pack(fill="x", padx=10, pady=(10, 4))
        ctk.CTkLabel(filters, text="Sécurité").pack(side="left")
        self.sec_var = ctk.StringVar(value=_ALL)
        ctk.CTkOptionMenu(
            filters,
            variable=self.sec_var,
            values=[_ALL, "C3", "C2", "C1", "C0", "N/A"],
            width=90,
            command=lambda _v: self.refresh(),
        ).pack(side="left", padx=(4, 10))
        ctk.CTkLabel(filters, text="RGPD").pack(side="left")
        self.rgpd_var = ctk.StringVar(value=_ALL)
        ctk.CTkOptionMenu(
            filters,
            variable=self.rgpd_var,
            values=[_ALL, "critical", "high", "medium", "low", "none"],
            width=100,
            command=lambda _v: self.refresh(),
        ).pack(side="left", padx=(4, 10))
        ctk.CTkLabel(filters, text="Revue").pack(side="left")
        self.review_var = ctk.StringVar(value=_ALL)
        ctk.CTkOptionMenu(
            filters,
            variable=self.review_var,
            values=[_ALL, "non revu", *REVIEW_STATUSES],
            width=120,
            command=lambda _v: self.refresh(),
        ).pack(side="left", padx=(4, 10))
        ctk.CTkLabel(filters, text="Recherche").pack(side="left")
        self.search_var = ctk.StringVar(value="")
        entry = ctk.CTkEntry(filters, textvariable=self.search_var, width=220)
        entry.pack(side="left", padx=(4, 10))
        entry.bind("<Return>", lambda _e: self.refresh())
        ctk.CTkButton(filters, text="Filtrer", width=80, command=self.refresh).pack(side="left")
        self.count_label = ctk.CTkLabel(filters, text="")
        self.count_label.pack(side="right")

        self.table = Table(ctk, p, columns=_COLUMNS, on_select=self._select, height=260)
        self.table.pack(fill="both", expand=True, padx=10, pady=4)

        detail = ctk.CTkFrame(p)
        detail.pack(fill="x", padx=10, pady=(4, 8))
        self.detail = ReadOnlyText(ctk, detail, height=150)
        self.detail.pack(fill="x", padx=6, pady=6)
        review = ctk.CTkFrame(detail, fg_color="transparent")
        review.pack(fill="x", padx=6, pady=(0, 6))
        ctk.CTkLabel(review, text="Vérification :").pack(side="left")
        self.status_var = ctk.StringVar(value="validated")
        ctk.CTkOptionMenu(
            review, variable=self.status_var, values=list(REVIEW_STATUSES), width=120
        ).pack(side="left", padx=4)
        ctk.CTkLabel(review, text="sécu corrigée").pack(side="left", padx=(8, 2))
        self.corr_sec_var = ctk.StringVar(value="")
        ctk.CTkOptionMenu(
            review, variable=self.corr_sec_var, values=["", "C0", "C1", "C2", "C3"], width=70
        ).pack(side="left")
        ctk.CTkLabel(review, text="RGPD corrigé").pack(side="left", padx=(8, 2))
        self.corr_rgpd_var = ctk.StringVar(value="")
        ctk.CTkOptionMenu(
            review,
            variable=self.corr_rgpd_var,
            values=["", "none", "low", "medium", "high", "critical"],
            width=90,
        ).pack(side="left")
        ctk.CTkLabel(review, text="commentaire").pack(side="left", padx=(8, 2))
        self.comment_var = ctk.StringVar(value="")
        ctk.CTkEntry(review, textvariable=self.comment_var, width=240).pack(side="left")
        ctk.CTkLabel(review, text="vérificateur").pack(side="left", padx=(8, 2))
        self.reviewer_var = ctk.StringVar(value="")
        ctk.CTkEntry(review, textvariable=self.reviewer_var, width=110).pack(side="left")
        ctk.CTkButton(review, text="Enregistrer la revue", command=self._save_review).pack(
            side="left", padx=8
        )

        exports = ctk.CTkFrame(p, fg_color="transparent")
        exports.pack(fill="x", padx=10, pady=(0, 10))
        ctk.CTkLabel(exports, text="Exporter :").pack(side="left")
        for fmt, label in (
            ("csv", "CSV"),
            ("json", "JSON"),
            ("xlsx", "Excel"),
            ("powerbi", "Power BI (dossier)"),
        ):
            ctk.CTkButton(
                exports, text=label, width=140, command=lambda f=fmt: self._export(f)
            ).pack(side="left", padx=4)

        self.app.on_refresh(self.refresh)

    # ---- table
    def refresh(self) -> None:
        if not self.app.db_path().exists():
            self.table.set_rows([])
            self.count_label.configure(text="")
            return
        with self.app.open_db() as db:
            rows = list(db.latest_analyses())
        sec, rgpd, rev, search = (
            self.sec_var.get(),
            self.rgpd_var.get(),
            self.review_var.get(),
            self.search_var.get().strip().lower(),
        )

        def keep(r: Any) -> bool:
            if sec != _ALL and (r["security_classification"] or "") != sec:
                return False
            if rgpd != _ALL and (r["rgpd_risk_level"] or "") != rgpd:
                return False
            status = r["review_status"] or ""
            if rev == "non revu" and status:
                return False
            if rev not in (_ALL, "non revu") and status != rev:
                return False
            haystack = f"{r['path']} {r['resume'] or ''} {r['owner'] or ''}".lower()
            return not (search and search not in haystack)

        filtered = [r for r in rows if keep(r)]
        self._rows = result_rows_v31(filtered, limit=500)
        self.table.set_rows([[row[c] for c in _COLUMNS] for row in self._rows])
        self.count_label.configure(
            text=f"{len(filtered)} fichier(s) — {min(len(filtered), 500)} affiché(s)"
        )

    def _select(self, index: int) -> None:
        row = self._rows[index]
        self._selected_id = int(row["id"]) if row["id"] else None
        if self._selected_id is None:
            return
        with self.app.open_db() as db:
            rec = next((r for r in db.latest_analyses() if str(r["path"]) == row["chemin"]), None)
        if rec is None:
            self.detail.set("fiche introuvable")
            return
        lines = [
            f"{rec['path']}",
            f"propriétaire : {rec['owner']}   taille : {rec['size_bytes']} o   statut : {rec['status']}   {rec['exclusion_reason'] or ''}",
            f"sécurité : {rec['security_classification']} ({rec['security_confidence']}) — {rec['security_justification']}",
            f"RGPD : {rec['rgpd_risk_level']} ({rec['rgpd_confidence']}) {rec['rgpd_data_types']}",
            f"finance : {rec['finance_document_type']} ({rec['finance_confidence']}) {rec['finance_amounts']}",
            f"juridique : {rec['legal_contract_type']} ({rec['legal_confidence']}) {rec['legal_parties']}",
            f"conservation : {'oui' if rec['retention_required'] else 'non'} {rec['retention_years'] or 0} ans ({rec['retention_basis']}) — {rec['retention_justification']}",
            f"modèle : {rec['model']}   prompt : {rec['prompt_hash']}   segments : {rec['segments']}   analysé le : {rec['created_at']}",
            f"revue : {rec['review_status'] or 'non revu'} {rec['review_comment'] or ''} {rec['reviewer'] or ''}",
            "",
            f"résumé : {rec['resume'] or ''}",
        ]
        self.detail.set("\n".join(lines))
        self.status_var.set(rec["review_status"] or "validated")
        self.corr_sec_var.set(rec["corrected_security"] or "")
        self.corr_rgpd_var.set(rec["corrected_rgpd"] or "")
        self.comment_var.set(rec["review_comment"] or "")

    def _save_review(self) -> None:
        if self._selected_id is None:
            self.app.log("sélectionne un fichier dans la table")
            return
        with self.app.open_db() as db:
            db.set_review(
                self._selected_id,
                self.status_var.get(),
                comment=self.comment_var.get().strip(),
                reviewer=self.reviewer_var.get().strip(),
                corrected_security=self.corr_sec_var.get() or None,
                corrected_rgpd=self.corr_rgpd_var.get() or None,
            )
        self.app.log(f"revue enregistrée pour le fichier {self._selected_id}")
        self.refresh()

    def _export(self, fmt: str) -> None:
        from tkinter import filedialog

        if fmt == "powerbi":
            path = filedialog.askdirectory(title="Dossier de sortie Power BI")
        else:
            path = filedialog.asksaveasfilename(
                defaultextension=f".{fmt}", filetypes=[(fmt.upper(), f"*.{fmt}")]
            )
        if not path:
            return
        app = self.app
        db_path = str(app.db_path())

        def work() -> None:
            from docia.cli import main as cli_main

            code = cli_main(["--db", db_path, "export", "--format", fmt, "--out", path])
            app.log(f"export {fmt} → {path} ({'OK' if code == 0 else 'échec'})")

        app.run_in_thread(work, "export")
