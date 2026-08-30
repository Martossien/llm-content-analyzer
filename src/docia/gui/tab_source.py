"""Onglet Source : CSV SMBeagle → base, plan (exclusions + priorité), état."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from docia.gui.helpers import status_lines


class SourceTab:
    def __init__(self, app: Any, parent: Any) -> None:
        self.app = app
        self.parent = parent
        self.ctk = app.ctk

    def build(self) -> None:
        ctk, p = self.ctk, self.parent
        ctk.CTkLabel(
            p,
            text="1. Importer le scan SMBeagle (CSV 19 colonnes), puis planifier : exclusions et score de priorité.",
            anchor="w",
        ).pack(fill="x", padx=10, pady=(10, 4))
        row = ctk.CTkFrame(p, fg_color="transparent")
        row.pack(fill="x", padx=10)
        ctk.CTkLabel(row, text="CSV SMBeagle").pack(side="left")
        self.csv_var = ctk.StringVar(value="")
        ctk.CTkEntry(row, textvariable=self.csv_var, width=560).pack(side="left", padx=6)
        ctk.CTkButton(row, text="…", width=36, command=self._pick_csv).pack(side="left")

        btns = ctk.CTkFrame(p, fg_color="transparent")
        btns.pack(fill="x", padx=10, pady=8)
        self.import_button = ctk.CTkButton(btns, text="Importer le CSV", command=self._import)
        self.import_button.pack(side="left", padx=(0, 8))
        self.plan_button = ctk.CTkButton(
            btns, text="Planifier (exclusions + priorité)", command=self._plan
        )
        self.plan_button.pack(side="left", padx=(0, 8))
        ctk.CTkButton(btns, text="Rafraîchir l'état", command=self.app.refresh_all).pack(
            side="left", padx=(0, 8)
        )
        self.lenient_var = ctk.BooleanVar(value=True)
        ctk.CTkCheckBox(
            btns,
            text="Tolérer les lignes invalides (comptées, ignorées)",
            variable=self.lenient_var,
        ).pack(side="left")

        ctk.CTkLabel(
            p,
            text="Analyse rapide sans CSV : un fichier ou un dossier local, résultat immédiat (onglet Résultats).",
            anchor="w",
        ).pack(fill="x", padx=10, pady=(14, 4))
        row2 = ctk.CTkFrame(p, fg_color="transparent")
        row2.pack(fill="x", padx=10)
        self.quick_var = ctk.StringVar(value="")
        ctk.CTkEntry(row2, textvariable=self.quick_var, width=560).pack(side="left", padx=(0, 6))
        ctk.CTkButton(row2, text="Dossier…", width=90, command=self._pick_quick_dir).pack(
            side="left", padx=(0, 4)
        )
        ctk.CTkButton(row2, text="Fichier…", width=90, command=self._pick_quick_file).pack(
            side="left", padx=(0, 8)
        )
        self.quick_button = ctk.CTkButton(
            row2, text="Analyser maintenant", command=self._quick, fg_color="#16a34a"
        )
        self.quick_button.pack(side="left")

        self.status_label = ctk.CTkLabel(p, text="", justify="left", anchor="w")
        self.status_label.pack(fill="x", padx=10, pady=(16, 6))
        self.reasons = ctk.CTkTextbox(p, height=160)
        self.reasons.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        self.reasons.configure(state="disabled")

        self.app.on_busy(self._busy)
        self.app.on_refresh(self.refresh)

    # ---- actions
    def _pick_csv(self) -> None:
        from tkinter import filedialog

        path = filedialog.askopenfilename(filetypes=[("CSV SMBeagle", "*.csv"), ("Tous", "*.*")])
        if path:
            self.csv_var.set(path)
            if self.app.db_var.get().strip() in ("", "docia.sqlite"):
                self.app.db_var.set(str(Path(path).with_suffix(".sqlite")))

    def _pick_quick_dir(self) -> None:
        from tkinter import filedialog

        path = filedialog.askdirectory()
        if path:
            self.quick_var.set(path)

    def _pick_quick_file(self) -> None:
        from tkinter import filedialog

        path = filedialog.askopenfilename()
        if path:
            self.quick_var.set(path)

    def _import(self) -> None:
        csv_path = Path(self.csv_var.get().strip())
        if not csv_path.is_file():
            self.app.log("choisis un CSV SMBeagle existant")
            return
        strict = not bool(self.lenient_var.get())
        app = self.app

        def work() -> None:
            from docia.ingest.smbeagle_csv import import_csv

            with app.open_db() as db:
                rep = import_csv(db, csv_path, strict=strict)
            app.log(
                f"import : {rep.total} lignes — {rep.new} nouveaux, {rep.updated} modifiés, "
                f"{rep.unchanged} inchangés, {rep.invalid} invalides"
            )
            for err in rep.errors[:5]:
                app.log(f"   ligne {err.line_number} : {err.reason}")

        app.run_in_thread(work, "import")

    def _plan(self) -> None:
        app = self.app
        cfg = app.collect_config()

        def work() -> None:
            from docia.filter import plan_files

            with app.open_db() as db:
                rep = plan_files(db, cfg.filter)
            app.log(f"plan : {rep.pending} à analyser, {rep.excluded} exclus")
            for reason, n in sorted(rep.by_reason.items(), key=lambda kv: -kv[1])[:8]:
                app.log(f"   {n:>7}  {reason}")

        app.run_in_thread(work, "plan")

    def _quick(self) -> None:
        target = Path(self.quick_var.get().strip())
        if not target.exists():
            self.app.log("chemin introuvable pour l'analyse rapide")
            return
        app = self.app
        cfg = app.collect_config()
        if cfg.validate():
            return
        db_path = app.db_path()

        def work() -> None:
            from docia.quick import quick_analyze

            rep = quick_analyze(cfg, [target], db_path=db_path, progress=app.log, cancel=app.cancel)
            for line in rep.as_lines():
                app.log(line)

        app.run_in_thread(work, "analyse rapide")

    # ---- état
    def _busy(self, busy: bool) -> None:
        state = "disabled" if busy else "normal"
        for b in (self.import_button, self.plan_button, self.quick_button):
            b.configure(state=state)

    def refresh(self) -> None:
        db_path = self.app.db_path()
        if not db_path.exists():
            self.status_label.configure(text=f"base absente : {db_path} (créée au premier import)")
            return
        from docia.views import status_summary

        with self.app.open_db() as db:
            counts, classes = db.counts(), db.classification_summary()
            summary = status_summary(db)
        self.status_label.configure(text="\n".join(status_lines(counts, classes)))
        self.reasons.configure(state="normal")
        self.reasons.delete("1.0", "end")
        reasons = getattr(summary, "top_reasons", None) or getattr(summary, "reasons", None) or []
        if reasons:
            self.reasons.insert("end", "Principales raisons d'exclusion / d'erreur :\n")
            for item in list(reasons)[:10]:
                self.reasons.insert("end", f"  {item}\n")
        self.reasons.configure(state="disabled")
