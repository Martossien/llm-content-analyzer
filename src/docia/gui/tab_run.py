"""Onglet Analyse : lancer / arrêter le pipeline, suivre l'avancement."""

from __future__ import annotations

from typing import Any

from docia.gui.helpers import parse_int, status_lines


class RunTab:
    def __init__(self, app: Any, parent: Any) -> None:
        self.app = app
        self.parent = parent
        self.ctk = app.ctk

    def build(self) -> None:
        ctk, p = self.ctk, self.parent
        ctk.CTkLabel(
            p,
            text=(
                "Blocs DocFuse → LLM → analyses. Reprenable : relancer ne renvoie que ce qui manque ; "
                "« Arrêter » laisse les blocs construits pour le run suivant."
            ),
            anchor="w",
            justify="left",
        ).pack(fill="x", padx=10, pady=(10, 6))
        row = ctk.CTkFrame(p, fg_color="transparent")
        row.pack(fill="x", padx=10)
        ctk.CTkLabel(row, text="Limite de fichiers (0 = tous)").pack(side="left")
        self.limit_var = ctk.StringVar(value="0")
        ctk.CTkEntry(row, textvariable=self.limit_var, width=80).pack(side="left", padx=(4, 12))
        self.dry_var = ctk.BooleanVar(value=False)
        ctk.CTkCheckBox(
            row, text="Construire les blocs seulement (sans LLM)", variable=self.dry_var
        ).pack(side="left", padx=(0, 12))
        self.run_button = ctk.CTkButton(
            row, text="Lancer l'analyse", command=self._start, fg_color="#16a34a"
        )
        self.run_button.pack(side="left", padx=(0, 8))
        self.stop_button = ctk.CTkButton(
            row, text="Arrêter", command=self._stop, state="disabled", fg_color="#ef4444"
        )
        self.stop_button.pack(side="left", padx=(0, 8))
        ctk.CTkButton(row, text="Remettre les erreurs à analyser", command=self._retry).pack(
            side="left"
        )

        self.status_label = ctk.CTkLabel(p, text="", justify="left", anchor="w")
        self.status_label.pack(fill="x", padx=10, pady=(12, 6))
        self.progress = ctk.CTkProgressBar(p)
        self.progress.pack(fill="x", padx=10, pady=(0, 10))
        self.progress.set(0)

        self.app.on_busy(self._busy)
        self.app.on_refresh(self.refresh)

    def _busy(self, busy: bool) -> None:
        self.run_button.configure(state="disabled" if busy else "normal")
        self.stop_button.configure(state="normal" if busy else "disabled")

    def _start(self) -> None:
        app = self.app
        cfg = app.collect_config()
        if cfg.validate():
            return
        limit = parse_int(self.limit_var.get(), 0, minimum=0) or None
        dry = bool(self.dry_var.get())

        def work() -> None:
            from docia.pipeline import run_pipeline

            with app.open_db() as db:
                rep = run_pipeline(
                    db, cfg, limit=limit, dry_run=dry, progress=app.log, cancel=app.cancel
                )
            app.log(
                f"run {rep.run_id} : {rep.files_selected} sélectionnés, {rep.files_done} analysés "
                f"({rep.files_duplicates} doublons hérités, {rep.files_segmented} découpés), "
                f"{rep.files_error} en erreur — blocs {rep.blocks_done}/{rep.blocks_built + rep.blocks_resumed} "
                f"— tokens {rep.prompt_tokens} prompt / {rep.completion_tokens} sortie"
            )
            for e in rep.errors[:10]:
                app.log(f"   {e}")

        app.run_in_thread(work, "analyse")

    def _stop(self) -> None:
        self.app.cancel.set()
        self.app.log("arrêt demandé — les requêtes en cours se terminent, rien n'est perdu")

    def _retry(self) -> None:
        with self.app.open_db() as db:
            n = db.reset_errors()
        self.app.log(f"{n} fichier(s) remis à analyser")
        self.app.refresh_all()

    def refresh(self) -> None:
        if not self.app.db_path().exists():
            self.status_label.configure(
                text="base absente — importe d'abord un CSV (onglet Source)"
            )
            self.progress.set(0)
            return
        with self.app.open_db() as db:
            counts, classes = db.counts(), db.classification_summary()
        self.status_label.configure(text="\n".join(status_lines(counts, classes)))
        total = (
            counts.get("pending", 0)
            + counts.get("queued", 0)
            + counts.get("done", 0)
            + counts.get("error", 0)
        )
        self.progress.set((counts.get("done", 0) / total) if total else 0)
