"""Écran Accueil : chiffres clés de la campagne, parcours guidé en 4 étapes
(source → serveur → analyse → consulter), relance d'une analyse, analyse rapide."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from docia.gui.helpers import parse_int, progress_fraction
from docia.gui.theme import (
    ACCENT,
    ACCENT_OK,
    ACCENT_STOP,
    FONT_FAMILY,
    FONT_SIZE_SMALL,
    format_bytes,
    format_duration,
    format_int,
    severity_color,
)
from docia.gui.widgets import Card, KpiTile


class HomeTab:
    def __init__(self, app: Any, parent: Any) -> None:
        self.app = app
        self.parent = parent
        self.ctk = app.ctk
        self._tiles: dict[str, KpiTile] = {}

    # ------------------------------------------------------------------ build
    def build(self) -> None:
        ctk, p = self.ctk, self.parent
        scroll = ctk.CTkScrollableFrame(p, fg_color="transparent")
        scroll.pack(fill="both", expand=True)

        tiles = ctk.CTkFrame(scroll, fg_color="transparent")
        tiles.pack(fill="x", padx=8, pady=(8, 4))
        for key, label, color, target in (
            ("files", "fichiers dans le scan", ACCENT, None),
            ("analyzed", "analysés", ACCENT_OK, "Résultats"),
            ("sensitive", "sensibles (C3 / RGPD élevé)", severity_color("C3"), "Résultats"),
            ("reclaimable", "récupérables (doublons)", severity_color("C2"), "Statistiques"),
            ("stale", "non accédés depuis 5 ans", severity_color("C1"), "Statistiques"),
            ("reviewed", "vérifiés par un humain", "#0369a1", "Résultats"),
        ):
            tile = KpiTile(
                ctk,
                tiles,
                label=label,
                color=color,
                on_click=self._goto(target) if target else None,
                width=180,
            )
            tile.pack(side="left", padx=4, pady=2)
            self._tiles[key] = tile

        steps = ctk.CTkFrame(scroll, fg_color="transparent")
        steps.pack(fill="x", padx=8, pady=4)
        steps.grid_columnconfigure((0, 1), weight=1, uniform="steps")
        self._build_step_source(steps)
        self._build_step_server(steps)
        self._build_step_run(steps)
        self._build_step_consult(steps)

        bottom = ctk.CTkFrame(scroll, fg_color="transparent")
        bottom.pack(fill="x", padx=8, pady=(4, 8))
        bottom.grid_columnconfigure((0, 1), weight=1, uniform="bottom")
        self._build_rerun(bottom)
        self._build_quick(bottom)

        self.app.on_busy(self._busy)
        self.app.on_refresh(self.refresh)

    def _small(self) -> Any:
        return self.ctk.CTkFont(family=FONT_FAMILY, size=FONT_SIZE_SMALL)

    def _build_step_source(self, parent: Any) -> None:
        ctk = self.ctk
        card = Card(ctk, parent, "1 · Source", subtitle="scan SMBeagle (CSV)")
        card.grid(row=0, column=0, sticky="nsew", padx=4, pady=4)
        row = ctk.CTkFrame(card.body, fg_color="transparent")
        row.pack(fill="x")
        self.csv_var = ctk.StringVar(value="")
        ctk.CTkEntry(
            row, textvariable=self.csv_var, placeholder_text="chemin du CSV SMBeagle"
        ).pack(side="left", fill="x", expand=True)
        ctk.CTkButton(row, text="Choisir…", width=90, command=self._pick_csv).pack(
            side="left", padx=(6, 0)
        )
        row2 = ctk.CTkFrame(card.body, fg_color="transparent")
        row2.pack(fill="x", pady=(8, 0))
        self.import_button = ctk.CTkButton(
            row2, text="Importer et préparer", width=180, command=self._import_and_plan
        )
        self.import_button.pack(side="left")
        self.plan_button = ctk.CTkButton(
            row2, text="Préparer seulement", width=150, command=self._plan, fg_color="#6b7280"
        )
        self.plan_button.pack(side="left", padx=(6, 0))
        self.source_status = ctk.CTkLabel(
            card.body, text="", anchor="w", justify="left", font=self._small()
        )
        self.source_status.pack(fill="x", pady=(8, 0))

    def _build_step_server(self, parent: Any) -> None:
        ctk = self.ctk
        card = Card(ctk, parent, "2 · Serveur LLM", subtitle="vérifier avant de lancer")
        card.grid(row=0, column=1, sticky="nsew", padx=4, pady=4)
        self.server_label = ctk.CTkLabel(card.body, text="", anchor="w", justify="left")
        self.server_label.pack(fill="x")
        row = ctk.CTkFrame(card.body, fg_color="transparent")
        row.pack(fill="x", pady=(8, 0))
        self.test_button = ctk.CTkButton(
            row, text="Tester la connexion", width=160, command=self._test_server
        )
        self.test_button.pack(side="left")
        ctk.CTkButton(
            row,
            text="Réglages (admin)",
            width=140,
            fg_color="#6b7280",
            command=lambda: self.app.show_tab("Serveur & performances", admin=True),
        ).pack(side="left", padx=(6, 0))
        self.server_result = ctk.CTkLabel(card.body, text="", anchor="w", font=self._small())
        self.server_result.pack(fill="x", pady=(8, 0))

    def _build_step_run(self, parent: Any) -> None:
        ctk = self.ctk
        card = Card(ctk, parent, "3 · Analyse", subtitle="reprenable à tout moment")
        card.grid(row=1, column=0, sticky="nsew", padx=4, pady=4)
        row = ctk.CTkFrame(card.body, fg_color="transparent")
        row.pack(fill="x")
        self.run_button = ctk.CTkButton(
            row, text="▶  Lancer l'analyse", width=170, fg_color=ACCENT_OK, command=self._start
        )
        self.run_button.pack(side="left")
        self.stop_button = ctk.CTkButton(
            row,
            text="■  Arrêter",
            width=100,
            fg_color=ACCENT_STOP,
            state="disabled",
            command=self._stop,
        )
        self.stop_button.pack(side="left", padx=(6, 0))
        ctk.CTkLabel(row, text="limite").pack(side="left", padx=(14, 4))
        self.limit_var = ctk.StringVar(value="0")
        ctk.CTkEntry(row, textvariable=self.limit_var, width=60).pack(side="left")
        ctk.CTkLabel(row, text="(0 = tous)", font=self._small()).pack(side="left", padx=4)
        self.progress = ctk.CTkProgressBar(card.body, height=14)
        self.progress.pack(fill="x", pady=(10, 4))
        self.progress.set(0)
        self.run_status = ctk.CTkLabel(card.body, text="", anchor="w", justify="left")
        self.run_status.pack(fill="x")
        self.run_eta = ctk.CTkLabel(card.body, text="", anchor="w", font=self._small())
        self.run_eta.pack(fill="x")

    def _build_step_consult(self, parent: Any) -> None:
        ctk = self.ctk
        card = Card(ctk, parent, "4 · Consulter", subtitle="résultats, statistiques, rapports")
        card.grid(row=1, column=1, sticky="nsew", padx=4, pady=4)
        for label, target, hint in (
            (
                "Résultats & vérification",
                "Résultats",
                "chaque fichier : classification, valider ou corriger",
            ),
            ("Statistiques", "Statistiques", "doublons, ancienneté, risque, conservation"),
            ("Rapports & exports", "Rapports", "HTML pour la direction, Excel, Power BI"),
        ):
            row = ctk.CTkFrame(card.body, fg_color="transparent")
            row.pack(fill="x", pady=2)
            ctk.CTkButton(row, text=label, width=210, command=self._goto(target)).pack(side="left")
            ctk.CTkLabel(row, text=hint, font=self._small()).pack(side="left", padx=8)

    def _build_rerun(self, parent: Any) -> None:
        ctk = self.ctk
        card = Card(ctk, parent, "Relancer une analyse", subtitle="quoi réanalyser ?")
        card.grid(row=0, column=0, sticky="nsew", padx=4, pady=4)
        self.rerun_var = ctk.StringVar(value="missing")
        for value, label in (
            (
                "missing",
                "Seulement ce qui manque — nouveaux fichiers, fichiers modifiés (recommandé)",
            ),
            ("errors", "Aussi les fichiers en erreur"),
            ("all", "Tout réanalyser — prompt ou modèle changé (sauvegarde automatique avant)"),
        ):
            ctk.CTkRadioButton(card.body, text=label, variable=self.rerun_var, value=value).pack(
                anchor="w", pady=2
            )
        self.rerun_button = ctk.CTkButton(
            card.body, text="Relancer", width=120, command=self._rerun
        )
        self.rerun_button.pack(anchor="w", pady=(6, 0))

    def _build_quick(self, parent: Any) -> None:
        ctk = self.ctk
        card = Card(ctk, parent, "Analyse rapide", subtitle="un fichier ou un dossier, sans scan")
        card.grid(row=0, column=1, sticky="nsew", padx=4, pady=4)
        row = ctk.CTkFrame(card.body, fg_color="transparent")
        row.pack(fill="x")
        self.quick_var = ctk.StringVar(value="")
        ctk.CTkEntry(
            row, textvariable=self.quick_var, placeholder_text="fichier ou dossier local"
        ).pack(side="left", fill="x", expand=True)
        ctk.CTkButton(row, text="Dossier…", width=80, command=self._pick_quick_dir).pack(
            side="left", padx=(6, 0)
        )
        ctk.CTkButton(row, text="Fichier…", width=80, command=self._pick_quick_file).pack(
            side="left", padx=(4, 0)
        )
        self.quick_button = ctk.CTkButton(
            card.body, text="Analyser maintenant", width=170, fg_color=ACCENT, command=self._quick
        )
        self.quick_button.pack(anchor="w", pady=(8, 0))
        ctk.CTkLabel(
            card.body,
            text="Le résultat s'ajoute à la campagne ouverte (onglet Résultats).",
            font=self._small(),
            anchor="w",
        ).pack(fill="x", pady=(6, 0))

    def _goto(self, target: str) -> Callable[[], None]:
        def go() -> None:
            self.app.show_tab(target)

        return go

    # ---------------------------------------------------------------- actions
    def _pick_csv(self) -> None:
        from tkinter import filedialog

        path = filedialog.askopenfilename(filetypes=[("CSV SMBeagle", "*.csv"), ("Tous", "*.*")])
        if path:
            self.csv_var.set(path)
            if not self.app.db_path().exists():
                self.app.open_campaign(str(Path(path).with_suffix(".sqlite")))

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

    def _import_and_plan(self) -> None:
        csv_path = Path(self.csv_var.get().strip())
        if not csv_path.is_file():
            self.app.log("choisis un CSV SMBeagle existant")
            return
        app, cfg = self.app, self.app.collect_config()

        def work() -> None:
            app.service.import_scan(csv_path, strict=False, log=app.log)
            app.service.plan(cfg, app.log)
            app.remember_campaign(csv_path=str(csv_path))

        app.run_in_thread(work, "import")

    def _plan(self) -> None:
        app, cfg = self.app, self.app.collect_config()
        app.run_in_thread(lambda: app.service.plan(cfg, app.log), "préparation")

    def _test_server(self) -> None:
        app = self.app
        cfg = app.collect_config()

        def work() -> None:
            import asyncio

            from docia.llm.client import LLMClient

            async def probe() -> bool:
                async with LLMClient(cfg.llm, "") as client:
                    return await client.health()

            ok = asyncio.run(probe())
            text = (
                f"{'✔ serveur joignable' if ok else '✖ serveur injoignable'} — {cfg.llm.base_url}"
            )
            app.log(text)
            color = ACCENT_OK if ok else ACCENT_STOP
            app.ui(lambda: self.server_result.configure(text=text, text_color=color))

        app.run_in_thread(work, "test de connexion")

    def _start(self, *, dry_run: bool = False) -> None:
        app = self.app
        cfg = app.collect_config()
        if cfg.validate():
            app.show_tab("Serveur & performances", admin=True)
            return
        limit = parse_int(self.limit_var.get(), 0, minimum=0) or None

        def on_event(event: Any) -> None:
            app.ui(lambda: self._show_event(event))

        def work() -> None:
            app.service.run(
                cfg, limit=limit, dry_run=dry_run, log=app.log, cancel=app.cancel, on_event=on_event
            )

        app.run_in_thread(work, "analyse")

    def _show_event(self, event: Any) -> None:
        """Met à jour barre, compteurs et estimation depuis un `RunEvent` (thread Tk)."""
        total = max(int(event.files_total), 1)
        done = int(event.files_done) + int(event.files_error)
        self.progress.set(min(1.0, done / total))
        self.run_status.configure(
            text=f"{format_int(event.files_done)} analysés · "
            f"{format_int(max(0, int(event.files_total) - done))} restants · "
            f"{format_int(event.files_error)} en erreur · "
            f"blocs {event.blocks_done}/{event.blocks_total}"
        )
        rate = f"{event.files_per_hour:,.0f}".replace(",", " ") if event.files_per_hour else "—"
        self.run_eta.configure(
            text=f"écoulé {format_duration(event.elapsed_s)} · {rate} fichiers/h · "
            f"restant ≈ {format_duration(event.eta_s)}"
        )

    def _stop(self) -> None:
        self.app.cancel.set()
        self.app.log("arrêt demandé — les requêtes en cours se terminent, rien n'est perdu")

    def _rerun(self) -> None:
        mode = self.rerun_var.get()
        app = self.app
        if mode == "all":
            from tkinter import messagebox

            if not messagebox.askyesno(
                "Tout réanalyser",
                "Toutes les analyses de cette campagne seront effacées (une sauvegarde de la base "
                "est faite avant). Les vérifications humaines sont conservées. Continuer ?",
            ):
                return
        if mode in ("errors", "all"):
            cfg = app.collect_config()
            try:
                app.service.reanalyze(cfg, mode, app.log)
            except Exception as exc:  # noqa: BLE001
                app.log(f"relance impossible : {exc}")
                return
        self._start()

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
        app.run_in_thread(
            lambda: app.service.quick(cfg, target, db_path, app.log, app.cancel), "analyse rapide"
        )

    # ------------------------------------------------------------------ état
    def _busy(self, busy: bool) -> None:
        state = "disabled" if busy else "normal"
        for b in (
            self.import_button,
            self.plan_button,
            self.test_button,
            self.run_button,
            self.rerun_button,
            self.quick_button,
        ):
            b.configure(state=state)
        self.stop_button.configure(state="normal" if busy else "disabled")

    def refresh(self) -> None:
        cfg = self.app.config
        self.server_label.configure(
            text=f"{cfg.llm.transport} · {cfg.llm.base_url}\nmodèle {cfg.llm.model}"
            f" · raisonnement {'activé' if cfg.llm.enable_thinking else 'désactivé'}"
        )
        if not self.app.db_path().exists():
            for tile in self._tiles.values():
                tile.set("—")
            self.source_status.configure(
                text="Aucune campagne : choisis un CSV puis « Importer et préparer »."
            )
            self.run_status.configure(text="")
            self.run_eta.configure(text="")
            self.progress.set(0)
            return
        from docia import views

        with self.app.open_db() as db:
            ov = views.overview(db, stale_years=5)
            counts = db.counts()
        t = self._tiles
        t["files"].set(format_int(ov.total_files))
        t["analyzed"].set(format_int(ov.analyzed))
        t["sensitive"].set(format_int(ov.sensitive_files))
        t["reclaimable"].set(format_bytes(ov.duplicate_reclaimable_bytes))
        t["stale"].set(format_int(ov.stale_files))
        t["reviewed"].set(format_int(ov.reviewed))
        self.source_status.configure(
            text=f"{format_int(ov.total_files)} fichiers ({format_bytes(ov.total_bytes)}) — "
            f"{format_int(ov.pending)} à analyser, {format_int(ov.excluded)} exclus, "
            f"{format_int(ov.errors)} en erreur"
        )
        self._refresh_run(counts)

    def _refresh_run(self, counts: dict[str, int]) -> None:
        frac = progress_fraction(counts)
        self.progress.set(frac)
        remaining = counts.get("pending", 0) + counts.get("queued", 0)
        self.run_status.configure(
            text=f"{format_int(counts.get('done', 0))} analysés · {format_int(remaining)} restants"
            f" · {format_int(counts.get('error', 0))} en erreur · {frac * 100:.0f} %"
        )
        if not self.app.is_busy():
            self.run_eta.configure(text="")
