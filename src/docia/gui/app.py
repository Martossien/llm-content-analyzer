"""Fenêtre principale CustomTkinter (extra `docia[gui]`) : un onglet par usage.

Source · Prompt · Analyse · Résultats & vérification · Statistiques · LLM & bench.

La fenêtre ne contient aucune logique métier : elle lit/écrit la config, lance
des travaux dans un thread (jamais plus d'un à la fois), affiche le journal et
appelle `db`, `views`, `pipeline`, `bench`, `quick`, `report` — les mêmes que la
CLI. `customtkinter` n'est importé qu'ici, à la construction (`launch()`), pour
que le cœur s'importe sans Tk. Aucune trace Python à l'écran : toute exception
d'un travail est journalisée et affichée en une ligne lisible.
"""

from __future__ import annotations

import logging
import queue
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any

from docia import __version__
from docia.config import DEFAULT_CONFIG_NAME, Config, load_config
from docia.db import Database
from docia.gui.helpers import config_to_toml

logger = logging.getLogger(__name__)

_POLL_MS = 200
_MAX_LOG_LINES = 2000
_DONE = "__done__"


class DociaApp:
    """Application : état partagé + onglets. Construite dans `launch()` uniquement."""

    def __init__(self, config_path: Path | None = None) -> None:
        import customtkinter as ctk

        self.ctk = ctk
        self.config_path = config_path or Path(DEFAULT_CONFIG_NAME)
        try:
            self.config = load_config(self.config_path if self.config_path.exists() else None)
        except ValueError as exc:
            logger.warning("config invalide, défauts utilisés : %s", exc)
            self.config = Config()
        self._log_queue: queue.Queue[str] = queue.Queue()
        self._worker: threading.Thread | None = None
        self.cancel = threading.Event()
        self._busy_listeners: list[Callable[[bool], None]] = []
        self._refresh_listeners: list[Callable[[], None]] = []

        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")
        self.root = ctk.CTk()
        self.root.title(f"Doc-IA analyzer {__version__}")
        self.root.geometry("1180x860")
        self.root.minsize(960, 700)

        self.db_var = ctk.StringVar(value=self.config.db_path)

        self._build()
        self.root.after(_POLL_MS, self._poll)

    # ------------------------------------------------------------ construction
    def _build(self) -> None:
        from docia.gui.tab_llm import LLMTab
        from docia.gui.tab_prompt import PromptTab
        from docia.gui.tab_results import ResultsTab
        from docia.gui.tab_run import RunTab
        from docia.gui.tab_source import SourceTab
        from docia.gui.tab_stats import StatsTab

        ctk = self.ctk
        top = ctk.CTkFrame(self.root, fg_color="transparent")
        top.pack(fill="x", padx=12, pady=(10, 0))
        ctk.CTkLabel(top, text="Base SQLite").pack(side="left", padx=(0, 6))
        ctk.CTkEntry(top, textvariable=self.db_var, width=520).pack(side="left")
        ctk.CTkButton(top, text="…", width=36, command=self._pick_db).pack(side="left", padx=4)
        self.busy_label = ctk.CTkLabel(top, text="", text_color="#f59e0b")
        self.busy_label.pack(side="left", padx=12)

        self.tabs = ctk.CTkTabview(self.root)
        self.tabs.pack(fill="both", expand=True, padx=12, pady=(6, 4))
        self.tab_objects: list[Any] = []
        for name, cls in (
            ("Source", SourceTab),
            ("Prompt", PromptTab),
            ("Analyse", RunTab),
            ("Résultats & vérification", ResultsTab),
            ("Statistiques", StatsTab),
            ("LLM & bench", LLMTab),
        ):
            frame = self.tabs.add(name)
            tab = cls(self, frame)
            tab.build()
            self.tab_objects.append(tab)

        self.log_box = ctk.CTkTextbox(self.root, height=130)
        self.log_box.pack(fill="x", padx=12, pady=(0, 10))
        self.log_box.configure(state="disabled")
        self.refresh_all()

    # ------------------------------------------------------------ état partagé
    def db_path(self) -> Path:
        return Path(self.db_var.get().strip() or self.config.db_path)

    def open_db(self) -> Database:
        self.config.db_path = str(self.db_path())
        return Database(self.db_path())

    def collect_config(self) -> Config:
        """Config effective : la fenêtre a pu modifier les champs LLM/blocs (onglet LLM)."""
        self.config.db_path = str(self.db_path())
        for tab in self.tab_objects:
            apply = getattr(tab, "apply_to_config", None)
            if apply is not None:
                apply(self.config)
        errors = self.config.validate()
        for e in errors:
            self.log(f"config : {e}")
        return self.config

    def save_config(self) -> None:
        cfg = self.collect_config()
        if cfg.validate():
            self.log("config non enregistrée (corrige les erreurs ci-dessus)")
            return
        self.config_path.write_text(config_to_toml(cfg), encoding="utf-8")
        self.log(f"config enregistrée : {self.config_path}")

    def _pick_db(self) -> None:
        from tkinter import filedialog

        path = filedialog.asksaveasfilename(
            defaultextension=".sqlite", filetypes=[("SQLite", "*.sqlite")], confirmoverwrite=False
        )
        if path:
            self.db_var.set(path)
            self.refresh_all()

    # ------------------------------------------------------------ travaux
    def run_in_thread(self, work: Callable[[], None], name: str) -> bool:
        """Lance `work` dans un thread ; un seul travail à la fois. False si occupé."""
        if self._worker and self._worker.is_alive():
            self.log("une opération est déjà en cours — attends sa fin ou arrête-la")
            return False

        def wrapped() -> None:
            try:
                work()
            except Exception as exc:  # noqa: BLE001 — affiché, jamais avalé
                logger.exception("échec %s", name)
                self.log(f"{name} : ERREUR — {exc}")
            finally:
                self._log_queue.put(_DONE)

        self.cancel.clear()
        self._worker = threading.Thread(target=wrapped, name=f"docia-{name}", daemon=True)
        self._set_busy(True, name)
        self._worker.start()
        return True

    def is_busy(self) -> bool:
        return bool(self._worker and self._worker.is_alive())

    def on_busy(self, listener: Callable[[bool], None]) -> None:
        self._busy_listeners.append(listener)

    def on_refresh(self, listener: Callable[[], None]) -> None:
        self._refresh_listeners.append(listener)

    def _set_busy(self, busy: bool, name: str = "") -> None:
        self.busy_label.configure(text=f"⏳ {name} en cours…" if busy else "")
        for listener in self._busy_listeners:
            listener(busy)

    def refresh_all(self) -> None:
        for listener in self._refresh_listeners:
            try:
                listener()
            except Exception as exc:  # noqa: BLE001
                logger.exception("rafraîchissement")
                self.log(f"rafraîchissement : {exc}")

    # ------------------------------------------------------------ journal
    def log(self, message: str) -> None:
        self._log_queue.put(message)

    def _poll(self) -> None:
        finished = False
        while True:
            try:
                msg = self._log_queue.get_nowait()
            except queue.Empty:
                break
            if msg == _DONE:
                finished = True
                continue
            self.log_box.configure(state="normal")
            self.log_box.insert("end", msg + "\n")
            if int(self.log_box.index("end-1c").split(".")[0]) > _MAX_LOG_LINES:
                self.log_box.delete("1.0", "200.0")
            self.log_box.see("end")
            self.log_box.configure(state="disabled")
        if finished:
            self._set_busy(False)
            self.refresh_all()
        self.root.after(_POLL_MS, self._poll)

    def run(self) -> None:
        self.root.mainloop()


def launch(config_path: Path | None = None) -> None:
    """Point d'entrée GUI (`python -m docia`, `docia gui`)."""
    DociaApp(config_path).run()
