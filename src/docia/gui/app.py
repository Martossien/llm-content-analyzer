"""Fenêtre principale CustomTkinter (extra `docia[gui]`).

Bandeau : campagne ouverte (Ouvrir / Récentes / Nouvelle), mode administrateur.
Onglets utilisateur : Accueil · Résultats · Statistiques · Rapports.
Onglets administrateur : Prompt · Serveur & performances.
Bas de fenêtre : dernière ligne du journal, journal complet dépliable.

La fenêtre ne contient aucune logique métier : elle passe par `GuiService`
(→ `docia.service`, la même couche que la CLI et, demain, le serveur web), lance
un seul travail à la fois dans un thread, et affiche toute exception en une ligne
lisible. `customtkinter` n'est importé qu'ici, à la construction (`launch()`).
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
from docia.gui.helpers import campaign_title, config_to_toml
from docia.gui.service_shim import GuiService, default_backup_dir, load_recent, remember_recent
from docia.gui.theme import ACCENT_ADMIN, FONT_FAMILY, FONT_SIZE, FONT_SIZE_SMALL, FONT_SIZE_TITLE

logger = logging.getLogger(__name__)

_POLL_MS = 200
_MAX_LOG_LINES = 2000
_DONE = "__done__"
USER_TABS = ("Accueil", "Résultats", "Statistiques", "Rapports")
ADMIN_TABS = ("Prompt", "Serveur & performances")


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
        self._log_queue: queue.Queue[str | Callable[[], object]] = queue.Queue()
        self._worker: threading.Thread | None = None
        self.cancel = threading.Event()
        self._busy_listeners: list[Callable[[bool], None]] = []
        self._refresh_listeners: list[Callable[[], None]] = []
        self._backup_dir: Path | None = None
        self.service = GuiService(self.open_db)

        ctk.set_appearance_mode("light")
        ctk.set_default_color_theme("blue")
        self.root = ctk.CTk()
        self.root.title(f"Doc-IA analyzer {__version__}")
        self.root.geometry("1280x900")
        self.root.minsize(1024, 720)

        self.db_var = ctk.StringVar(value=self.config.db_path)
        self.admin_var = ctk.BooleanVar(value=False)

        self._build()
        self.root.after(_POLL_MS, self._poll)

    # ------------------------------------------------------------ construction
    def _build(self) -> None:
        ctk = self.ctk
        header = ctk.CTkFrame(self.root, corner_radius=0, fg_color="#e5e7eb")
        header.pack(fill="x")
        ctk.CTkLabel(
            header,
            text="Doc-IA",
            font=ctk.CTkFont(family=FONT_FAMILY, size=FONT_SIZE_TITLE, weight="bold"),
        ).pack(side="left", padx=(14, 4), pady=8)
        ctk.CTkLabel(
            header,
            text="audit des partages",
            font=ctk.CTkFont(family=FONT_FAMILY, size=FONT_SIZE_SMALL),
        ).pack(side="left", padx=(0, 20))
        ctk.CTkLabel(
            header, text="Campagne :", font=ctk.CTkFont(family=FONT_FAMILY, size=FONT_SIZE)
        ).pack(side="left")
        self.campaign_label = ctk.CTkLabel(
            header, text="", font=ctk.CTkFont(family=FONT_FAMILY, size=FONT_SIZE, weight="bold")
        )
        self.campaign_label.pack(side="left", padx=(4, 10))
        ctk.CTkButton(header, text="Ouvrir…", width=80, command=self._open_dialog).pack(
            side="left", padx=2
        )
        self.recent_menu = ctk.CTkOptionMenu(
            header, values=["Récentes"], width=120, command=self._open_recent
        )
        self.recent_menu.pack(side="left", padx=2)
        ctk.CTkButton(header, text="Nouvelle…", width=90, command=self._new_dialog).pack(
            side="left", padx=2
        )
        self.admin_switch = ctk.CTkSwitch(
            header,
            text="mode administrateur",
            variable=self.admin_var,
            command=self._toggle_admin,
            progress_color=ACCENT_ADMIN,
        )
        self.admin_switch.pack(side="right", padx=14)
        self.busy_label = ctk.CTkLabel(header, text="", text_color="#b45309")
        self.busy_label.pack(side="right", padx=12)

        self.tabs = ctk.CTkTabview(self.root, anchor="w")
        self.tabs.pack(fill="both", expand=True, padx=10, pady=(4, 2))
        self.tab_objects: dict[str, Any] = {}
        self._admin_built = False
        self._build_user_tabs()

        footer = ctk.CTkFrame(self.root, fg_color="transparent")
        footer.pack(fill="x", padx=10, pady=(0, 6))
        self.status_line = ctk.CTkLabel(
            footer,
            text="prêt",
            anchor="w",
            font=ctk.CTkFont(family=FONT_FAMILY, size=FONT_SIZE_SMALL),
        )
        self.status_line.pack(side="left", fill="x", expand=True)
        self.journal_button = ctk.CTkButton(
            footer, text="Journal ▴", width=90, fg_color="#6b7280", command=self._toggle_journal
        )
        self.journal_button.pack(side="right")
        self.log_box = ctk.CTkTextbox(
            self.root, height=140, font=ctk.CTkFont(family="Consolas", size=FONT_SIZE_SMALL)
        )
        self.log_box.configure(state="disabled")
        self._journal_visible = False

        self._refresh_campaign_header()
        self.refresh_all()

    def _build_user_tabs(self) -> None:
        from docia.gui.tab_home import HomeTab
        from docia.gui.tab_reports import ReportsTab
        from docia.gui.tab_results import ResultsTab
        from docia.gui.tab_stats import StatsTab

        classes: tuple[type[Any], ...] = (HomeTab, ResultsTab, StatsTab, ReportsTab)
        for name, cls in zip(USER_TABS, classes, strict=True):
            frame = self.tabs.add(name)
            tab = cls(self, frame)
            tab.build()
            self.tab_objects[name] = tab

    def _build_admin_tabs(self) -> None:
        from docia.gui.tab_llm import LLMTab
        from docia.gui.tab_prompt import PromptTab

        classes: tuple[type[Any], ...] = (PromptTab, LLMTab)
        for name, cls in zip(ADMIN_TABS, classes, strict=True):
            frame = self.tabs.add(name)
            tab = cls(self, frame)
            tab.build()
            self.tab_objects[name] = tab
        self._admin_built = True

    def _toggle_admin(self) -> None:
        if self.admin_var.get():
            if not self._admin_built:
                self._build_admin_tabs()
                self.refresh_all()
        else:
            current = self.tabs.get()
            for name in ADMIN_TABS:
                if name in self.tab_objects:
                    self.tabs.delete(name)
                    del self.tab_objects[name]
            self._admin_built = False
            if current in ADMIN_TABS:
                self.tabs.set("Accueil")

    def show_tab(self, name: str, *, admin: bool = False) -> None:
        if admin and not self.admin_var.get():
            self.admin_var.set(True)
            self._toggle_admin()
        if name in self.tab_objects:
            self.tabs.set(name)

    # ------------------------------------------------------------ campagne
    def db_path(self) -> Path:
        return Path(self.db_var.get().strip() or self.config.db_path)

    def open_db(self) -> Database:
        self.config.db_path = str(self.db_path())
        return Database(self.db_path())

    def backup_dir(self) -> Path:
        return self._backup_dir or default_backup_dir(self.db_path())

    def set_backup_dir(self, path: Path) -> None:
        self._backup_dir = path

    def open_campaign(self, db_path: str) -> None:
        self.db_var.set(db_path)
        self.config.db_path = db_path
        self.remember_campaign()
        self._refresh_campaign_header()
        self.refresh_all()
        self.log(f"campagne : {db_path}")

    def remember_campaign(self, csv_path: str | None = None) -> None:
        if self.db_path().exists():
            remember_recent(str(self.db_path()), csv_path)

    def _refresh_campaign_header(self) -> None:
        path = self.db_path()
        exists = path.exists()
        self.campaign_label.configure(
            text=f"{campaign_title(str(path))}{'' if exists else ' (nouvelle)'}",
            text_color="#111827" if exists else "#6b7280",
        )
        recent = [str(r.db_path) for r in load_recent()]
        self.recent_menu.configure(
            values=["Récentes", *[campaign_title(p) + "  —  " + p for p in recent]]
        )
        self.recent_menu.set("Récentes")

    def _open_dialog(self) -> None:
        from tkinter import filedialog

        path = filedialog.askopenfilename(
            title="Ouvrir une campagne", filetypes=[("Campagne Doc-IA", "*.sqlite")]
        )
        if path:
            self.open_campaign(path)

    def _new_dialog(self) -> None:
        from tkinter import filedialog

        path = filedialog.asksaveasfilename(
            title="Nouvelle campagne",
            defaultextension=".sqlite",
            filetypes=[("Campagne Doc-IA", "*.sqlite")],
            initialfile="campagne.sqlite",
        )
        if path:
            self.open_campaign(path)
            self.show_tab("Accueil")

    def _open_recent(self, choice: str) -> None:
        if "  —  " in choice:
            self.open_campaign(choice.split("  —  ", 1)[1])
        self.recent_menu.set("Récentes")

    # ------------------------------------------------------------ config
    def collect_config(self) -> Config:
        """Config effective : les onglets admin ont pu modifier les champs LLM/blocs."""
        self.config.db_path = str(self.db_path())
        for tab in self.tab_objects.values():
            apply = getattr(tab, "apply_to_config", None)
            if apply is not None:
                apply(self.config)
        for e in self.config.validate():
            self.log(f"config : {e}")
        return self.config

    def save_config(self) -> None:
        cfg = self.collect_config()
        if cfg.validate():
            self.log("config non enregistrée (corrige les erreurs ci-dessus)")
            return
        self.config_path.write_text(config_to_toml(cfg), encoding="utf-8")
        self.log(f"config enregistrée : {self.config_path}")

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
        self._refresh_campaign_header()

    # ------------------------------------------------------------ journal
    def log(self, message: str) -> None:
        self._log_queue.put(message)

    def ui(self, action: Callable[[], object]) -> None:
        """Exécute `action` dans le thread Tk (depuis un travail en arrière-plan)."""
        self._log_queue.put(action)

    def _toggle_journal(self) -> None:
        self._journal_visible = not self._journal_visible
        if self._journal_visible:
            self.log_box.pack(fill="x", padx=10, pady=(0, 8))
            self.journal_button.configure(text="Journal ▾")
        else:
            self.log_box.pack_forget()
            self.journal_button.configure(text="Journal ▴")

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
            if callable(msg):
                try:
                    msg()
                except Exception:  # noqa: BLE001
                    logger.exception("action UI")
                continue
            self.status_line.configure(text=msg)
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


def launch(config_path: Path | None = None, *, smoke: bool = False) -> None:
    """Point d'entrée GUI (`python -m docia`, `docia gui`). `smoke` : construit toute la
    fenêtre (onglets admin compris) puis la ferme — contrôle d'un exécutable empaqueté."""
    app = DociaApp(config_path)
    if smoke:
        app.admin_var.set(True)
        app._toggle_admin()
        app.root.after(1500, app.root.destroy)
    app.run()
