"""Fenêtre principale CustomTkinter (extra `docia[gui]`).

Bandeau : campagne ouverte (Ouvrir / Récentes / Nouvelle), mode administrateur.
Onglets utilisateur : Accueil · Résultats · Statistiques · Rapports.
Onglets administrateur : Prompt · Serveur & performances.
Bas de fenêtre : dernière ligne du journal, journal complet dépliable.

La fenêtre ne contient aucune logique métier. Doctrine `service` / `db`, en deux
règles vérifiables :

* **toute écriture** (import, plan, run, réanalyse, sauvegarde, vérification humaine)
  passe par `GuiService` → `docia.service`, la même couche que la CLI et, demain, le
  serveur web ;
* **les écrans de lecture** (Accueil, Résultats, Statistiques) ouvrent eux-mêmes une
  `Database` sur le chemin de campagne capturé par `LazyScreen._start`, dans leur
  thread de calcul : c'est justement ce qui les empêche de relire l'état de la
  fenêtre depuis un thread.

La fenêtre lance un seul travail à la fois dans un thread et affiche toute exception
en une ligne lisible. `customtkinter` n'est importé qu'ici, à la construction
(`launch()`).
"""

from __future__ import annotations

import contextlib
import logging
import queue
import sqlite3
import threading
import traceback
from collections.abc import Callable
from pathlib import Path
from types import TracebackType
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

NEW = "neuve"
DOCIA = "docia"
FOREIGN = "étrangère"

WINDOW_SKIP = "fenetre_deja_dite"
"""Attribut d'enregistrement (`extra={WINDOW_SKIP: True}`) qui réserve un message au
fichier : la fenêtre a déjà dit la même chose à sa façon, `_JournalToWindow` ne le
répète pas."""

WINDOW_LOG_LEVEL = logging.WARNING
"""Seuil des messages des couches basses repris dans le journal de la fenêtre.

En dessous (INFO, DEBUG), `docia.service`, `docia.db` et DocFuse émettent plusieurs
lignes par fichier : le journal de la fenêtre deviendrait illisible, et il n'est pas
là pour doubler `docia.log`. Ce qui concerne l'utilisateur — un fichier illisible, un
serveur qui bronche, une rotation impossible — est émis à `WARNING` ou au-dessus."""


def _journal_path() -> str:
    """Chemin de `docia.log` tel que la CLI l'a ouvert, sinon son nom générique."""
    with contextlib.suppress(Exception):
        from docia import cli

        if cli._JOURNAL is not None:
            return str(cli._JOURNAL)
    return "docia.log"


def crash_line(where: str, exc: BaseException, journal: str) -> str:
    """La **ligne unique** montrée à l'utilisateur pour une anomalie non prévue (pure).

    Une trace Python n'a jamais aidé personne devant une fenêtre : on nomme l'endroit,
    le type de la panne, son message (première ligne, coupée), et où lire le reste.
    """
    detail = str(exc).strip().splitlines()
    message = detail[0].strip() if detail and detail[0].strip() else type(exc).__name__
    if len(message) > 200:
        message = message[:197] + "…"
    return (
        f"{where} : anomalie non prévue — {type(exc).__name__} : {message} (détail dans {journal})"
    )


class _JournalToWindow(logging.Handler):
    """Passe dans le journal de la fenêtre ce que les couches basses écrivent au fichier.

    Sans ce pont, `docia.service`, `docia.db` et DocFuse parlaient à `docia.log` et à
    une console que l'exe fenêtré n'affiche pas : l'utilisateur ne voyait rien d'un
    avertissement qui le concernait. Le gestionnaire ne fait qu'**empiler dans la file**
    (`app.log`), jamais toucher à Tk : il est appelé depuis n'importe quel thread.
    """

    def __init__(self, sink: Callable[[str], None]) -> None:
        super().__init__(level=WINDOW_LOG_LEVEL)
        self._sink = sink
        self.setFormatter(logging.Formatter("%(levelname)s %(name)s : %(message)s"))

    def emit(self, record: logging.LogRecord) -> None:
        if getattr(record, WINDOW_SKIP, False):
            return
        try:
            # Jamais la pile : `exc_info` reste au fichier, la fenêtre a une ligne.
            copie = logging.makeLogRecord(record.__dict__)
            copie.exc_info, copie.exc_text, copie.stack_info = None, None, None
            self._sink(self.format(copie))
        except Exception:  # noqa: BLE001 — un journal ne fait jamais tomber l'application
            self.handleError(record)


def campaign_kind(target: Path) -> str:
    """`neuve`, `docia` ou `étrangère` — sans rien créer ni modifier.

    `Database(chemin)` greffe les douze tables docia dans **n'importe quel** SQLite
    ouvrable : une base « contacts » d'un autre logiciel s'en retrouvait enrichie,
    pendant que le journal affirmait « aucune donnée effacée ». On regarde donc avant
    d'ouvrir : un fichier non vide sans `meta.schema_version` n'est pas une campagne.
    """
    try:
        if not target.exists() or target.stat().st_size == 0:
            return NEW
        uri = target.resolve().as_uri() + "?mode=ro"
        con = sqlite3.connect(uri, uri=True)
    except (OSError, ValueError, sqlite3.Error):
        return FOREIGN
    try:
        names = {
            str(r[0]) for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        if not names:
            return NEW  # fichier SQLite vide : utilisable comme campagne neuve
        if "meta" not in names:
            return FOREIGN
        row = con.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
        return DOCIA if row else FOREIGN
    except sqlite3.Error:
        return FOREIGN  # pas un fichier SQLite du tout (texte, archive, base corrompue)
    finally:
        con.close()


def _owner_of(listener: Callable[..., Any]) -> Any:
    """Widget propriétaire d'un rappel — le cadre de l'onglet — s'il est identifiable.

    Sert de filet : un rappel dont le widget a été détruit est écarté d'office, même
    si l'onglet a oublié de se retirer.
    """
    return getattr(getattr(listener, "__self__", None), "parent", None)


def _alive(owner: Any) -> bool:
    if owner is None:
        return True
    try:
        return bool(owner.winfo_exists())
    except Exception:  # noqa: BLE001 — widget déjà détruit, interpréteur Tcl parti
        return False


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
        # (rappel, widget propriétaire) : le propriétaire permet d'écarter d'office un
        # rappel dont le widget n'existe plus (onglets administrateur refermés).
        self._busy_listeners: list[tuple[Callable[[bool], None], Any]] = []
        self._refresh_listeners: list[tuple[Callable[[], None], Any]] = []
        self._backup_dir: Path | None = None
        self._poll_failures = 0
        self.service = GuiService(self.open_db)

        ctk.set_appearance_mode("light")
        ctk.set_default_color_theme("blue")
        self.root = ctk.CTk()
        self.root.title(f"Doc-IA analyzer {__version__}")
        self.root.geometry("1280x900")
        self.root.minsize(1024, 720)

        # Chemin de la campagne en Python pur (et non dans une variable Tk) : les calculs
        # de fond le lisent depuis leur thread, où toucher à Tk est interdit.
        self._db_path = str(self.config.db_path)
        self.admin_var = ctk.BooleanVar(value=False)

        self._install_safety_net()
        self._build()
        self.root.after(_POLL_MS, self._poll)

    # ------------------------------------------------------- filet d'exception
    def _install_safety_net(self) -> None:
        """Rien de ce qui casse ne doit rester invisible (fenêtre **et** journal).

        Trois trous étaient ouverts : une exception non prévue dans un rappel Tk partait
        sur `sys.stderr` — une console que `Docia.exe` fenêtré n'affiche pas ; celle d'un
        thread hors `run_in_thread`/`run_background` aussi ; et les couches basses
        (`docia.service`, `docia.db`, DocFuse) écrivaient dans `docia.log` sans qu'un mot
        n'atteigne l'écran. L'utilisateur voyait une fenêtre muette, parfois figée.
        """
        self.root.report_callback_exception = self._on_tk_exception
        self._previous_thread_hook: Callable[[Any], Any] | None = threading.excepthook
        threading.excepthook = self._on_thread_exception
        self._window_handler: logging.Handler | None = _JournalToWindow(self.log)
        logging.getLogger().addHandler(self._window_handler)

    def _remove_safety_net(self) -> None:
        """Rend au processus ce qui lui appartient (fin de `run()`, tests)."""
        if self._window_handler is not None:
            logging.getLogger().removeHandler(self._window_handler)
            self._window_handler = None
        if self._previous_thread_hook is not None:
            threading.excepthook = self._previous_thread_hook
            self._previous_thread_hook = None

    def _on_tk_exception(
        self,
        exc_type: type[BaseException],
        exc_value: BaseException | None,
        exc_tb: TracebackType | None,  # noqa: ARG002 — signature imposée par Tk
    ) -> None:
        """`root.report_callback_exception` : un rappel Tk (bouton, `after`) a lâché."""
        self.report_crash("action de la fenêtre", exc_value or exc_type())

    def _on_thread_exception(self, args: Any) -> None:
        """`threading.excepthook` : un thread est mort sans que personne le voie."""
        if args.exc_type is SystemExit:
            return
        where = f"tâche de fond « {args.thread.name if args.thread else '?'} »"
        self.report_crash(where, args.exc_value or args.exc_type())

    def report_crash(self, where: str, exc: BaseException) -> None:
        """Trace complète dans `docia.log`, **une ligne lisible** dans la fenêtre.

        Appelable depuis n'importe quel thread : `log()` ne fait qu'empiler dans la file
        que `_poll` vide dans le thread Tk.
        """
        # Journal indisponible (disque plein…) : la fenêtre parle quand même.
        with contextlib.suppress(Exception):
            logger.error(
                "anomalie non prévue (%s)\n%s",
                where,
                "".join(traceback.format_exception(exc)).rstrip(),
                extra={WINDOW_SKIP: True},
            )
        self.log(crash_line(where, exc, _journal_path()))

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

        self.tabs = ctk.CTkTabview(self.root, anchor="w", command=self._tab_changed)
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

        self._touch_campaign()
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
            return
        current = self.tabs.get()
        for name in ADMIN_TABS:
            tab = self.tab_objects.pop(name, None)
            if tab is None:
                continue
            dispose = getattr(tab, "dispose", None)
            if dispose is not None:
                dispose()  # retire ses rappels AVANT que ses widgets disparaissent
            self.tabs.delete(name)
        self._admin_built = False
        if current in ADMIN_TABS:
            self.tabs.set("Accueil")
            self._tab_changed()  # `CTkTabview.set` n'appelle pas `command`

    def show_tab(self, name: str, *, admin: bool = False) -> None:
        if admin and not self.admin_var.get():
            self.admin_var.set(True)
            self._toggle_admin()
        if name in self.tab_objects:
            self.tabs.set(name)
            self._tab_changed()

    def current_tab(self) -> str:
        """Nom de l'onglet visible — les écrans coûteux ne calculent que s'ils sont à l'écran.

        `CTkTabview.get()` rend un attribut Python (aucun appel Tcl, aucune exception) :
        le seul cas à couvrir est celui d'un écran qui appelle avant que `_build` n'ait
        posé le `CTkTabview`, d'où le `getattr`.
        """
        tabs = getattr(self, "tabs", None)
        return str(tabs.get()) if tabs is not None else ""

    def _tab_changed(self) -> None:
        """Un onglet vient d'être affiché : il rattrape le rafraîchissement qu'il a sauté."""
        tab = self.tab_objects.get(self.current_tab())
        catch_up = getattr(tab, "refresh_if_needed", None)
        if catch_up is not None:
            catch_up()

    # ------------------------------------------------------------ campagne
    def db_path(self) -> Path:
        return Path(self._db_path.strip() or self.config.db_path)

    def open_db(self) -> Database:
        self.config.db_path = str(self.db_path())
        return Database(self.db_path())

    def backup_dir(self) -> Path:
        return self._backup_dir or default_backup_dir(self.db_path())

    def set_backup_dir(self, path: Path) -> None:
        self._backup_dir = path

    def create_campaign(self, db_path: str) -> bool:
        """Crée le fichier de campagne (dossier + schéma) puis l'ouvre.

        Sans cette création, « Nouvelle… » ne faisait que retenir un nom : le fichier
        n'existait pas, et « Scanner » refusait de démarrer faute de campagne. Un fichier
        déjà présent est **ouvert tel quel** : une campagne ne s'écrase jamais. Un fichier
        qui existe mais n'est **pas** une campagne Doc-IA est refusé, sans y toucher.

        L'ouverture faite ici est la seule : `open_campaign(touch=False)` ne la refait pas.
        """
        raw = db_path.strip()
        if not raw:
            self.log("indique un nom de fichier pour la campagne")
            return False
        target = Path(raw)
        kind = campaign_kind(target)
        if kind == FOREIGN:
            self.log(
                f"ce fichier n'est pas une campagne Doc-IA : {target} — "
                "choisis un autre nom (le fichier n'a pas été touché)"
            )
            return False
        try:
            Database(target).close()
        except Exception as exc:  # noqa: BLE001 — chemin invalide, disque plein, droits
            self.log(f"campagne impossible à créer ({target}) : {exc}")
            return False
        self.log(
            f"campagne existante ouverte : {target} (aucune donnée effacée)"
            if kind == DOCIA
            else f"campagne créée : {target}"
        )
        self.open_campaign(str(target), touch=False)
        return True

    def _touch_campaign(self) -> None:
        """Ouvre la base une fois, ici, dans le thread Tk.

        C'est cette ouverture qui déclenche une éventuelle migration de schéma (et sa
        sauvegarde préalable). Les écrans calculent **ensuite**, en parallèle : laisser
        trois threads découvrir en même temps une base à migrer serait un désastre.

        La garantie tient parce que `open_campaign` migre ici, dans l'ordre, avant le
        `refresh_all` qui lance les calculs, et parce que chaque calcul ouvre le chemin
        que `LazyScreen._start` a capturé — jamais un chemin relu depuis le thread.
        """
        if not self.db_path().exists():
            return
        try:
            Database(self.db_path()).close()
        except Exception as exc:  # noqa: BLE001 — base illisible : on le dit, on continue
            logger.exception("ouverture de la campagne", extra={WINDOW_SKIP: True})
            self.log(f"campagne illisible ({self.db_path()}) : {exc}")

    def ensure_campaign(self) -> bool:
        """Garantit que la campagne courante existe sur le disque (créée au besoin)."""
        return True if self.db_path().exists() else self.create_campaign(str(self.db_path()))

    def open_campaign(self, db_path: str, *, touch: bool = True) -> None:
        """`touch=False` : la base vient d'être ouverte par l'appelant (`create_campaign`)."""
        self._db_path = db_path
        self.config.db_path = db_path
        if touch:
            self._touch_campaign()
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
        if path and self.create_campaign(path):
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
                logger.exception("échec %s", name, extra={WINDOW_SKIP: True})
                self.log(f"{name} : ERREUR — {exc}")
            finally:
                self._log_queue.put(_DONE)

        self.cancel.clear()
        self._worker = threading.Thread(target=wrapped, name=f"docia-{name}", daemon=True)
        self._set_busy(True, name)
        self._worker.start()
        return True

    def run_background(
        self,
        compute: Callable[[], Any],
        apply: Callable[[Any], None],
        *,
        name: str = "calcul",
    ) -> None:
        """Calcul de lecture (statistiques, compteurs) hors du thread Tk.

        Contrairement à `run_in_thread`, n'occupe pas l'unique emplacement de travail :
        consulter un écran ne doit jamais empêcher de lancer une analyse. `apply(résultat)`
        s'exécute ensuite dans le thread Tk, via la file du journal.
        """

        def worker() -> None:
            try:
                result = compute()
            except Exception as exc:  # noqa: BLE001 — affiché, jamais avalé
                logger.exception("échec %s", name, extra={WINDOW_SKIP: True})
                self.log(f"{name} : {exc}")
                return
            self.ui(lambda: apply(result))

        threading.Thread(target=worker, name=f"docia-{name}", daemon=True).start()

    def is_busy(self) -> bool:
        return bool(self._worker and self._worker.is_alive())

    def on_busy(self, listener: Callable[[bool], None], owner: Any = None) -> None:
        self._busy_listeners.append((listener, owner if owner is not None else _owner_of(listener)))

    def off_busy(self, listener: Callable[[bool], None]) -> None:
        self._busy_listeners = [e for e in self._busy_listeners if e[0] != listener]

    def on_refresh(self, listener: Callable[[], None], owner: Any = None) -> None:
        self._refresh_listeners.append(
            (listener, owner if owner is not None else _owner_of(listener))
        )

    def off_refresh(self, listener: Callable[[], None]) -> None:
        self._refresh_listeners = [e for e in self._refresh_listeners if e[0] != listener]

    def _dispatch(self, listeners: list[Any], call: Callable[[Any], None], what: str) -> None:
        """Appelle chaque rappel encore vivant ; un échec n'empêche pas les suivants.

        Un rappel dont le widget propriétaire a été détruit est retiré au passage.
        Sans ces deux précautions, un aller-retour en mode administrateur faisait
        lever `TclError` à `_set_busy` : le travail ne démarrait plus (l'exception
        précède `Thread.start()`) et, pire, `_poll` mourait avant sa réinscription —
        plus de journal, plus de progression, alors que la fenêtre paraît vivante.
        """
        for entry in list(listeners):
            if not _alive(entry[1]):
                if entry in listeners:
                    listeners.remove(entry)
                continue
            try:
                call(entry[0])
            except Exception as exc:  # noqa: BLE001 — affiché, jamais avalé
                logger.exception(what, extra={WINDOW_SKIP: True})
                self.log(f"{what} : {exc}")

    def _set_busy(self, busy: bool, name: str = "") -> None:
        try:
            self.busy_label.configure(text=f"⏳ {name} en cours…" if busy else "")
        except Exception:  # noqa: BLE001 — fenêtre en cours de destruction
            logger.exception("bandeau d'occupation")
        self._dispatch(self._busy_listeners, lambda cb: cb(busy), "état occupé")

    def refresh_all(self) -> None:
        self._dispatch(self._refresh_listeners, lambda cb: cb(), "rafraîchissement")
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
        """Vide la file dans le thread Tk — et **se réinscrit quoi qu'il arrive**.

        `_poll` est le cœur vivant de la fenêtre : journal, avancement, résultats des
        calculs de fond passent tous par lui. Une exception qui le traversait le tuait
        sans le réinscrire — la fenêtre restait affichée mais sourde et définitivement
        muette, exactement le « figé sans un mot » signalé. D'où le `finally`.
        """
        try:
            self._drain()
        except Exception as exc:  # noqa: BLE001 — le journal ne fait pas tomber la fenêtre
            # Une seule ligne à l'écran, même si l'affichage lui-même est cassé :
            # `report_crash` réempile dans la file que `_drain` vient de rater.
            self._poll_failures += 1
            if self._poll_failures == 1:
                self.report_crash("journal de la fenêtre", exc)
            else:
                logger.exception("journal de la fenêtre", extra={WINDOW_SKIP: True})
        finally:
            with contextlib.suppress(Exception):  # fenêtre détruite : plus rien à replanifier
                self.root.after(_POLL_MS, self._poll)

    def _drain(self) -> None:
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

    def run(self) -> None:
        try:
            self.root.mainloop()
        finally:
            self._remove_safety_net()


def launch(config_path: Path | None = None, *, smoke: bool = False) -> None:
    """Point d'entrée GUI (`python -m docia`, `docia gui`). `smoke` : construit toute la
    fenêtre (onglets admin compris) puis la ferme — contrôle d'un exécutable empaqueté."""
    app = DociaApp(config_path)
    if smoke:
        app.admin_var.set(True)
        app._toggle_admin()
        app.root.after(1500, app.root.destroy)
    app.run()
