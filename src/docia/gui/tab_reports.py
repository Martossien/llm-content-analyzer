"""Écran Rapports & exports : rapport HTML/Markdown, Excel, Power BI, CSV/JSON,
sauvegarde et restauration de la base."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from docia.gui.dialogs import produce_document
from docia.gui.service_shim import list_backups
from docia.gui.theme import FONT_FAMILY, FONT_SIZE_SMALL
from docia.gui.widgets import Card

_EXPORTS = (
    ("html", "Rapport HTML", "synthèse autonome pour la direction (un seul fichier)", "report"),
    ("md", "Rapport Markdown", "même contenu, pour un wiki ou un mail", "report"),
    (
        "xlsx",
        "Classeur Excel",
        "un onglet par vue : fichiers, doublons, sensibles, conservation…",
        "export",
    ),
    ("powerbi", "Dossier Power BI", "CSV au schéma stable pour Power BI Report Server", "export"),
    ("csv", "CSV des fichiers", "toutes les colonnes, un fichier par ligne", "export"),
    ("json", "JSON", "pour un autre outil", "export"),
)


class ReportsTab:
    """Onglet Rapports : rapport HTML, classeur, Power BI, sauvegardes."""

    def __init__(self, app: Any, parent: Any) -> None:
        self.app = app
        self.parent = parent
        self.ctk = app.ctk
        self._buttons: list[Any] = []

    def build(self) -> None:
        """Construit les widgets de l'onglet (une fois)."""
        ctk, p = self.ctk, self.parent
        wrap = ctk.CTkFrame(p, fg_color="transparent")
        wrap.pack(fill="both", expand=True, padx=8, pady=8)
        wrap.grid_columnconfigure((0, 1), weight=1, uniform="rep")

        card = Card(ctk, wrap, "Produire un document", subtitle="choisis, puis l'emplacement")
        card.grid(row=0, column=0, sticky="nsew", padx=4, pady=4)
        for fmt, label, hint, kind in _EXPORTS:
            row = ctk.CTkFrame(card.body, fg_color="transparent")
            row.pack(fill="x", pady=3)
            b = ctk.CTkButton(
                row, text=label, width=170, command=lambda f=fmt, k=kind: self._produce(f, k)
            )
            b.pack(side="left")
            self._buttons.append(b)
            ctk.CTkLabel(
                row, text=hint, font=ctk.CTkFont(family=FONT_FAMILY, size=FONT_SIZE_SMALL)
            ).pack(side="left", padx=8)
        self.last_label = ctk.CTkLabel(card.body, text="", anchor="w", justify="left")
        self.last_label.pack(fill="x", pady=(10, 0))
        self.open_button = ctk.CTkButton(
            card.body,
            text="Ouvrir le dernier document",
            width=200,
            state="disabled",
            command=self._open_last,
        )
        self.open_button.pack(anchor="w", pady=(4, 0))
        self._last_path: Path | None = None

        backup = Card(
            ctk, wrap, "Sauvegarde de la base", subtitle="la campagne tient dans un seul fichier"
        )
        backup.grid(row=0, column=1, sticky="nsew", padx=4, pady=4)
        self.backup_dir_var = ctk.StringVar(value=str(self.app.backup_dir()))
        row = ctk.CTkFrame(backup.body, fg_color="transparent")
        row.pack(fill="x")
        ctk.CTkLabel(row, text="dossier").pack(side="left")
        ctk.CTkEntry(row, textvariable=self.backup_dir_var).pack(
            side="left", fill="x", expand=True, padx=6
        )
        ctk.CTkButton(row, text="…", width=36, command=self._pick_backup_dir).pack(side="left")
        row2 = ctk.CTkFrame(backup.body, fg_color="transparent")
        row2.pack(fill="x", pady=(8, 0))
        self.backup_button = ctk.CTkButton(
            row2, text="Sauvegarder maintenant", width=190, command=self._backup
        )
        self.backup_button.pack(side="left")
        self.restore_button = ctk.CTkButton(
            row2, text="Restaurer depuis…", width=160, fg_color="#6b7280", command=self._restore
        )
        self.restore_button.pack(side="left", padx=(6, 0))
        self.backups_label = ctk.CTkLabel(
            backup.body,
            text="",
            anchor="w",
            justify="left",
            font=ctk.CTkFont(family=FONT_FAMILY, size=FONT_SIZE_SMALL),
        )
        self.backups_label.pack(fill="x", pady=(10, 0))
        ctk.CTkLabel(
            backup.body,
            text="Une sauvegarde est aussi faite automatiquement avant toute réanalyse complète "
            "et avant une migration du schéma.",
            anchor="w",
            justify="left",
            wraplength=420,
            font=ctk.CTkFont(family=FONT_FAMILY, size=FONT_SIZE_SMALL),
        ).pack(fill="x", pady=(8, 0))

        self.app.on_busy(self._busy)
        self.app.on_refresh(self.refresh)

    def _busy(self, busy: bool) -> None:
        state = "disabled" if busy else "normal"
        for b in (*self._buttons, self.backup_button, self.restore_button):
            b.configure(state=state)

    def dispose(self) -> None:
        """Retire les rappels avant destruction (symétrie avec les onglets administrateur)."""
        self.app.off_busy(self._busy)
        self.app.off_refresh(self.refresh)

    def refresh(self) -> None:
        """Rafraîchit la liste des sauvegardes et l'état de la campagne."""
        if not self.app.db_path().exists():
            self.backups_label.configure(text="aucune campagne ouverte")
            return
        d = Path(self.backup_dir_var.get().strip() or self.app.backup_dir())
        if d == self.app.backup_dir():
            files = list_backups(self.app.db_path())
        elif d.is_dir():
            files = sorted(d.glob("*.sqlite"), key=lambda f: f.stat().st_mtime, reverse=True)
        else:
            files = []
        if not files:
            self.backups_label.configure(text="aucune sauvegarde dans ce dossier")
            return
        lines = [f"{len(files)} sauvegarde(s) — dernières :"] + [f"  {f.name}" for f in files[:4]]
        self.backups_label.configure(text="\n".join(lines))

    # ---- documents
    def _produce(self, fmt: str, kind: str) -> None:
        produce_document(self.app, fmt, kind, on_done=self._remember)

    def _remember(self, path: Path) -> None:
        """Retient le dernier document produit (thread Tk)."""
        self._last_path = path
        self.last_label.configure(text=f"dernier document : {path}")
        self.open_button.configure(state="normal")

    def _open_last(self) -> None:
        if self._last_path is None:
            return
        import webbrowser

        webbrowser.open(self._last_path.as_uri())

    # ---- sauvegarde
    def _pick_backup_dir(self) -> None:
        from tkinter import filedialog

        path = filedialog.askdirectory(title="Dossier des sauvegardes")
        if path:
            self.backup_dir_var.set(path)
            self.app.set_backup_dir(Path(path))
            self.refresh()

    def _backup(self) -> None:
        app = self.app
        if not app.db_path().exists():
            app.log("aucune campagne ouverte")
            return
        out_dir = Path(self.backup_dir_var.get().strip() or app.backup_dir())
        app.set_backup_dir(out_dir)
        db_path = app.db_path()

        def work() -> None:
            out = app.service.backup(db_path, out_dir)
            app.log(f"sauvegarde : {out}")

        app.run_in_thread(work, "sauvegarde")

    def _restore(self) -> None:
        from tkinter import filedialog, messagebox

        source = filedialog.askopenfilename(
            title="Sauvegarde à restaurer",
            initialdir=self.backup_dir_var.get(),
            filetypes=[("SQLite", "*.sqlite")],
        )
        if not source:
            return
        db_path = self.app.db_path()
        if not messagebox.askyesno(
            "Restaurer",
            f"La base actuelle\n{db_path}\nsera remplacée par\n{source}\n"
            "(l'actuelle est d'abord sauvegardée, étiquette avant_restauration). Continuer ?",
        ):
            return
        app = self.app

        def work() -> None:
            app.service.restore(db_path, Path(source))
            app.log(f"base restaurée depuis {source}")

        app.run_in_thread(work, "restauration")
