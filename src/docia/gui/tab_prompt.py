"""Onglet Prompt : le prompt est une variable — éditer, enregistrer, charger, activer,
réinitialiser, compter les tokens, tester sur un fichier."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from docia.gui.helpers import estimate_prompt_tokens
from docia.gui.widgets import ReadOnlyText
from docia.llm.schema import load_system_prompt

_EMBEDDED = "(embarqué)"


class PromptTab:
    """Onglet administrateur « Prompt » : profils en base, édition, import/export."""

    def __init__(self, app: Any, parent: Any) -> None:
        self.app = app
        self.parent = parent
        self.ctk = app.ctk

    def build(self) -> None:
        """Construit les widgets de l'onglet (une fois)."""
        ctk, p = self.ctk, self.parent
        top = ctk.CTkFrame(p, fg_color="transparent")
        top.pack(fill="x", padx=10, pady=(10, 4))
        ctk.CTkLabel(top, text="Profil de prompt").pack(side="left")
        self.profile_var = ctk.StringVar(value=_EMBEDDED)
        self.profile_menu = ctk.CTkOptionMenu(
            top,
            variable=self.profile_var,
            values=[_EMBEDDED],
            width=220,
            command=lambda _v: self._load_selected(),
        )
        self.profile_menu.pack(side="left", padx=6)
        self.active_label = ctk.CTkLabel(top, text="")
        self.active_label.pack(side="left", padx=10)
        self.tokens_label = ctk.CTkLabel(top, text="")
        self.tokens_label.pack(side="right")

        self.editor = ctk.CTkTextbox(
            p, height=300, wrap="word", font=ctk.CTkFont(family="Consolas", size=12)
        )
        self.editor.pack(fill="both", expand=True, padx=10, pady=4)
        self.editor.bind("<KeyRelease>", lambda _e: self._update_tokens())

        btns = ctk.CTkFrame(p, fg_color="transparent")
        btns.pack(fill="x", padx=10, pady=6)
        ctk.CTkLabel(btns, text="Nom du profil").pack(side="left")
        self.name_var = ctk.StringVar(value="")
        ctk.CTkEntry(btns, textvariable=self.name_var, width=200).pack(side="left", padx=(4, 10))
        ctk.CTkButton(btns, text="Enregistrer", width=110, command=self._save).pack(
            side="left", padx=(0, 6)
        )
        ctk.CTkButton(
            btns,
            text="Enregistrer et activer",
            width=170,
            command=lambda: self._save(activate=True),
        ).pack(side="left", padx=(0, 6))
        ctk.CTkButton(btns, text="Activer ce profil", width=130, command=self._activate).pack(
            side="left", padx=(0, 6)
        )
        btns2 = ctk.CTkFrame(p, fg_color="transparent")
        btns2.pack(fill="x", padx=10, pady=(0, 6))
        ctk.CTkButton(
            btns2,
            text="Revenir au prompt embarqué",
            width=200,
            fg_color="#6b7280",
            command=self._reset,
        ).pack(side="left", padx=(0, 6))
        ctk.CTkButton(
            btns2,
            text="Importer un fichier…",
            width=150,
            fg_color="#6b7280",
            command=self._import_file,
        ).pack(side="left", padx=(0, 6))
        ctk.CTkButton(
            btns2, text="Exporter…", width=110, fg_color="#6b7280", command=self._export_file
        ).pack(side="left", padx=(0, 6))
        ctk.CTkButton(
            btns2, text="Supprimer le profil", width=150, command=self._delete, fg_color="#7f1d1d"
        ).pack(side="right")

        test = ctk.CTkFrame(p, fg_color="transparent")
        test.pack(fill="x", padx=10, pady=(4, 4))
        ctk.CTkLabel(test, text="Tester ce prompt sur un fichier :").pack(side="left")
        self.test_path_var = ctk.StringVar(value="")
        ctk.CTkEntry(test, textvariable=self.test_path_var, width=420).pack(side="left", padx=6)
        ctk.CTkButton(test, text="…", width=36, command=self._pick_test_file).pack(
            side="left", padx=(0, 6)
        )
        self.test_button = ctk.CTkButton(test, text="Tester", command=self._test)
        self.test_button.pack(side="left")
        self.test_output = ReadOnlyText(ctk, p, height=150)
        self.test_output.pack(fill="x", padx=10, pady=(0, 10))

        self.app.on_refresh(self.refresh)
        self.app.on_busy(self._busy)
        self._set_editor(load_system_prompt(None))

    def _busy(self, busy: bool) -> None:
        self.test_button.configure(state="disabled" if busy else "normal")

    def dispose(self) -> None:
        """Retire les rappels avant que l'onglet soit détruit (mode administrateur coupé)."""
        self.app.off_busy(self._busy)
        self.app.off_refresh(self.refresh)

    # ---- état
    def _set_editor(self, text: str) -> None:
        self.editor.delete("1.0", "end")
        self.editor.insert("end", text)
        self._update_tokens()

    def _editor_text(self) -> str:
        return str(self.editor.get("1.0", "end")).rstrip() + "\n"

    def _update_tokens(self) -> None:
        text = self._editor_text()
        self.tokens_label.configure(
            text=f"≈ {estimate_prompt_tokens(text)} tokens · {len(text)} caractères"
        )

    def refresh(self) -> None:
        """Recharge la liste des profils et le profil actif depuis la base."""
        names = [_EMBEDDED]
        active = None
        if self.app.db_path().exists():
            with self.app.open_db() as db:
                names += [str(r["name"]) for r in db.list_prompts()]
                active_row = db.active_prompt()
                active = active_row[0] if active_row else None
        self.profile_menu.configure(values=names)
        self.active_label.configure(text=f"profil utilisé par l'analyse : {active or _EMBEDDED}")

    def _load_selected(self) -> None:
        name = self.profile_var.get()
        if name == _EMBEDDED:
            self._set_editor(load_system_prompt(None))
            self.name_var.set("")
            return
        with self.app.open_db() as db:
            text = db.get_prompt(name)
        if text is None:
            self.app.log(f"profil inconnu : {name}")
            return
        self._set_editor(text)
        self.name_var.set(name)

    # ---- actions
    def _save(self, activate: bool = False) -> None:
        name = self.name_var.get().strip()
        text = self._editor_text()
        if not name or name == _EMBEDDED:
            self.app.log("donne un nom au profil avant d'enregistrer")
            return
        if len(text.strip()) < 50:
            self.app.log("prompt trop court (< 50 caractères)")
            return
        with self.app.open_db() as db:
            db.save_prompt(name, text, activate=activate)
        self.app.log(
            f"profil « {name} » enregistré"
            + (
                " et activé — les fichiers analysés avec un autre prompt seront réanalysés"
                if activate
                else ""
            )
        )
        self.refresh()
        self.profile_var.set(name)

    def _activate(self) -> None:
        name = self.profile_var.get()
        with self.app.open_db() as db:
            ok = db.set_active_prompt(None if name == _EMBEDDED else name)
        self.app.log(f"profil actif : {name}" if ok else f"profil inconnu : {name}")
        self.refresh()

    def _reset(self) -> None:
        self.profile_var.set(_EMBEDDED)
        self._load_selected()
        with self.app.open_db() as db:
            db.set_active_prompt(None)
        self.app.log("prompt embarqué actif")
        self.refresh()

    def _delete(self) -> None:
        name = self.profile_var.get()
        if name == _EMBEDDED:
            return
        with self.app.open_db() as db:
            ok = db.delete_prompt(name)
        self.app.log(f"profil « {name} » supprimé" if ok else f"profil inconnu : {name}")
        self.profile_var.set(_EMBEDDED)
        self._load_selected()
        self.refresh()

    def _import_file(self) -> None:
        from tkinter import filedialog

        path = filedialog.askopenfilename(
            filetypes=[("Markdown / texte", "*.md *.txt"), ("Tous", "*.*")]
        )
        if path:
            self._set_editor(Path(path).read_text(encoding="utf-8"))
            self.name_var.set(Path(path).stem)

    def _export_file(self) -> None:
        from tkinter import filedialog

        path = filedialog.asksaveasfilename(
            defaultextension=".md", filetypes=[("Markdown", "*.md")]
        )
        if path:
            Path(path).write_text(self._editor_text(), encoding="utf-8")
            self.app.log(f"prompt exporté → {path}")

    def _pick_test_file(self) -> None:
        from tkinter import filedialog

        path = filedialog.askopenfilename()
        if path:
            self.test_path_var.set(path)

    def _test(self) -> None:
        target = Path(self.test_path_var.get().strip())
        if not target.is_file():
            self.app.log("choisis un fichier à tester")
            return
        app = self.app
        cfg = app.collect_config()
        if cfg.validate():
            return
        prompt_text = self._editor_text()

        def work() -> None:
            import tempfile

            from docia.quick import quick_analyze

            with tempfile.TemporaryDirectory(prefix="docia-prompt-") as tmp:
                prompt_file = Path(tmp) / "prompt.md"
                prompt_file.write_text(prompt_text, encoding="utf-8")
                from dataclasses import replace

                cfg_test = replace(cfg, prompt_path=str(prompt_file))
                rep = quick_analyze(cfg_test, [target], progress=app.log, cancel=app.cancel)
            payload = rep.as_dict()
            text = (
                "\n".join(rep.as_lines())
                + "\n\n"
                + json.dumps(payload.get("files", payload), ensure_ascii=False, indent=2)[:6000]
            )
            app.ui(lambda: self.test_output.set(text))  # écrire dans un widget = thread Tk

        app.run_in_thread(work, "test du prompt")
