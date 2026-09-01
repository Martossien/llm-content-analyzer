"""Fenêtre principale (`docia.gui.app.DociaApp`) construite **sans Tk**.

`customtkinter`, `tkinter` et `tkinter.ttk` sont remplacés par des doublures : les
widgets mémorisent leurs options, le `CTkTabview` connaît ses onglets, la zone de
journal se laisse écrire. Ce que ces tests attrapent : une construction qui lève,
un onglet administrateur mal refermé, une campagne créée sans fichier, une config
enregistrée qui perd ses commentaires, un plantage de thread qui n'atteint pas la
fenêtre. C'est la partie qui n'avait que le smoke de l'exe (46 % couvert).
"""

from __future__ import annotations

import sys
import threading
import types
from pathlib import Path
from typing import Any

import pytest

from docia.config import default_toml


class _Widget:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.options: dict[str, Any] = dict(kwargs)
        self.children: list[_Widget] = []
        self.value: Any = kwargs.get("value")
        self.text_lines: list[str] = []
        parent = args[0] if args else None
        if isinstance(parent, _Widget):
            parent.children.append(self)

    def configure(self, *_a: Any, **kwargs: Any) -> None:
        self.options.update(kwargs)

    config = configure

    def cget(self, key: str) -> Any:
        return self.options.get(key)

    def set(self, value: Any) -> None:
        self.value = value

    def get(self, *_a: Any) -> Any:
        return self.value if self.value is not None else ""

    def winfo_children(self) -> list[_Widget]:
        return list(self.children)

    def winfo_exists(self) -> bool:
        return True

    def index(self, _what: str) -> str:
        return f"{len(self.text_lines) + 1}.0"

    def insert(self, _where: Any, text: str = "", *_a: Any) -> None:
        self.text_lines.append(str(text))

    def get_children(self, *_a: Any) -> list[str]:
        return []

    def after(self, _ms: int, _callback: Any = None) -> str:
        return "after#1"

    def __getattr__(self, name: str) -> Any:
        return lambda *_a, **_k: None


class _Tabview(_Widget):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.frames: dict[str, _Widget] = {}
        self.current = ""

    def add(self, name: str) -> _Widget:
        frame = _Widget(self)
        self.frames[name] = frame
        if not self.current:
            self.current = name
        return frame

    def delete(self, name: str) -> None:
        self.frames.pop(name, None)

    def set(self, name: str) -> None:
        self.current = name

    def get(self, *_a: Any) -> str:
        return self.current


class _Var:
    def __init__(self, value: Any = None, **_k: Any) -> None:
        self._value = value

    def get(self) -> Any:
        return self._value

    def set(self, value: Any) -> None:
        self._value = value

    def trace_add(self, *_a: Any) -> None:
        pass


def _fake_module(name: str, **attrs: Any) -> Any:
    module = types.ModuleType(name)
    module.__getattr__ = lambda _n: _Widget  # type: ignore[attr-defined]
    for key, value in attrs.items():
        setattr(module, key, value)
    return module


@pytest.fixture
def app(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Any:
    ctk = _fake_module(
        "customtkinter",
        CTkTabview=_Tabview,
        StringVar=_Var,
        BooleanVar=_Var,
        IntVar=_Var,
        CTkFont=lambda **k: k,
        set_appearance_mode=lambda *_a: None,
        set_default_color_theme=lambda *_a: None,
    )
    ttk = _fake_module("tkinter.ttk")
    tk = _fake_module("tkinter", ttk=ttk, END="end")
    monkeypatch.setitem(sys.modules, "customtkinter", ctk)
    monkeypatch.setitem(sys.modules, "tkinter", tk)
    monkeypatch.setitem(sys.modules, "tkinter.ttk", ttk)
    monkeypatch.chdir(tmp_path)
    from docia.gui.app import DociaApp

    application = DociaApp(tmp_path / "docia.toml")
    yield application
    application._remove_safety_net()


def _drain_all(app: Any) -> None:
    """Vide la file de la fenêtre comme `_poll` le ferait dans le thread Tk."""
    app._drain()


def test_construction_pose_les_onglets_utilisateur(app: Any) -> None:
    from docia.gui.app import USER_TABS

    assert list(app.tab_objects) == list(USER_TABS)
    assert app.current_tab() == "Accueil"
    assert app.status_line.options["text"] == "prêt"
    assert not app.is_busy()


def test_mode_administrateur_ouvre_puis_referme_ses_onglets(app: Any) -> None:
    from docia.gui.app import ADMIN_TABS, USER_TABS

    app.admin_var.set(True)
    app._toggle_admin()
    assert all(name in app.tab_objects for name in ADMIN_TABS)
    app.show_tab(ADMIN_TABS[0], admin=True)
    assert app.current_tab() == ADMIN_TABS[0]
    app.admin_var.set(False)
    app._toggle_admin()
    assert not any(name in app.tab_objects for name in ADMIN_TABS)
    assert app.current_tab() == "Accueil"  # revenu sur un onglet qui existe encore
    assert list(app.tab_objects) == list(USER_TABS)


def test_nouvelle_campagne_cree_le_fichier(app: Any, tmp_path: Path) -> None:
    target = tmp_path / "campagnes" / "audit.sqlite"
    assert app.create_campaign(str(target)) is True
    assert target.is_file(), "« Nouvelle… » doit créer la base, pas seulement retenir un nom"
    assert app.db_path() == target
    assert app.create_campaign("   ") is False
    _drain_all(app)
    assert any("indique un nom" in line for line in app.log_box.text_lines)


def test_fichier_etranger_refuse_sans_etre_touche(app: Any, tmp_path: Path) -> None:
    foreign = tmp_path / "contacts.sqlite"
    foreign.write_bytes(b"pas une base sqlite du tout")
    before = foreign.read_bytes()
    assert app.create_campaign(str(foreign)) is False
    assert foreign.read_bytes() == before
    _drain_all(app)
    assert any("n'est pas une campagne" in line for line in app.log_box.text_lines)


def test_enregistrer_garde_les_commentaires_du_toml(app: Any, tmp_path: Path) -> None:
    config_path = tmp_path / "docia.toml"
    config_path.write_text(default_toml(), encoding="utf-8")
    app.config.llm.model = "qwen-test"
    app.save_config()
    text = config_path.read_text(encoding="utf-8")
    assert 'model = "qwen-test"' in text
    assert text.count("#") >= default_toml().count("#") - 1, "les commentaires doivent survivre"


def test_plantage_de_thread_atteint_la_fenetre(app: Any) -> None:
    """Un thread hors `run_in_thread` qui meurt : une ligne lisible dans le journal."""
    done = threading.Event()

    def casse() -> None:
        try:
            raise RuntimeError("disque arraché")
        finally:
            done.set()

    thread = threading.Thread(target=casse, name="docia-test")
    thread.start()
    thread.join(timeout=5)
    assert done.wait(5)
    _drain_all(app)
    assert any("disque arraché" in line for line in app.log_box.text_lines)


def test_travail_en_thread_occupe_puis_libere(app: Any) -> None:
    fini = threading.Event()

    def work() -> None:
        app.log("travail fait")
        fini.set()

    assert app.run_in_thread(work, "test") is True
    assert app.is_busy() or fini.wait(5)
    app._worker.join(timeout=5)
    assert app.run_in_thread(lambda: None, "autre") is True  # le précédent est terminé
    app._worker.join(timeout=5)
    _drain_all(app)
    assert any("travail fait" in line for line in app.log_box.text_lines)
    assert app.busy_label.options["text"] == ""


def test_run_background_rend_le_resultat_dans_le_thread_tk(app: Any) -> None:
    received: list[int] = []
    fini = threading.Event()

    def compute() -> int:
        return 42

    def apply(value: int) -> None:
        received.append(value)
        fini.set()

    app.run_background(compute, apply, name="calcul-test")
    for _ in range(50):
        _drain_all(app)
        if fini.is_set():
            break
        threading.Event().wait(0.05)
    assert received == [42]


def test_journal_se_deplie_et_se_replie(app: Any) -> None:
    app._toggle_journal()
    assert app.journal_button.options["text"] == "Journal ▾"
    app._toggle_journal()
    assert app.journal_button.options["text"] == "Journal ▴"
