"""Écran Accueil (`docia.gui.tab_home`) construit **sans Tk** : une doublure de
`customtkinter` qui fabrique des widgets inertes.

Ce que ces tests attrapent : une faute de frappe dans `build()` (attribut manquant,
mauvais nom de widget), un bouton oublié dans `_busy`, un libellé d'état qui ne
suit plus les compteurs. C'est ce qui n'était couvert que par un écran réel
(`tests/test_gui_ecran.py`, ignoré en CI) — 14 % de couverture avant.
"""

from __future__ import annotations

import types
from pathlib import Path
from typing import Any

import pytest

from docia.config import Config
from docia.gui.theme import ACCENT_STOP


class _Widget:
    """Un widget inerte : mémorise ses options, ses enfants, et sa valeur."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.options: dict[str, Any] = dict(kwargs)
        self.children: list[_Widget] = []
        self.value: Any = kwargs.get("value")
        parent = args[0] if args else None
        if isinstance(parent, _Widget):
            parent.children.append(self)

    def configure(self, **kwargs: Any) -> None:
        self.options.update(kwargs)

    def cget(self, key: str) -> Any:
        return self.options.get(key)

    def set(self, value: Any) -> None:
        self.value = value

    def get(self) -> Any:
        return self.value

    def winfo_children(self) -> list[_Widget]:
        return list(self.children)

    def __getattr__(self, name: str) -> Any:
        # pack, grid, bind, grid_columnconfigure, pack_forget, tag_config…
        return lambda *_a, **_k: None


class _Var:
    def __init__(self, value: Any = None, **_k: Any) -> None:
        self._value = value
        self._traces: list[Any] = []

    def get(self) -> Any:
        return self._value

    def set(self, value: Any) -> None:
        self._value = value
        for callback in self._traces:
            callback()

    def trace_add(self, _mode: str, callback: Any) -> None:
        self._traces.append(callback)


def _fake_ctk() -> Any:
    module = types.ModuleType("customtkinter")

    def factory(_name: str) -> Any:
        return _Widget

    module.__getattr__ = factory  # type: ignore[attr-defined]
    module.StringVar = _Var  # type: ignore[attr-defined]
    module.BooleanVar = _Var  # type: ignore[attr-defined]
    module.IntVar = _Var  # type: ignore[attr-defined]
    module.CTkFont = lambda **k: k  # type: ignore[attr-defined]
    return module


class FakeApp:
    """Le strict nécessaire d'un `DociaApp` vu par l'onglet Accueil."""

    def __init__(self, tmp: Path) -> None:
        self.ctk = _fake_ctk()
        self.config = Config()
        self._db = tmp / "campagne.sqlite"
        self.logs: list[str] = []
        self.busy = False
        self.listeners: dict[str, list[Any]] = {"busy": [], "refresh": []}
        self.tab = "Accueil"

    def db_path(self) -> Path:
        return self._db

    def on_busy(self, listener: Any, _owner: Any = None) -> None:
        self.listeners["busy"].append(listener)

    def on_refresh(self, listener: Any, _owner: Any = None) -> None:
        self.listeners["refresh"].append(listener)

    def is_busy(self) -> bool:
        return self.busy

    def log(self, message: str) -> None:
        self.logs.append(message)

    def ui(self, action: Any) -> None:
        action()

    def current_tab(self) -> str:
        return self.tab

    def show_tab(self, name: str, *, _admin: bool = False) -> None:
        self.tab = name


@pytest.fixture
def accueil(tmp_path: Path) -> Any:
    from docia.gui.tab_home import HomeTab

    app = FakeApp(tmp_path)
    tab = HomeTab(app, _Widget())
    tab.build()
    return tab


def test_build_sans_ecran_cree_tous_les_widgets(accueil: Any) -> None:
    for name in (
        "scan_button",
        "import_button",
        "plan_button",
        "test_button",
        "run_button",
        "stop_button",
        "scan_stop_button",
        "rerun_button",
        "quick_button",
        "source_status",
        "run_status",
        "run_eta",
        "progress",
        "server_label",
        "scanner_label",
    ):
        assert hasattr(accueil, name), name
    assert set(accueil._tiles) == {
        "files",
        "analyzed",
        "sensitive",
        "reclaimable",
        "stale",
        "reviewed",
    }
    assert accueil.app.listeners["busy"] == [accueil._busy]
    assert accueil.app.listeners["refresh"] == [accueil.refresh]


def test_busy_desactive_les_actions_et_active_les_arrets(accueil: Any) -> None:
    accueil._busy(True)
    for b in (accueil.scan_button, accueil.run_button, accueil.quick_button, accueil.rerun_button):
        assert b.options["state"] == "disabled"
    assert accueil.stop_button.options["state"] == "normal"
    assert accueil.scan_stop_button.options["state"] == "normal"
    accueil._busy(False)
    assert accueil.run_button.options["state"] == "normal"
    assert accueil.stop_button.options["state"] == "disabled"


def test_refresh_sans_campagne_guide_l_utilisateur(accueil: Any) -> None:
    """Aucune base : tuiles à « — », consigne « Nouvelle… », scanner signalé absent."""
    accueil.refresh()
    assert all(t.value_label.options["text"] == "—" for t in accueil._tiles.values())
    assert "Nouvelle" in accueil.source_status.options["text"]
    assert "introuvable" in accueil.scanner_label.options["text"]
    assert accueil.scanner_label.options["text_color"] == ACCENT_STOP
    assert accueil.progress.value == 0
    assert accueil._dirty is False


def test_apply_overview_remplit_tuiles_et_avancement(accueil: Any) -> None:
    ov = types.SimpleNamespace(
        total_files=12345,
        total_bytes=2_500_000,
        analyzed=100,
        sensitive_files=7,
        duplicate_reclaimable_bytes=1_000_000,
        stale_files=3,
        reviewed=2,
        pending=50,
        excluded=20,
        errors=1,
    )
    counts = {"done": 100, "pending": 50, "queued": 0, "error": 1, "excluded": 20, "files": 171}
    accueil._apply_overview((ov, counts))
    assert accueil._tiles["files"].value_label.options["text"].replace("\u00a0", " ") == "12 345"
    assert accueil._tiles["sensitive"].value_label.options["text"] == "7"
    assert "50 à analyser" in accueil.source_status.options["text"].replace("\u00a0", " ")
    assert "100 analysés" in accueil.run_status.options["text"]
    assert "1 en erreur" in accueil.run_status.options["text"]
    assert accueil.run_eta.options["text"] == ""  # pas de run en cours : pas d'ETA


def test_scan_sans_cible_journalise_au_lieu_de_lancer(accueil: Any) -> None:
    accueil.target_var.set("   ")
    accueil._scan()
    assert accueil.app.logs
    assert "indique un dossier" in accueil.app.logs[-1]


def test_bascule_scan_csv_montre_le_bon_cadre(accueil: Any) -> None:
    """La bascule ne doit lever sur aucun des deux modes (les cadres existent)."""
    accueil.source_mode.set("csv")
    accueil._source_mode_changed()
    accueil.source_mode.set("scan")
    accueil._source_mode_changed()
