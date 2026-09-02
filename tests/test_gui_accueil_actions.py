"""Actions de l'écran Accueil (`docia.gui.tab_home`) — **sans écran**, doublures de
`customtkinter`, `tkinter.filedialog` et `tkinter.messagebox`.

Complète `test_gui_accueil_sans_ecran.py` (construction, état) par les boutons :
choix de fichiers, import, préparation, test du serveur, lancement, relance, arrêt,
analyse rapide. Le `FakeApp` exécute les travaux de fond **sur place** : ce que le
vrai `run_in_thread` ferait dans un thread, ici on l'observe tout de suite.
"""

from __future__ import annotations

import sys
import types
from pathlib import Path
from typing import Any

import pytest

from docia.config import Config
from tests.test_gui_accueil_sans_ecran import FakeApp, _fake_ctk


class FakeService:
    """Journal des appels de la couche service vue par l'onglet."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def import_scan(self, csv_path: Path, *, strict: bool, _log: Any) -> None:
        self.calls.append(("import_scan", (csv_path, strict)))

    def plan(self, cfg: Config, _log: Any) -> None:
        self.calls.append(("plan", ()))

    def run(self, cfg: Config, **kwargs: Any) -> None:
        self.calls.append(("run", (kwargs.get("limit"), kwargs.get("dry_run"))))

    def reanalyze(self, cfg: Config, mode: str, _log: Any) -> int:
        self.calls.append(("reanalyze", (mode,)))
        return 0

    def quick(self, cfg: Config, target: Path, db_path: Path, _log: Any, cancel: Any) -> None:
        self.calls.append(("quick", (target,)))


class ActingApp(FakeApp):
    """`FakeApp` qui exécute les travaux sur place et retient les onglets demandés."""

    def __init__(self, tmp: Path) -> None:
        super().__init__(tmp)
        self.service = FakeService()
        self.cancel = types.SimpleNamespace(set=lambda: self.logs.append("cancel.set"))
        self.threads: list[str] = []
        self.opened: list[str] = []
        self.remembered: list[str | None] = []

    def collect_config(self) -> Config:
        return self.config

    def run_in_thread(self, work: Any, name: str) -> bool:
        self.threads.append(name)
        work()
        return True

    def open_campaign(self, db_path: str, *, touch: bool = True) -> None:
        self.opened.append(db_path)

    def remember_campaign(self, csv_path: str | None = None) -> None:
        self.remembered.append(csv_path)

    def open_db(self) -> Any:
        raise AssertionError("aucun test ici n'ouvre la base")


@pytest.fixture
def accueil(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Any:
    from docia.gui.tab_home import HomeTab

    monkeypatch.setitem(sys.modules, "customtkinter", _fake_ctk())
    app = ActingApp(tmp_path)
    tab = HomeTab(app, object())
    tab.build()
    return tab


def _fake_dialogs(monkeypatch: pytest.MonkeyPatch, *, path: str = "", yes: bool = True) -> None:
    filedialog = types.ModuleType("tkinter.filedialog")
    filedialog.askopenfilename = lambda **_k: path  # type: ignore[attr-defined]
    filedialog.askdirectory = lambda **_k: path  # type: ignore[attr-defined]
    messagebox = types.ModuleType("tkinter.messagebox")
    messagebox.askyesno = lambda *_a, **_k: yes  # type: ignore[attr-defined]
    tk = types.ModuleType("tkinter")
    tk.filedialog = filedialog  # type: ignore[attr-defined]
    tk.messagebox = messagebox  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "tkinter", tk)
    monkeypatch.setitem(sys.modules, "tkinter.filedialog", filedialog)
    monkeypatch.setitem(sys.modules, "tkinter.messagebox", messagebox)


def test_choisir_un_csv_ouvre_une_campagne_a_cote(
    accueil: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    csv = tmp_path / "scan.csv"
    _fake_dialogs(monkeypatch, path=str(csv))
    accueil._pick_csv()
    assert accueil.csv_var.get() == str(csv)
    assert accueil.app.opened == [str(csv.with_suffix(".sqlite"))]


def test_choisir_cible_et_dossier_rapide(
    accueil: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _fake_dialogs(monkeypatch, path=str(tmp_path))
    accueil._pick_target()
    accueil._pick_quick_dir()
    assert accueil.target_var.get() == str(tmp_path)
    assert accueil.quick_var.get() == str(tmp_path)
    _fake_dialogs(monkeypatch, path="")  # dialogue annulé : rien ne change
    accueil._pick_quick_file()
    assert accueil.quick_var.get() == str(tmp_path)


def test_import_exige_un_csv_existant(accueil: Any, tmp_path: Path) -> None:
    accueil.csv_var.set(str(tmp_path / "absent.csv"))
    accueil._import_and_plan()
    assert accueil.app.service.calls == []
    assert "choisis un CSV" in accueil.app.logs[-1]


def test_import_puis_preparation_puis_memorisation(accueil: Any, tmp_path: Path) -> None:
    csv = tmp_path / "scan.csv"
    csv.write_text("Name\n", encoding="utf-8")
    accueil.csv_var.set(str(csv))
    accueil._import_and_plan()
    assert [c[0] for c in accueil.app.service.calls] == ["import_scan", "plan"]
    assert accueil.app.service.calls[0][1] == (csv, False)  # tolérant aux lignes invalides
    assert accueil.app.remembered == [str(csv)]
    assert accueil.app.threads == ["import"]


def test_preparation_seule(accueil: Any) -> None:
    accueil._plan()
    assert accueil.app.service.calls == [("plan", ())]


def test_lancer_avec_config_invalide_envoie_vers_les_reglages(accueil: Any) -> None:
    accueil.app.config.llm.base_url = "pas une url"
    accueil._start_run()
    assert accueil.app.service.calls == []
    assert accueil.app.tab == "Serveur & performances"


def test_lancer_avec_limite(accueil: Any) -> None:
    accueil.limit_var.set("25")
    accueil._start_run()
    assert accueil.app.service.calls == [("run", (25, False))]
    accueil.limit_var.set("0")  # 0 = pas de limite
    accueil._start_run(dry_run=True)
    assert accueil.app.service.calls[-1] == ("run", (None, True))


def test_evenement_de_run_met_a_jour_barre_et_estimation(accueil: Any) -> None:
    event = types.SimpleNamespace(
        files_total=200,
        files_done=50,
        files_error=10,
        blocks_done=3,
        blocks_total=10,
        files_per_hour=1234.0,
        elapsed_s=90.0,
        eta_s=270.0,
    )
    accueil._show_event(event)
    assert accueil.progress.value == pytest.approx(0.3)
    assert "50 analysés" in accueil.run_status.options["text"]
    assert "140 restants" in accueil.run_status.options["text"]
    assert "blocs 3/10" in accueil.run_status.options["text"]
    assert "1 234 fichiers/h" in accueil.run_eta.options["text"]


def test_arret_demande(accueil: Any) -> None:
    accueil._stop()
    assert "cancel.set" in accueil.app.logs
    assert "arrêt demandé" in accueil.app.logs[-1]


def test_relance_des_erreurs_puis_run(accueil: Any) -> None:
    accueil.rerun_var.set("errors")
    accueil._rerun()
    assert [c[0] for c in accueil.app.service.calls] == ["reanalyze", "run"]
    assert accueil.app.service.calls[0][1] == ("errors",)


def test_tout_reanalyser_demande_confirmation(
    accueil: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    accueil.rerun_var.set("all")
    _fake_dialogs(monkeypatch, yes=False)
    accueil._rerun()
    assert accueil.app.service.calls == []  # refusé : rien n'est effacé
    _fake_dialogs(monkeypatch, yes=True)
    accueil._rerun()
    assert [c[0] for c in accueil.app.service.calls] == ["reanalyze", "run"]


def test_relance_manquante_reste_dans_le_journal(accueil: Any) -> None:
    def casse(cfg: Config, mode: str, log: Any) -> int:
        raise RuntimeError("base verrouillée")

    accueil.app.service.reanalyze = casse  # type: ignore[method-assign]
    accueil.rerun_var.set("errors")
    accueil._rerun()
    assert "relance impossible : base verrouillée" in accueil.app.logs[-1]
    assert accueil.app.service.calls == []  # pas de run après un échec


def test_analyse_rapide(accueil: Any, tmp_path: Path) -> None:
    accueil.quick_var.set(str(tmp_path / "absent"))
    accueil._quick()
    assert "introuvable" in accueil.app.logs[-1]
    accueil.quick_var.set(str(tmp_path))
    accueil._quick()
    assert accueil.app.service.calls == [("quick", (tmp_path,))]
    assert accueil.app.threads == ["analyse rapide"]


def test_test_du_serveur(accueil: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    from docia.llm import client as client_mod

    class FakeClient:
        def __init__(self, *_a: Any, **_k: Any) -> None:
            pass

        async def __aenter__(self) -> FakeClient:
            return self

        async def __aexit__(self, *_a: Any) -> None:
            pass

        async def health(self) -> bool:
            return False

    monkeypatch.setattr(client_mod, "LLMClient", FakeClient)
    accueil._test_server()
    assert "injoignable" in accueil.server_result.options["text"]
    assert accueil.app.logs and "injoignable" in accueil.app.logs[-1]
