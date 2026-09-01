"""Tests qui exigent un **vrai écran** : ils construisent la fenêtre CustomTkinter.

Ignorés par défaut — la CI (Windows et Ubuntu sans session graphique) ne les joue pas.
Pour les lancer sur le poste de développement, **sur un affichage dédié** :

    Xvnc :99 -geometry 1280x1024 -SecurityTypes None &     # ou tout autre serveur X libre
    DISPLAY=:99 DOCIA_GUI_SCREEN=1 .venv/bin/python -m pytest tests/test_gui_ecran.py -v

**Ne les lancez pas sur votre session graphique** (`DISPLAY=:1` ici) : chaque test y
ouvre une fenêtre par-dessus votre travail, et la suite s'y est trouvée bloquée sans
rendre la main — deux fois, à 150 s et 300 s — là où elle passe en 19 s sur un
affichage sans gestionnaire de fenêtres. La cause exacte n'a pas été cherchée : il n'y
a aucune raison de jouer ces tests sur un bureau occupé.

Ils couvrent ce que les tests sans écran ne peuvent pas atteindre :

* B1 — un aller-retour en mode administrateur ne laisse aucun rappel orphelin, et les
  boutons répondent encore après ;
* B2 — les onglets administrateur n'écrivent dans leurs widgets que depuis le thread Tk ;
* B6 — un tri par en-tête suivi d'un clic ouvre la fiche de la ligne cliquée ;
* B7 — un changement de campagne vide les quatre sous-onglets des Statistiques ;
* B8 — le filet d'exception : un rappel Tk ou un thread qui lâche, et les messages des
  couches basses (`docia.service`, `docia.db`, DocFuse), donnent **une ligne lisible**
  dans le journal de la fenêtre — jamais une trace brute, jamais rien du tout.
"""

from __future__ import annotations

import os
import threading
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.screen

if not os.environ.get("DOCIA_GUI_SCREEN"):  # pragma: no cover - dépend de l'environnement
    pytest.skip("écran requis : DOCIA_GUI_SCREEN=1 (voir le docstring)", allow_module_level=True)

pytest.importorskip("customtkinter")

from docia.db import Database  # noqa: E402
from docia.gui.app import ADMIN_TABS, DociaApp  # noqa: E402
from tests.test_views import db  # noqa: E402, F401 — campagne de démonstration partagée


@pytest.fixture
def app(db: Database, tmp_path: Path) -> Any:  # noqa: F811
    """Fenêtre complète ouverte sur la campagne de démonstration de `test_views`."""
    config = tmp_path / "docia.toml"
    config.write_text(f'db_path = "{db.path.as_posix()}"\n', encoding="utf-8")
    application = DociaApp(config)
    yield application
    application.root.destroy()


def _pomper(app: Any, tours: int = 60) -> None:
    """Fait tourner la boucle Tk : `_poll` applique les résultats des calculs de fond."""
    import time

    for _ in range(tours):
        app.root.update()
        time.sleep(0.02)


# --------------------------------------------------------------------------- B1
def test_aller_retour_admin_ne_laisse_aucun_rappel_orphelin(app: Any) -> None:
    depart = (len(app._busy_listeners), len(app._refresh_listeners))
    for _ in range(5):
        app.admin_var.set(True)
        app._toggle_admin()
        app.admin_var.set(False)
        app._toggle_admin()
    assert (len(app._busy_listeners), len(app._refresh_listeners)) == depart
    assert all(nom not in app.tab_objects for nom in ADMIN_TABS)


def test_les_boutons_repondent_encore_apres_un_aller_retour_admin(app: Any) -> None:
    """`_set_busy` ne doit plus lever : sinon le travail ne part pas et `_poll` meurt."""
    app.admin_var.set(True)
    app._toggle_admin()
    app.admin_var.set(False)
    app._toggle_admin()

    parti = threading.Event()
    assert app.run_in_thread(parti.set, "essai") is True
    assert parti.wait(5.0), "le travail doit démarrer malgré les onglets détruits"
    _pomper(app)
    assert app.busy_label.cget("text") == "", "le bandeau doit se libérer à la fin"


# --------------------------------------------------------------------------- B2
def test_les_onglets_admin_ecrivent_depuis_le_thread_tk(
    app: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """« Diagnostic du poste » écrivait dans son widget depuis le thread de travail.

    Tant que la boucle principale tourne, Tcl marshalle et « ça marche » ; à la
    fermeture de la fenêtre pendant un diagnostic (ou en `--smoke`), on obtenait
    `RuntimeError: main thread is not in main loop`.
    """
    import docia.cli_tools

    monkeypatch.setattr(docia.cli_tools, "doctor_report", lambda _cfg: {"ocr_essai": "ok"})
    app.admin_var.set(True)
    app._toggle_admin()
    llm = app.tab_objects["Serveur & performances"]
    threads: list[str] = []
    vrai = type(llm.output).set

    def espion(self: Any, text: str) -> None:
        threads.append(threading.current_thread().name)
        vrai(self, text)

    type(llm.output).set = espion  # type: ignore[method-assign]
    try:
        llm._doctor()
        for _ in range(200):
            _pomper(app, tours=1)
            if threads:
                break
    finally:
        type(llm.output).set = vrai  # type: ignore[method-assign]
    assert threads, "le diagnostic doit finir par écrire dans le widget"
    assert threads == [threading.main_thread().name], f"écrit depuis {threads}"


# --------------------------------------------------------------------------- B6
def test_apres_un_tri_le_clic_ouvre_la_bonne_fiche(app: Any) -> None:
    resultats = app.tab_objects["Résultats"]
    app.tabs.set("Résultats")
    app._tab_changed()
    for _ in range(200):
        _pomper(app, tours=1)
        if resultats.table.rows:
            break
    assert resultats.table.rows, "le tableau doit se charger"

    table = resultats.table
    table._sort(1)  # clic sur l'en-tête « dossier »
    table._sort(1)  # second clic : ordre décroissant
    dossier_attendu = table.rows[0][1]
    nom_attendu = table.rows[0][0]

    resultats._select(table.keys[0])
    affiche = resultats.path_label.cget("text")
    assert affiche.startswith(dossier_attendu), f"{affiche!r} ne vient pas de {dossier_attendu!r}"
    assert affiche.endswith(nom_attendu)
    assert resultats._selected_id == table.keys[0]


# --------------------------------------------------------------------------- B7
def test_changer_de_campagne_vide_les_sous_onglets_non_rouverts(app: Any, tmp_path: Path) -> None:
    stats = app.tab_objects["Statistiques"]
    app.tabs.set("Statistiques")
    stats.sub.set("Risque")
    stats.refresh_if_needed()
    for _ in range(300):
        _pomper(app, tours=1)
        if stats.sections["Risque"].table.rows:
            break
    avant = stats.sections["Risque"].tiles["c3"].value_label.cget("text")
    assert avant != "—"

    stats.sub.set("Hygiène")
    stats.refresh_if_needed()
    autre = tmp_path / "autre.sqlite"
    Database(autre).close()
    app.open_campaign(str(autre))
    _pomper(app, tours=20)

    risque = stats.sections["Risque"]
    assert risque.tiles["c3"].value_label.cget("text") == "—", "chiffre de l'ancienne campagne"
    assert risque.table.rows == []


# --------------------------------------------------------------------------- B8
def _journal(app: Any) -> str:
    return str(app.log_box.get("1.0", "end"))


def test_une_exception_dans_un_rappel_tk_se_voit_dans_la_fenetre(app: Any) -> None:
    """B8 — avant : la trace partait sur une console que l'exe fenêtré n'affiche pas.

    L'utilisateur avait une fenêtre muette. Maintenant : une ligne lisible dans le
    journal, la pile complète dans `docia.log`, et la fenêtre continue de vivre.
    """

    def rappel_qui_lache() -> None:
        raise ZeroDivisionError("division par zéro dans un rappel d'onglet")

    app.root.after(0, rappel_qui_lache)  # le chemin exact d'un rappel Tk
    _pomper(app, tours=20)

    texte = _journal(app)
    assert "anomalie non prévue" in texte, texte[-500:]
    assert "ZeroDivisionError" in texte
    assert "docia.log" in texte
    assert "Traceback" not in texte, "jamais de trace brute à l'écran"
    lignes = [ligne for ligne in texte.splitlines() if "anomalie non prévue" in ligne]
    assert len(lignes) == 1, lignes

    # La fenêtre répond encore : `_poll` s'est réinscrit.
    app.log("toujours vivante")
    _pomper(app, tours=20)
    assert "toujours vivante" in _journal(app)


def test_une_exception_de_thread_se_voit_dans_la_fenetre(app: Any) -> None:
    """B8 — un thread hors `run_in_thread` mourait en silence (stderr invisible)."""
    fil = threading.Thread(target=lambda: [][1], name="essai-fil")
    fil.start()
    fil.join(5.0)
    _pomper(app, tours=20)

    texte = _journal(app)
    assert "tâche de fond « essai-fil »" in texte, texte[-500:]
    assert "IndexError" in texte
    assert "Traceback" not in texte


def test_les_couches_basses_parlent_dans_la_fenetre(app: Any) -> None:
    """B8 — `docia.service` / `docia.db` / DocFuse n'étaient visibles que dans le fichier."""
    import logging

    logging.getLogger("docia.service").warning("serveur LLM lent : 3 renvois")
    logging.getLogger("docia.service").info("détail interne")
    _pomper(app, tours=20)

    texte = _journal(app)
    assert "serveur LLM lent : 3 renvois" in texte, texte[-500:]
    assert "détail interne" not in texte, "INFO n'inonde pas la fenêtre"
