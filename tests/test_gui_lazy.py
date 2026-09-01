"""Patron des écrans paresseux, rappels de la fenêtre, garde-fous de campagne.

**Aucun écran n'est nécessaire** : `run_background` est remplacé par une file qu'on
vide à la main, et `DociaApp` n'est jamais construite — on n'instancie que ce que la
fonction testée touche. C'est le but du découpage : le patron qui a produit B3, B4 et
B5 se teste maintenant sans Tk, donc aussi sous une CI Windows sans session graphique.
"""

from __future__ import annotations

import queue
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

import pytest

from docia import db as db_module
from docia.config import Config, load_config
from docia.db import Database
from docia.gui.app import DOCIA, FOREIGN, NEW, DociaApp, campaign_kind
from docia.gui.lazy import LazyScreen
from docia.gui.widgets import sort_rows


# --------------------------------------------------------------------- doublures
class FakeApp:
    """Le strict nécessaire d'un `DociaApp` pour `LazyScreen` : campagne, onglet, file."""

    def __init__(self, db_path: Path) -> None:
        self._db_path = Path(db_path)
        self.tab = ""
        self.jobs: list[tuple[Any, Any]] = []
        self.noms: list[str] = []
        self.logs: list[str] = []
        self.retires: list[str] = []

    def db_path(self) -> Path:
        return self._db_path

    def current_tab(self) -> str:
        return self.tab

    def log(self, message: str) -> None:
        self.logs.append(message)

    def run_background(self, compute: Any, apply: Any, *, name: str = "calcul") -> None:
        self.noms.append(name)
        self.jobs.append((compute, apply))

    def flush(self, index: int = 0) -> None:
        """Rejoue un travail comme le vrai : `apply` n'a lieu que si `compute` réussit."""
        compute, apply = self.jobs.pop(index)
        try:
            result = compute()
        except Exception as exc:  # noqa: BLE001 — `run_background` journalise et s'arrête
            self.logs.append(f"calcul : {exc}")
            return
        apply(result)

    def off_refresh(self, _cb: Any) -> None:
        self.retires.append("refresh")

    def off_busy(self, _cb: Any) -> None:
        self.retires.append("busy")


class Ecran(LazyScreen):
    """Écran minimal : rend le chemin de la base que son calcul a réellement ouverte."""

    TAB_NAME = "Écran"

    def __init__(self, app: FakeApp, *, casse: bool = False) -> None:
        self.app = app
        self.parent = None
        self.casse = casse
        self.applique: list[str] = []
        self._lazy_setup()

    def refresh_if_needed(self) -> None:
        if not self._dirty or not self.visible():
            return

        def compute(db: Database) -> str:
            if self.casse:
                raise OSError("lecteur réseau indisponible")
            return str(db.path)

        self._start(compute, self.applique.append, name="test")


@pytest.fixture
def campagnes(tmp_path: Path) -> tuple[Path, Path]:
    a, b = tmp_path / "A.sqlite", tmp_path / "B.sqlite"
    Database(a).close()
    Database(b).close()
    return a, b


# ------------------------------------------------------------------ B3 : jetons
def test_un_calcul_perime_necrase_pas_un_calcul_recent(campagnes: tuple[Path, Path]) -> None:
    """Deux calculs lancés, appliqués dans le désordre : seul le dernier compte.

    Sans jeton, ouvrir une grosse campagne puis une autre affichait durablement les
    chiffres de la première sur la seconde.
    """
    a, b = campagnes
    app = FakeApp(a)
    app.tab = "Écran"
    ecran = Ecran(app)

    ecran.refresh()  # calcul n°1 sur A
    app._db_path = b
    ecran.refresh()  # calcul n°2 sur B
    assert len(app.jobs) == 2

    app.flush(1)  # le récent (B) arrive d'abord
    app.flush(0)  # le périmé (A) arrive ensuite : il doit être ignoré
    assert ecran.applique == [str(b)]


# ------------------------------------------------------- B4 : campagne capturée
def test_le_calcul_lit_la_campagne_de_son_lancement(campagnes: tuple[Path, Path]) -> None:
    """Le chemin est lu dans le thread Tk au lancement, pas relu dans le worker."""
    a, b = campagnes
    app = FakeApp(a)
    app.tab = "Écran"
    ecran = Ecran(app)

    ecran.refresh()  # lancé pour A
    app._db_path = b  # l'utilisateur ouvre B pendant le calcul
    app.flush()
    assert ecran.applique == [str(a)]


# ---------------------------------------------------------- B5 : échec de calcul
def test_un_calcul_en_echec_laisse_lecran_a_reessayer(campagnes: tuple[Path, Path]) -> None:
    """`_dirty` est remis dans `apply` : un échec n'immobilise pas l'écran."""
    a, _ = campagnes
    app = FakeApp(a)
    app.tab = "Écran"
    ecran = Ecran(app, casse=True)

    ecran.refresh()
    app.flush()
    assert ecran.applique == []
    assert ecran._dirty is True, "l'écran doit rester à recalculer après un échec"

    ecran.casse = False
    ecran.refresh_if_needed()  # revenir sur l'onglet suffit à réessayer
    app.flush()
    assert ecran.applique == [str(a)]
    assert ecran._dirty is False


def test_rien_ne_calcule_quand_lecran_est_invisible(campagnes: tuple[Path, Path]) -> None:
    a, _ = campagnes
    app = FakeApp(a)
    app.tab = "Autre onglet"
    ecran = Ecran(app)
    ecran.refresh()
    assert app.jobs == []
    app.tab = "Écran"
    ecran.refresh_if_needed()  # le rattrapage a bien lieu au retour sur l'onglet
    assert len(app.jobs) == 1


# ---------------------------------------------------------------- B1 : fin de vie
def test_dispose_retire_les_rappels_et_perime_les_calculs(campagnes: tuple[Path, Path]) -> None:
    a, _ = campagnes
    app = FakeApp(a)
    app.tab = "Écran"
    ecran = Ecran(app)
    ecran.refresh()
    ecran.dispose()
    app.flush()
    assert ecran.applique == [], "un écran détruit n'écrit plus dans ses widgets"
    assert app.retires == ["refresh"]


# ------------------------------------------------ B1 : rappels morts et exceptions
class _WidgetVivant:
    dernier: str = ""
    """Dernier texte posé — le bandeau d'occupation est ce que l'utilisateur voit."""

    def winfo_exists(self) -> int:
        return 1

    def configure(self, **kw: Any) -> None:
        self.dernier = str(kw.get("text", self.dernier))


class _WidgetMort:
    def winfo_exists(self) -> int:
        return 0

    def configure(self, **_kw: Any) -> None:
        raise RuntimeError("invalid command name .!ctkbutton2")


def _app_nu() -> Any:
    """Un `DociaApp` sans fenêtre : seuls les attributs que les méthodes testées lisent."""
    app = object.__new__(DociaApp)
    app._busy_listeners = []
    app._refresh_listeners = []
    app._log_queue = queue.Queue()
    app.busy_label = _WidgetVivant()
    app._worker = None
    app.cancel = threading.Event()
    app._campaign_ready = threading.Event()
    app._campaign_ready.set()
    app._touch_pending = False
    return app


def _journal(app: Any) -> list[str]:
    lignes: list[str] = []
    while not app._log_queue.empty():
        lignes.append(str(app._log_queue.get_nowait()))
    return lignes


def test_set_busy_survit_a_un_rappel_qui_leve() -> None:
    """Un rappel qui lève ne doit ni arrêter les suivants ni remonter à l'appelant.

    C'est ce qui, avant, empêchait `run_in_thread` d'atteindre `Thread.start()` : le
    travail ne partait jamais et le bandeau restait figé sur « ⏳ en cours… ».
    """
    app = _app_nu()
    mort = _WidgetMort()
    vus: list[bool] = []
    app.on_busy(mort.configure, owner=None)  # rappel sans propriétaire identifiable
    app.on_busy(vus.append)

    app._set_busy(True, "analyse")

    assert vus == [True], "les rappels suivants doivent quand même être appelés"
    assert any("état occupé" in ligne for ligne in _journal(app))


def test_un_rappel_dont_le_widget_est_detruit_est_ecarte() -> None:
    """Filet de sécurité : le rappel disparaît avec son widget, sans être appelé."""
    app = _app_nu()
    mort = _WidgetMort()
    appels: list[bool] = []
    app.on_busy(appels.append, owner=mort)
    app.on_busy(appels.append, owner=_WidgetVivant())

    app._set_busy(False)

    assert appels == [False], "seul le rappel vivant est appelé"
    assert len(app._busy_listeners) == 1, "le rappel mort est retiré de la liste"


def test_set_busy_survit_a_un_bandeau_detruit() -> None:
    """La fenêtre se ferme pendant un travail : `_poll` doit continuer sa boucle."""
    app = _app_nu()
    app.busy_label = _WidgetMort()
    app._set_busy(False)  # ne doit pas lever


def test_off_busy_et_off_refresh_retirent_le_rappel() -> None:
    app = _app_nu()
    app._refresh_campaign_header = lambda: None
    appels: list[str] = []

    def busy(_b: bool) -> None:
        appels.append("busy")

    def refresh() -> None:
        appels.append("refresh")

    app.on_busy(busy)
    app.on_refresh(refresh)
    app.off_busy(busy)
    app.off_refresh(refresh)
    app._set_busy(True)
    app.refresh_all()
    assert appels == []
    assert (app._busy_listeners, app._refresh_listeners) == ([], [])


# ------------------------------------------------------------------ B6 : tri stable
def test_le_tri_emporte_les_identites() -> None:
    """Trier réordonne lignes, couleurs **et** identités ensemble.

    Sans cela, cliquer une ligne triée ouvrait la fiche d'un autre fichier — et
    « Valider » enregistrait la vérification humaine dessus.
    """
    rows = [["b.txt", "z"], ["a.txt", "y"], ["c.txt", "x"]]
    tags = ["C3", "C2", "ok"]
    keys = [11, 22, 33]

    tri, tri_tags, tri_keys = sort_rows(rows, tags, keys, 0)
    assert [r[0] for r in tri] == ["a.txt", "b.txt", "c.txt"]
    assert tri_tags == ["C2", "C3", "ok"]
    assert tri_keys == [22, 11, 33]

    desc, _, desc_keys = sort_rows(rows, tags, keys, 1, desc=True)
    assert [r[0] for r in desc] == ["b.txt", "a.txt", "c.txt"]
    assert desc_keys == [11, 22, 33]


def test_le_tri_numerique_ne_trie_pas_comme_du_texte() -> None:
    rows = [["512"], ["1 024"], ["64"]]
    tri, _, keys = sort_rows(rows, ["ok"] * 3, ["a", "b", "c"], 0)
    assert [r[0] for r in tri] == ["64", "512", "1 024"]
    assert keys == ["c", "a", "b"]


# --------------------------------------------- B8 : refus d'un SQLite étranger
def test_campaign_kind_distingue_neuve_docia_et_etrangere(tmp_path: Path) -> None:
    assert campaign_kind(tmp_path / "absente.sqlite") == NEW

    vide = tmp_path / "vide.sqlite"
    vide.touch()
    assert campaign_kind(vide) == NEW

    camp = tmp_path / "camp.sqlite"
    Database(camp).close()
    assert campaign_kind(camp) == DOCIA

    contacts = tmp_path / "contacts.sqlite"
    con = sqlite3.connect(contacts)
    con.execute("CREATE TABLE contacts(nom TEXT)")
    con.execute("INSERT INTO contacts VALUES ('Dupont')")
    con.commit()
    con.close()
    assert campaign_kind(contacts) == FOREIGN

    texte = tmp_path / "notes.sqlite"
    texte.write_text("ceci n'est pas une base de données", encoding="utf-8")
    assert campaign_kind(texte) == FOREIGN


def test_create_campaign_refuse_un_sqlite_etranger(tmp_path: Path) -> None:
    """Une base d'un autre logiciel ne doit pas recevoir les tables docia."""
    contacts = tmp_path / "contacts.sqlite"
    con = sqlite3.connect(contacts)
    con.execute("CREATE TABLE contacts(nom TEXT)")
    con.commit()
    con.close()
    avant = contacts.stat().st_size

    app = _app_nu()
    app.config = Config()
    app._db_path = str(tmp_path / "courante.sqlite")
    ouvertes: list[str] = []
    app.open_campaign = lambda p, **_kw: ouvertes.append(p)

    assert app.create_campaign(str(contacts)) is False
    assert ouvertes == [], "la campagne courante ne change pas"
    tables = [
        str(r[0])
        for r in sqlite3.connect(contacts).execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    ]
    assert tables == ["contacts"]
    assert contacts.stat().st_size == avant
    assert any("n'est pas une campagne Doc-IA" in ligne for ligne in _journal(app))


def test_create_campaign_refuse_un_nom_vide(tmp_path: Path) -> None:
    app = _app_nu()
    app.config = Config()
    app._db_path = str(tmp_path / "courante.sqlite")
    assert app.create_campaign("   ") is False


def test_create_campaign_cree_puis_ouvre_dans_le_bon_ordre(tmp_path: Path) -> None:
    """« campagne créée » avant « campagne : … », et une seule ouverture de la base."""
    cible = tmp_path / "neuve.sqlite"
    app = _app_nu()
    app.config = Config()
    app._db_path = str(tmp_path / "courante.sqlite")
    ouvertes: list[tuple[str, bool]] = []

    def open_campaign(path: str, *, touch: bool = True) -> None:
        ouvertes.append((path, touch))
        app.log(f"campagne : {path}")

    app.open_campaign = open_campaign

    assert app.create_campaign(str(cible)) is True
    assert cible.exists()
    assert ouvertes == [(str(cible), False)], "pas de seconde ouverture par `_touch_campaign`"
    lignes = _journal(app)
    assert lignes[0].startswith("campagne créée")
    assert lignes[1].startswith("campagne : ")

    # rouvrir la même campagne ne l'écrase pas et le dit
    app.create_campaign(str(cible))
    assert any("aucune donnée effacée" in ligne for ligne in _journal(app))


# --------------------------------- D3 : un seul `campaign_kind`, celui de `docia.db`
def test_la_fenetre_delegue_campaign_kind_a_docia_db() -> None:
    """Deux copies d'un contrôle qui décide si on ouvre le fichier de quelqu'un divergent.

    La fenêtre a découvert le danger (une base « contacts » enrichie de douze tables
    docia) ; `docia.db` en porte la version que la CLI utilise aussi. Ce test interdit
    de recréer la seconde copie : la fenêtre **délègue**, elle ne transpose pas.
    """
    assert campaign_kind is db_module.campaign_kind
    assert (NEW, DOCIA, FOREIGN) == (
        db_module.CAMPAIGN_NEW,
        db_module.CAMPAIGN_DOCIA,
        db_module.CAMPAIGN_FOREIGN,
    )


# ------------------------ D2 : la migration de schéma ne se joue plus dans le thread Tk
class _DatabaseLente:
    """Doublure de `Database` qui retient l'ouverture — ce que fait une migration."""

    def __init__(self, verrou: threading.Event) -> None:
        self.verrou = verrou
        self.threads: list[str] = []

    def __call__(self, chemin: Any) -> Any:
        self.threads.append(threading.current_thread().name)
        self.verrou.wait(5)
        return db_module.Database(chemin)


def _app_campagne(tmp_path: Path) -> tuple[Any, Path]:
    base = tmp_path / "camp.sqlite"
    Database(base).close()
    app = _app_nu()
    app.config = Config()
    app._db_path = str(base)
    app._refresh_campaign_header = lambda: None
    return app, base


def test_ouvrir_une_campagne_a_migrer_ne_fige_pas_le_thread_tk(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """10,0 s de fenêtre figée mesurées sur une campagne de 817 Mo à migrer.

    L'ouverture — donc la migration de schéma et sa sauvegarde préalable — était jouée
    dans le thread Tk : plus de redessin, plus de sablier, plus de journal, pendant toute
    la durée. Elle part désormais dans un thread, et `_touch_campaign` rend la main tout
    de suite en affichant « ⏳ ouverture de la campagne en cours… ».
    """
    from docia.gui import app as app_module

    app, _ = _app_campagne(tmp_path)
    verrou = threading.Event()
    lente = _DatabaseLente(verrou)
    monkeypatch.setattr(app_module, "Database", lente)

    depart = time.perf_counter()
    demarre = app._touch_campaign()
    rendu = time.perf_counter() - depart

    assert rendu < 0.5, f"le thread appelant est resté bloqué {rendu:.2f} s"
    assert demarre is True, "un travail d'ouverture doit avoir démarré"
    assert app.busy_label.dernier == "⏳ ouverture de la campagne en cours…"
    verrou.set()
    app._worker.join(10)
    assert lente.threads != []
    assert lente.threads[0] != threading.current_thread().name, "ouverte hors du thread Tk"


def test_aucun_calcul_de_fond_ne_touche_la_base_pendant_la_migration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Deux threads découvrant en même temps une base à migrer : `cannot rollback`.

    Le correctif d'alors ouvrait la base dans le thread Tk, avant tout calcul. Puisque
    l'ouverture est maintenant en tâche de fond, c'est `_campaign_ready` qui tient la
    garantie : un calcul d'écran (`run_background`, qui ouvre la base lui-même) attend la
    fin de l'ouverture — dans **son** thread, jamais dans celui de Tk.
    """
    from docia.gui import app as app_module

    app, _ = _app_campagne(tmp_path)
    verrou = threading.Event()
    monkeypatch.setattr(app_module, "Database", _DatabaseLente(verrou))
    ordre: list[str] = []

    assert app._touch_campaign() is True
    app.run_background(lambda: ordre.append("calcul"), lambda _r: None, name="stats")

    time.sleep(0.2)
    assert ordre == [], "le calcul ne doit pas ouvrir la base pendant la migration"
    verrou.set()
    app._worker.join(10)
    for _ in range(100):
        if ordre:
            break
        time.sleep(0.02)
    assert ordre == ["calcul"], "le calcul reprend dès l'ouverture terminée"


def test_une_campagne_ouverte_pendant_un_travail_est_migree_a_la_fin(tmp_path: Path) -> None:
    """La place unique de travail est prise : l'ouverture est **reprise**, pas perdue.

    Sans reprise, ouvrir une campagne pendant une analyse laissait une base non migrée
    aux calculs d'écran — exactement la course qu'on cherche à interdire.
    """
    from docia.gui import app as app_module

    app, base = _app_campagne(tmp_path)
    occupe = threading.Event()
    app._worker = threading.Thread(target=lambda: occupe.wait(5), daemon=True)
    app._worker.start()

    assert app._touch_campaign() is False, "aucune place libre"
    assert app._touch_pending is True
    assert app._campaign_ready.is_set(), "sinon plus aucun calcul ne lirait la base"

    occupe.set()
    app._worker.join(5)
    app._worker = None
    _journal(app)  # « une opération est déjà en cours » : lu, la file ne garde que _DONE
    app._log_queue.put(app_module._DONE)
    app._drain()  # ce que fait `_poll` à la fin d'un travail

    assert app._touch_pending is False
    assert app._worker is not None
    assert app._worker.name.endswith("ouverture de la campagne")
    app._worker.join(10)
    assert base.exists()


# ------------------------------- D1 : « Enregistrer » ne mange plus les commentaires
def test_save_config_preserve_les_commentaires_de_docia_toml(tmp_path: Path) -> None:
    """Le bouton « Enregistrer » de l'onglet Serveur & performances, bout en bout."""
    from docia.config import default_toml

    cible = tmp_path / "docia.toml"
    cible.write_text(default_toml(), encoding="utf-8")
    app = _app_nu()
    app.config = load_config(cible)
    app.config_path = cible
    app._db_path = str(tmp_path / "camp.sqlite")
    app.tab_objects = {}
    app.config.llm.max_in_flight = 16

    app.save_config()

    ecrit = cible.read_text(encoding="utf-8")
    assert "TEXTE INTÉGRAL des documents" in ecrit, "l'avertissement RSSI doit survivre"
    assert ecrit.count("#") == default_toml().count("#")
    assert load_config(cible).llm.max_in_flight == 16
    assert any("config enregistrée" in ligne for ligne in _journal(app))


def test_save_config_part_du_gabarit_commente_si_le_fichier_manque(tmp_path: Path) -> None:
    """Un `docia.toml` créé par la fenêtre naît commenté, avertissement RSSI compris."""
    cible = tmp_path / "docia.toml"
    app = _app_nu()
    app.config = Config()
    app.config_path = cible
    app._db_path = str(tmp_path / "camp.sqlite")
    app.tab_objects = {}

    app.save_config()

    ecrit = cible.read_text(encoding="utf-8")
    assert "TEXTE INTÉGRAL des documents" in ecrit
    assert load_config(cible).db_path == str(tmp_path / "camp.sqlite")
