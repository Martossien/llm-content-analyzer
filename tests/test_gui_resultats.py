"""Écran Résultats **sans écran** : filtres descendus en SQL, validation ciblée.

`ResultsTab` ne construit ses widgets que dans `build()` ; tout le reste — choix des
filtres, rendu d'une ligne, réécriture après validation — ne demande que des objets
qui répondent à `configure`, `get` et `set`. On les remplace donc par des doublures
et on teste l'écran comme une fonction, y compris sous une CI sans session graphique.

Ce qui est vérifié ici :

* les filtres, le tri et la limite passent bien en SQL (l'écran relisait les 934 028
  lignes d'une campagne pour en afficher 1 000) ;
* une validation ne réécrit **que** sa ligne — `_save` appelait `refresh()`, donc un
  relecteur qui validait cent fichiers relisait cent fois toute la campagne ;
* elle recharge quand même quand la ligne sort du filtre de vérification affiché.
"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from docia.db import Database
from docia.gui.service_shim import GuiService
from docia.gui.tab_results import ResultsTab, _display_order, _row_cells
from docia.models import DomainAnalysis, FileAnalysis, FileStatus, SmbeagleRow


# --------------------------------------------------------------------- doublures
class Widget:
    """Étiquette / pastille : retient le dernier texte qu'on lui a donné."""

    def __init__(self) -> None:
        self.text = ""

    def configure(self, **kwargs: Any) -> None:
        self.text = str(kwargs.get("text", self.text))

    def set(self, text: str, _kind: object = None) -> None:
        self.text = text


class Var:
    """`ctk.StringVar` : une valeur, rien de plus."""

    def __init__(self, value: str = "") -> None:
        self.value = value

    def get(self) -> str:
        return self.value

    def set(self, value: str) -> None:
        self.value = value


class Tree:
    """Le `Treeview` : on n'a besoin que de savoir quelles cases ont été réécrites."""

    def __init__(self) -> None:
        self.ecrits: list[tuple[str, list[str], tuple[str, ...]]] = []

    def item(self, iid: str, *, values: list[str], tags: tuple[str, ...]) -> None:
        self.ecrits.append((iid, values, tags))


class FakeTable:
    """`widgets.Table` réduit à ce que l'écran manipule : lignes, couleurs, identités."""

    def __init__(self) -> None:
        self.rows: list[list[str]] = []
        self.row_tags: list[str] = []
        self.keys: list[Any] = []
        self.tree = Tree()
        self.rendus = 0

    def set_rows(
        self,
        rows: list[list[str]],
        tags: list[str] | None = None,
        keys: list[Any] | None = None,
    ) -> None:
        self.rows = [list(r) for r in rows]
        self.row_tags = list(tags or ["ok"] * len(rows))
        self.keys = list(keys if keys is not None else range(len(rows)))
        self.rendus += 1


class FakeApp:
    """Le strict nécessaire d'un `DociaApp` : campagne, onglet courant, file de travaux."""

    def __init__(self, db_path: Path) -> None:
        self._db_path = Path(db_path)
        self.tab = "Résultats"
        self.jobs: list[tuple[Any, Any]] = []
        self.noms: list[str] = []
        self.logs: list[str] = []
        self.service = GuiService(lambda: Database(self._db_path))

    def db_path(self) -> Path:
        return self._db_path

    def current_tab(self) -> str:
        return self.tab

    def log(self, message: str) -> None:
        self.logs.append(message)

    def run_background(self, compute: Any, apply: Any, *, name: str = "calcul") -> None:
        self.jobs.append((compute, apply))
        self.noms.append(name)

    def flush(self) -> None:
        """Rejoue tous les travaux en attente, comme la boucle Tk le ferait."""
        while self.jobs:
            compute, apply = self.jobs.pop(0)
            apply(compute())

    def on_refresh(self, _cb: Any) -> None:
        pass

    def off_refresh(self, _cb: Any) -> None:
        pass


def _ecran(path: Path) -> tuple[ResultsTab, FakeApp]:
    """`ResultsTab` monté sur des doublures, sans passer par `build()` (donc sans Tk)."""
    app = FakeApp(path)
    tab = ResultsTab.__new__(ResultsTab)
    tab.app = app  # type: ignore[attr-defined]
    tab.parent = None  # type: ignore[attr-defined]
    tab._rows = []
    tab._selected_id = None
    tab._lazy_setup()
    tab.table = FakeTable()  # type: ignore[attr-defined]
    tab.count_label = Widget()  # type: ignore[attr-defined]
    tab.path_label = Widget()  # type: ignore[attr-defined]
    tab.meta_label = Widget()  # type: ignore[attr-defined]
    tab.detail_label = Widget()  # type: ignore[attr-defined]
    tab.sec_badge = Widget()  # type: ignore[attr-defined]
    tab.rgpd_badge = Widget()  # type: ignore[attr-defined]
    tab.ret_badge = Widget()  # type: ignore[attr-defined]
    tab.review_badge = Widget()  # type: ignore[attr-defined]
    carte = Widget()
    carte.subtitle = Widget()  # type: ignore[attr-defined]
    tab.card = carte  # type: ignore[attr-defined]
    tab.sec_var = Var("(tous)")  # type: ignore[attr-defined]
    tab.rgpd_var = Var("(tous)")  # type: ignore[attr-defined]
    tab.review_var = Var("(tous)")  # type: ignore[attr-defined]
    tab.search_var = Var("")  # type: ignore[attr-defined]
    tab.reviewer_var = Var("AB")  # type: ignore[attr-defined]
    tab.comment_var = Var("")  # type: ignore[attr-defined]
    tab.corr_sec_var = Var("")  # type: ignore[attr-defined]
    tab.corr_rgpd_var = Var("")  # type: ignore[attr-defined]
    return tab, app


# --------------------------------------------------------------------- campagne
def _smbeagle(name: str, ordre: int) -> SmbeagleRow:
    return SmbeagleRow(
        name=name,
        host="srv",
        extension=name.rsplit(".", 1)[-1],
        username="u",
        hostname="srv.dom",
        unc_directory="\\\\srv\\part\\dossier",
        creation_time="01/01/2025 10:00:00",
        last_write_time="01/01/2026 10:00:00",
        readable=True,
        writeable=False,
        deletable=False,
        directory_type="SMB",
        base="\\\\srv\\part\\",
        file_size=1000 + ordre,
        access_time="02/01/2026 10:00:00",
        file_attributes="Archive",
        owner="DOM\\dupont",
        fast_hash=f"h{ordre}",
        file_signature="unknown",
    )


_MODELE = FileAnalysis(
    file_ref="",
    resume="résumé",
    security=DomainAnalysis("C1", 80, {"justification": "j"}),
    rgpd=DomainAnalysis("low", 70, {"data_types": ["identite"]}),
    finance=DomainAnalysis("none", 90, {"amounts": []}),
    legal=DomainAnalysis("none", 90, {"parties": []}),
    raw={},
)

_CAMPAGNE = (
    ("secret.txt", "C3", "critical"),
    ("confidentiel.txt", "C2", "high"),
    ("interne.txt", "C1", "low"),
    ("public.txt", "C0", "none"),
)


@pytest.fixture
def campagne(tmp_path: Path) -> Path:
    """Quatre fichiers analysés de chaque gravité, un cinquième en erreur, un exclu."""
    path = tmp_path / "docia.sqlite"
    with Database(path) as db:
        scan = db.start_scan("a.csv")
        noms = [n for n, _s, _r in _CAMPAGNE] + ["casse.txt", "exclu.zip"]
        db.upsert_files([_smbeagle(n, i) for i, n in enumerate(noms)], scan)
        par_nom = {f.name: f for f in db.iter_files()}
        for nom, sec, risque in _CAMPAGNE:
            db.store_analysis(
                par_nom[nom].id,
                None,
                1,
                prompt_hash="p",
                model="m",
                analysis=replace(
                    _MODELE,
                    file_ref=nom,
                    resume=f"résumé de {nom}",
                    security=DomainAnalysis(sec, 80, {"justification": "j"}),
                    rgpd=DomainAnalysis(risque, 70, {"data_types": []}),
                ),
            )
        db.set_file_status(par_nom["casse.txt"].id, FileStatus.ERROR, "boum")
        db.set_file_status(par_nom["exclu.zip"].id, FileStatus.EXCLUDED, "extension exclue")
    return path


def _charge(tab: ResultsTab, app: FakeApp) -> None:
    tab.refresh()
    app.flush()


def _noms_affiches(tab: ResultsTab) -> list[str]:
    return [r[0] for r in tab.table.rows]


# ------------------------------------------------------------------------ tests
def test_l_ecran_affiche_la_campagne_triee_par_gravite(campagne: Path) -> None:
    tab, app = _ecran(campagne)
    _charge(tab, app)
    assert _noms_affiches(tab) == [
        "secret.txt",
        "confidentiel.txt",
        "interne.txt",
        "public.txt",
        "casse.txt",
        "exclu.zip",
    ]
    assert tab.table.row_tags[:4] == ["C3", "C2", "C1", "ok"]
    assert tab.table.keys == [int(r["id"]) for r in tab._rows]
    assert tab.count_label.text == "6 fichier(s)"


@pytest.mark.parametrize(
    ("champ", "valeur", "attendu"),
    [
        ("sec_var", "C3 secret", ["secret.txt"]),
        ("sec_var", "C0 public", ["public.txt"]),
        ("rgpd_var", "critique", ["secret.txt"]),
        ("rgpd_var", "faible", ["interne.txt"]),
        ("review_var", "non vérifié", None),  # tous : personne n'a encore vérifié
        ("review_var", "validé", []),
        ("search_var", "secret", ["secret.txt"]),
        ("search_var", "DUPONT", None),  # propriétaire, casse ASCII repliée
        ("search_var", "introuvable", []),
    ],
)
def test_chaque_filtre_est_applique_en_sql(
    campagne: Path, champ: str, valeur: str, attendu: list[str] | None
) -> None:
    """Les six lignes de la campagne : celles que le filtre retient, ni plus ni moins."""
    tab, app = _ecran(campagne)
    getattr(tab, champ).set(valeur)
    _charge(tab, app)
    tous = ["secret.txt", "confidentiel.txt", "interne.txt", "public.txt", "casse.txt", "exclu.zip"]
    assert _noms_affiches(tab) == (tous if attendu is None else attendu)
    assert tab.count_label.text.startswith(
        f"{len(tous if attendu is None else attendu)} fichier(s)"
    )


def test_valider_ne_recharge_que_la_ligne_concernee(campagne: Path) -> None:
    """Le point de la correction : cent validations ne relisent plus cent fois la campagne."""
    tab, app = _ecran(campagne)
    _charge(tab, app)
    rendus_avant = tab.table.rendus
    avant = [list(r) for r in tab.table.rows]

    tab._select(tab.table.keys[1])  # « confidentiel.txt »
    tab._validate()

    assert app.jobs == [], "aucun rechargement de la campagne n'a été demandé"
    assert tab.table.rendus == rendus_avant, "le tableau n'a pas été reconstruit"
    assert tab.table.tree.ecrits == [("1", tab.table.rows[1], ("C2",))]
    assert tab.table.rows[1][7] == "validé"
    assert [r for i, r in enumerate(tab.table.rows) if i != 1] == [
        r for i, r in enumerate(avant) if i != 1
    ], "les autres lignes n'ont pas bougé"
    assert tab._rows[1]["revue"] == "validated"
    assert tab.review_badge.text == "validé"
    assert tab.count_label.text == "6 fichier(s)", "le total ne change pas"

    with Database(campagne) as db:
        assert db.review_counts()["validated"] == 1


def test_corriger_met_a_jour_la_ligne_et_la_fiche(campagne: Path) -> None:
    tab, app = _ecran(campagne)
    _charge(tab, app)
    tab._select(tab.table.keys[0])
    tab.corr_sec_var.set("C1")
    tab.comment_var.set("plutôt interne")
    tab._save_correction()

    assert app.jobs == []
    assert tab.table.rows[0][7] == "corrigé"
    assert "plutôt interne" in tab.detail_label.text
    with Database(campagne) as db:
        fiche = next(iter(db.latest_analyses(file_id=int(tab.table.keys[0]))))
        assert (fiche["review_status"], fiche["corrected_security"]) == ("corrected", "C1")
        assert fiche["reviewer"] == "AB"


def test_valider_recharge_quand_la_ligne_sort_du_filtre_de_verification(campagne: Path) -> None:
    """Sous « non vérifié », une ligne validée n'a plus sa place : là, on relit."""
    tab, app = _ecran(campagne)
    tab.review_var.set("non vérifié")
    _charge(tab, app)
    assert len(tab.table.rows) == 6

    tab._select(tab.table.keys[0])
    tab._validate()
    assert len(app.jobs) == 1, "un rechargement doit être demandé"
    app.flush()
    assert len(tab.table.rows) == 5
    assert "secret.txt" not in _noms_affiches(tab)
    assert tab.count_label.text == "5 fichier(s)"


def test_valider_sans_selection_ne_fait_rien(campagne: Path) -> None:
    tab, app = _ecran(campagne)
    _charge(tab, app)
    tab._validate()
    assert app.logs[-1] == "sélectionne un fichier dans le tableau"
    assert app.jobs == []
    with Database(campagne) as db:
        assert db.review_counts()["validated"] == 0


def test_une_campagne_absente_vide_l_ecran(tmp_path: Path) -> None:
    tab, app = _ecran(tmp_path / "jamais_creee.sqlite")
    tab.refresh()
    assert app.jobs == []
    assert tab.table.rows == []
    assert tab.count_label.text == ""


def test_row_cells_et_display_order_restent_d_accord(campagne: Path) -> None:
    """Une seule définition du rendu d'une ligne, un seul ordre : les deux se recoupent."""
    with Database(campagne) as db:
        lignes = sorted(db.latest_analyses(display_order=True), key=_display_order)
    from docia.gui.helpers import result_rows_v31

    for ligne, brute in zip(result_rows_v31(lignes, limit=1000), lignes, strict=True):
        cells, tag = _row_cells(ligne)
        assert cells[0] == str(brute["name"])
        assert tag in ("C3", "C2", "C1", "error", "ok")
        assert len(cells) == 9
