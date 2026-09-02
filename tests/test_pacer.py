"""Alimentation adaptative (`llm/pacer.py`) : régulateur pur, testé à froid."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from docia.llm.pacer import DOWN, STRAIN, UP, WINDOW, Pacer, PacerMemory


class Horloge:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def serveur_simule(
    pacer: Pacer, horloge: Horloge, debit: float, *, blocs: int = WINDOW, tokens: int = 10_000
) -> None:
    """`blocs` blocs rendus par un serveur qui tient `debit` tokens/s au budget courant."""
    for _ in range(blocs):
        horloge.now += tokens / debit
        pacer.release(tokens, ok=True)


def debit_en_cloche(budget: int, optimum: int = 300_000, pic: float = 3_000.0) -> float:
    """Débit d'un serveur : croît avec les tokens en vol jusqu'à `optimum`, puis
    s'effondre (préemptions du cache KV)."""
    if budget <= optimum:
        return pic * (0.3 + 0.7 * budget / optimum)
    return pic * max(0.2, 1 - 1.5 * (budget - optimum) / optimum)


def test_montee_tant_que_le_debit_suit_puis_palier_sous_l_optimum() -> None:
    horloge = Horloge()
    pacer = Pacer(budget_tokens=50_000, min_tokens=20_000, max_tokens=2_000_000, clock=horloge)
    trajet: list[int] = []
    for _ in range(25):
        serveur_simule(pacer, horloge, debit_en_cloche(pacer.budget))
        trajet.append(pacer.budget)
    assert trajet[0] == int(50_000 * UP)  # premier relevé : montée
    assert max(trajet) < 2_000_000  # jamais parti au plafond : le débit a cessé de suivre
    assert 200_000 <= pacer.budget <= 340_000, trajet  # au coude (optimum 300 K)
    assert pacer.stats().throughput_tok_s is not None
    assert pacer.decisions == 25


def test_les_sondes_s_espacent_sur_un_serveur_stable() -> None:
    """Chaque retour au cran d'avant double la patience : un serveur qui ne change
    pas est sondé de moins en moins souvent (3, 6, 12… fenêtres)."""
    horloge = Horloge()
    dits: list[str] = []
    pacer = Pacer(
        budget_tokens=200_000,
        min_tokens=20_000,
        max_tokens=2_000_000,
        clock=horloge,
        on_decision=dits.append,
    )
    sondes: list[int] = []
    for i in range(60):
        avant = len(dits)
        serveur_simule(pacer, horloge, debit_en_cloche(pacer.budget))
        if any("sonde" in d for d in dits[avant:]):
            sondes.append(i)
    ecarts = [b - a for a, b in zip(sondes, sondes[1:], strict=False)]
    assert len(ecarts) >= 2, sondes
    assert ecarts[-1] > ecarts[0], ecarts


def test_serveur_qui_ralentit_descente_puis_palier() -> None:
    horloge = Horloge()
    pacer = Pacer(budget_tokens=200_000, min_tokens=20_000, max_tokens=1_000_000, clock=horloge)
    for _ in range(6):  # s'installe
        serveur_simule(pacer, horloge, 3_000.0)
    installe = pacer.budget
    serveur_simule(pacer, horloge, 1_200.0)  # un autre usage prend le GPU : -60 %
    serveur_simule(pacer, horloge, 1_200.0)
    assert pacer.budget < installe
    stable = pacer.budget
    for _ in range(3):  # débit stable à ce niveau : palier, on ne s'effondre pas
        serveur_simule(pacer, horloge, 1_200.0)
    assert pacer.budget >= int(stable * DOWN)
    assert pacer.budget >= 20_000


def test_detresse_divise_le_budget_sans_attendre_la_fenetre() -> None:
    horloge = Horloge()
    dits: list[str] = []
    pacer = Pacer(
        budget_tokens=100_000,
        min_tokens=30_000,
        max_tokens=500_000,
        clock=horloge,
        on_decision=dits.append,
    )
    pacer.release(10_000, ok=False, strain=True)
    assert pacer.budget == int(100_000 * STRAIN)
    assert any("détresse" in d for d in dits)
    pacer.distress("préemptions")
    pacer.distress("préemptions")
    assert pacer.budget == 30_000  # jamais sous le plancher


def test_un_bloc_en_erreur_ne_compte_pas_dans_le_debit() -> None:
    horloge = Horloge()
    pacer = Pacer(budget_tokens=100_000, min_tokens=30_000, max_tokens=500_000, clock=horloge)
    for _ in range(WINDOW):
        pacer.release(10_000, ok=False)
    assert pacer.decisions == 0


def test_acquire_attend_la_place_et_laisse_passer_un_bloc_trop_gros() -> None:
    async def scenario() -> list[str]:
        pacer = Pacer(budget_tokens=100, min_tokens=100, max_tokens=100)
        journal: list[str] = []

        async def bloc(nom: str, tokens: int, duree: float) -> None:
            await pacer.acquire(tokens)
            journal.append(f"{nom} part")
            await asyncio.sleep(duree)
            journal.append(f"{nom} revient")
            pacer.release(tokens, ok=True)

        # a et b tiennent ensemble (60 + 40) ; c (60) attend qu'un des deux revienne ;
        # d (500 > budget) ne part que lorsque plus rien n'est en vol.
        await asyncio.gather(
            bloc("a", 60, 0.05), bloc("b", 40, 0.02), bloc("c", 60, 0.01), bloc("d", 500, 0.01)
        )
        return journal

    journal = asyncio.run(scenario())
    assert journal[:2] == ["a part", "b part"]
    assert journal.index("c part") > journal.index("b revient")
    assert journal.index("d part") > journal.index("a revient")
    assert journal.index("d part") > journal.index("c revient")


def test_memoire_par_serveur_et_modele(tmp_path: Path) -> None:
    memoire = PacerMemory(tmp_path / "pacer.json")
    cle = PacerMemory.key("http://srv:8000/v1/", "qwen38")
    assert memoire.load(cle) is None
    memoire.save(cle, 240_000)
    memoire.save(PacerMemory.key("http://autre:8000/v1", "qwen38"), 90_000)
    assert PacerMemory(tmp_path / "pacer.json").load(cle) == 240_000
    (tmp_path / "pacer.json").write_text("{corrompu", encoding="utf-8")
    assert memoire.load(cle) is None
    memoire.save(cle, 100_000)  # réécrit un fichier sain
    assert memoire.load(cle) == 100_000


@pytest.mark.parametrize("depart", [1, 10_000_000])
def test_le_budget_de_depart_est_borne(depart: int) -> None:
    pacer = Pacer(budget_tokens=depart, min_tokens=20_000, max_tokens=800_000)
    assert 20_000 <= pacer.budget <= 800_000
