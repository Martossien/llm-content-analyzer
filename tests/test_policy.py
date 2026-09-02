"""Politique de découpage (`blocks/policy.py`) et compteur exact du serveur."""

from __future__ import annotations

from docia.blocks.policy import MIN_PIECE_BUDGET, PIECE_SAFETY, SegmentPolicy, plan_file
from docia.config import LLMConfig
from docia.llm.server import ServerTokenCounter, tokenize_url
from tests.fake_openai import FakeOpenAIServer


def test_sans_compteur_le_plafond_estime_fait_foi() -> None:
    plan = plan_file("texte", estimated=9_000, cap_estimated=5_000, policy=None)
    assert (plan.piece_budget, plan.exact_tokens, plan.whole) == (5_000, None, False)
    plan = plan_file("texte", 9_000, 5_000, SegmentPolicy(cap_exact=7_000))
    assert plan.piece_budget == 5_000


def test_serveur_muet_meme_chose() -> None:
    plan = plan_file("texte", 9_000, 5_000, SegmentPolicy(7_000, lambda _t: None))
    assert (plan.piece_budget, plan.exact_tokens) == (5_000, None)
    assert "muet" in plan.reason


def test_compte_exact_sous_le_plafond_fichier_entier() -> None:
    plan = plan_file("texte", 9_000, 5_000, SegmentPolicy(7_000, lambda _t: 6_500))
    assert (plan.whole, plan.exact_tokens) == (True, 6_500)
    assert "envoyé entier" in plan.reason


def test_compte_exact_au_dessus_segments_calibres() -> None:
    # Estimation 9 000 pour 12 000 réels : rapport 0,75 → chaque segment vise
    # 7 000 × 0,75 × 0,9 tokens estimés, soit ≈ 6 300 réels sous le plafond.
    plan = plan_file("texte", 9_000, 5_000, SegmentPolicy(7_000, lambda _t: 12_000))
    assert plan.piece_budget == int(7_000 * 0.75 * PIECE_SAFETY)
    assert (plan.exact_tokens, plan.whole) == (12_000, False)


def test_budget_par_segment_ne_descend_pas_sous_le_plancher() -> None:
    plan = plan_file("texte", 100, 10, SegmentPolicy(50, lambda _t: 100_000))
    assert plan.piece_budget == MIN_PIECE_BUDGET


def test_tokenize_url_hors_prefixe_v1() -> None:
    assert tokenize_url(LLMConfig(base_url="http://h:8000/v1/")) == "http://h:8000/tokenize"
    assert tokenize_url(LLMConfig(base_url="http://h:8000")) == "http://h:8000/tokenize"
    assert tokenize_url(LLMConfig(transport="openwebui")) is None


def test_compteur_synchrone_compte_avec_le_serveur(fake_server: FakeOpenAIServer) -> None:
    fake_server.tokens_per_char = 0.5
    with ServerTokenCounter(LLMConfig(base_url=fake_server.base_url_vllm)) as counter:
        assert counter.available
        assert counter("a" * 100) == 50
        assert counter("a" * 100) == 50
    assert fake_server.tokenize_calls == 2


def test_compteur_muet_sur_transport_openwebui_ou_endpoint_absent(
    fake_server: FakeOpenAIServer,
) -> None:
    assert not ServerTokenCounter(LLMConfig(transport="openwebui")).available
    # Un serveur qui répond 404 : on le retient, plus aucun appel ensuite.
    with ServerTokenCounter(LLMConfig(base_url=fake_server.base_url_vllm + "/nope")) as counter:
        assert counter("texte") is None
        assert not counter.available
        assert counter("texte") is None
    assert fake_server.tokenize_calls == 0


def test_compteur_reste_disponible_apres_une_erreur_reseau() -> None:
    with ServerTokenCounter(LLMConfig(base_url="http://127.0.0.1:1/v1")) as counter:
        assert counter("texte") is None
        assert counter.available  # panne passagère : on réessaiera au fichier suivant
