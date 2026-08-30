"""Tests de la validation et de la corrélation des réponses de bloc."""

from __future__ import annotations

import json
from typing import Any

import pytest

from docia.llm.parse import ParseError, parse_block_response
from docia.models import BlockFile
from tests.fake_openai import build_content, make_entry

REFS = ["dossier/rapport.md", "dossier/contrat.txt", "autre/note.txt"]


def block_files(refs: list[str] | None = None) -> list[BlockFile]:
    refs = refs if refs is not None else REFS
    return [BlockFile(file_id=i + 1, file_ref=ref, content_version=1) for i, ref in enumerate(refs)]


def wrap(*entries: dict[str, Any]) -> str:
    return json.dumps({"files": list(entries)}, ensure_ascii=False)


def test_reponse_complete() -> None:
    parsed = parse_block_response(build_content(REFS, "ok"), block_files())
    assert set(parsed.analyses) == {1, 2, 3}
    assert parsed.missing == []
    assert parsed.unknown_refs == []
    assert parsed.invalid == []

    analysis = parsed.analyses[1]
    assert analysis.file_ref == "dossier/rapport.md"
    assert analysis.resume == "Résumé de dossier/rapport.md"
    assert analysis.security.label == "C1"
    assert analysis.security.confidence == 80
    assert analysis.security.details["justification"]
    assert analysis.rgpd.details["data_types"] == ["nom"]
    assert analysis.finance.details["amounts"] == [
        {"value": 1234.5, "currency": "EUR", "context": "total TTC"}
    ]
    assert analysis.legal.details["parties"] == ["ACME"]
    assert analysis.raw["file_ref"] == "dossier/rapport.md"


def test_fichier_absent_de_la_reponse() -> None:
    parsed = parse_block_response(build_content(REFS, "drop_last"), block_files())
    assert set(parsed.analyses) == {1, 2}
    assert [bf.file_ref for bf in parsed.missing] == ["autre/note.txt"]


def test_reference_inconnue() -> None:
    parsed = parse_block_response(build_content(REFS, "extra_ref"), block_files())
    assert len(parsed.analyses) == 3
    assert parsed.unknown_refs == ["inconnu/fichier_fantome.txt"]
    assert parsed.missing == []


def test_enumeration_invalide_rejette_l_entree() -> None:
    parsed = parse_block_response(build_content(REFS, "bad_enum"), block_files())
    assert set(parsed.analyses) == {2, 3}
    assert len(parsed.invalid) == 1
    ref, reason = parsed.invalid[0]
    assert ref == "dossier/rapport.md"
    assert "security.classification" in reason
    assert [bf.file_ref for bf in parsed.missing] == ["dossier/rapport.md"]


def test_correlation_insensible_casse_et_separateurs() -> None:
    content = wrap(make_entry("DOSSIER\\Rapport.MD"))
    parsed = parse_block_response(content, block_files(["dossier/rapport.md"]))
    assert set(parsed.analyses) == {1}
    assert parsed.unknown_refs == []


def test_correlation_par_nom_de_base_unique() -> None:
    content = wrap(make_entry("rapport.md"))
    parsed = parse_block_response(content, block_files(["dossier/sous/rapport.md"]))
    assert set(parsed.analyses) == {1}


def test_nom_de_base_ambigu_non_correle() -> None:
    files = block_files(["a/rapport.md", "b/rapport.md"])
    parsed = parse_block_response(wrap(make_entry("rapport.md")), files)
    assert parsed.analyses == {}
    assert parsed.unknown_refs == ["rapport.md"]
    assert len(parsed.missing) == 2


def test_doublon_garde_la_premiere_entree() -> None:
    first = make_entry("dossier/rapport.md")
    second = make_entry("dossier/rapport.md")
    second["resume"] = "Second passage"
    parsed = parse_block_response(wrap(first, second), block_files(["dossier/rapport.md"]))
    assert parsed.analyses[1].resume == "Résumé de dossier/rapport.md"
    assert parsed.invalid == [("dossier/rapport.md", "doublon : fichier déjà analysé")]


def test_json_illisible() -> None:
    with pytest.raises(ParseError):
        parse_block_response(build_content(REFS, "garbage"), block_files())


def test_racine_sans_cle_files() -> None:
    with pytest.raises(ParseError):
        parse_block_response('{"resultats": []}', block_files())


def test_racine_non_objet() -> None:
    with pytest.raises(ParseError):
        parse_block_response("[1, 2, 3]", block_files())


def test_confidence_float_entier_accepte() -> None:
    entry = make_entry("dossier/rapport.md")
    entry["security"]["confidence"] = 85.0
    parsed = parse_block_response(wrap(entry), block_files(["dossier/rapport.md"]))
    assert parsed.analyses[1].security.confidence == 85


@pytest.mark.parametrize("value", [120, -1, 85.5, "80", True, None])
def test_confidence_hors_plage_ou_mal_typee_rejetee(value: object) -> None:
    entry = make_entry("dossier/rapport.md")
    entry["security"]["confidence"] = value
    parsed = parse_block_response(wrap(entry), block_files(["dossier/rapport.md"]))
    assert parsed.analyses == {}
    assert "security.confidence" in parsed.invalid[0][1]


def test_cle_requise_manquante() -> None:
    entry = make_entry("dossier/rapport.md")
    del entry["legal"]
    parsed = parse_block_response(wrap(entry), block_files(["dossier/rapport.md"]))
    assert parsed.invalid == [("dossier/rapport.md", "clé manquante : legal")]


def test_montants_mal_formes_rejetes() -> None:
    entry = make_entry("dossier/rapport.md")
    entry["finance"]["amounts"] = [{"value": "1234", "currency": "EUR", "context": "total"}]
    parsed = parse_block_response(wrap(entry), block_files(["dossier/rapport.md"]))
    assert parsed.analyses == {}
    assert "finance.amounts" in parsed.invalid[0][1]


def test_listes_de_chaines_verifiees() -> None:
    entry = make_entry("dossier/rapport.md")
    entry["rgpd"]["data_types"] = ["nom", 42]
    parsed = parse_block_response(wrap(entry), block_files(["dossier/rapport.md"]))
    assert "rgpd.data_types" in parsed.invalid[0][1]


def test_entree_non_objet_rejetee() -> None:
    content = json.dumps({"files": ["ceci n'est pas un objet", make_entry("dossier/rapport.md")]})
    parsed = parse_block_response(content, block_files(["dossier/rapport.md"]))
    assert set(parsed.analyses) == {1}
    assert parsed.invalid == [("", "entrée non-objet")]


def test_bloc_vide_tout_est_manquant() -> None:
    parsed = parse_block_response('{"files": []}', block_files())
    assert parsed.analyses == {}
    assert len(parsed.missing) == 3


def test_thinking_block_is_ignored() -> None:
    from docia.llm.parse import strip_thinking

    content = '<think>je réfléchis\nlonguement</think>\n{"files": []}'
    assert strip_thinking(content) == '{"files": []}'
    assert strip_thinking('Voici le JSON :\n{"files": []}') == '{"files": []}'
    assert strip_thinking('{"files": []}') == '{"files": []}'
