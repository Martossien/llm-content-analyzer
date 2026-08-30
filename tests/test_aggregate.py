"""Agrégation conservatrice des segments d'un fichier découpé."""

from __future__ import annotations

import pytest

from docia.llm.aggregate import aggregate_segments


def _seg(
    sec: str, rgpd: str, fin: str = "none", leg: str = "none", conf: int = 80, **extra: object
) -> dict[str, object]:
    return {
        "file_ref": "x",
        "resume": extra.get("resume", f"segment {sec}"),
        "security": {
            "classification": sec,
            "confidence": conf,
            "justification": f"parce que {sec}",
        },
        "rgpd": {"risk_level": rgpd, "data_types": extra.get("data_types", []), "confidence": conf},
        "finance": {"document_type": fin, "amounts": extra.get("amounts", []), "confidence": conf},
        "legal": {"contract_type": leg, "parties": extra.get("parties", []), "confidence": conf},
    }


def test_severity_is_max_of_segments() -> None:
    a = aggregate_segments(
        "gros.txt", [_seg("C0", "none"), _seg("C3", "high", conf=60), _seg("C1", "low")]
    )
    assert (a.security.label, a.security.confidence) == ("C3", 60)
    assert a.security.details["justification"] == "parce que C3"
    assert a.rgpd.label == "high"
    assert a.resume.startswith("Fichier analysé en 3 parties.")
    assert "[2] segment C3" in a.resume
    assert a.raw["segments"] == 3


def test_types_pick_most_confident_non_none_and_lists_are_unioned() -> None:
    a = aggregate_segments(
        "gros.txt",
        [
            _seg(
                "C1",
                "low",
                fin="invoice",
                conf=50,
                amounts=[{"value": 10, "currency": "EUR", "context": "a"}],
                data_types=["identite"],
            ),
            _seg(
                "C1",
                "low",
                fin="contract",
                leg="employment",
                conf=90,
                amounts=[
                    {"value": 10, "currency": "EUR", "context": "a"},
                    {"value": 20, "currency": "EUR", "context": "b"},
                ],
                data_types=["identite", "nir"],
                parties=["A", "B"],
            ),
            _seg("C1", "none", conf=70),
        ],
    )
    assert (a.finance.label, a.finance.confidence) == ("contract", 90)
    assert (a.legal.label, a.legal.confidence) == ("employment", 90)
    assert a.finance.details["amounts"] == [
        {"value": 10, "currency": "EUR", "context": "a"},
        {"value": 20, "currency": "EUR", "context": "b"},
    ]
    assert a.rgpd.details["data_types"] == ["identite", "nir"]
    assert a.legal.details["parties"] == ["A", "B"]


def test_all_none_stays_none_and_garbage_is_tolerated() -> None:
    a = aggregate_segments("x", [_seg("N/A", "N/A", conf=10), {"resume": "", "security": "??"}])
    assert a.security.label in ("N/A", "C0")
    assert a.finance.label == "none"
    assert a.legal.label == "none"


def test_empty_raises() -> None:
    with pytest.raises(ValueError, match="aucun segment"):
        aggregate_segments("x", [])


def test_retention_keeps_longest_required_duration() -> None:
    segs = [_seg("C1", "low"), _seg("C1", "low"), _seg("C1", "low")]
    segs[0]["retention"] = {
        "required": False,
        "years": 0,
        "basis": "none",
        "justification": "",
        "confidence": 60,
    }
    segs[1]["retention"] = {
        "required": True,
        "years": 10,
        "basis": "fiscal",
        "justification": "facture",
        "confidence": 80,
    }
    segs[2]["retention"] = {
        "required": True,
        "years": 5,
        "basis": "contractual",
        "justification": "contrat",
        "confidence": 90,
    }
    a = aggregate_segments("x", segs)
    assert a.retention.label == "fiscal"
    assert a.retention.details == {"required": True, "years": 10, "justification": "facture"}
    assert a.retention.confidence == 80
