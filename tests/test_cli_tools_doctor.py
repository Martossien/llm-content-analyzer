"""`docia doctor` — le diagnostic du poste, **sans** Tesseract ni serveur.

Le rapport est pur (`doctor_report`) : on remplace les sondes réelles (essai OCR,
scanner, serveur LLM) par des doublures et on vérifie ce qui compte pour un
administrateur : le code de retour dit si les PDF scannés sortiront vides, le
`--json` est exploitable, et un serveur éteint est une information, pas une erreur.
"""

from __future__ import annotations

import argparse
import json
from typing import Any

import pytest

from docia import cli_tools
from docia.config import Config


@pytest.fixture
def sondes(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Doublures des trois sondes ; `etat["probe"]` est ce que `self_test` rendra."""
    from docfuse.core.ocr import tesseract as tess

    etat: dict[str, Any] = {
        "probe": {
            "binary": "/usr/bin/tesseract",
            "version": "5.3",
            "available_languages": ["fra", "eng"],
            "effective_lang": "fra",
            "tessdata_prefix": "",
            "ocr_ok": True,
            "ocr_text": "FACTURE 4711",
        }
    }
    monkeypatch.setattr(tess, "self_test", lambda: etat["probe"])
    monkeypatch.setattr(cli_tools, "find_smbeagle", lambda _c: None, raising=False)
    import docia.scan

    monkeypatch.setattr(docia.scan, "find_smbeagle", lambda _c: None)
    return etat


def _cfg(transport: str = "openwebui") -> Config:
    cfg = Config()
    cfg.llm.transport = transport
    cfg.llm.base_url = "http://127.0.0.1:9/v1"  # port 9 : rien n'écoute
    return cfg


@pytest.mark.usefixtures("sondes")
def test_doctor_rapport_complet_quand_l_ocr_lit() -> None:
    report = cli_tools.doctor_report(_cfg())
    assert report["ocr_ok"] is True
    assert "4711" in report["ocr_essai"]
    assert report["pdfium_raster"] == "ok"
    assert report["tesseract_langs"] == ["fra", "eng"]
    assert "introuvable" in report["smbeagle"]
    assert "llm_contexte_servi" not in report  # transport openwebui : pas de sonde vLLM


def test_doctor_code_1_quand_tesseract_ne_lit_pas(
    sondes: dict[str, Any], capsys: pytest.CaptureFixture[str]
) -> None:
    """Le binaire est là mais l'essai échoue (langue en quarantaine) : code 1 et le
    `stderr` du binaire remonte tel quel — c'est la seule chose utile à lire."""
    sondes["probe"].update(ocr_ok=False, returncode=1, stderr="Error opening data file fra")
    code = cli_tools.cmd_doctor(argparse.Namespace(json=False), _cfg())
    assert code == 1
    err = capsys.readouterr().err
    assert "les PDF scannés sortiront vides" in err
    assert "Error opening data file fra" in err


@pytest.mark.usefixtures("sondes")
def test_doctor_json_exploitable(capsys: pytest.CaptureFixture[str]) -> None:
    code = cli_tools.cmd_doctor(argparse.Namespace(json=True), _cfg())
    assert code == 0
    data = json.loads(capsys.readouterr().out)
    assert data["ocr_ok"] is True
    assert data["pdfium_raster"] == "ok"


@pytest.mark.usefixtures("sondes")
def test_doctor_serveur_vllm_eteint_est_une_information() -> None:
    report = cli_tools.doctor_report(_cfg("vllm"))
    assert report["llm_contexte_servi"].startswith("serveur injoignable")
    assert report["ocr_ok"] is True  # un serveur éteint ne dégrade pas le diagnostic OCR


@pytest.mark.usefixtures("sondes")
def test_doctor_sans_moteur_ocr(monkeypatch: pytest.MonkeyPatch) -> None:
    """`self_test` qui lève (module absent de l'exe) : `ocr_ok` faux, message qui le dit."""
    from docfuse.core.ocr import tesseract as tess

    def casse() -> dict[str, Any]:
        raise RuntimeError("tesseract absent")

    monkeypatch.setattr(tess, "self_test", casse)
    report = cli_tools.doctor_report(_cfg())
    assert report["ocr_ok"] is False
    assert "tesseract absent" in report["ocr"]
    assert cli_tools.cmd_doctor(argparse.Namespace(json=True), _cfg()) == 1
