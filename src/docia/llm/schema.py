"""Schéma JSON de sortie, prompt système et empreinte de prompt.

Le schéma est celui validé sur le banc du 30/08/2026 (vLLM + xgrammar,
`bench_vllm/test_qwen38.py`), avec les trois leçons intégrées : montants en
`number`, échelle 0–100 énoncée dans le prompt, tableaux bornés (`maxItems`).
Contraintes xgrammar : pas de `pattern`+`maxLength` combinés, pas de
`multipleOf`, `uniqueItems`, `patternProperties`.
"""

from __future__ import annotations

import hashlib
import json
from importlib import resources
from pathlib import Path

SECURITY_CLASSES = ["C0", "C1", "C2", "C3", "N/A"]
RGPD_LEVELS = ["none", "low", "medium", "high", "critical", "N/A"]
FINANCE_TYPES = ["none", "invoice", "contract", "budget", "accounting", "payment", "N/A"]
LEGAL_TYPES = ["none", "employment", "lease", "sale", "nda", "compliance", "litigation", "N/A"]

_CONFIDENCE = {"type": "integer", "minimum": 0, "maximum": 100}

OUTPUT_SCHEMA: dict[str, object] = {
    "type": "object",
    "properties": {
        "files": {
            "type": "array",
            "maxItems": 500,
            "items": {
                "type": "object",
                "properties": {
                    "file_ref": {"type": "string"},
                    "resume": {"type": "string"},
                    "security": {
                        "type": "object",
                        "properties": {
                            "classification": {"type": "string", "enum": SECURITY_CLASSES},
                            "confidence": _CONFIDENCE,
                            "justification": {"type": "string"},
                        },
                        "required": ["classification", "confidence", "justification"],
                    },
                    "rgpd": {
                        "type": "object",
                        "properties": {
                            "risk_level": {"type": "string", "enum": RGPD_LEVELS},
                            "data_types": {
                                "type": "array",
                                "maxItems": 12,
                                "items": {"type": "string"},
                            },
                            "confidence": _CONFIDENCE,
                        },
                        "required": ["risk_level", "data_types", "confidence"],
                    },
                    "finance": {
                        "type": "object",
                        "properties": {
                            "document_type": {"type": "string", "enum": FINANCE_TYPES},
                            "amounts": {
                                "type": "array",
                                "maxItems": 8,
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "value": {"type": "number"},
                                        "currency": {"type": "string"},
                                        "context": {"type": "string"},
                                    },
                                    "required": ["value", "currency", "context"],
                                },
                            },
                            "confidence": _CONFIDENCE,
                        },
                        "required": ["document_type", "amounts", "confidence"],
                    },
                    "legal": {
                        "type": "object",
                        "properties": {
                            "contract_type": {"type": "string", "enum": LEGAL_TYPES},
                            "parties": {
                                "type": "array",
                                "maxItems": 12,
                                "items": {"type": "string"},
                            },
                            "confidence": _CONFIDENCE,
                        },
                        "required": ["contract_type", "parties", "confidence"],
                    },
                },
                "required": ["file_ref", "resume", "security", "rgpd", "finance", "legal"],
            },
        }
    },
    "required": ["files"],
}


def response_format() -> dict[str, object]:
    """Objet `response_format` OpenAI (relayé tel quel par open-webui et vLLM)."""
    return {"type": "json_schema", "json_schema": {"name": "docia", "schema": OUTPUT_SCHEMA}}


def load_system_prompt(path: Path | None = None) -> str:
    """Prompt système : fichier utilisateur, sinon le prompt embarqué."""
    if path is not None:
        return path.read_text(encoding="utf-8")
    return resources.files("docia.prompts").joinpath("docia_v3.md").read_text(encoding="utf-8")


def prompt_hash(system_prompt: str, model: str) -> str:
    """Empreinte courte (16 hex) du triplet prompt + schéma + modèle : une analyse
    n'est réutilisable que si les trois sont identiques."""
    payload = json.dumps(
        {"prompt": system_prompt, "schema": OUTPUT_SCHEMA, "model": model},
        sort_keys=True,
        ensure_ascii=False,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
