"""Validation de la réponse JSON du modèle et corrélation avec les fichiers du bloc.

Le modèle peut mentir sur la forme : ici on ne fait confiance à rien. Une entrée
mal formée est écartée (`invalid`) sans faire échouer le bloc entier ; seule une
réponse globalement illisible lève `ParseError`.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from docia.llm.schema import FINANCE_TYPES, LEGAL_TYPES, RGPD_LEVELS, SECURITY_CLASSES
from docia.models import BlockFile, DomainAnalysis, FileAnalysis

logger = logging.getLogger(__name__)

_REQUIRED_KEYS = ("file_ref", "resume", "security", "rgpd", "finance", "legal")


_THINK_BLOCK = re.compile(r"<think>.*?</think>\s*", re.DOTALL)


def strip_thinking(content: str) -> str:
    """Retire un bloc de raisonnement `<think>…</think>` laissé dans la réponse
    (serveur sans `--reasoning-parser`), puis tout ce qui précède le premier `{`
    si le modèle a bavardé avant le JSON."""
    text = _THINK_BLOCK.sub("", content)
    start = text.find("{")
    return text[start:] if start > 0 else text


class ParseError(Exception):
    """Réponse inexploitable : JSON illisible ou sans tableau `files`."""


@dataclass(frozen=True)
class ParsedBlock:
    """Résultat de la validation d'une réponse de bloc."""

    analyses: dict[int, FileAnalysis] = field(default_factory=dict)
    """`file_id` → analyse validée."""
    missing: list[BlockFile] = field(default_factory=list)
    """Fichiers du bloc absents de la réponse."""
    unknown_refs: list[str] = field(default_factory=list)
    """`file_ref` renvoyés ne correspondant à aucun fichier du bloc."""
    invalid: list[tuple[str, str]] = field(default_factory=list)
    """`(file_ref, raison)` des entrées rejetées."""


# -- corrélation ---------------------------------------------------------


def _normalize(ref: str) -> str:
    """Casse et séparateurs neutralisés pour comparer deux chemins."""
    return ref.replace("\\", "/").strip().strip("/").casefold()


def _basename(ref: str) -> str:
    return _normalize(ref).rsplit("/", 1)[-1]


class _Index:
    """Trois niveaux de correspondance : exact, normalisé, nom de base unique."""

    def __init__(self, files: Sequence[BlockFile]) -> None:
        self._exact: dict[str, BlockFile] = {}
        self._normalized: dict[str, BlockFile] = {}
        base_counts: dict[str, int] = {}
        base_first: dict[str, BlockFile] = {}
        for bf in files:
            self._exact.setdefault(bf.file_ref, bf)
            self._normalized.setdefault(_normalize(bf.file_ref), bf)
            base = _basename(bf.file_ref)
            base_counts[base] = base_counts.get(base, 0) + 1
            base_first.setdefault(base, bf)
        self._basenames = {b: bf for b, bf in base_first.items() if base_counts[b] == 1}

    def match(self, ref: str) -> BlockFile | None:
        found = self._exact.get(ref)
        if found is not None:
            return found
        found = self._normalized.get(_normalize(ref))
        if found is not None:
            return found
        return self._basenames.get(_basename(ref))


# -- validation d'une entrée --------------------------------------------


def _as_confidence(value: object) -> int | None:
    """Entier 0–100 ; un float entier (85.0) est accepté, le reste rejeté."""
    if isinstance(value, bool):
        return None
    if isinstance(value, float):
        if not value.is_integer():
            return None
        value = int(value)
    if not isinstance(value, int):
        return None
    if not 0 <= value <= 100:
        return None
    return value


def _as_str_list(value: object) -> list[str] | None:
    if not isinstance(value, list):
        return None
    if not all(isinstance(item, str) for item in value):
        return None
    return [str(item) for item in value]


def _as_amounts(value: object) -> list[dict[str, Any]] | None:
    """Liste d'objets `{value: nombre, currency: str, context: str}`."""
    if not isinstance(value, list):
        return None
    amounts: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, dict):
            return None
        amount = item.get("value")
        if isinstance(amount, bool) or not isinstance(amount, int | float):
            return None
        currency = item.get("currency")
        context = item.get("context")
        if not isinstance(currency, str) or not isinstance(context, str):
            return None
        amounts.append({"value": amount, "currency": currency, "context": context})
    return amounts


def _domain(entry: dict[str, Any], key: str) -> tuple[dict[str, Any] | None, str]:
    block = entry.get(key)
    if not isinstance(block, dict):
        return None, f"`{key}` doit être un objet"
    return block, ""


def _build_analysis(entry: dict[str, Any]) -> tuple[FileAnalysis | None, str]:
    """Valide une entrée et construit son `FileAnalysis`, ou rend la raison du rejet."""
    for key in _REQUIRED_KEYS:
        if key not in entry:
            return None, f"clé manquante : {key}"
    file_ref = entry["file_ref"]
    if not isinstance(file_ref, str) or not file_ref.strip():
        return None, "`file_ref` doit être une chaîne non vide"
    resume = entry["resume"]
    if not isinstance(resume, str):
        return None, "`resume` doit être une chaîne"

    domains: dict[str, DomainAnalysis] = {}
    specs = (
        ("security", "classification", SECURITY_CLASSES),
        ("rgpd", "risk_level", RGPD_LEVELS),
        ("finance", "document_type", FINANCE_TYPES),
        ("legal", "contract_type", LEGAL_TYPES),
    )
    for key, label_key, allowed in specs:
        block, reason = _domain(entry, key)
        if block is None:
            return None, reason
        label = block.get(label_key)
        if not isinstance(label, str) or label not in allowed:
            return None, f"`{key}.{label_key}` invalide : {label!r}"
        confidence = _as_confidence(block.get("confidence"))
        if confidence is None:
            return None, f"`{key}.confidence` doit être un entier 0–100"

        details: dict[str, object]
        if key == "security":
            justification = block.get("justification", "")
            if not isinstance(justification, str):
                return None, "`security.justification` doit être une chaîne"
            details = {"justification": justification}
        elif key == "rgpd":
            data_types = _as_str_list(block.get("data_types"))
            if data_types is None:
                return None, "`rgpd.data_types` doit être une liste de chaînes"
            details = {"data_types": data_types}
        elif key == "finance":
            amounts = _as_amounts(block.get("amounts"))
            if amounts is None:
                return None, "`finance.amounts` doit être une liste de montants valides"
            details = {"amounts": amounts}
        else:
            parties = _as_str_list(block.get("parties"))
            if parties is None:
                return None, "`legal.parties` doit être une liste de chaînes"
            details = {"parties": parties}
        domains[key] = DomainAnalysis(label=label, confidence=confidence, details=details)

    return (
        FileAnalysis(
            file_ref=file_ref,
            resume=resume,
            security=domains["security"],
            rgpd=domains["rgpd"],
            finance=domains["finance"],
            legal=domains["legal"],
            raw=dict(entry),
        ),
        "",
    )


# -- point d'entrée ------------------------------------------------------


def parse_block_response(content: str, files: Sequence[BlockFile]) -> ParsedBlock:
    """Valide la réponse d'un bloc et l'associe aux fichiers envoyés.

    Raises:
        ParseError: JSON illisible, racine non-objet ou clé `files` absente/non liste.
    """
    try:
        data = json.loads(strip_thinking(content))
    except (json.JSONDecodeError, TypeError) as exc:
        raise ParseError(f"réponse JSON illisible : {exc}") from exc
    if not isinstance(data, dict):
        raise ParseError("racine JSON : objet attendu")
    entries = data.get("files")
    if not isinstance(entries, list):
        raise ParseError("clé `files` absente ou non liste")

    index = _Index(files)
    analyses: dict[int, FileAnalysis] = {}
    unknown_refs: list[str] = []
    invalid: list[tuple[str, str]] = []

    for raw in entries:
        if not isinstance(raw, dict):
            invalid.append(("", "entrée non-objet"))
            continue
        ref_hint = raw.get("file_ref")
        ref_label = ref_hint if isinstance(ref_hint, str) else ""
        analysis, reason = _build_analysis(raw)
        if analysis is None:
            invalid.append((ref_label, reason))
            continue
        target = index.match(analysis.file_ref)
        if target is None:
            unknown_refs.append(analysis.file_ref)
            continue
        if target.file_id in analyses:
            invalid.append((analysis.file_ref, "doublon : fichier déjà analysé"))
            continue
        analyses[target.file_id] = analysis

    missing = [bf for bf in files if bf.file_id not in analyses]
    if missing or unknown_refs or invalid:
        logger.warning(
            "bloc validé : %d analyses, %d absents, %d refs inconnues, %d entrées rejetées",
            len(analyses),
            len(missing),
            len(unknown_refs),
            len(invalid),
        )
    return ParsedBlock(
        analyses=analyses, missing=missing, unknown_refs=unknown_refs, invalid=invalid
    )
