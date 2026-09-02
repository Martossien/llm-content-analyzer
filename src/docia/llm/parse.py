"""Validation de la réponse JSON du modèle et corrélation avec les fichiers du bloc.

Le modèle peut mentir sur la forme : ici on ne fait confiance à rien. Une entrée
mal formée est écartée (`invalid`) sans faire échouer le bloc entier ; seule une
réponse globalement illisible lève `ParseError`.
"""

from __future__ import annotations

import json
import logging
import math
import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

from docia.llm.schema import (
    FINANCE_TYPES,
    LEGAL_TYPES,
    RETENTION_BASIS,
    RGPD_LEVELS,
    SECURITY_CLASSES,
)
from docia.models import BlockFile, DomainAnalysis, FileAnalysis

logger = logging.getLogger(__name__)

_REQUIRED_KEYS = ("file_ref", "resume", "security", "rgpd", "finance", "legal", "retention")


_THINK_BLOCK = re.compile(r"<think>.*?</think>\s*", re.DOTALL)

MAX_TEXT_CHARS = 4_000
"""Plafond, en caractères, de toute chaîne rendue par le modèle (`resume`,
justifications, parties, contextes de montants…) et de toute chaîne conservée
dans `raw`.

Le schéma JSON borne `maxItems` mais AUCUNE longueur de chaîne : xgrammar ne
sait pas contraindre `maxLength`. Le garde-fou est donc ici, à la validation.
Sans lui, un modèle qui part en boucle écrit des dizaines de Mo par entrée —
`resume` ET `raw` partant tous deux en base. 4 000 caractères, c'est déjà une
page dense : au-delà, ce n'est plus un résumé."""

_TRUNCATION_MARK = " […tronqué]"

_MAX_JSON_STARTS = 5
"""Nombre de débuts d'objet essayés avant d'abandonner le décodage (borne le coût)."""


def strip_thinking(content: str) -> str:
    """Retire un bloc de raisonnement `<think>…</think>` laissé dans la réponse
    (serveur sans `--reasoning-parser`), puis tout ce qui précède le premier `{`
    si le modèle a bavardé avant le JSON."""
    text = _THINK_BLOCK.sub("", content)
    start = text.find("{")
    return text[start:] if start > 0 else text


class ParseError(Exception):
    """Réponse inexploitable : JSON illisible ou sans tableau `files`."""


def _decode_json(content: str) -> Any:
    """Décode la valeur JSON contenue dans `content`, même mal emballée.

    Décodage INCRÉMENTAL (`raw_decode`) : on s'arrête au premier document JSON
    complet et on ignore tout ce qui suit. Sans cela, un seul caractère de trop
    après le JSON — clôture markdown ``` ```, phrase de politesse — coûtait le
    bloc entier (`Extra data`), soit jusqu'à 500 fichiers repartis puis mis en
    `error`. Une réponse **tronquée**, elle, reste bien une `ParseError` : rien
    ne s'y décode complètement.

    Raises:
        ParseError: aucun document JSON complet trouvé.
    """
    text = _THINK_BLOCK.sub("", content).strip()
    decoder = json.JSONDecoder()

    # Candidats : le premier caractère structurant (pour qu'une racine tableau
    # reste diagnostiquée « objet attendu »), puis les `{` suivants — un modèle
    # peut avoir bavardé une accolade avant le vrai JSON.
    starts: list[int] = []
    first = min((i for i in (text.find("{"), text.find("[")) if i >= 0), default=-1)
    if first >= 0:
        starts.append(first)
        pos = text.find("{", first + 1)
        while pos >= 0 and len(starts) < _MAX_JSON_STARTS:
            starts.append(pos)
            pos = text.find("{", pos + 1)

    head_error: str | None = None
    fallback: list[Any] = []  # vide = rien décodé (un `None` décodé reste distinguable)
    for rank, start in enumerate(starts):
        try:
            value, _end = decoder.raw_decode(text, start)
        except ValueError as exc:  # JSONDecodeError
            if rank == 0:
                head_error = str(exc)
            continue
        if isinstance(value, dict) and isinstance(value.get("files"), list):
            return value
        if not fallback:
            fallback.append(value)

    if head_error is not None:
        # Le document commencé au premier `{` ne se referme pas : réponse coupée
        # en plein vol. Ce qu'on a pu décoder plus loin n'est qu'un fragment.
        raise ParseError(f"réponse JSON illisible : {head_error}")
    if fallback:
        return fallback[0]
    raise ParseError("réponse JSON illisible : aucun objet JSON trouvé dans la réponse")


def _clip(text: str) -> str:
    """Tronque proprement une chaîne rendue par le modèle à `MAX_TEXT_CHARS`."""
    if len(text) <= MAX_TEXT_CHARS:
        return text
    return text[: MAX_TEXT_CHARS - len(_TRUNCATION_MARK)] + _TRUNCATION_MARK


def _clip_deep(value: Any, depth: int = 0) -> Any:
    """Applique `_clip` à toutes les chaînes d'une structure (clés comprises).

    Sert à borner `raw`, qui part en base tel quel."""
    if isinstance(value, str):
        return _clip(value)
    if depth >= 6:
        return value
    if isinstance(value, dict):
        return {_clip(str(k)): _clip_deep(v, depth + 1) for k, v in value.items()}
    if isinstance(value, list):
        return [_clip_deep(item, depth + 1) for item in value]
    return value


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


_PART_SUFFIX = re.compile(r"\s*\[partie\s*\d+\s*/\s*\d+\]\s*$", re.IGNORECASE)
"""Suffixe ajouté par `blocks.builder._segment_blocks` au `file_ref` d'un segment."""


def _bare(ref: str) -> str:
    """Nom de base privé du suffixe « [partie i/K] ».

    Le `file_ref` d'un segment PORTE ce suffixe : ni la comparaison exacte, ni la
    normalisée, ni le nom de base ne rattrapent une référence nue. Or les modèles
    raccourcissent spontanément les chemins ornés, et un segment perdu suffisait à
    mettre tout un gros fichier en erreur.

    Le suffixe est retiré AVANT de découper sur « / » : il en contient un
    (« [partie 2/7] »), et `_basename` seul rendrait « 7] »."""
    return _basename(_PART_SUFFIX.sub("", _normalize(ref)))


class _Index:
    """Quatre niveaux : exact, normalisé, nom de base unique, nom de base sans
    suffixe de segment (unique lui aussi)."""

    def __init__(self, files: Sequence[BlockFile]) -> None:
        self._exact: dict[str, BlockFile] = {}
        self._normalized: dict[str, BlockFile] = {}
        base_counts: dict[str, int] = {}
        base_first: dict[str, BlockFile] = {}
        bare_counts: dict[str, int] = {}
        bare_first: dict[str, BlockFile] = {}
        for bf in files:
            self._exact.setdefault(bf.file_ref, bf)
            self._normalized.setdefault(_normalize(bf.file_ref), bf)
            base = _basename(bf.file_ref)
            base_counts[base] = base_counts.get(base, 0) + 1
            base_first.setdefault(base, bf)
            bare = _bare(bf.file_ref)
            bare_counts[bare] = bare_counts.get(bare, 0) + 1
            bare_first.setdefault(bare, bf)
        self._basenames = {b: bf for b, bf in base_first.items() if base_counts[b] == 1}
        self._bare = {b: bf for b, bf in bare_first.items() if bare_counts[b] == 1 and b}

    def match(self, ref: str) -> BlockFile | None:
        """Fichier du bloc désigné par une référence rendue par le modèle, ou None."""
        found = self._exact.get(ref)
        if found is not None:
            return found
        found = self._normalized.get(_normalize(ref))
        if found is not None:
            return found
        found = self._basenames.get(_basename(ref))
        if found is not None:
            return found
        return self._bare.get(_bare(ref))


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
        if isinstance(amount, float) and not math.isfinite(amount):
            # `json.loads` accepte `NaN`/`Infinity` : un tel montant serait sommé,
            # exporté en Excel/Power BI et réécrit en JSON invalide dans `raw`.
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


_Details = tuple[dict[str, object] | None, str]
"""(détails validés, `""`) ou (`None`, raison du rejet)."""


def _details_security(block: dict[str, Any]) -> _Details:
    justification = block.get("justification", "")
    if not isinstance(justification, str):
        return None, "`security.justification` doit être une chaîne"
    return {"justification": justification}, ""


def _details_rgpd(block: dict[str, Any]) -> _Details:
    data_types = _as_str_list(block.get("data_types"))
    if data_types is None:
        return None, "`rgpd.data_types` doit être une liste de chaînes"
    return {"data_types": data_types}, ""


def _details_finance(block: dict[str, Any]) -> _Details:
    amounts = _as_amounts(block.get("amounts"))
    if amounts is None:
        return None, "`finance.amounts` doit être une liste de montants valides"
    return {"amounts": amounts}, ""


def _details_legal(block: dict[str, Any]) -> _Details:
    parties = _as_str_list(block.get("parties"))
    if parties is None:
        return None, "`legal.parties` doit être une liste de chaînes"
    return {"parties": parties}, ""


def _details_retention(block: dict[str, Any]) -> _Details:
    required = block.get("required")
    years = block.get("years")
    justification = block.get("justification", "")
    if not isinstance(required, bool):
        return None, "`retention.required` doit être un booléen"
    if isinstance(years, float) and years.is_integer():
        years = int(years)
    if not isinstance(years, int) or isinstance(years, bool) or not (0 <= years <= 100):
        return None, "`retention.years` doit être un entier 0–100"
    if not isinstance(justification, str):
        return None, "`retention.justification` doit être une chaîne"
    return {"required": required, "years": years, "justification": justification}, ""


_DOMAINS: tuple[tuple[str, str, Sequence[str], Callable[[dict[str, Any]], _Details]], ...] = (
    ("security", "classification", SECURITY_CLASSES, _details_security),
    ("rgpd", "risk_level", RGPD_LEVELS, _details_rgpd),
    ("finance", "document_type", FINANCE_TYPES, _details_finance),
    ("legal", "contract_type", LEGAL_TYPES, _details_legal),
    ("retention", "basis", RETENTION_BASIS, _details_retention),
)
"""Un domaine = (clé, champ étiquette, valeurs admises, validateur des détails)."""


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
    file_ref = _clip(file_ref)
    resume = _clip(resume)

    domains: dict[str, DomainAnalysis] = {}
    for key, label_key, allowed, details_of in _DOMAINS:
        block, reason = _domain(entry, key)
        if block is None:
            return None, reason
        label = block.get(label_key)
        if not isinstance(label, str) or label not in allowed:
            return None, f"`{key}.{label_key}` invalide : {label!r}"
        confidence = _as_confidence(block.get("confidence"))
        if confidence is None:
            return None, f"`{key}.confidence` doit être un entier 0–100"
        details, reason = details_of(block)
        if details is None:
            return None, reason
        # `_clip_deep` : justifications, types de données, parties et contextes de
        # montants partent aussi en base — aucun n'a de longueur bornée par le schéma.
        domains[key] = DomainAnalysis(
            label=label, confidence=confidence, details=_clip_deep(details)
        )

    return (
        FileAnalysis(
            file_ref=file_ref,
            resume=resume,
            security=domains["security"],
            rgpd=domains["rgpd"],
            finance=domains["finance"],
            legal=domains["legal"],
            raw=_clip_deep(dict(entry)),
            retention=domains["retention"],
        ),
        "",
    )


# -- point d'entrée ------------------------------------------------------


def parse_block_response(content: str, files: Sequence[BlockFile]) -> ParsedBlock:
    """Valide la réponse d'un bloc et l'associe aux fichiers envoyés.

    Raises:
        ParseError: JSON illisible, racine non-objet ou clé `files` absente/non liste.
    """
    if not isinstance(content, str):
        raise ParseError("réponse JSON illisible : contenu non textuel")
    data = _decode_json(content)
    if not isinstance(data, dict):
        raise ParseError("racine JSON : objet attendu")
    entries = data.get("files")
    if not isinstance(entries, list):
        raise ParseError("clé `files` absente ou non liste")

    # Un bloc à un seul fichier et une réponse à une seule entrée : la référence,
    # même raccourcie ou inventée, ne peut désigner que ce fichier-là.
    unambiguous = files[0] if len(files) == 1 and len(entries) == 1 else None
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
        if target is None and unambiguous is not None:
            logger.warning(
                "bloc à un seul fichier : référence « %s » attribuée à « %s »",
                analysis.file_ref,
                unambiguous.file_ref,
            )
            target = unambiguous
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
