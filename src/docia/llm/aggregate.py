"""Agrégation des analyses des K segments d'un fichier découpé (module pur).

Règle : **conservatrice**. La sévérité d'un fichier est le maximum de celle de
ses segments (sécurité C0<C1<C2<C3, RGPD none<low<…<critical) ; les types
finance/juridique retiennent le type non-`none` le plus sûr ; listes (types de
données, montants, parties) = union bornée ; résumé = « analysé en K parties »
suivi des résumés. Rien n'est inventé, rien n'est perdu : on ne peut que
sur-estimer la sensibilité, jamais la sous-estimer.
"""

from __future__ import annotations

from collections.abc import Sequence

from docia.models import DomainAnalysis, FileAnalysis

SECURITY_ORDER = ["N/A", "C0", "C1", "C2", "C3"]
RGPD_ORDER = ["N/A", "none", "low", "medium", "high", "critical"]
MAX_LIST = 12
MAX_AMOUNTS = 8
RESUME_MAX_CHARS = 900


def _rank(value: object, order: list[str]) -> int:
    return order.index(str(value)) if str(value) in order else 0


def _max_level(values: Sequence[tuple[str, int]], order: list[str]) -> tuple[str, int]:
    """(niveau max, confiance associée). La confiance retenue est celle du
    segment le plus sévère (moyenne s'il y a égalité)."""
    if not values:
        return order[0], 0
    top = max(_rank(v, order) for v, _ in values)
    confs = [c for v, c in values if _rank(v, order) == top]
    return order[top], round(sum(confs) / len(confs))


def _best_type(values: Sequence[tuple[str, int]]) -> tuple[str, int]:
    """Type non-`none` à la confiance la plus élevée ; sinon `none`/`N/A`."""
    real = [(t, c) for t, c in values if t not in ("none", "N/A", "")]
    if real:
        return max(real, key=lambda tc: tc[1])
    if values:
        return values[0][0] or "none", round(sum(c for _, c in values) / len(values))
    return "none", 0


def _union(lists: Sequence[object], limit: int) -> list[object]:
    seen: list[object] = []
    for lst in lists:
        if not isinstance(lst, list):
            continue
        for item in lst:
            key = repr(item)
            if key not in {repr(x) for x in seen}:
                seen.append(item)
            if len(seen) >= limit:
                return seen
    return seen


def _int(value: object) -> int:
    try:
        return max(0, min(100, int(round(float(str(value))))))
    except (TypeError, ValueError):
        return 0


def aggregate_segments(file_ref: str, raws: Sequence[dict[str, object]]) -> FileAnalysis:
    """Agrège les JSON bruts des segments (ordre = index de segment) en une analyse."""
    if not raws:
        raise ValueError("aucun segment à agréger")
    k = len(raws)

    def dom(raw: dict[str, object], name: str) -> dict[str, object]:
        d = raw.get(name)
        return d if isinstance(d, dict) else {}

    sec = _max_level(
        [
            (
                str(dom(r, "security").get("classification", "N/A")),
                _int(dom(r, "security").get("confidence")),
            )
            for r in raws
        ],
        SECURITY_ORDER,
    )
    rgpd = _max_level(
        [
            (str(dom(r, "rgpd").get("risk_level", "N/A")), _int(dom(r, "rgpd").get("confidence")))
            for r in raws
        ],
        RGPD_ORDER,
    )
    fin = _best_type(
        [
            (
                str(dom(r, "finance").get("document_type", "none")),
                _int(dom(r, "finance").get("confidence")),
            )
            for r in raws
        ]
    )
    leg = _best_type(
        [
            (
                str(dom(r, "legal").get("contract_type", "none")),
                _int(dom(r, "legal").get("confidence")),
            )
            for r in raws
        ]
    )

    justifications = [str(dom(r, "security").get("justification", "")).strip() for r in raws]
    justification = next(
        (
            j
            for j, r in zip(justifications, raws, strict=True)
            if str(dom(r, "security").get("classification")) == sec[0] and j
        ),
        "",
    )
    resumes = [str(r.get("resume", "")).strip() for r in raws]
    resume = f"Fichier analysé en {k} parties. " + " | ".join(
        f"[{i + 1}] {t}" for i, t in enumerate(resumes) if t
    )
    if len(resume) > RESUME_MAX_CHARS:
        resume = resume[: RESUME_MAX_CHARS - 1] + "…"

    data_types = _union([dom(r, "rgpd").get("data_types") for r in raws], MAX_LIST)
    amounts = _union([dom(r, "finance").get("amounts") for r in raws], MAX_AMOUNTS)
    parties = _union([dom(r, "legal").get("parties") for r in raws], MAX_LIST)
    raw_out: dict[str, object] = {
        "file_ref": file_ref,
        "segments": k,
        "resume": resume,
        "security": {
            "classification": sec[0],
            "confidence": sec[1],
            "justification": justification,
        },
        "rgpd": {"risk_level": rgpd[0], "data_types": data_types, "confidence": rgpd[1]},
        "finance": {"document_type": fin[0], "amounts": amounts, "confidence": fin[1]},
        "legal": {"contract_type": leg[0], "parties": parties, "confidence": leg[1]},
        "segment_raws": list(raws),
    }
    return FileAnalysis(
        file_ref=file_ref,
        resume=resume,
        security=DomainAnalysis(sec[0], sec[1], {"justification": justification}),
        rgpd=DomainAnalysis(rgpd[0], rgpd[1], {"data_types": data_types}),
        finance=DomainAnalysis(fin[0], fin[1], {"amounts": amounts}),
        legal=DomainAnalysis(leg[0], leg[1], {"parties": parties}),
        raw=raw_out,
    )
