"""Fonctions pures de l'interface (testables sans fenêtre) : sérialisation TOML,
lignes d'état, parsing des champs, lignes du tableau de résultats, estimation de tokens.
"""

from __future__ import annotations

import json
import logging
import tomllib
from collections.abc import Iterable
from dataclasses import asdict
from typing import Any

from docia import __version__
from docia.config import Config

logger = logging.getLogger(__name__)

TRANSPORTS = ("vllm", "openwebui")
TOKENIZERS = ("approx", "mistral", "openai")


# ---------------------------------------------------------------- helpers purs
def config_to_toml(cfg: Config) -> str:
    """Sérialise une `Config` en TOML lisible (tomllib ne sait qu'écrire → on
    formate à la main, champs simples uniquement)."""

    def value(v: object) -> str:
        if isinstance(v, bool):
            return "true" if v else "false"
        if isinstance(v, int | float):
            return str(v)
        if isinstance(v, list):
            return "[" + ", ".join(json.dumps(str(x), ensure_ascii=False) for x in v) + "]"
        return json.dumps(str(v), ensure_ascii=False)

    data = asdict(cfg)
    lines = [f"# docia.toml — écrit par l'interface docia {__version__}"]
    for key in ("db_path", "prompt_path"):
        lines.append(f"{key} = {value(data[key])}")
    for section in ("llm", "blocks", "filter"):
        lines.append("")
        lines.append(f"[{section}]")
        for key, v in data[section].items():
            lines.append(f"{key} = {value(v)}")
    text = "\n".join(lines) + "\n"
    tomllib.loads(text)  # garantit qu'on écrit du TOML valide
    return text


def status_lines(counts: dict[str, int], classes: dict[str, dict[str, int]]) -> list[str]:
    """Lignes du panneau de compteurs (pur)."""
    lines = [
        f"fichiers : {counts.get('files', 0)} — à analyser {counts.get('pending', 0)}, "
        f"en cours {counts.get('queued', 0)}, analysés {counts.get('done', 0)}, "
        f"exclus {counts.get('excluded', 0)}, en erreur {counts.get('error', 0)}",
        f"blocs : construits {counts.get('blocks_built', 0)}, envoyés {counts.get('blocks_sent', 0)}, "
        f"terminés {counts.get('blocks_done', 0)}, en erreur {counts.get('blocks_error', 0)} — "
        f"analyses : {counts.get('analyses', 0)}",
    ]
    for domain, label in (
        ("security", "sécurité"),
        ("rgpd", "RGPD"),
        ("finance", "finance"),
        ("legal", "juridique"),
    ):
        dist = classes.get(domain) or {}
        if dist:
            lines.append(f"{label} : " + ", ".join(f"{k} {v}" for k, v in sorted(dist.items())))
    return lines


def parse_int(raw: str, fallback: int, *, minimum: int = 1) -> int:
    """Entier saisi dans un champ texte, sinon `fallback` (pur)."""
    try:
        v = int(raw.strip().replace(" ", "").replace(" ", ""))
    except ValueError:
        return fallback
    return v if v >= minimum else fallback


def result_rows(rows: Iterable[Any], limit: int = 500) -> list[tuple[str, str, str, str, str, str]]:
    """Lignes du tableau de résultats : (nom, sécurité, RGPD, finance, juridique, résumé)."""
    out: list[tuple[str, str, str, str, str, str]] = []
    for r in rows:
        if len(out) >= limit:
            break
        out.append(
            (
                str(r["name"]),
                str(r["security_classification"] or (r["status"] if r["status"] != "done" else "")),
                str(r["rgpd_risk_level"] or ""),
                str(r["finance_document_type"] or ""),
                str(r["legal_contract_type"] or ""),
                (str(r["resume"] or r["exclusion_reason"] or ""))[:120],
            )
        )
    return out


def estimate_prompt_tokens(text: str) -> int:
    """Estimation rapide (octets/4, comme DocFuse `approx`) pour l'éditeur de prompt."""
    return max(1, (len(text.encode("utf-8")) + 3) // 4)


def result_rows_v31(rows: Iterable[Any], limit: int = 500) -> list[dict[str, str]]:
    """Lignes enrichies du tableau de résultats (v3.1) : id, nom, chemin, 5 domaines,
    conservation, revue, résumé/raison — dicts pour l'onglet Résultats."""
    out: list[dict[str, str]] = []
    for r in rows:
        if len(out) >= limit:
            break
        # sqlite3.Row : `.keys()` obligatoire (itérer la ligne donne les valeurs).
        data: dict[str, Any] = (
            {k: r[k] for k in r.keys()} if hasattr(r, "keys") else {}  # noqa: SIM118
        )
        get = data.get
        status = str(get("status") or "")
        sec = str(get("security_classification") or "")
        ret = ""
        if get("retention_required"):
            ret = f"{get('retention_basis') or ''} {get('retention_years') or 0} ans".strip()
        out.append(
            {
                "id": str(get("id") or ""),
                "nom": str(get("name") or ""),
                "chemin": str(get("path") or ""),
                "sécu": sec or (status if status != "done" else ""),
                "rgpd": str(get("rgpd_risk_level") or ""),
                "finance": str(get("finance_document_type") or ""),
                "juridique": str(get("legal_contract_type") or ""),
                "conservation": ret,
                "revue": str(get("review_status") or ""),
                "résumé": (str(get("resume") or get("exclusion_reason") or ""))[:160],
            }
        )
    return out
