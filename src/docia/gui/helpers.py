"""Fonctions pures de l'interface (testables sans fenêtre) : sérialisation TOML,
parsing des champs, lignes du tableau de résultats, avancement, estimation de tokens.
"""

from __future__ import annotations

import json
import logging
import tomllib
from collections.abc import Iterable
from dataclasses import asdict
from typing import Any

from docia import __version__
from docia.config import ROOT_KEYS, SECTIONS, Config, toml_value

logger = logging.getLogger(__name__)

TRANSPORTS = ("vllm", "openwebui")
TOKENIZERS = ("approx", "mistral", "openai")


# ---------------------------------------------------------------- helpers purs
def config_to_toml(cfg: Config) -> str:
    """Sérialise une `Config` en TOML lisible (tomllib ne sait qu'écrire → on
    formate à la main, champs simples uniquement).

    **Regénération complète, sans commentaires** : `DociaApp.save_config` ne s'en sert
    plus que de repli, quand `config.update_toml` ne peut pas modifier le fichier
    existant sans risque."""
    data = asdict(cfg)
    lines = [f"# docia.toml — écrit par l'interface docia {__version__}"]
    for key in ROOT_KEYS:
        lines.append(f"{key} = {toml_value(data[key])}")
    for section in SECTIONS:
        lines.append("")
        lines.append(f"[{section}]")
        for key, v in data[section].items():
            lines.append(f"{key} = {toml_value(v)}")
    text = "\n".join(lines) + "\n"
    tomllib.loads(text)  # garantit qu'on écrit du TOML valide
    return text


def parse_int(raw: str, fallback: int, *, minimum: int = 1) -> int:
    """Entier saisi dans un champ texte, sinon `fallback` (pur)."""
    try:
        v = int(raw.strip().replace(" ", "").replace(" ", ""))
    except ValueError:
        return fallback
    return v if v >= minimum else fallback


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


# ---------------------------------------------------------------- avancement (v3.2 GUI)
def progress_fraction(counts: dict[str, int]) -> float:
    """Part des fichiers traités (analysés + en erreur) parmi ceux retenus par le plan."""
    total = sum(counts.get(k, 0) for k in ("pending", "queued", "done", "error"))
    if total <= 0:
        return 0.0
    done = counts.get("done", 0) + counts.get("error", 0)
    return max(0.0, min(1.0, done / total))


def campaign_title(db_path: str) -> str:
    """Nom lisible d'une campagne = nom du fichier SQLite sans extension."""
    from pathlib import PurePosixPath, PureWindowsPath

    pure = PureWindowsPath(db_path) if "\\" in db_path else PurePosixPath(db_path)
    return pure.stem or "campagne"


def pretty_list(raw: object) -> str:
    """Liste JSON (`["identite","rh"]`) ou texte → `identite, rh` ; vide → `—`."""
    items = _as_list(raw)
    return ", ".join(str(i) for i in items) if items else "—"


def pretty_amounts(raw: object) -> str:
    """Montants JSON (`[{"value":3766.65,"currency":"EUR","context":"Salaire brut"}]`)
    → `3 766,65 EUR (Salaire brut) ; …`."""
    items = _as_list(raw)
    parts: list[str] = []
    for item in items:
        if isinstance(item, dict):
            value = item.get("value")
            try:
                number = f"{float(str(value)):,.2f}".replace(",", " ").replace(".", ",")
            except ValueError:
                number = str(value or "")
            currency = str(item.get("currency") or "").strip()
            context = str(item.get("context") or "").strip()
            text = f"{number} {currency}".strip()
            parts.append(f"{text} ({context})" if context else text)
        else:
            parts.append(str(item))
    return " ; ".join(parts) if parts else "—"


def _as_list(raw: object) -> list[Any]:
    if raw is None or raw == "":
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        try:
            data = json.loads(raw)
        except ValueError:
            return [raw]
        return data if isinstance(data, list) else [data]
    return [raw]
