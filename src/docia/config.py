"""Configuration `docia.toml` (tomllib, stdlib) → `Config`.

Un seul fichier, des valeurs par défaut sûres, une validation explicite. Les
secrets (clé API) peuvent venir de l'environnement (`DOCIA_API_KEY`) pour ne
pas traîner dans un fichier versionné.
"""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any

DEFAULT_CONFIG_NAME = "docia.toml"


@dataclass
class LLMConfig:
    transport: str = "vllm"
    """`vllm` (OpenAI-compatible direct) ou `openwebui` (API native, fichiers texte inline)."""
    base_url: str = "http://127.0.0.1:8000/v1"
    """vLLM : `http://host:8000/v1` — open-webui : `http://host:8080/api`."""
    api_key: str = ""
    """Vide = lue dans `DOCIA_API_KEY` ; `dummy` suffit pour vLLM sans `--api-key`."""
    model: str = "qwen38"
    max_in_flight: int = 8
    timeout_s: int = 900
    max_retries: int = 3
    temperature: float = 0.0
    max_tokens_per_file: int = 700
    """Budget de sortie par fichier (5 domaines + justifications ≈ 500–600 tokens)."""
    max_tokens_floor: int = 800
    max_tokens_cap: int = 32000
    max_context_tokens: int = 250_000
    """Plafond du modèle servi (tokens avec marge, prompt compris) — aligner sur
    `--max-model-len` (servir le contexte natif du modèle, 262144 pour Qwen3.8). Un fichier seul au-delà n'est ni
    tronqué ni mis en erreur : il est découpé en segments complets analysés
    séparément puis agrégés (sévérité = max des segments)."""
    enable_thinking: bool = True
    """Raisonnement activé par défaut (décision du 30/08 : c'est le point fort du
    modèle, et le même serveur sert d'autres usages) : envoie
    `chat_template_kwargs.enable_thinking` et réserve `thinking_budget_tokens`
    en plus dans `max_tokens`. Le JSON reste exigé dans la réponse finale ; un
    bloc `<think>…</think>` resté dans le contenu est ignoré."""
    thinking_budget_tokens: int = 12_000
    reasoning_effort: str = "low"
    """Effort de raisonnement demandé au modèle (Qwen3.8 : `low` / `medium` / `xhigh`,
    vide = défaut du modèle). `low` garde l'essentiel du bénéfice pour une
    classification sans les milliers de tokens d'un raisonnement long."""

    def resolved_api_key(self) -> str:
        return self.api_key or os.environ.get("DOCIA_API_KEY", "") or "dummy"


@dataclass
class BlocksConfig:
    block_tokens: int = 32_000
    """Plafond par bloc (tokens avec marge). 16–64K recommandé (banc du 30/08)."""
    margin: float = 0.15
    tokenizer_engine: str = "approx"
    """`approx` | `mistral` | `openai` (moteurs DocFuse)."""
    batch_files: int = 200
    """Fichiers passés à DocFuse par appel (extraction parallèle interne)."""
    work_dir: str = ""
    """Dossier des blocs `.md` ; vide = `<db>.blocks/` à côté de la base."""
    max_file_tokens: int = 0
    """Budget (tokens avec marge) au-delà duquel un fichier seul est découpé en
    segments. 0 = dérivé du pipeline : `llm.max_context_tokens` moins une réserve
    pour le prompt et la réponse."""
    keep_blocks: bool = True


@dataclass
class FilterConfig:
    excluded_extensions: list[str] = field(
        default_factory=lambda: [
            ".tmp",
            ".temp",
            ".log",
            ".bak",
            ".cache",
            ".zip",
            ".7z",
            ".rar",
            ".gz",
            ".iso",
            ".exe",
            ".dll",
            ".sys",
            ".msi",
            ".lnk",
            ".db",
            ".sqlite",
            ".mdb",
            ".ldb",
            ".jpg",
            ".jpeg",
            ".png",
            ".gif",
            ".bmp",
            ".tif",
            ".tiff",
            ".ico",
            ".svg",
            ".mp3",
            ".mp4",
            ".avi",
            ".mov",
            ".wav",
            ".mkv",
        ]
    )
    min_size_bytes: int = 100
    max_size_bytes: int = 100 * 1024 * 1024
    excluded_dir_markers: list[str] = field(
        default_factory=lambda: [
            "\\admin$\\",
            "\\Windows\\",
            "\\Program Files",
            "\\$RECYCLE.BIN\\",
            "\\System Volume Information\\",
            "\\AppData\\",
        ]
    )


@dataclass
class Config:
    db_path: str = "docia.sqlite"
    llm: LLMConfig = field(default_factory=LLMConfig)
    blocks: BlocksConfig = field(default_factory=BlocksConfig)
    filter: FilterConfig = field(default_factory=FilterConfig)
    prompt_path: str = ""
    """Vide = prompt embarqué `docia/prompts/docia_v3.md`."""

    def validate(self) -> list[str]:
        errors: list[str] = []
        if self.llm.transport not in ("vllm", "openwebui"):
            errors.append(
                f"llm.transport doit être 'vllm' ou 'openwebui' (valeur: {self.llm.transport})"
            )
        if not self.llm.base_url.startswith(("http://", "https://")):
            errors.append(
                f"llm.base_url doit commencer par http(s):// (valeur: {self.llm.base_url})"
            )
        if not (1 <= self.llm.max_in_flight <= 256):
            errors.append(
                f"llm.max_in_flight doit être entre 1 et 256 (valeur: {self.llm.max_in_flight})"
            )
        if self.llm.timeout_s < 10:
            errors.append("llm.timeout_s doit être >= 10")
        if not (1_000 <= self.blocks.block_tokens <= 1_000_000):
            errors.append(
                f"blocks.block_tokens hors plage 1000–1000000 (valeur: {self.blocks.block_tokens})"
            )
        if not (0.0 <= self.blocks.margin <= 1.0):
            errors.append("blocks.margin doit être entre 0 et 1")
        if self.blocks.tokenizer_engine not in ("approx", "mistral", "openai"):
            errors.append(f"blocks.tokenizer_engine inconnu : {self.blocks.tokenizer_engine}")
        if self.blocks.batch_files < 1:
            errors.append("blocks.batch_files doit être >= 1")
        if self.llm.thinking_budget_tokens < 0:
            errors.append("llm.thinking_budget_tokens doit être >= 0")
        if self.llm.reasoning_effort not in ("", "low", "medium", "high", "xhigh"):
            errors.append(f"llm.reasoning_effort inconnu : {self.llm.reasoning_effort}")
        if self.llm.max_context_tokens < self.blocks.block_tokens:
            errors.append(
                "llm.max_context_tokens doit être >= blocks.block_tokens "
                f"({self.llm.max_context_tokens} < {self.blocks.block_tokens})"
            )
        return errors

    def work_dir(self) -> Path:
        if self.blocks.work_dir:
            return Path(self.blocks.work_dir)
        db = Path(self.db_path)
        return db.with_name(f"{db.stem}.blocks")


def _merge(target: Any, data: dict[str, Any], section: str) -> None:
    known = {f.name: f for f in fields(target)}
    for key, value in data.items():
        if key not in known:
            raise ValueError(f"[{section}] clé inconnue : {key}")
        current = getattr(target, key)
        if isinstance(current, bool):
            if not isinstance(value, bool):
                raise ValueError(f"[{section}] {key} doit être un booléen")
        elif isinstance(current, int) and not isinstance(value, int):
            raise ValueError(f"[{section}] {key} doit être un entier")
        elif isinstance(current, float) and not isinstance(value, int | float):
            raise ValueError(f"[{section}] {key} doit être un nombre")
        elif isinstance(current, str) and not isinstance(value, str):
            raise ValueError(f"[{section}] {key} doit être une chaîne")
        elif isinstance(current, list) and not isinstance(value, list):
            raise ValueError(f"[{section}] {key} doit être une liste")
        setattr(target, key, float(value) if isinstance(current, float) else value)


def load_config(path: Path | None) -> Config:
    """Charge `docia.toml` ; `None` ou fichier absent → défauts.

    Raises:
        ValueError: clé inconnue ou type invalide (on ne dégrade pas en silence :
            une config fausse doit arrêter un batch de 50 000 fichiers avant de
            partir).
    """
    config = Config()
    if path is None or not path.exists():
        return config
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    for section_name, target in (
        ("llm", config.llm),
        ("blocks", config.blocks),
        ("filter", config.filter),
    ):
        section = data.pop(section_name, None)
        if section is not None:
            if not isinstance(section, dict):
                raise ValueError(f"[{section_name}] doit être une table")
            _merge(target, section, section_name)
    _merge(config, data, "racine")
    return config


def default_toml() -> str:
    """Contenu d'un `docia.toml` de départ (commande `docia init`)."""
    return """# Doc-IA analyzer v3 — configuration
db_path = "docia.sqlite"
# prompt_path = "mon_prompt.md"   # vide = prompt embarqué

[llm]
transport = "vllm"                 # "vllm" (direct) ou "openwebui" (API native, auth par clé sk-)
base_url = "http://127.0.0.1:8000/v1"   # open-webui : "http://serveur:8080/api"
api_key = ""                       # vide = variable DOCIA_API_KEY (ou "dummy" pour vLLM)
model = "qwen38"
max_in_flight = 8
timeout_s = 900
max_retries = 3
max_context_tokens = 250000        # ≈ --max-model-len du serveur (262144 = natif Qwen3.8) ; au-delà, fichier découpé en segments agrégés
enable_thinking = true             # raisonnement activé (qualité) ; false pour du volume pur
thinking_budget_tokens = 12000     # réservé en plus de max_tokens pour le raisonnement
reasoning_effort = "low"           # low | medium | xhigh | "" (défaut du modèle) — Qwen3.8

[blocks]
block_tokens = 32000               # 16–64K recommandé
margin = 0.15
tokenizer_engine = "approx"        # approx | mistral | openai
batch_files = 200
work_dir = ""                      # vide = <db>.blocks/
keep_blocks = true

[filter]
min_size_bytes = 100
max_size_bytes = 104857600
"""
