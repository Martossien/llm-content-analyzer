"""Comptage exact des tokens par le serveur (`POST /tokenize`, vLLM).

Deux visages d'un même appel : `LLMClient.count_tokens` (asynchrone, contrôle
avant envoi) et `ServerTokenCounter` (synchrone, pour le builder qui tourne dans
le fil de l'extraction et décide s'il découpe un fichier). Le serveur ne sait
pas toujours compter (open-webui, endpoint absent, réseau coupé) : les deux
rendent alors None, et l'appelant se rabat sur son estimation — on ne bloque
jamais une campagne sur un comptage.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

from docia.config import LLMConfig

logger = logging.getLogger(__name__)

COUNT_TIMEOUT_S = 60.0
"""Patience d'un comptage synchrone : tokeniser 300 K tokens prend une seconde,
mais le serveur peut être occupé à préremplir pour d'autres."""


def tokenize_url(cfg: LLMConfig) -> str | None:
    """URL de l'endpoint `/tokenize` de vLLM (hors préfixe `/v1`), None si le
    transport ne l'offre pas."""
    if cfg.transport != "vllm":
        return None
    base = cfg.base_url.rstrip("/")
    root = base[: -len("/v1")] if base.endswith("/v1") else base
    return f"{root}/tokenize"


def parse_token_count(response: httpx.Response) -> int | None:
    """Le champ `count` d'une réponse `/tokenize` ; None sur tout autre cas."""
    if response.status_code != 200:
        return None
    try:
        data: Any = response.json()
    except ValueError:
        return None
    count = data.get("count") if isinstance(data, dict) else None
    return int(count) if isinstance(count, int) else None


class ServerTokenCounter:
    """Compteur **synchrone** (`blocks.policy.TokenCounter`), une connexion
    persistante, à fermer après le lot (`close()` ou `with`).

    Mémorise si le serveur a répondu qu'il ne sait pas compter (404, transport
    inadapté) pour ne pas réessayer à chaque fichier ; une erreur réseau
    passagère, elle, n'éteint pas le compteur."""

    def __init__(self, cfg: LLMConfig, headers: dict[str, str] | None = None) -> None:
        self._url = tokenize_url(cfg)
        self._model = cfg.model
        self._headers = headers or {}
        self._http: httpx.Client | None = None
        self._unavailable = self._url is None

    @property
    def available(self) -> bool:
        return not self._unavailable

    def __call__(self, text: str) -> int | None:
        if self._unavailable or self._url is None:
            return None
        if self._http is None:
            self._http = httpx.Client(timeout=COUNT_TIMEOUT_S)
        try:
            response = self._http.post(
                self._url, json={"model": self._model, "prompt": text}, headers=self._headers
            )
        except httpx.HTTPError as exc:
            logger.warning("comptage exact indisponible (%s) : estimation locale", exc)
            return None
        if response.status_code in (404, 405, 501):
            logger.warning(
                "le serveur n'offre pas /tokenize (HTTP %d) : découpage sur l'estimation locale",
                response.status_code,
            )
            self._unavailable = True
            return None
        return parse_token_count(response)

    def close(self) -> None:
        if self._http is not None:
            self._http.close()
            self._http = None

    def __enter__(self) -> ServerTokenCounter:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
