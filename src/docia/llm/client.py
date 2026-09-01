"""Client LLM asynchrone (httpx) pour les transports `vllm` et `openwebui`.

Un bloc `.md` = une requête. Le contenu des documents n'est jamais journalisé :
seuls le nom du bloc, le nombre de fichiers et les compteurs de tokens le sont.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from types import TracebackType
from typing import Any

import httpx

from docia.config import LLMConfig
from docia.llm.schema import response_format
from docia.models import BlockSpec, LLMResult, LLMUsage

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT_TOKENS = 1_500
_MIN_OUTPUT_TOKENS = 512

CONNECT_TIMEOUT_S = 10.0
"""Un serveur injoignable doit se voir vite ; la génération, elle, peut être longue."""

BACKOFF_BASE_S = 1.0
BACKOFF_CAP_S = 30.0


class LLMError(Exception):
    """Base de toutes les erreurs du client LLM."""


class LLMRequestError(LLMError):
    """Réponse HTTP 4xx définitive (hors 429) : ne pas retenter."""

    def __init__(self, status: int, body: str) -> None:
        super().__init__(f"HTTP {status} : {body}")
        self.status = status
        self.body = body


class BlockTooLongError(LLMError):
    """Le bloc, compté exactement par le serveur (`/tokenize`), ne tient pas dans le
    contexte avec sa réponse : il n'est PAS envoyé. Le pipeline re-découpe le
    fichier avec le ratio mesuré (réel / estimé) et renvoie."""

    def __init__(self, real_tokens: int, room: int, estimated: int) -> None:
        super().__init__(
            f"bloc trop long pour le contexte : {real_tokens} tokens réels > {room} disponibles "
            f"(estimation {estimated})"
        )
        self.real_tokens = real_tokens
        self.room = room
        self.estimated = estimated

    @property
    def ratio(self) -> float:
        """Réel / estimé — sert à re-découper avec un budget corrigé."""
        return self.real_tokens / max(1, self.estimated)


class LLMTransportError(LLMError):
    """Échec réseau ou serveur après épuisement des tentatives."""


class LLMResponseError(LLMError):
    """Réponse HTTP 200 mais inexploitable (structure ou contenu vide)."""


class LLMEmptyContentError(LLMResponseError):
    """`message.content` vide alors que le serveur a répondu 200.

    Comportement connu et transitoire du modèle servi (Qwen3 : raisonnement
    produit, puis rien après `</think>`). C'est donc un cas RENVOYABLE, traité
    dans la boucle de `_analyze` ; il ne devient définitif qu'après épuisement
    des tentatives."""


def _is_retryable_status(status: int) -> bool:
    """429 et 5xx sont transitoires ; les autres 4xx sont des erreurs de requête."""
    return status == 429 or 500 <= status <= 599


def _backoff_delay(attempt: int) -> float:
    """Backoff exponentiel 1, 2, 4… plafonné à 30 s, avec jitter (attempt commence à 0)."""
    delay = min(BACKOFF_BASE_S * (2.0**attempt), BACKOFF_CAP_S)
    return delay * (0.5 + random.random() / 2.0)


class LLMClient:
    """Envoie un bloc au modèle et rend la réponse brute (`LLMResult`)."""

    def __init__(self, cfg: LLMConfig, system_prompt: str) -> None:
        self.cfg = cfg
        self.system_prompt = system_prompt
        self._semaphore = asyncio.Semaphore(cfg.max_in_flight)
        self._client: httpx.AsyncClient | None = None

    # -- cycle de vie ----------------------------------------------------

    async def __aenter__(self) -> LLMClient:
        timeout = httpx.Timeout(
            connect=CONNECT_TIMEOUT_S,
            read=float(self.cfg.timeout_s),
            write=float(self.cfg.timeout_s),
            pool=float(self.cfg.timeout_s),
        )
        limits = httpx.Limits(
            max_connections=max(self.cfg.max_in_flight * 2, 10),
            max_keepalive_connections=max(self.cfg.max_in_flight, 5),
        )
        self._client = httpx.AsyncClient(timeout=timeout, limits=limits)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    @property
    def _http(self) -> httpx.AsyncClient:
        if self._client is None:
            raise LLMError(
                "LLMClient doit être utilisé comme gestionnaire de contexte (async with)"
            )
        return self._client

    # -- construction de la requête --------------------------------------

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.cfg.resolved_api_key()}",
            "Content-Type": "application/json",
        }

    def max_tokens_for(self, spec: BlockSpec) -> int:
        """Budget de sortie : plancher + quota par fichier, plafonné."""
        wanted = self.cfg.max_tokens_floor + self.cfg.max_tokens_per_file * len(spec.files)
        budget = min(self.cfg.max_tokens_cap, wanted)
        if self.cfg.enable_thinking:
            budget += self.cfg.thinking_budget_tokens
        return self.clamp_to_context(budget, spec)

    def clamp_to_context(self, max_tokens: int, spec: BlockSpec) -> int:
        """Jamais plus que la place restante sous `max_context_tokens` (= `--max-model-len`
        servi) après le bloc et le prompt système ; sinon vLLM refuse la requête."""
        room = self.cfg.max_context_tokens - spec.tokens_with_margin - _SYSTEM_PROMPT_TOKENS
        return max(_MIN_OUTPUT_TOKENS, min(max_tokens, room))

    def build_payload(self, spec: BlockSpec, *, max_tokens: int | None = None) -> dict[str, Any]:
        """Corps JSON de la requête, selon le transport configuré."""
        max_tokens = (
            self.max_tokens_for(spec)
            if max_tokens is None
            else self.clamp_to_context(max_tokens, spec)
        )
        payload: dict[str, Any] = {
            "model": self.cfg.model,
            "temperature": self.cfg.temperature,
            "max_tokens": max_tokens,
            "response_format": response_format(),
            **(
                {"chat_template_kwargs": self._template_kwargs()}
                if self.cfg.enable_thinking
                else {}
            ),
            "stream": False,
        }
        if (
            self.cfg.transport == "vllm"
            and self.cfg.enable_thinking
            and self.cfg.thinking_budget_tokens > 0
        ):
            # vLLM (≥ 0.11, `--reasoning-parser`) coupe le raisonnement à ce nombre de
            # tokens et force `</think>` : le JSON garde toujours sa place dans max_tokens.
            payload["thinking_token_budget"] = self.cfg.thinking_budget_tokens
        if self.cfg.transport == "openwebui":
            payload["messages"] = [
                {"role": "system", "content": self.system_prompt},
                {"role": "user", "content": "Analyse les fichiers fournis."},
            ]
            payload["files"] = [
                {
                    "type": "text",
                    "context": "full",
                    "name": spec.path.name,
                    "file": {"data": {"content": spec.text}},
                }
            ]
        else:
            payload["messages"] = [
                {"role": "system", "content": self.system_prompt},
                {"role": "user", "content": spec.text},
            ]
        return payload

    def _url(self, suffix: str) -> str:
        return f"{self.cfg.base_url.rstrip('/')}/{suffix.lstrip('/')}"

    # -- appels ----------------------------------------------------------

    def _template_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {"enable_thinking": True}
        if self.cfg.reasoning_effort:
            kwargs["reasoning_effort"] = self.cfg.reasoning_effort
        return kwargs

    def prompt_room(self, spec: BlockSpec) -> int:
        """Tokens de prompt admissibles pour ce bloc : contexte servi moins la réponse
        (raisonnement compris) moins une marge de gabarit."""
        return self.cfg.max_context_tokens - self.max_tokens_for(spec) - _SYSTEM_PROMPT_TOKENS // 3

    async def count_tokens(self, text: str) -> int | None:
        """Comptage exact par le serveur (`POST /tokenize`, vLLM) ; None si indisponible
        (autre transport, endpoint absent, erreur réseau) — on ne bloque jamais dessus."""
        if self.cfg.transport != "vllm":
            return None
        base = self.cfg.base_url.rstrip("/")
        root = base[: -len("/v1")] if base.endswith("/v1") else base
        try:
            response = await self._http.post(
                f"{root}/tokenize",
                json={"model": self.cfg.model, "prompt": text},
                headers=self._headers(),
            )
        except httpx.HTTPError:
            return None
        if response.status_code != 200:
            return None
        try:
            data = response.json()
        except ValueError:
            return None
        count = data.get("count") if isinstance(data, dict) else None
        return int(count) if isinstance(count, int) else None

    async def check_fits(self, spec: BlockSpec) -> None:
        """Lève `BlockTooLongError` si le comptage exact dépasse la place disponible."""
        real = await self.count_tokens(self.system_prompt + "\n" + spec.text)
        if real is None:
            return
        room = self.prompt_room(spec)
        if real > room:
            raise BlockTooLongError(real, room, spec.tokens_with_margin)

    async def analyze_block(self, spec: BlockSpec) -> LLMResult:
        """Analyse un bloc ; sérialisé par le sémaphore `max_in_flight`.

        Une réponse coupée par `max_tokens` (`finish_reason == "length"`, JSON
        incomplet) est renvoyée **une fois** avec un budget doublé — mais
        seulement si ce budget AUGMENTE réellement une fois passé par
        `clamp_to_context`. Quand le bloc occupe déjà tout le contexte, le
        doublement est absorbé par le clamp : renvoyer coûterait une génération
        complète pour rien, et le conseil « augmentez max_tokens_cap » serait
        faux (c'est le clamp qui contraint). On échoue alors tout de suite avec
        le vrai diagnostic.
        """
        async with self._semaphore:
            await self.check_fits(spec)
            result = await self._analyze(spec)
            if result.finish_reason != "length":
                return result
            first_budget = self.max_tokens_for(spec)
            doubled = self.clamp_to_context(first_budget * 2, spec)
            if doubled <= first_budget:
                raise LLMResponseError(self._truncation_diagnosis(spec, first_budget))
            logger.warning(
                "bloc %s : réponse tronquée à %d tokens (finish_reason=length) — renvoi avec %d",
                spec.path.name,
                first_budget,
                doubled,
            )
            result = await self._analyze(spec, max_tokens=doubled)
            if result.finish_reason == "length":
                raise LLMResponseError(
                    f"réponse tronquée même avec {doubled} tokens de sortie : augmentez "
                    "llm.thinking_budget_tokens / max_tokens_cap, ou réduisez blocks.block_tokens"
                )
            return result

    def _truncation_diagnosis(self, spec: BlockSpec, budget: int) -> str:
        """Message d'échec quand doubler le budget ne changerait rien (clamp saturé)."""
        room = self.cfg.max_context_tokens - spec.tokens_with_margin - _SYSTEM_PROMPT_TOKENS
        return (
            f"réponse tronquée à {budget} tokens et le budget ne peut pas augmenter : le bloc "
            f"occupe {spec.tokens_with_margin} des {self.cfg.max_context_tokens} tokens de "
            f"contexte, il ne reste que {max(0, room)} tokens pour la réponse — réduisez "
            "blocks.block_tokens (augmenter llm.max_tokens_cap n'y changerait rien)"
        )

    async def _analyze(self, spec: BlockSpec, *, max_tokens: int | None = None) -> LLMResult:
        payload = self.build_payload(spec, max_tokens=max_tokens)
        url = self._url("chat/completions")
        label = spec.path.name
        attempts = self.cfg.max_retries + 1
        extra_empty_attempt = 1
        """Le contenu vide est transitoire : au moins un renvoi, même si `max_retries` vaut 0."""
        last_error: Exception | None = None
        started = time.monotonic()

        attempt = -1
        while attempt + 1 < attempts:
            attempt += 1
            try:
                response = await self._http.post(url, json=payload, headers=self._headers())
            # `httpx.HTTPError` et non deux sous-classes : une coupure en plein corps de
            # réponse lève `RemoteProtocolError` (vLLM tué par l'OOM-killer, service
            # redémarré, reverse-proxy ou VPN qui lâche). Laisser passer une exception
            # httpx ferait tomber le run entier, hors de portée du `except LLMError` du
            # pipeline : tout échec de transport devient une `LLMError` renvoyable.
            except httpx.HTTPError as exc:
                last_error = exc
                logger.warning(
                    "bloc %s : tentative %d/%d en échec réseau (%s)",
                    label,
                    attempt + 1,
                    attempts,
                    type(exc).__name__,
                )
            else:
                if response.status_code == 200:
                    latency_ms = int((time.monotonic() - started) * 1000)
                    try:
                        result = self._to_result(response, latency_ms)
                    except LLMEmptyContentError as exc:
                        last_error = exc
                        if attempts == 1 and extra_empty_attempt:
                            attempts += 1
                            extra_empty_attempt = 0
                        logger.warning(
                            "bloc %s : tentative %d/%d, contenu vide "
                            "(le modèle n'a rendu que du raisonnement)",
                            label,
                            attempt + 1,
                            attempts,
                        )
                    else:
                        logger.info(
                            "bloc %s : réponse en %d ms (%d fichiers, tentative %d)",
                            label,
                            latency_ms,
                            len(spec.files),
                            attempt + 1,
                        )
                        return result
                else:
                    body = response.text[:500]
                    if not _is_retryable_status(response.status_code):
                        logger.warning("bloc %s : HTTP %d définitif", label, response.status_code)
                        raise LLMRequestError(response.status_code, _explain(response, body))
                    last_error = LLMRequestError(response.status_code, body)
                    logger.warning(
                        "bloc %s : tentative %d/%d, HTTP %d",
                        label,
                        attempt + 1,
                        attempts,
                        response.status_code,
                    )

            if attempt + 1 < attempts:
                await asyncio.sleep(_backoff_delay(attempt))

        if isinstance(last_error, LLMEmptyContentError):
            raise LLMResponseError(
                f"bloc {label} : contenu de réponse vide sur les {attempts} tentative(s) — "
                "le modèle a épuisé son allocation en raisonnement, sans écrire de réponse. "
                "Trois causes, dans l'ordre où il faut les vérifier : "
                "(1) le serveur est démarré sans « --reasoning-parser qwen3 », et il ignore "
                "alors « thinking_token_budget » — augmenter llm.thinking_budget_tokens ne "
                "change rien, c'est le symptôme le plus parlant ; "
                "(2) llm.max_context_tokens dépasse le « --max-model-len » réellement servi, "
                "et il ne reste plus de place pour la réponse (« docia bench » affiche "
                "maintenant le contexte servi) ; "
                "(3) le bloc est trop gros pour ce serveur — réduisez blocks.max_block_tokens. "
                "En dernier recours, désactivez llm.enable_thinking."
            ) from last_error
        raise LLMTransportError(f"bloc {label} : {attempts} tentatives en échec ({last_error})")

    def _to_result(self, response: httpx.Response, latency_ms: int) -> LLMResult:
        """Extrait contenu, usage et `finish_reason` d'une réponse OpenAI-compatible."""
        try:
            data = response.json()
        except ValueError as exc:
            raise LLMResponseError(f"réponse non JSON : {exc}") from exc
        if not isinstance(data, dict):
            raise LLMResponseError("réponse JSON de type inattendu (objet attendu)")

        choices = data.get("choices")
        if not isinstance(choices, list) or not choices:
            raise LLMResponseError("réponse sans `choices`")
        first = choices[0]
        if not isinstance(first, dict):
            raise LLMResponseError("`choices[0]` de type inattendu")
        message = first.get("message")
        content = message.get("content") if isinstance(message, dict) else None
        if not isinstance(content, str) or not content.strip():
            raise LLMEmptyContentError("contenu de réponse vide")

        usage_raw = data.get("usage")
        usage_dict: dict[str, Any] = usage_raw if isinstance(usage_raw, dict) else {}
        finish = first.get("finish_reason")
        # vLLM : `reasoning_content` (≤ 0.10) puis `reasoning` (≥ 0.11, nom OpenAI)
        reasoning = (
            (message.get("reasoning") or message.get("reasoning_content"))
            if isinstance(message, dict)
            else None
        )
        return LLMResult(
            content=content,
            reasoning_chars=len(reasoning) if isinstance(reasoning, str) else 0,
            usage=LLMUsage(
                prompt_tokens=_as_int(usage_dict.get("prompt_tokens")),
                completion_tokens=_as_int(usage_dict.get("completion_tokens")),
                latency_ms=latency_ms,
                model=self.cfg.model,
            ),
            finish_reason=finish if isinstance(finish, str) else None,
        )

    async def server_max_model_len(self) -> int | None:
        """`--max-model-len` réellement servi (`GET /v1/models`, vLLM) ; None si inconnu.

        `llm.max_context_tokens` ne pilote pas le serveur : il doit le DÉCRIRE. Le
        pipeline compare les deux au début du run et se borne à la valeur du serveur."""
        if self.cfg.transport != "vllm":
            return None
        try:
            response = await self._http.get(self._url("models"), headers=self._headers())
        except httpx.HTTPError:
            return None
        if response.status_code != 200:
            return None
        try:
            data = response.json()
        except ValueError:
            return None
        items = data.get("data") if isinstance(data, dict) else None
        for item in items if isinstance(items, list) else []:
            if isinstance(item, dict) and item.get("id") == self.cfg.model:
                value = item.get("max_model_len")
                return int(value) if isinstance(value, int) and value > 0 else None
        return None

    async def health(self) -> bool:
        """`GET {base_url}/models` : True si le serveur répond 200."""
        url = self._url("models")
        try:
            response = await self._http.get(url, headers=self._headers())
        except httpx.HTTPError as exc:
            logger.warning("health : %s injoignable (%s)", url, type(exc).__name__)
            return False
        return response.status_code == 200


def _as_int(value: object) -> int:
    """Compteur de tokens absent ou farfelu → 0.

    Un compteur négatif (serveur bogué, proxy qui recopie mal) serait sommé dans
    `report.prompt_tokens` puis écrit en base : on le borne à 0 comme les types
    non numériques."""
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return max(0, value)
    if isinstance(value, float):
        return max(0, int(value)) if value == value and abs(value) != float("inf") else 0
    return 0


def _explain(response: httpx.Response, body: str) -> str:
    """Complète le corps d'une réponse définitive quand le code seul n'est pas parlant.

    httpx ne suit pas les redirections par défaut : une 30x (reverse-proxy qui
    ajoute une barre oblique finale, bascule http→https) tomberait sinon dans le
    même sac que les 4xx, avec un corps généralement vide."""
    status = response.status_code
    if 300 <= status < 400:
        target = response.headers.get("location", "destination non indiquée")
        return (
            f"redirection non suivie vers « {target} » — corrigez llm.base_url "
            f"(barre oblique finale, http/https, préfixe /v1) ; corps : {body or '(vide)'}"
        )
    return body
