"""Serveur OpenAI factice (stdlib) pour tester le client LLM sans modèle réel.

Expose `POST /v1/chat/completions` (transport vllm) et `POST /api/chat/completions`
(transport openwebui), plus `GET /v1/models` et `GET /api/models`. Le comportement
est piloté par l'attribut `mode` de l'instance.
"""

from __future__ import annotations

import json
import re
import threading
import time
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, cast

import pytest

from docia.llm.schema import FINANCE_TYPES, LEGAL_TYPES, RGPD_LEVELS, SECURITY_CLASSES

SOURCE_RE = re.compile(r"^## SOURCE: (.+)$", re.MULTILINE)

MODES = (
    "ok",
    "drop_last",
    "garbage",
    "http500_then_ok",
    "length_once",
    "length_always",
    "http400",
    "slow",
    "extra_ref",
    "bad_enum",
)

SLOW_DELAY_S = 2.0


def extract_sources(text: str) -> list[str]:
    """Les `file_ref` déclarés par les lignes `## SOURCE:` du bloc."""
    return [match.strip() for match in SOURCE_RE.findall(text)]


def block_text_from_payload(payload: dict[str, Any]) -> str:
    """Retrouve le texte du bloc : message utilisateur (vllm) ou fichier inline (openwebui)."""
    files = payload.get("files")
    if isinstance(files, list) and files:
        first = files[0]
        if isinstance(first, dict):
            data = first.get("file", {}).get("data", {})
            content = data.get("content")
            if isinstance(content, str):
                return content
    messages = payload.get("messages") or []
    if messages:
        last = messages[-1]
        if isinstance(last, dict) and isinstance(last.get("content"), str):
            return cast(str, last["content"])
    return ""


def make_entry(file_ref: str, *, bad_enum: bool = False) -> dict[str, Any]:
    """Une entrée d'analyse conforme au schéma (ou volontairement hors énumération)."""
    return {
        "file_ref": file_ref,
        "resume": f"Résumé de {file_ref}",
        "security": {
            "classification": "C9" if bad_enum else SECURITY_CLASSES[1],
            "confidence": 80,
            "justification": "Document interne sans donnée sensible.",
        },
        "rgpd": {"risk_level": RGPD_LEVELS[1], "data_types": ["nom"], "confidence": 70},
        "finance": {
            "document_type": FINANCE_TYPES[1],
            "amounts": [{"value": 1234.5, "currency": "EUR", "context": "total TTC"}],
            "confidence": 60,
        },
        "legal": {"contract_type": LEGAL_TYPES[0], "parties": ["ACME"], "confidence": 50},
        "retention": {
            "required": True,
            "years": 10,
            "basis": "fiscal",
            "justification": "Pièce comptable.",
            "confidence": 65,
        },
    }


def build_content(sources: list[str], mode: str) -> str:
    """Le JSON que le modèle est censé renvoyer, selon le mode."""
    if mode == "garbage":
        return "Voici mon analyse : {files: [ceci n'est pas du JSON"
    refs = sources[:-1] if mode == "drop_last" and sources else list(sources)
    entries = [
        make_entry(ref, bad_enum=(mode == "bad_enum" and i == 0)) for i, ref in enumerate(refs)
    ]
    if mode == "extra_ref":
        entries.append(make_entry("inconnu/fichier_fantome.txt"))
    return json.dumps({"files": entries}, ensure_ascii=False)


class FakeOpenAIServer(ThreadingHTTPServer):
    """Serveur HTTP de test, état partagé entre threads de requêtes."""

    daemon_threads = True
    allow_reuse_address = True

    def __init__(self) -> None:
        super().__init__(("127.0.0.1", 0), _Handler)
        self.tokens_per_char: float = 0.25
        self.max_model_len: int | None = None
        self.tokenize_calls: int = 0
        self.mode: str = "ok"
        self.handler_delay: float = 0.0
        """Retard artificiel (s) pour observer la concurrence."""
        self.requests: list[dict[str, Any]] = []
        """Corps JSON bruts des requêtes POST reçues, dans l'ordre."""
        self.records: list[dict[str, Any]] = []
        """Même chose enrichie : `path`, `authorization`, `payload`."""
        self.post_count: int = 0
        self.in_flight: int = 0
        self.max_in_flight_seen: int = 0
        self.lock = threading.Lock()

    @property
    def base_url_vllm(self) -> str:
        host, port = self.server_address[0], self.server_address[1]
        return f"http://{host!s}:{port}/v1"

    @property
    def base_url_openwebui(self) -> str:
        host, port = self.server_address[0], self.server_address[1]
        return f"http://{host!s}:{port}/api"

    def reset(self) -> None:
        with self.lock:
            self.requests.clear()
            self.records.clear()
            self.post_count = 0
            self.in_flight = 0
            self.max_in_flight_seen = 0

    def last_request(self) -> dict[str, Any]:
        """Corps JSON de la dernière requête."""
        with self.lock:
            return self.requests[-1]

    def last_record(self) -> dict[str, Any]:
        """Dernière requête avec son chemin et son en-tête d'authentification."""
        with self.lock:
            return self.records[-1]


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    @property
    def state(self) -> FakeOpenAIServer:
        return cast(FakeOpenAIServer, self.server)

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002, ARG002 - signature imposée
        """Silence : les tests n'ont pas besoin du journal HTTP."""

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_text(self, status: int, text: str) -> None:
        body = text.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        if self.path in ("/v1/models", "/api/models"):
            entry: dict[str, Any] = {"id": "qwen38"}
            if self.state.max_model_len is not None:
                entry["max_model_len"] = self.state.max_model_len
            self._send_json(200, {"object": "list", "data": [entry]})
        else:
            self._send_text(404, "not found")

    def do_POST(self) -> None:  # noqa: N802
        if self.path == "/tokenize":
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)
            payload = json.loads(raw.decode("utf-8")) if raw else {}
            prompt = str(payload.get("prompt", ""))
            with self.state.lock:
                self.state.tokenize_calls += 1
            # `tokens_per_char` : 0.25 ≈ octets/4 ; plus haut = tokenizer « gourmand »
            self._send_json(200, {"count": int(len(prompt) * self.state.tokens_per_char)})
            return
        if self.path not in ("/v1/chat/completions", "/api/chat/completions"):
            self._send_text(404, "not found")
            return
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length)
        payload = json.loads(raw.decode("utf-8")) if raw else {}
        state = self.state

        with state.lock:
            state.post_count += 1
            attempt = state.post_count
            state.in_flight += 1
            state.max_in_flight_seen = max(state.max_in_flight_seen, state.in_flight)
            state.requests.append(payload)
            state.records.append(
                {
                    "path": self.path,
                    "authorization": self.headers.get("Authorization"),
                    "payload": payload,
                }
            )
        try:
            self._respond(payload, state, attempt)
        finally:
            with state.lock:
                state.in_flight -= 1

    def _respond(self, payload: dict[str, Any], state: FakeOpenAIServer, attempt: int) -> None:
        mode = state.mode
        if state.handler_delay:
            time.sleep(state.handler_delay)
        if mode == "http400":
            self._send_text(400, "requête invalide : modèle inconnu")
            return
        if mode == "http500_then_ok" and attempt == 1:
            self._send_text(500, "erreur interne")
            return
        if mode == "slow":
            time.sleep(SLOW_DELAY_S)

        block = block_text_from_payload(payload)
        content = build_content(extract_sources(block), mode)
        finish_reason = "stop"
        if mode == "length_always" or (mode == "length_once" and attempt == 1):
            content = content[: len(content) // 2]  # JSON coupé net, comme un max_tokens épuisé
            finish_reason = "length"
        body: dict[str, Any] = {
            "id": "chatcmpl-fake",
            "object": "chat.completion",
            "model": payload.get("model", "qwen38"),
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": content},
                    "finish_reason": finish_reason,
                }
            ],
            "usage": {
                "prompt_tokens": len(block) // 4,
                "completion_tokens": len(content) // 4,
                "total_tokens": (len(block) + len(content)) // 4,
            },
        }
        if self.path.startswith("/api/"):
            body["sources"] = []
        self._send_json(200, body)


@pytest.fixture
def fake_server() -> Iterator[FakeOpenAIServer]:
    """Serveur factice démarré sur un port libre, arrêté en fin de test."""
    server = FakeOpenAIServer()
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
