"""Tests du client LLM asynchrone contre le serveur OpenAI factice."""

from __future__ import annotations

import asyncio
import json
import socket
from pathlib import Path
from typing import Any

import httpx
import pytest

from docia.config import LLMConfig
from docia.llm import client as client_mod
from docia.llm.client import LLMClient, LLMRequestError, LLMResponseError, LLMTransportError
from docia.models import BlockFile, BlockSpec
from tests import fake_openai
from tests.fake_openai import FakeOpenAIServer

fake_server = fake_openai.fake_server
"""Fixture réexportée : pytest la résout par son nom dans ce module."""

SYSTEM_PROMPT = "Tu es un analyste documentaire."

REFS = ["dossier/rapport.md", "dossier/contrat.txt", "autre/note.txt"]


def make_block(
    tmp_path: Path, refs: list[str] | None = None, name: str = "block_001.md"
) -> BlockSpec:
    """Écrit un bloc `.md` minimal et rend son `BlockSpec`."""
    refs = refs if refs is not None else REFS
    body = "".join(f"## SOURCE: {ref}\n\nContenu factice de {ref}.\n\n" for ref in refs)
    path = tmp_path / name
    path.write_text(body, encoding="utf-8")
    files = [
        BlockFile(file_id=i + 1, file_ref=ref, content_version=1) for i, ref in enumerate(refs)
    ]
    return BlockSpec(path=path, files=files, tokens_estimated=100, tokens_with_margin=115)


def cfg_for(base_url: str, **kwargs: Any) -> LLMConfig:
    params: dict[str, Any] = {
        "base_url": base_url,
        "api_key": "sk-test-123",
        "model": "qwen38",
        "max_retries": 2,
        "timeout_s": 10,
    }
    params.update(kwargs)
    return LLMConfig(**params)


@pytest.fixture(autouse=True)
def _fast_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    """Le backoff réel (1 s, 2 s…) rendrait la suite inutilement lente."""
    monkeypatch.setattr(client_mod, "BACKOFF_BASE_S", 0.01)
    monkeypatch.setattr(client_mod, "BACKOFF_CAP_S", 0.05)


async def test_vllm_reponse_complete(fake_server: FakeOpenAIServer, tmp_path: Path) -> None:
    spec = make_block(tmp_path)
    cfg = cfg_for(fake_server.base_url_vllm, transport="vllm")
    async with LLMClient(cfg, SYSTEM_PROMPT) as client:
        result = await client.analyze_block(spec)

    payload = json.loads(result.content)
    assert [entry["file_ref"] for entry in payload["files"]] == REFS
    assert result.usage.model == "qwen38"
    assert result.usage.prompt_tokens > 0
    assert result.usage.completion_tokens > 0
    assert result.usage.latency_ms >= 0
    assert result.finish_reason == "stop"

    request = fake_server.last_record()
    assert request["path"] == "/v1/chat/completions"
    assert request["authorization"] == "Bearer sk-test-123"
    body = request["payload"]
    assert body["messages"][0]["role"] == "system"
    assert body["messages"][-1]["content"] == spec.text
    assert body["response_format"]["type"] == "json_schema"
    assert body["stream"] is False
    assert "files" not in body


async def test_openwebui_fichier_inline(fake_server: FakeOpenAIServer, tmp_path: Path) -> None:
    spec = make_block(tmp_path, name="block_042.md")
    cfg = cfg_for(fake_server.base_url_openwebui, transport="openwebui")
    async with LLMClient(cfg, SYSTEM_PROMPT) as client:
        result = await client.analyze_block(spec)

    payload = json.loads(result.content)
    assert len(payload["files"]) == len(REFS)

    request = fake_server.last_record()
    assert request["path"] == "/api/chat/completions"
    assert request["authorization"] == "Bearer sk-test-123"
    body = request["payload"]
    file_entry = body["files"][0]
    assert file_entry["type"] == "text"
    assert file_entry["context"] == "full"
    assert file_entry["name"] == "block_042.md"
    assert file_entry["file"]["data"]["content"] == spec.text
    assert body["messages"][-1]["content"] == "Analyse les fichiers fournis."
    assert body["response_format"]["json_schema"]["name"] == "docia"
    assert body["stream"] is False


async def test_max_tokens_calcule(fake_server: FakeOpenAIServer, tmp_path: Path) -> None:
    spec = make_block(tmp_path)
    cfg = cfg_for(fake_server.base_url_vllm, max_tokens_floor=500, max_tokens_per_file=400)
    async with LLMClient(cfg, SYSTEM_PROMPT) as client:
        assert client.max_tokens_for(spec) == 500 + 400 * 3
        await client.analyze_block(spec)
    assert fake_server.last_request()["max_tokens"] == 1700


async def test_max_tokens_plafonne(tmp_path: Path) -> None:
    spec = make_block(tmp_path)
    cfg = cfg_for("http://127.0.0.1:1/v1", max_tokens_cap=900)
    client = LLMClient(cfg, SYSTEM_PROMPT)
    # Le plafond porte sur le JSON ; le budget de raisonnement (activé par
    # défaut) s'ajoute par-dessus.
    expected = 900 + (cfg.thinking_budget_tokens if cfg.enable_thinking else 0)
    assert client.max_tokens_for(spec) == expected


async def test_retry_sur_500_puis_succes(fake_server: FakeOpenAIServer, tmp_path: Path) -> None:
    fake_server.mode = "http500_then_ok"
    spec = make_block(tmp_path)
    cfg = cfg_for(fake_server.base_url_vllm, max_retries=2)
    async with LLMClient(cfg, SYSTEM_PROMPT) as client:
        result = await client.analyze_block(spec)
    assert json.loads(result.content)["files"]
    assert fake_server.post_count == 2


async def test_http400_sans_retry(fake_server: FakeOpenAIServer, tmp_path: Path) -> None:
    fake_server.mode = "http400"
    spec = make_block(tmp_path)
    cfg = cfg_for(fake_server.base_url_vllm, max_retries=3)
    async with LLMClient(cfg, SYSTEM_PROMPT) as client:
        with pytest.raises(LLMRequestError) as excinfo:
            await client.analyze_block(spec)
    assert excinfo.value.status == 400
    assert "modèle inconnu" in excinfo.value.body
    assert fake_server.post_count == 1


async def test_timeout_epuise_les_tentatives(fake_server: FakeOpenAIServer, tmp_path: Path) -> None:
    fake_server.mode = "slow"
    spec = make_block(tmp_path)
    cfg = cfg_for(fake_server.base_url_vllm, timeout_s=1, max_retries=1)
    async with LLMClient(cfg, SYSTEM_PROMPT) as client:
        with pytest.raises(LLMTransportError):
            await client.analyze_block(spec)
    assert fake_server.post_count == 2


async def test_connect_error_retente_puis_echoue(tmp_path: Path) -> None:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    spec = make_block(tmp_path)
    cfg = cfg_for(f"http://127.0.0.1:{port}/v1", max_retries=1)
    async with LLMClient(cfg, SYSTEM_PROMPT) as client:
        with pytest.raises(LLMTransportError):
            await client.analyze_block(spec)


async def test_semaphore_limite_la_concurrence(
    fake_server: FakeOpenAIServer, tmp_path: Path
) -> None:
    fake_server.handler_delay = 0.15
    specs = [make_block(tmp_path, name=f"block_{i:03d}.md") for i in range(6)]
    cfg = cfg_for(fake_server.base_url_vllm, max_in_flight=2)
    async with LLMClient(cfg, SYSTEM_PROMPT) as client:
        results = await asyncio.gather(*(client.analyze_block(spec) for spec in specs))
    assert len(results) == 6
    assert fake_server.post_count == 6
    assert fake_server.max_in_flight_seen <= 2


async def test_health_ok(fake_server: FakeOpenAIServer) -> None:
    for base_url in (fake_server.base_url_vllm, fake_server.base_url_openwebui):
        async with LLMClient(cfg_for(base_url), SYSTEM_PROMPT) as client:
            assert await client.health() is True


async def test_health_serveur_absent() -> None:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    async with LLMClient(cfg_for(f"http://127.0.0.1:{port}/v1"), SYSTEM_PROMPT) as client:
        assert await client.health() is False


async def test_contenu_vide_leve_response_error() -> None:
    cfg = cfg_for("http://127.0.0.1:1/v1")
    client = LLMClient(cfg, SYSTEM_PROMPT)
    response = httpx.Response(
        200,
        json={"choices": [{"message": {"role": "assistant", "content": "   "}}]},
        request=httpx.Request("POST", "http://127.0.0.1:1/v1/chat/completions"),
    )
    with pytest.raises(LLMResponseError):
        client._to_result(response, 12)


async def test_usage_absent_vaut_zero() -> None:
    cfg = cfg_for("http://127.0.0.1:1/v1")
    client = LLMClient(cfg, SYSTEM_PROMPT)
    response = httpx.Response(
        200,
        json={"choices": [{"message": {"content": '{"files": []}'}}]},
        request=httpx.Request("POST", "http://127.0.0.1:1/v1/chat/completions"),
    )
    result = client._to_result(response, 42)
    assert result.usage.prompt_tokens == 0
    assert result.usage.completion_tokens == 0
    assert result.usage.latency_ms == 42
    assert result.finish_reason is None


def test_thinking_adds_template_kwargs_and_budget(tmp_path: Path) -> None:
    from docia.config import LLMConfig
    from docia.llm.client import LLMClient
    from docia.models import BlockFile, BlockSpec

    block = tmp_path / "b.md"
    block.write_text("## SOURCE: a.txt\n\ntexte\n", encoding="utf-8")
    spec = BlockSpec(
        path=block, files=[BlockFile(1, "a.txt", 1)], tokens_estimated=5, tokens_with_margin=6
    )
    cfg = LLMConfig(enable_thinking=True, thinking_budget_tokens=1234)
    client = LLMClient(cfg, "system")
    payload = client.build_payload(spec)
    assert payload["chat_template_kwargs"] == {"enable_thinking": True}
    assert (
        payload["max_tokens"]
        == min(cfg.max_tokens_cap, cfg.max_tokens_floor + cfg.max_tokens_per_file) + 1234
    )
    assert "chat_template_kwargs" not in LLMClient(
        LLMConfig(enable_thinking=False), "s"
    ).build_payload(spec)
