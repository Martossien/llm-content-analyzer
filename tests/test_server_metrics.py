"""`/metrics` de vLLM : lecture des préemptions (`llm/server.py`, `LLMClient.preemptions`)."""

from __future__ import annotations

import asyncio

from docia.config import LLMConfig
from docia.llm.client import LLMClient
from docia.llm.server import parse_preemptions, server_url
from tests.fake_openai import FakeOpenAIServer

PAGE = """# HELP vllm:num_preemptions_total Cumulative number of preemption from the engine.
# TYPE vllm:num_preemptions_total counter
vllm:num_preemptions_total{engine="0",model_name="qwen38"} 12.0
vllm:num_preemptions_total{engine="1",model_name="qwen38"} 3.0
vllm:num_preemptions_total_created{model_name="qwen38"} 1.7e+09
vllm:num_requests_waiting{model_name="qwen38"} 0.0
"""


def test_parse_preemptions_somme_les_moteurs_et_ignore_les_derives() -> None:
    assert parse_preemptions(PAGE) == 15
    assert parse_preemptions("vllm:num_requests_waiting 0.0\n") is None
    assert parse_preemptions("") is None


def test_server_url_selon_le_transport() -> None:
    assert server_url(LLMConfig(base_url="http://h:8000/v1"), "metrics") == "http://h:8000/metrics"
    assert server_url(LLMConfig(transport="openwebui"), "metrics") is None


def test_client_lit_les_preemptions_ou_rend_none(fake_server: FakeOpenAIServer) -> None:
    async def lire(cfg: LLMConfig) -> int | None:
        async with LLMClient(cfg, "prompt") as client:
            return await client.preemptions()

    fake_server.preemptions = 4
    assert asyncio.run(lire(LLMConfig(base_url=fake_server.base_url_vllm))) == 4
    fake_server.preemptions = None  # pas de page /metrics (autre serveur)
    assert asyncio.run(lire(LLMConfig(base_url=fake_server.base_url_vllm))) is None
    assert asyncio.run(lire(LLMConfig(base_url="http://127.0.0.1:1/v1"))) is None
