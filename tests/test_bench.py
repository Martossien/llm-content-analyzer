"""Banc de vitesse (`docia.bench`) contre le serveur OpenAI factice."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from docia.bench import BenchReport, build_bench_blocks, run_bench, synthetic_french
from docia.cli_tools import register
from docia.config import Config


def _config(tmp_path: Path, base_url: str) -> Config:
    cfg = Config(db_path=str(tmp_path / "docia.sqlite"))
    cfg.llm.base_url = base_url
    cfg.llm.transport = "vllm"
    cfg.llm.max_in_flight = 3
    cfg.llm.timeout_s = 30
    cfg.llm.max_retries = 1
    cfg.llm.enable_thinking = False
    return cfg


def test_synthetic_corpus_is_french_and_deterministic() -> None:
    text = synthetic_french(2_000, seed=7)
    assert len(text) >= 2_000
    assert text == synthetic_french(2_000, seed=7)
    assert "é" in text  # du français, pas du lorem ipsum


def test_blocks_carry_one_source_line_per_file(tmp_path: Path) -> None:
    specs = build_bench_blocks(tmp_path, blocks=2, block_tokens=1_000, files_per_block=3)
    assert len(specs) == 2
    for spec in specs:
        text = spec.path.read_text(encoding="utf-8")
        assert len(spec.files) == 3
        for block_file in spec.files:
            assert text.count(f"## SOURCE: {block_file.file_ref}\n") == 1
    refs = {bf.file_ref for spec in specs for bf in spec.files}
    assert len(refs) == 6  # pas de collision entre blocs


def test_bench_measures_throughput(tmp_path: Path, fake_server) -> None:  # type: ignore[no-untyped-def]
    cfg = _config(tmp_path, fake_server.base_url_vllm)
    report = run_bench(cfg, blocks=3, block_tokens=1_500)

    assert report.ok
    assert (report.json_valid, report.blocks_sent) == (3, 3)
    assert report.files_analyzed == report.files_expected
    assert report.files_missing == 0
    assert report.prefill_tok_s > 0
    assert report.decode_tok_s > 0
    assert report.latency_max_ms >= report.latency_min_ms >= 0
    assert report.files_per_hour > 0
    assert report.errors == []

    text = "\n".join(report.as_lines())
    assert "fichiers/heure" in text
    assert "JSON valides 3/3" in text
    assert "tokens/s prefill" in text
    assert report.as_dict()["json_valid"] == 3


def test_bench_reports_unreachable_server_without_raising(tmp_path: Path) -> None:
    cfg = _config(tmp_path, "http://127.0.0.1:1/v1")
    report = run_bench(cfg, blocks=2, block_tokens=800)

    assert report.ok is False
    assert "injoignable" in report.message
    assert report.blocks == []
    assert "ÉCHEC" in "\n".join(report.as_lines())


def test_bench_failure_shows_server_errors() -> None:
    """Un banc en échec doit DIRE pourquoi : « aucun bloc exploitable » sans le message
    du serveur ne permet ni à l'utilisateur ni au support de diagnostiquer quoi que ce soit.
    """
    report = BenchReport(model="m", base_url="http://x/v1", transport="vllm", blocks_sent=6)
    report.ok = False
    report.message = "aucun bloc exploitable (6 erreur(s)) — HTTP 400 : contexte dépassé"
    report.errors = [f"bench_{i:03d}.md : HTTP 400 : contexte dépassé" for i in range(1, 7)]

    lines = report.as_lines()
    text = "\n".join(lines)
    assert "ÉCHEC" in text
    assert "HTTP 400 : contexte dépassé" in text
    assert sum(1 for line in lines if line.startswith("  erreur :")) == 5
    assert "et 1 autre(s) erreur(s)" in text


def test_bench_measures_thinking_overhead(tmp_path: Path, fake_server) -> None:  # type: ignore[no-untyped-def]
    cfg = _config(tmp_path, fake_server.base_url_vllm)
    cfg.llm.enable_thinking = True
    report = run_bench(cfg, blocks=2, block_tokens=1_000, files_per_block=2)

    assert report.ok
    assert report.thinking_enabled
    # Le faux serveur ne raisonne pas : on vérifie que les deux mesures existent.
    assert report.thinking_measured
    assert report.thinking_completion_tokens > 0
    assert report.plain_completion_tokens > 0
    assert "thinking :" in "\n".join(report.as_lines())
    # Un aller-retour de plus que les blocs mesurés (le bloc témoin sans thinking).
    assert fake_server.post_count == 3
    assert fake_server.requests[0]["chat_template_kwargs"]["enable_thinking"] is True
    assert "chat_template_kwargs" not in fake_server.requests[-1]


def test_bench_honours_in_flight(tmp_path: Path, fake_server) -> None:  # type: ignore[no-untyped-def]
    cfg = _config(tmp_path, fake_server.base_url_vllm)
    fake_server.handler_delay = 0.15
    report = run_bench(cfg, blocks=4, block_tokens=600, files_per_block=1, in_flight=2)

    assert report.in_flight == 2
    assert report.ok
    assert fake_server.max_in_flight_seen <= 2


def test_register_adds_bench_and_quick(tmp_path: Path, fake_server, capsys) -> None:  # type: ignore[no-untyped-def]
    """`register()` branche les deux sous-commandes et rend leurs gestionnaires."""
    parser = argparse.ArgumentParser(prog="docia")
    sub = parser.add_subparsers(dest="command", required=True)
    handlers = register(sub)
    assert set(handlers) == {"bench", "quick"}

    args = parser.parse_args(["bench", "--blocks", "2", "--block-tokens", "800", "--json"])
    assert (args.blocks, args.block_tokens, args.json) == (2, 800, True)
    cfg = _config(tmp_path, fake_server.base_url_vllm)
    assert handlers["bench"](args, cfg) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True
    assert payload["json_valid"] == 2

    args = parser.parse_args(["quick", str(tmp_path)])
    assert args.paths == [tmp_path]
    assert args.keep_db is None


def test_bench_survit_a_une_coupure_de_flux(tmp_path: Path, fake_server) -> None:  # type: ignore[no-untyped-def]
    """Le serveur coupe en plein corps de réponse (`RemoteProtocolError`) :
    `docia bench` doit rendre un rapport en échec, jamais une trace httpx."""
    fake_server.mode = "cut_always"
    cfg = _config(tmp_path, fake_server.base_url_vllm)
    report = run_bench(cfg, blocks=2, block_tokens=600, files_per_block=1)

    assert report.ok is False
    assert report.json_valid == 0
    assert len(report.errors) == 2
    assert "tentatives en échec" in report.message
    assert "ÉCHEC" in "\n".join(report.as_lines())


def test_one_block_rattrape_une_erreur_httpx_nue(tmp_path: Path) -> None:
    """Défense en profondeur : même une `httpx.HTTPError` qui remonterait du
    client devient une erreur de bloc, pas une exception qui tue le banc."""
    import asyncio

    import httpx

    from docia.bench import _one_block
    from docia.models import BlockFile, BlockSpec

    class _Casse:
        async def analyze_block(self, spec: BlockSpec) -> None:  # noqa: ARG002
            raise httpx.RemoteProtocolError("peer closed connection")

    block = tmp_path / "b.md"
    block.write_text("## SOURCE: a.txt\n\ntexte\n", encoding="utf-8")
    spec = BlockSpec(
        path=block, files=[BlockFile(1, "a.txt", 1)], tokens_estimated=5, tokens_with_margin=6
    )
    outcome = asyncio.run(_one_block(_Casse(), spec))  # type: ignore[arg-type]
    assert outcome.ok is False
    assert "RemoteProtocolError" in outcome.error
