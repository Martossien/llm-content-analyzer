"""Bout en bout : CSV SMBeagle → base → plan → blocs DocFuse → LLM factice → analyses → reprise.

Le serveur OpenAI factice (`tests/fake_openai.py`) rend une entrée par ligne
`## SOURCE:` du bloc reçu ; il sert pour les deux transports.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from docia.cli import main
from docia.config import Config
from docia.db import Database
from docia.filter import plan_files
from docia.ingest.smbeagle_csv import import_csv
from docia.models import FileStatus
from docia.pipeline import run_pipeline
from tests.fake_openai import FakeOpenAIServer

HEADER = (
    "Name,Host,Extension,Username,Hostname,UNCDirectory,CreationTime,LastWriteTime,Readable,"
    "Writeable,Deletable,DirectoryType,Base,FileSize,AccessTime,FileAttributes,Owner,FastHash,FileSignature"
)


def _csv_line(path: Path, fast_hash: str) -> str:
    ext = path.suffix.lstrip(".")
    return (
        f'"{path.name}","localhost","{ext}","tester","localhost","{path.parent}",'
        f"01/06/2026 10:00:00,15/08/2026 09:30:00,True,True,True,LOCAL_FIXED,"
        f'"\\\\localhost\\LOCAL_SCAN\\",{path.stat().st_size},20/08/2026 08:00:00,"Archive",'
        f'"tester","{fast_hash}","unknown"'
    )


@pytest.fixture
def corpus(tmp_path: Path) -> tuple[Path, Path]:
    """Six fichiers texte distincts + un CSV SMBeagle qui les décrit (mode --local-path)."""
    src = tmp_path / "partage"
    (src / "rh").mkdir(parents=True)
    (src / "compta").mkdir()
    files = []
    for i, (sub, name) in enumerate(
        [
            ("rh", "contrat_dupont.txt"),
            ("rh", "note.txt"),
            ("compta", "facture_0912.txt"),
            ("compta", "budget.txt"),
            ("", "procedure.txt"),
            ("", "readme.md"),
        ]
    ):
        path = src / sub / name if sub else src / name
        path.write_text(f"Document {i} {name} " * 120, encoding="utf-8")
        files.append(path)
    csv_path = tmp_path / "scan.csv"
    csv_path.write_text(
        HEADER + "\n" + "\n".join(_csv_line(p, f"hash{i:04d}") for i, p in enumerate(files)) + "\n",
        encoding="utf-8",
    )
    return src, csv_path


def _config(tmp_path: Path, base_url: str, transport: str = "vllm", **blocks: object) -> Config:
    cfg = Config(db_path=str(tmp_path / "docia.sqlite"))
    cfg.llm.base_url = base_url
    cfg.llm.transport = transport
    cfg.llm.max_in_flight = 3
    cfg.llm.timeout_s = 30
    cfg.llm.max_retries = 1
    cfg.blocks.block_tokens = int(blocks.get("block_tokens", 100_000))
    cfg.blocks.batch_files = int(blocks.get("batch_files", 200))
    # Sous Windows, tmp_path est dans %LOCALAPPDATA%\Temp → marqueur `\AppData\` exclu par défaut.
    cfg.filter.excluded_dir_markers = []
    return cfg


def test_full_run_then_resume_is_idempotent(
    tmp_path: Path, corpus: tuple[Path, Path], fake_server
) -> None:  # type: ignore[no-untyped-def]
    _src, csv_path = corpus
    cfg = _config(tmp_path, fake_server.base_url_vllm, block_tokens=1_200)  # ≈ 2 fichiers par bloc
    with Database(cfg.db_path) as db:
        report = import_csv(db, csv_path)
        assert (report.new, report.invalid) == (6, 0)
        plan = plan_files(db, cfg.filter)
        assert plan.pending == 6

        run1 = run_pipeline(db, cfg)
        assert run1.errors == []
        assert (run1.files_selected, run1.files_done, run1.files_error) == (6, 6, 0)
        assert run1.blocks_built >= 2
        assert run1.blocks_done == run1.blocks_built
        counts = db.counts()
        assert (counts["done"], counts["analyses"], counts["blocks_error"]) == (6, 6, 0)
        rows = list(db.latest_analyses())
        assert all(r["security_classification"] for r in rows)
        assert {r["name"] for r in rows} == {
            "contrat_dupont.txt",
            "note.txt",
            "facture_0912.txt",
            "budget.txt",
            "procedure.txt",
            "readme.md",
        }

        # relance : rien à faire, aucun appel LLM
        calls_before = len(fake_server.requests)
        run2 = run_pipeline(db, cfg)
        assert (run2.files_selected, run2.blocks_built) == (0, 0)
        assert len(fake_server.requests) == calls_before

        # rescan avec un fichier modifié → une seule réanalyse
        lines = csv_path.read_text(encoding="utf-8").splitlines()
        lines[1] = lines[1].replace("hash0000", "hashFFFF")
        csv_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        report2 = import_csv(db, csv_path)
        assert (report2.updated, report2.unchanged) == (1, 5)
        run3 = run_pipeline(db, cfg)
        assert (run3.files_selected, run3.files_done) == (1, 1)
        assert db.counts()["analyses"] == 7  # nouvelle version de contenu = nouvelle analyse


def test_openwebui_transport(tmp_path: Path, corpus: tuple[Path, Path], fake_server) -> None:  # type: ignore[no-untyped-def]
    _src, csv_path = corpus
    cfg = _config(tmp_path, fake_server.base_url_openwebui, transport="openwebui")
    with Database(cfg.db_path) as db:
        import_csv(db, csv_path)
        plan_files(db, cfg.filter)
        report = run_pipeline(db, cfg)
        assert (report.files_done, report.errors) == (6, [])
    last = fake_server.requests[-1]
    assert last["files"][0]["type"] == "text"
    assert last["files"][0]["context"] == "full"
    assert last["files"][0]["name"].endswith(".md")
    assert "response_format" in last


def test_missing_file_in_response_is_retried_then_errored(
    tmp_path: Path, corpus: tuple[Path, Path], fake_server
) -> None:  # type: ignore[no-untyped-def]
    _src, csv_path = corpus
    fake_server.mode = "drop_last"
    cfg = _config(tmp_path, fake_server.base_url_vllm)  # un seul bloc de 6
    with Database(cfg.db_path) as db:
        import_csv(db, csv_path)
        plan_files(db, cfg.filter)
        run1 = run_pipeline(db, cfg)
        assert (run1.files_done, run1.files_error) == (5, 0)
        assert db.counts()["pending"] == 1  # le fichier absent repart pour un autre bloc
        run2 = run_pipeline(db, cfg)
        assert (run2.files_selected, run2.files_done, run2.files_error) == (1, 0, 1)
        errored = list(db.iter_files(FileStatus.ERROR))
        assert len(errored) == 1
        assert "absent" in (errored[0].exclusion_reason or "")


def test_server_error_marks_block_and_keeps_files_pending(
    tmp_path: Path, corpus: tuple[Path, Path], fake_server
) -> None:  # type: ignore[no-untyped-def]
    _src, csv_path = corpus
    fake_server.mode = "http400"
    cfg = _config(tmp_path, fake_server.base_url_vllm)
    with Database(cfg.db_path) as db:
        import_csv(db, csv_path)
        plan_files(db, cfg.filter)
        report = run_pipeline(db, cfg)
        assert report.blocks_error == 1
        assert report.files_done == 0
        assert db.counts()["pending"] == 6  # première tentative : les fichiers repartent
        assert db.counts()["blocks_error"] == 1


def test_missing_source_file_is_flagged(
    tmp_path: Path, corpus: tuple[Path, Path], fake_server
) -> None:  # type: ignore[no-untyped-def]
    src, csv_path = corpus
    (src / "readme.md").unlink()
    cfg = _config(tmp_path, fake_server.base_url_vllm)
    with Database(cfg.db_path) as db:
        import_csv(db, csv_path)
        plan_files(db, cfg.filter)
        report = run_pipeline(db, cfg)
        assert (report.files_done, report.files_error) == (5, 1)
        errored = list(db.iter_files(FileStatus.ERROR))
        assert errored[0].name == "readme.md"
        assert "introuvable" in (errored[0].exclusion_reason or "")


def test_cli_end_to_end(
    tmp_path: Path,
    corpus: tuple[Path, Path],
    fake_server,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:  # type: ignore[no-untyped-def]
    _src, csv_path = corpus
    monkeypatch.chdir(tmp_path)
    assert main(["init"]) == 0
    toml = (tmp_path / "docia.toml").read_text(encoding="utf-8")
    toml = toml.replace(
        'base_url = "http://127.0.0.1:8000/v1"', f'base_url = "{fake_server.base_url_vllm}"'
    )
    toml = toml.replace("timeout_s = 900", "timeout_s = 30")
    toml += "\n[filter]\nexcluded_dir_markers = []\n" if "[filter]" not in toml else ""
    toml = toml.replace(
        "max_size_bytes = 104857600", "max_size_bytes = 104857600\nexcluded_dir_markers = []"
    )
    (tmp_path / "docia.toml").write_text(toml, encoding="utf-8")
    assert main(["ingest", str(csv_path)]) == 0
    assert main(["plan"]) == 0
    assert main(["run", "--dry-run"]) == 0
    assert list((tmp_path / "docia.blocks").rglob("*.md"))
    assert main(["run"]) == 0
    capsys.readouterr()
    assert main(["status", "--json"]) == 0
    status = json.loads(capsys.readouterr().out)
    assert status["counts"]["done"] == 6
    out = tmp_path / "export.csv"
    assert main(["export", "--format", "csv", "--out", str(out)]) == 0
    text = out.read_text(encoding="utf-8-sig")
    assert text.count("\n") >= 7  # en-tête + 6 lignes
    assert "security_classification" in text
    assert main(["retry"]) == 0


def test_file_over_model_context_is_segmented_and_aggregated(
    tmp_path: Path, corpus: tuple[Path, Path], fake_server
) -> None:  # type: ignore[no-untyped-def]
    src, csv_path = corpus
    (src / "enorme.txt").write_text(
        "".join(f"Paragraphe {i} : " + "texte volumineux " * 30 + "\n\n" for i in range(600)),
        encoding="utf-8",
    )
    csv_path.write_text(
        csv_path.read_text(encoding="utf-8") + _csv_line(src / "enorme.txt", "hashBIG") + "\n",
        encoding="utf-8",
    )
    cfg = _config(tmp_path, fake_server.base_url_vllm, block_tokens=8_000)
    cfg.llm.max_context_tokens = 12_000  # → segments de ≤ 6 000 tokens
    with Database(cfg.db_path) as db:
        import_csv(db, csv_path)
        plan_files(db, cfg.filter)
        report = run_pipeline(db, cfg)
        assert report.blocks_skipped == 0
        assert report.files_segmented == 1
        assert (report.files_done, report.files_error) == (7, 0)
        assert db.counts()["error"] == 0
        rows = {r["name"]: r for r in db.latest_analyses()}
        big = rows["enorme.txt"]
        assert big["segments"] >= 2
        assert big["resume"].startswith(f"Fichier analysé en {big['segments']} parties")
        assert big["security_classification"]
    refs = [
        r for r in fake_server.requests if "enorme.txt [partie" in json.dumps(r, ensure_ascii=False)
    ]
    assert len(refs) == big["segments"]


def test_exact_duplicate_inherits_original_analysis(
    tmp_path: Path, corpus: tuple[Path, Path], fake_server
) -> None:  # type: ignore[no-untyped-def]
    src, csv_path = corpus
    original = src / "rh" / "contrat_dupont.txt"
    copy = src / "compta" / "contrat_dupont_copie.txt"
    copy.write_text(original.read_text(encoding="utf-8"), encoding="utf-8")
    csv_path.write_text(
        csv_path.read_text(encoding="utf-8") + _csv_line(copy, "hashDUP") + "\n", encoding="utf-8"
    )
    cfg = _config(tmp_path, fake_server.base_url_vllm)
    with Database(cfg.db_path) as db:
        import_csv(db, csv_path)
        plan_files(db, cfg.filter)
        report = run_pipeline(db, cfg)
        assert report.files_duplicates == 1
        assert (report.files_done, report.files_error) == (7, 0)
        rows = {r["name"]: r for r in db.latest_analyses()}
        assert (
            rows["contrat_dupont_copie.txt"]["security_classification"]
            == rows["contrat_dupont.txt"]["security_classification"]
        )
        assert rows["contrat_dupont_copie.txt"]["status"] == "done"
    sent = json.dumps(fake_server.requests, ensure_ascii=False)
    assert "contrat_dupont_copie.txt" not in sent or "Contenu identique" in sent


def test_big_file_resplit_when_exact_count_exceeds_context(
    fake_server: FakeOpenAIServer, corpus: tuple[Path, Path], tmp_path: Path
) -> None:
    """Le comptage exact du serveur dépasse l'estimation : les segments trop longs ne sont
    pas envoyés, le fichier est re-découpé dans le même run et finit `done` — jamais `error`."""
    src, csv_path = corpus
    (src / "enorme.txt").write_text(
        "".join(f"Paragraphe {i} : " + "texte volumineux " * 30 + "\n\n" for i in range(600)),
        encoding="utf-8",
    )
    csv_path.write_text(
        csv_path.read_text(encoding="utf-8") + _csv_line(src / "enorme.txt", "hashBIG") + "\n",
        encoding="utf-8",
    )
    cfg = _config(tmp_path, fake_server.base_url_vllm, block_tokens=8_000)
    cfg.llm.max_context_tokens = 12_000
    cfg.llm.enable_thinking = False
    fake_server.tokens_per_char = 0.5  # deux fois plus de tokens que l'estimation octets/4
    with Database(cfg.db_path) as db:
        import_csv(db, csv_path)
        plan_files(db, cfg.filter)
        report = run_pipeline(db, cfg)
        assert report.files_resplit >= 1
        assert report.blocks_error >= 1  # les premiers segments, refusés avant envoi
        assert (report.files_done, report.files_error) == (7, 0)
        assert db.counts()["error"] == 0
        big = {r["name"]: r for r in db.latest_analyses()}["enorme.txt"]
        assert big["segments"] >= 2
        assert big["security_classification"]
    assert fake_server.tokenize_calls >= 2


def test_pipeline_clamps_to_served_context(
    fake_server: FakeOpenAIServer, corpus: tuple[Path, Path], tmp_path: Path
) -> None:
    """`max_context_tokens` décrit le serveur : si le serveur sert moins, le run s'y borne
    en le disant — pas d'erreurs 400 silencieuses."""
    src, csv_path = corpus
    cfg = _config(tmp_path, fake_server.base_url_vllm, block_tokens=8_000)
    cfg.llm.max_context_tokens = 200_000
    fake_server.max_model_len = 50_000
    lines: list[str] = []
    with Database(cfg.db_path) as db:
        import_csv(db, csv_path)
        plan_files(db, cfg.filter)
        report = run_pipeline(db, cfg, progress=lines.append)
        assert report.files_error == 0
    assert cfg.llm.max_context_tokens == 50_000
    assert any("la valeur du serveur fait foi" in line for line in lines)
