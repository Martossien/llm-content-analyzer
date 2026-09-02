"""Bout en bout : CSV SMBeagle → base → plan → blocs DocFuse → LLM factice → analyses → reprise.

Le serveur OpenAI factice (`tests/fake_openai.py`) rend une entrée par ligne
`## SOURCE:` du bloc reçu ; il sert pour les deux transports.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from docia.cli import main
from docia.config import Config
from docia.db import Database
from docia.filter import plan_files
from docia.ingest.smbeagle_csv import import_csv
from docia.models import FileStatus
from docia.pipeline import RunReport, run_pipeline
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
        # Toujours 6 **fichiers** analysés : le fichier modifié en a une deuxième dans
        # la table, mais une seule fait foi. L'assertion valait 7 et verrouillait le
        # défaut : `counts()["analyses"]` comptait les lignes d'`analyses`, historique
        # des réanalyses compris. `docia status`, `docia status --json` et l'onglet
        # Risque annonçaient donc jusqu'au double du « analysés » du rapport HTML sur
        # la même base. Les trois chemins comptent maintenant la même chose.
        assert db.counts()["analyses"] == 6
        assert db.query_values("SELECT COUNT(*) FROM analyses")[0][0] == 7  # l'historique reste


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
    fake_server: FakeOpenAIServer,
    corpus: tuple[Path, Path],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Le comptage exact du serveur dépasse l'estimation : les segments trop longs ne sont
    pas envoyés, le fichier est re-découpé dans le même run et finit `done` — jamais `error`.

    Le compteur du builder est rendu muet : c'est la seconde passe qu'on éprouve ici
    (elle reste le filet quand le rapport estimation/réel varie au sein d'un fichier)."""
    from docia.llm.server import ServerTokenCounter

    monkeypatch.setattr(ServerTokenCounter, "__call__", lambda _self, _text: None)
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
    cfg.blocks.max_file_share = 1.0  # un segment peut prendre tout le contexte
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


def _corpus_avec_gros_fichier(corpus: tuple[Path, Path], paragraphes: int) -> Path:
    src, csv_path = corpus
    (src / "enorme.txt").write_text(
        "".join(
            f"Paragraphe {i} : " + "texte volumineux " * 30 + "\n\n" for i in range(paragraphes)
        ),
        encoding="utf-8",
    )
    csv_path.write_text(
        csv_path.read_text(encoding="utf-8") + _csv_line(src / "enorme.txt", "hashBIG") + "\n",
        encoding="utf-8",
    )
    return csv_path


def test_exact_count_before_splitting_avoids_the_second_pass(
    fake_server: FakeOpenAIServer, corpus: tuple[Path, Path], tmp_path: Path
) -> None:
    """Même scénario (serveur deux fois plus gourmand que l'estimation), mais le builder
    demande le compte exact AVANT de découper : segments calibrés du premier coup,
    aucun bloc refusé, aucune seconde passe."""
    csv_path = _corpus_avec_gros_fichier(corpus, 600)
    cfg = _config(tmp_path, fake_server.base_url_vllm, block_tokens=8_000)
    cfg.llm.max_context_tokens = 12_000
    cfg.llm.enable_thinking = False
    fake_server.tokens_per_char = 0.5
    with Database(cfg.db_path) as db:
        import_csv(db, csv_path)
        plan_files(db, cfg.filter)
        report = run_pipeline(db, cfg)
        assert (report.files_resplit, report.blocks_error) == (0, 0)
        assert (report.files_done, report.files_error) == (7, 0)
        big = {r["name"]: r for r in db.latest_analyses()}["enorme.txt"]
        assert big["segments"] >= 2


def test_file_estimated_too_long_but_counted_short_is_sent_whole(
    fake_server: FakeOpenAIServer, corpus: tuple[Path, Path], tmp_path: Path
) -> None:
    """L'estimation locale dépasse le plafond, le serveur compte bien moins : le
    fichier part entier — découper aurait coûté du contexte pour rien."""
    csv_path = _corpus_avec_gros_fichier(corpus, 120)
    cfg = _config(tmp_path, fake_server.base_url_vllm, block_tokens=8_000)
    cfg.llm.max_context_tokens = 60_000  # plafond par fichier ≈ 16 000 tokens réels
    cfg.llm.enable_thinking = False
    fake_server.tokens_per_char = 0.05  # tokenizer cinq fois plus économe que octets/4
    with Database(cfg.db_path) as db:
        import_csv(db, csv_path)
        plan_files(db, cfg.filter)
        report = run_pipeline(db, cfg)
        assert (report.files_done, report.files_error, report.files_resplit) == (7, 0, 0)
        big = {r["name"]: r for r in db.latest_analyses()}["enorme.txt"]
        assert big["segments"] == 1
    assert fake_server.tokenize_calls >= 1


# ----------------------------------- alimentation adaptative (llm.adaptive)


def test_adaptive_feeding_runs_to_completion_and_remembers_its_budget(
    fake_server: FakeOpenAIServer, corpus: tuple[Path, Path], tmp_path: Path
) -> None:
    """Mode adaptatif de bout en bout : le run se termine comme en mode fixe, les
    événements portent le budget et les tokens en vol, et le budget trouvé est
    mémorisé pour ce serveur et ce modèle."""
    from docia.home import docia_home
    from docia.llm.pacer import PACER_FILE, PacerMemory
    from docia.service import run_campaign

    csv_path = _corpus_avec_gros_fichier(corpus, 300)
    cfg = _config(tmp_path, fake_server.base_url_vllm, block_tokens=8_000)
    cfg.llm.max_context_tokens = 12_000
    cfg.llm.enable_thinking = False
    cfg.llm.adaptive = True
    fake_server.handler_delay = 0.01
    events: list[Any] = []
    lines: list[str] = []
    with Database(cfg.db_path) as db:
        import_csv(db, csv_path)
        plan_files(db, cfg.filter)
        report = run_campaign(db, cfg, on_event=events.append)
        assert (report.files_done, report.files_error) == (7, 0)
        assert report.blocks_done >= 10
    assert any(e.budget_tokens > 0 for e in events)
    assert any(e.throughput_tok_s for e in events if e.kind == "block_done")
    memory = PacerMemory(docia_home() / PACER_FILE)
    remembered = memory.load(PacerMemory.key(cfg.llm.base_url, cfg.llm.model))
    assert remembered is not None
    assert remembered >= cfg.blocks.block_tokens
    # Le run suivant (autre prompt : tout à refaire) repart du budget mémorisé.
    autre_prompt = tmp_path / "prompt2.md"
    autre_prompt.write_text("Classe chaque document. Réponds en JSON.", encoding="utf-8")
    cfg.prompt_path = str(autre_prompt)
    with Database(cfg.db_path) as db:
        db.set_files_status(list(range(1, 8)), FileStatus.PENDING)
        run_pipeline(db, cfg, progress=lines.append)
    assert any(f"départ à {remembered} tokens en vol (mémorisé)" in line for line in lines)


def test_adaptive_feeding_backs_off_on_vllm_preemptions(
    fake_server: FakeOpenAIServer,
    corpus: tuple[Path, Path],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Le compteur `vllm:num_preemptions_total` grimpe entre deux lectures : détresse,
    budget divisé — jusqu'au plancher d'un bloc — et le run se termine quand même."""
    import docia.pipeline as pipeline_mod

    monkeypatch.setattr(pipeline_mod, "PREEMPTIONS_POLL_S", 0.0)
    csv_path = _corpus_avec_gros_fichier(corpus, 300)
    cfg = _config(tmp_path, fake_server.base_url_vllm, block_tokens=8_000)
    cfg.llm.max_context_tokens = 12_000
    cfg.llm.enable_thinking = False
    cfg.llm.adaptive = True
    cfg.llm.adaptive_start_tokens = 200_000
    fake_server.preemptions_step = 1
    lines: list[str] = []
    with Database(cfg.db_path) as db:
        import_csv(db, csv_path)
        plan_files(db, cfg.filter)
        report = run_pipeline(db, cfg, progress=lines.append)
        assert (report.files_done, report.files_error) == (7, 0)
    assert any("détresse (1 préemption(s) vLLM)" in line for line in lines)
    assert any(f"→ {cfg.blocks.block_tokens} tokens en vol" in line for line in lines)


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


# ----------------------------------- sélection par identifiants, lot par lot (P1)


def _selection_en_memoire(db: Database, monkeypatch: pytest.MonkeyPatch) -> None:
    """Rétablit l'ancien chemin : toute la campagne chargée en `FileRow` d'un coup.

    `run_pipeline` prend la liste rendue par `select_pending_ids`, la tranche par
    lots et passe chaque tranche à `files_by_ids`. En rendant les `FileRow` complets
    d'un côté et l'identité de l'autre, le pipeline se comporte **exactement** comme
    avant la correction — c'est ce qui permet de comparer les deux rapports.
    """
    monkeypatch.setattr(
        type(db),
        "select_pending_ids",
        lambda self, limit, *, prompt_hash, model: self.select_pending(
            limit, prompt_hash=prompt_hash, model=model
        ),
    )
    monkeypatch.setattr(type(db), "files_by_ids", lambda _self, rows: list(rows))


def _rapport_comparable(report: RunReport) -> dict[str, object]:
    """Le rapport, sans `run_id` (il numérote la campagne, pas le travail fait)."""
    return {k: v for k, v in report.as_dict().items() if k != "run_id"}


def test_le_rapport_est_identique_avec_la_selection_par_identifiants(
    tmp_path: Path, corpus: tuple[Path, Path], fake_server, monkeypatch: pytest.MonkeyPatch
) -> None:  # type: ignore[no-untyped-def]
    """P1 — deux campagnes identiques, deux chemins de sélection, un seul rapport.

    Le pipeline gardait les 700 797 `FileRow` d'une campagne (1 722 Mo mesurés) du
    début à la fin d'un run de plusieurs heures. Il ne garde plus que les
    identifiants (28 Mo) et recharge un lot de `blocks.batch_files` à la fois. Le
    `RunReport` — compteurs, blocs, erreurs — doit être le même au caractère près.
    """
    _src, csv_path = corpus
    rapports: list[dict[str, object]] = []
    analyses: list[list[tuple[str, str]]] = []
    for chemin in ("memoire", "identifiants"):
        cfg = _config(
            tmp_path / chemin, fake_server.base_url_vllm, block_tokens=1_200, batch_files=2
        )
        with Database(cfg.db_path) as db:
            import_csv(db, csv_path)
            plan_files(db, cfg.filter)
            with monkeypatch.context() as patch:
                if chemin == "memoire":
                    _selection_en_memoire(db, patch)
                rapports.append(_rapport_comparable(run_pipeline(db, cfg)))
            analyses.append(
                sorted(
                    (str(r["name"]), str(r["security_classification"] or ""))
                    for r in db.latest_analyses()
                )
            )
    assert rapports[0] == rapports[1], "le rapport a changé de contenu"
    assert rapports[0]["files_selected"] == 6
    assert rapports[0]["files_done"] == 6
    assert analyses[0] == analyses[1], "les analyses écrites diffèrent"


def test_la_reprise_apres_interruption_ne_reanalyse_rien_deux_fois(
    tmp_path: Path, corpus: tuple[Path, Path], fake_server
) -> None:  # type: ignore[no-untyped-def]
    """P1 — run coupé puis relancé : mêmes fichiers repris, aucune analyse en double.

    Le run **écrit** dans `files` au fil des lots, sur la connexion qui a servi à
    sélectionner. La liste d'identifiants est un instantané : elle reste exacte quoi
    qu'il advienne des statuts pendant le run, et la reprise ne reprend que ce qui
    reste à faire.
    """
    import threading

    _src, csv_path = corpus
    cfg = _config(tmp_path, fake_server.base_url_vllm, block_tokens=1_200, batch_files=2)
    cancel = threading.Event()
    cancel.set()  # annulation demandée d'entrée : rien n'est construit ni envoyé
    with Database(cfg.db_path) as db:
        import_csv(db, csv_path)
        plan_files(db, cfg.filter)
        coupe = run_pipeline(db, cfg, cancel=cancel)
        assert coupe.files_selected == 6, "la sélection a bien eu lieu"
        assert (coupe.files_done, coupe.blocks_built) == (0, 0)
        assert db.counts()["pending"] == 6, "aucun fichier perdu par l'annulation"

        appels_avant = len(fake_server.requests)
        reprise = run_pipeline(db, cfg)
        assert (reprise.files_selected, reprise.files_done, reprise.files_error) == (6, 6, 0)
        assert db.counts()["analyses"] == 6
        assert len(fake_server.requests) > appels_avant

        # relance à vide : plus rien à faire, aucun appel de plus, aucun doublon
        appels = len(fake_server.requests)
        vide = run_pipeline(db, cfg)
        assert (vide.files_selected, vide.blocks_built) == (0, 0)
        assert len(fake_server.requests) == appels
        assert db.counts()["analyses"] == 6
        envois = db.query(
            "SELECT file_id, COUNT(*) AS n FROM block_files GROUP BY file_id HAVING n > 1"
        )
        assert envois == [], "un fichier a été envoyé dans deux blocs"


def test_la_limite_borne_la_selection_et_les_lots(
    tmp_path: Path, corpus: tuple[Path, Path], fake_server
) -> None:  # type: ignore[no-untyped-def]
    """P1 — `--limit` : autant de fichiers analysés que demandé, les autres attendent."""
    _src, csv_path = corpus
    cfg = _config(tmp_path, fake_server.base_url_vllm, block_tokens=1_200, batch_files=2)
    with Database(cfg.db_path) as db:
        import_csv(db, csv_path)
        plan_files(db, cfg.filter)
        attendus = db.select_pending_ids(4, prompt_hash="x", model="y")

        report = run_pipeline(db, cfg, limit=4)
        assert (report.files_selected, report.files_done) == (4, 4)
        assert db.counts()["done"] == 4
        assert db.counts()["pending"] == 2
        analyses = {int(r["file_id"]) for r in db.query("SELECT file_id FROM analyses")}
        assert analyses == set(attendus), "ce ne sont pas les 4 premiers du plan"

        reste = run_pipeline(db, cfg, limit=10)
        assert (reste.files_selected, reste.files_done) == (2, 2)
        assert db.counts()["done"] == 6


def test_le_pipeline_ne_charge_quun_lot_de_fichiers_a_la_fois(
    tmp_path: Path, corpus: tuple[Path, Path], fake_server
) -> None:  # type: ignore[no-untyped-def]
    """P1 — la mémoire du run suit la taille d'un lot, pas celle de la campagne."""
    _src, csv_path = corpus
    cfg = _config(tmp_path, fake_server.base_url_vllm, block_tokens=1_200, batch_files=2)
    tailles: list[int] = []
    with Database(cfg.db_path) as db:
        import_csv(db, csv_path)
        plan_files(db, cfg.filter)
        vrai = Database.files_by_ids

        def espion(self: Database, ids: object) -> list[object]:
            rows = vrai(self, ids)  # type: ignore[arg-type]
            tailles.append(len(rows))
            return rows  # type: ignore[return-value]

        Database.files_by_ids = espion  # type: ignore[assignment,method-assign]
        try:
            report = run_pipeline(db, cfg)
        finally:
            Database.files_by_ids = vrai  # type: ignore[method-assign]
    assert report.files_done == 6
    assert tailles == [2, 2, 2], f"lots chargés : {tailles}"
