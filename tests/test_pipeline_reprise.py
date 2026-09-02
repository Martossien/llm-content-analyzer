"""Reprises et modes de panne du pipeline : annulation, coupure, gros fichiers.

Ces tests reproduisent des pannes observées en campagne réelle. Ils portent tous
sur la même exigence : **aucun fichier ne doit être déclaré analysé sans l'être**,
et **aucun run ne doit rester ouvert ou se clore « done » en laissant du travail
en plan**. Le serveur est programmable (`responder`) : chaque test décrit sa panne.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Callable, Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, cast

import pytest

from docia.config import Config
from docia.db import Database
from docia.filter import plan_files
from docia.ingest.smbeagle_csv import import_csv
from docia.models import FileStatus
from docia.pipeline import run_pipeline
from tests.conftest import prompt_court
from tests.fake_openai import block_text_from_payload, extract_sources, make_entry

Responder = Callable[[dict[str, Any], int, list[str]], tuple[int, str, bytes]]
"""(payload, n° de requête, `file_ref` du bloc) → (statut HTTP, content-type, corps).
Statut `-1` : le serveur annonce un corps long puis coupe en plein flux."""


def ok_body(sources: list[str]) -> bytes:
    """Réponse JSON conforme, une entrée par `## SOURCE:` du bloc."""
    content = json.dumps({"files": [make_entry(ref) for ref in sources]}, ensure_ascii=False)
    body = {
        "id": "chatcmpl-fake",
        "object": "chat.completion",
        "model": "qwen38",
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": content},
                "finish_reason": "stop",
            }
        ],
        "usage": {"prompt_tokens": 100, "completion_tokens": len(content) // 4},
    }
    return json.dumps(body, ensure_ascii=False).encode("utf-8")


def repond_ok(_payload: dict[str, Any], _n: int, sources: list[str]) -> tuple[int, str, bytes]:
    return 200, "application/json", ok_body(sources)


class ServeurProgrammable(ThreadingHTTPServer):
    """Serveur OpenAI factice dont chaque réponse est décidée par `responder`."""

    daemon_threads = True
    allow_reuse_address = True

    def __init__(self) -> None:
        super().__init__(("127.0.0.1", 0), _Handler)
        self.responder: Responder = repond_ok
        self.max_model_len: int | None = None
        self.tokens_par_caractere: float = 0.25
        """0.25 ≈ octets/4 (l'estimation du builder) ; plus haut = tokenizer gourmand."""
        self.post_count = 0
        self.sources_vues: list[list[str]] = []
        self.lock = threading.Lock()

    @property
    def base_url(self) -> str:
        return f"http://{self.server_address[0]}:{self.server_address[1]}/v1"


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    @property
    def etat(self) -> ServeurProgrammable:
        return cast(ServeurProgrammable, self.server)

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002, ARG002 - signature imposée
        """Silence."""

    def _send(self, status: int, ctype: str, body: bytes) -> None:
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/v1/models":
            entry: dict[str, Any] = {"id": "qwen38"}
            if self.etat.max_model_len is not None:
                entry["max_model_len"] = self.etat.max_model_len
            self._send(200, "application/json", json.dumps({"data": [entry]}).encode())
        else:
            self._send(404, "text/plain", b"not found")

    def do_POST(self) -> None:  # noqa: N802
        raw = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        payload = json.loads(raw.decode("utf-8")) if raw else {}
        if self.path == "/tokenize":
            prompt = str(payload.get("prompt", ""))
            compte = int(len(prompt) * self.etat.tokens_par_caractere)
            self._send(200, "application/json", json.dumps({"count": compte}).encode())
            return
        if self.path != "/v1/chat/completions":
            self._send(404, "text/plain", b"not found")
            return
        sources = extract_sources(block_text_from_payload(payload))
        with self.etat.lock:
            self.etat.post_count += 1
            n = self.etat.post_count
            self.etat.sources_vues.append(sources)
        status, ctype, body = self.etat.responder(payload, n, sources)
        if status == -1:  # corps annoncé long, connexion coupée en plein flux
            self.wfile.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                b"Content-Length: 100000\r\n\r\n" + body
            )
            self.wfile.flush()
            self.close_connection = True
            return
        self._send(status, ctype, body)


@pytest.fixture
def serveur() -> Iterator[ServeurProgrammable]:
    srv = ServeurProgrammable()
    fil = threading.Thread(target=srv.serve_forever, daemon=True)
    fil.start()
    try:
        yield srv
    finally:
        srv.shutdown()
        srv.server_close()
        fil.join(timeout=5)


# ------------------------------------------------------------------ corpus

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


def corpus(tmp_path: Path, *, petits: int = 0, gros: dict[str, str] | None = None) -> Path:
    """Écrit les fichiers et le CSV SMBeagle qui les décrit ; rend le chemin du CSV."""
    src = tmp_path / "partage"
    src.mkdir(parents=True, exist_ok=True)
    fichiers: list[Path] = []
    for i in range(petits):
        chemin = src / f"doc_{i}.txt"
        chemin.write_text(f"Document {i} " * 200, encoding="utf-8")
        fichiers.append(chemin)
    for nom, texte in (gros or {}).items():
        chemin = src / nom
        chemin.write_text(texte, encoding="utf-8")
        fichiers.append(chemin)
    csv_path = tmp_path / "scan.csv"
    csv_path.write_text(
        HEADER
        + "\n"
        + "\n".join(_csv_line(p, f"hash{i:04d}") for i, p in enumerate(fichiers))
        + "\n",
        encoding="utf-8",
    )
    return csv_path


def config(tmp_path: Path, base_url: str, **blocs: object) -> Config:
    cfg = Config(db_path=str(tmp_path / "docia.sqlite"))
    cfg.llm.base_url = base_url
    cfg.llm.transport = "vllm"
    cfg.llm.max_in_flight = int(cast(int, blocs.get("max_in_flight", 3)))
    cfg.llm.timeout_s = 30
    cfg.llm.max_retries = int(cast(int, blocs.get("max_retries", 0)))
    cfg.llm.enable_thinking = False
    cfg.llm.max_context_tokens = int(cast(int, blocs.get("max_context_tokens", 60_000)))
    cfg.blocks.block_tokens = int(cast(int, blocs.get("block_tokens", 100_000)))
    cfg.blocks.batch_files = int(cast(int, blocs.get("batch_files", 200)))
    cfg.blocks.max_file_tokens = int(cast(int, blocs.get("max_file_tokens", 0)))
    cfg.filter.excluded_dir_markers = []
    return cfg


def texte_volumineux(paragraphes: int) -> str:
    return "".join(
        f"Paragraphe {i} : " + "texte volumineux " * 20 + "\n\n" for i in range(paragraphes)
    )


def prepare(db: Database, cfg: Config, csv_path: Path) -> None:
    import_csv(db, csv_path)
    plan_files(db, cfg.filter)


def statut_du_run(db: Database) -> str:
    return str(db.query("SELECT status FROM runs ORDER BY id")[-1]["status"])


def indice_de_segment(ref: str) -> int:
    return int(ref.split("[partie ")[1].split("/")[0]) if "[partie " in ref else 0


# ------------------------------------------------- CRITIQUE 2 : `done` prématuré


def test_un_gros_fichier_nest_done_quapres_tous_ses_segments(
    tmp_path: Path, serveur: ServeurProgrammable
) -> None:
    """Deux découpages successifs du même fichier : le second ne doit jamais hériter
    des segments périmés du premier.

    Avant correction : run 1 en 34 segments coupé au 8ᵉ (7 lignes en base), run 2 en
    6 segments → le compte `len(segments) >= segment_count` était atteint dès le
    PREMIER segment du nouveau découpage. Le fichier passait `done` avec une analyse
    agrégeant 4 segments périmés (≈ 20 % du document) et `files_done` valait 5 pour
    un seul fichier. Si le processus mourait là, l'état restait `done` pour toujours.
    """
    csv_path = corpus(tmp_path, gros={"enorme.txt": texte_volumineux(300)})
    cfg = config(tmp_path, serveur.base_url, block_tokens=1_500, max_file_tokens=1_500)
    phase = ["run1"]
    photos: list[tuple[int, str, int]] = []

    def photo_de_la_base() -> tuple[str, int]:
        con = sqlite3.connect(cfg.db_path)
        con.row_factory = sqlite3.Row
        try:
            statut = str(con.execute("SELECT status FROM files WHERE id=1").fetchone()["status"])
            lignes = con.execute("SELECT segments FROM analyses WHERE file_id=1").fetchall()
        finally:
            con.close()
        return statut, (int(lignes[0]["segments"]) if lignes else 0)

    def responder(_p: dict[str, Any], _n: int, sources: list[str]) -> tuple[int, str, bytes]:
        idx = indice_de_segment(sources[0] if sources else "?")
        if phase[0] == "run2":
            statut, segments = photo_de_la_base()
            photos.append((idx, statut, segments))
        if phase[0] == "run1" and idx >= 8:
            return 500, "text/plain", b"erreur interne"
        return 200, "application/json", ok_body(sources)

    serveur.responder = responder
    with Database(cfg.db_path) as db:
        prepare(db, cfg, csv_path)
        run1 = run_pipeline(db, cfg)
        assert run1.files_done == 0, "aucun fichier complet : le run 1 s'est cassé au 8e segment"
        segments_run1 = db.query("SELECT COUNT(*) AS n FROM segment_analyses")[0]["n"]
        assert segments_run1 == 7

        db.reset_errors()
        phase[0] = "run2"
        cfg.blocks.block_tokens = 8_000
        cfg.blocks.max_file_tokens = 8_000
        run2 = run_pipeline(db, cfg)

        total = int(db.query("SELECT COUNT(*) AS n FROM segment_analyses")[0]["n"])
        analyse = db.query("SELECT segments, resume FROM analyses WHERE file_id=1")[0]
        # Le fichier n'est `done` à AUCUN moment avant le dernier segment du run 2.
        avant_le_dernier = photos[:-1]
        assert avant_le_dernier, "le run 2 a bien envoyé plusieurs segments"
        assert all(statut != "done" for _idx, statut, _seg in avant_le_dernier), photos
        # Un seul fichier fait : `files_done` compte des fichiers, pas des segments.
        assert (run2.files_done, run2.files_error) == (1, 0)
        # Les segments périmés du premier découpage ont disparu, l'agrégat est complet.
        assert total == int(analyse["segments"]) == len(photos)
        assert str(analyse["resume"]).startswith(f"Fichier analysé en {total} parties")
        fichier = db.get_file(1)
        assert fichier is not None
        assert fichier.status == FileStatus.DONE


def test_les_segments_dun_autre_decoupage_ne_comptent_pas(tmp_path: Path) -> None:
    """`segment_analyses` filtre sur le découpage demandé, et écrire un segment purge
    les lignes d'un découpage différent du même contenu."""
    csv_path = corpus(tmp_path, petits=1)
    cfg = config(tmp_path, "http://127.0.0.1:1/v1")
    with Database(cfg.db_path) as db:
        prepare(db, cfg, csv_path)
        for index in range(1, 8):
            db.store_segment_analysis(
                1,
                None,
                1,
                prompt_hash="p",
                model="m",
                segment_index=index,
                segment_count=34,
                raw={"file_ref": f"x [partie {index}/34]"},
            )
        assert len(db.segment_analyses(1, 1, prompt_hash="p", model="m", segment_count=34)) == 7
        assert db.segment_analyses(1, 1, prompt_hash="p", model="m", segment_count=6) == []

        db.store_segment_analysis(
            1,
            None,
            1,
            prompt_hash="p",
            model="m",
            segment_index=1,
            segment_count=6,
            raw={"file_ref": "x [partie 1/6]"},
        )
        # Le nouveau découpage remplace l'ancien : plus aucune ligne en 34 parties.
        assert db.segment_analyses(1, 1, prompt_hash="p", model="m", segment_count=34) == []
        restants = db.segment_analyses(1, 1, prompt_hash="p", model="m")
        assert [(i, c) for i, c, _raw in restants] == [(1, 6)]


# ------------------------------------------------- CRITIQUE 1 : coupure de flux


def test_une_coupure_de_flux_nemporte_pas_le_run(
    tmp_path: Path, serveur: ServeurProgrammable
) -> None:
    """Le serveur coupe la connexion en plein corps (`httpx.RemoteProtocolError`, ni
    `TimeoutException` ni `ConnectError`) : avant correction l'exception traversait le
    `gather`, tuait tous les blocs en vol, sautait `finish_run` (`runs.status='running'`
    pour toujours) et laissait les fichiers `queued`."""
    csv_path = corpus(tmp_path, petits=6)
    cfg = config(tmp_path, serveur.base_url, block_tokens=1_200, max_in_flight=2)

    def responder(_p: dict[str, Any], n: int, sources: list[str]) -> tuple[int, str, bytes]:
        if n == 2:
            return -1, "application/json", b'{"choices": [{"index": 0, "message": {"role"'
        return 200, "application/json", ok_body(sources)

    serveur.responder = responder
    with Database(cfg.db_path) as db:
        prepare(db, cfg, csv_path)
        report = run_pipeline(db, cfg)  # ne lève pas
        assert report.blocks_error == 1
        assert report.errors, "la coupure est signalée dans le rapport"
        assert statut_du_run(db) == "error", "un run est TOUJOURS clos"
        counts = db.counts()
        assert counts["queued"] == 0, "aucun fichier laissé en vol"
        assert counts["done"] + counts["pending"] == 6

        # Les blocs sains sont passés malgré la coupure du voisin, la reprise finit.
        serveur.responder = repond_ok
        reprise = run_pipeline(db, cfg)
        assert reprise.files_error == 0
        assert db.counts()["done"] == 6
        assert statut_du_run(db) == "done"


# ------------------------------------- CRITIQUE 3 : ni analysé, ni en erreur, « done »


def test_le_contexte_servi_est_connu_avant_le_decoupage(
    tmp_path: Path, serveur: ServeurProgrammable
) -> None:
    """Le serveur sert 4 000 tokens alors que la config en annonce 262 144.

    Avant correction, le contexte servi n'était lu qu'APRÈS la construction des
    blocs : le fichier était bâti en un bloc de 6 000 tokens que le serveur ne
    pouvait pas prendre — `envois=0 done=0 error=0 errors=[] | fichier=pending |
    run='done'` à chaque relance, indéfiniment, avec le code de sortie 0. Lu avant,
    le budget de découpage en dérive et le fichier est réellement analysé.
    """
    csv_path = corpus(tmp_path, gros={"rapport.txt": texte_volumineux(60)})
    serveur.max_model_len = 4_000
    cfg = config(tmp_path, serveur.base_url, max_context_tokens=262_144)
    cfg.prompt_path = str(prompt_court(tmp_path))
    with Database(cfg.db_path) as db:
        prepare(db, cfg, csv_path)
        run1 = run_pipeline(db, cfg)
        assert cfg.llm.max_context_tokens == 4_000, "le serveur fait foi, avant le découpage"
        assert cfg.blocks.block_tokens <= 4_000, "les blocs sont bâtis à la mesure du serveur"
        assert serveur.post_count > 0, "le fichier a bien été envoyé, en segments"
        assert (run1.files_segmented, run1.files_done, run1.files_error) == (1, 1, 0)
        assert run1.errors == []
        assert statut_du_run(db) == "done"


def test_un_fichier_qui_ne_tient_jamais_finit_en_erreur_et_le_run_le_dit(
    tmp_path: Path, serveur: ServeurProgrammable
) -> None:
    """Comptage exact démesuré : même re-découpé, le bloc ne rentre pas.

    Avant correction le chemin `BlockTooLongError` remettait les fichiers `pending`
    sans rien ajouter à `report.errors` : le run se closait « done », `docia run`
    sortait en 0, et chaque relance reproduisait exactement la même chose — un
    fichier ni analysé, ni en erreur, indéfiniment.
    """
    csv_path = corpus(tmp_path, gros={"rapport.txt": texte_volumineux(60)})
    serveur.max_model_len = 4_000
    serveur.tokens_par_caractere = 2.0  # aucun découpage ne tiendra jamais
    cfg = config(tmp_path, serveur.base_url, max_context_tokens=262_144)
    with Database(cfg.db_path) as db:
        prepare(db, cfg, csv_path)
        run1 = run_pipeline(db, cfg)
        assert serveur.post_count == 0, "rien n'a été envoyé : tout a été refusé au comptage"
        assert run1.errors, "un fichier non analysé doit remonter dans le rapport"
        assert statut_du_run(db) == "error", "jamais « done » quand rien n'a été analysé"
        fichier = db.get_file(1)
        assert fichier is not None
        assert fichier.status == FileStatus.ERROR
        assert "contexte servi" in (fichier.exclusion_reason or "")
        assert run1.files_error == 1

        # Pas de boucle infinie : le fichier ne repart pas indéfiniment `pending`.
        run2 = run_pipeline(db, cfg)
        assert (run2.files_selected, run2.files_error) == (0, 0)


def test_le_redecoupage_se_calcule_sur_la_place_reellement_disponible(
    tmp_path: Path, serveur: ServeurProgrammable, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Comptage exact deux fois plus gros que l'estimation : la seconde passe doit
    re-découper sur `BlockTooLongError.room` et le fichier finir `done` dans le run.

    Compteur du builder muet et part par fichier à 1 : c'est le filet de la seconde
    passe qu'on éprouve, pas le découpage calibré du premier coup (`test_policy`)."""
    from docia.llm.server import ServerTokenCounter

    monkeypatch.setattr(ServerTokenCounter, "__call__", lambda _self, _text: None)
    csv_path = corpus(tmp_path, petits=1, gros={"enorme.txt": texte_volumineux(200)})
    cfg = config(tmp_path, serveur.base_url, block_tokens=8_000, max_context_tokens=12_000)
    cfg.blocks.max_file_share = 1.0
    serveur.tokens_par_caractere = 0.5  # deux fois plus de tokens que l'estimation octets/4
    with Database(cfg.db_path) as db:
        prepare(db, cfg, csv_path)
        report = run_pipeline(db, cfg)
        assert report.files_resplit >= 1
        assert report.blocks_error >= 1  # les segments refusés avant envoi
        assert (report.files_done, report.files_error) == (2, 0)
        assert db.counts()["error"] == 0
        gros = {r["name"]: r for r in db.latest_analyses()}["enorme.txt"]
        assert int(gros["segments"]) >= 2
        segments = db.query("SELECT COUNT(*) AS n FROM segment_analyses WHERE file_id=2")[0]["n"]
        assert int(segments) == int(gros["segments"]), "agrégé sur le découpage courant"


# ------------------------------------------------- MAJEUR 4 : annulation en vol


def test_lannulation_arrete_vraiment_les_envois(
    tmp_path: Path, serveur: ServeurProgrammable
) -> None:
    """`cancel` posé pendant le premier bloc : plus aucun bloc ne part.

    Avant correction, `gather` ordonnançait les 8 coroutines d'emblée : le test
    d'annulation était évalué pour tous les blocs à l'instant zéro, les 8 blocs
    partaient, et le run se closait « cancelled » en annonçant « relancer pour
    reprendre » alors que tout était déjà fait.
    """
    csv_path = corpus(tmp_path, petits=8)
    cfg = config(tmp_path, serveur.base_url, block_tokens=1_200, max_in_flight=1)
    cancel = threading.Event()

    def responder(_p: dict[str, Any], n: int, sources: list[str]) -> tuple[int, str, bytes]:
        if n == 1:
            cancel.set()  # l'utilisateur clique « Annuler » pendant le 1er bloc
        return 200, "application/json", ok_body(sources)

    serveur.responder = responder
    with Database(cfg.db_path) as db:
        prepare(db, cfg, csv_path)
        report = run_pipeline(db, cfg, cancel=cancel)
        assert report.blocks_built >= 4
        assert serveur.post_count == 1, f"{serveur.post_count} bloc(s) envoyés malgré l'annulation"
        assert report.blocks_done == 1
        assert statut_du_run(db) == "cancelled"
        restants = db.query("SELECT COUNT(*) AS n FROM blocks WHERE status='built'")[0]["n"]
        assert int(restants) == report.blocks_built - 1, "les autres blocs restent à reprendre"

        # « relancer pour reprendre » doit être vrai : la reprise finit la campagne.
        serveur.responder = repond_ok
        reprise = run_pipeline(db, cfg)
        assert reprise.blocks_resumed == int(restants)
        assert db.counts()["done"] == 8


# ------------------------------- MAJEUR 5 : blocs effacés = campagne bloquée


def test_les_blocs_non_envoyes_ne_sont_pas_effaces(
    tmp_path: Path, serveur: ServeurProgrammable
) -> None:
    """`keep_blocks = false` + annulation pendant la construction.

    Avant correction, les `.md` de TOUS les specs étaient effacés, y compris ceux
    restés `built` : à la reprise `BlockSpec.text` levait `FileNotFoundError`, le run
    plantait, le bloc passait `sent` au passage — et replantait au run suivant. Sans
    intervention en base, la campagne ne redémarrait plus jamais.

    Construction et envoi se recouvrent : au moment de l'annulation, des blocs du
    premier lot peuvent déjà être partis — ceux-là sont `done` et effacés, les autres
    restent `built` avec leur `.md`.
    """
    csv_path = corpus(tmp_path, petits=8)
    cfg = config(tmp_path, serveur.base_url, block_tokens=1_200, batch_files=2)
    cfg.blocks.keep_blocks = False
    cancel = threading.Event()

    def journal(ligne: str) -> None:
        if ligne.startswith("lot b0002"):  # annulation au 2e lot de construction
            cancel.set()

    with Database(cfg.db_path) as db:
        prepare(db, cfg, csv_path)
        run1 = run_pipeline(db, cfg, progress=journal, cancel=cancel)
        assert run1.blocks_built >= 2
        restants = run1.blocks_built - run1.blocks_done
        assert restants >= 1, "l'annulation a laissé des blocs non envoyés"
        sur_disque = list(cfg.work_dir().rglob("*.md"))
        assert len(sur_disque) == restants, "un bloc `built` garde son `.md`"

        run2 = run_pipeline(db, cfg)
        assert run2.blocks_resumed == restants
        assert db.counts()["done"] == 8
        assert db.counts()["error"] == 0


def test_un_bloc_perdu_sur_le_disque_est_repris_proprement(
    tmp_path: Path, serveur: ServeurProgrammable
) -> None:
    """Même symptôme sans annulation : dossier de travail nettoyé (temporaire,
    antivirus, ménage disque). Le run ne plante pas, le bloc est clos en erreur et
    ses fichiers repartent à analyser — la campagne redémarre."""
    csv_path = corpus(tmp_path, petits=4)
    cfg = config(tmp_path, serveur.base_url, block_tokens=1_200, batch_files=2)
    cancel = threading.Event()
    cancel.set()  # rien n'est construit ni envoyé au run 1
    with Database(cfg.db_path) as db:
        prepare(db, cfg, csv_path)
        run_pipeline(db, cfg, cancel=cancel)
        cfg.blocks.batch_files = 2
        cancel2 = threading.Event()

        def journal(ligne: str) -> None:
            if ligne.startswith("lot b0001"):
                cancel2.set()

        run1 = run_pipeline(db, cfg, progress=journal, cancel=cancel2)
        assert run1.blocks_built >= 1
        for md in cfg.work_dir().rglob("*.md"):
            md.unlink()  # le ménage passe entre deux runs

        run2 = run_pipeline(db, cfg)  # ne lève pas de FileNotFoundError
        assert run2.blocks_error == run1.blocks_built
        assert db.counts()["queued"] == 0
        run3 = run_pipeline(db, cfg)
        assert db.counts()["done"] == 4, f"campagne bloquée : {db.counts()}"
        assert run3.files_error == 0


# --------------------------- MAJEUR 6 : un segment en échec ne tue pas le fichier


def test_un_seul_segment_en_echec_ne_met_pas_le_fichier_en_erreur(
    tmp_path: Path, serveur: ServeurProgrammable
) -> None:
    """Un 503 sur un seul segment (redémarrage de vLLM) ne doit pas condamner un
    fichier de K segments ni faire repayer les K−1 déjà analysés.

    Avant correction, `file_attempts` comptait les BLOCS contenant le fichier : un
    fichier en K parties avait K tentatives dès le premier run, donc `error` dès K ≥ 2.
    """
    csv_path = corpus(tmp_path, gros={"enorme.txt": texte_volumineux(120)})
    cfg = config(tmp_path, serveur.base_url, block_tokens=2_000, max_file_tokens=2_000)

    casse = [True]

    def responder(_p: dict[str, Any], _n: int, sources: list[str]) -> tuple[int, str, bytes]:
        if casse[0] and indice_de_segment(sources[0]) == 3:
            return 503, "text/plain", b"service momentanement indisponible"
        return 200, "application/json", ok_body(sources)

    serveur.responder = responder
    with Database(cfg.db_path) as db:
        prepare(db, cfg, csv_path)
        run1 = run_pipeline(db, cfg)
        segments = run1.blocks_built
        assert segments >= 4
        fichier = db.get_file(1)
        assert fichier is not None
        assert fichier.status == FileStatus.PENDING, "un segment raté n'est pas un fichier raté"
        assert run1.files_error == 0
        assert run1.errors, "le fichier resté à analyser est signalé"
        assert statut_du_run(db) == "error"

        casse[0] = False
        envois_avant = serveur.post_count
        run2 = run_pipeline(db, cfg)
        assert serveur.post_count - envois_avant == 1, "seul le segment manquant est renvoyé"
        assert (run2.files_done, run2.files_error) == (1, 0)
        analyse = db.query("SELECT segments FROM analyses WHERE file_id=1")[0]
        assert int(analyse["segments"]) == segments
        assert db.counts()["done"] == 1


def test_les_tentatives_se_comptent_par_segment(tmp_path: Path) -> None:
    """`file_attempts` : un fichier découpé compte ses essais segment par segment."""
    from docia.models import BlockFile, BlockSpec

    csv_path = corpus(tmp_path, petits=1)
    cfg = config(tmp_path, "http://127.0.0.1:1/v1")
    with Database(cfg.db_path) as db:
        prepare(db, cfg, csv_path)
        fichier = next(iter(db.iter_files()))
        run_id = db.start_run(model="m", prompt_hash="p", config_json="{}")
        for index in (1, 2, 3):
            spec = BlockSpec(
                path=tmp_path / f"seg{index}.md",
                files=[
                    BlockFile(
                        fichier.id,
                        f"doc_0.txt [partie {index}/3]",
                        1,
                        segment_index=index,
                        segment_count=3,
                    )
                ],
                tokens_estimated=10,
                tokens_with_margin=12,
            )
            block_id = db.create_block(run_id, spec, prompt_hash="p", model="m")
            db.mark_block_sent(block_id)
        assert db.file_attempts(fichier.id) == 3, "trois blocs, donc trois envois du fichier"
        assert db.file_attempts(fichier.id, segment_index=2, segment_count=3) == 1
        assert db.file_attempts(fichier.id, segment_index=2, segment_count=7) == 0


# ------------------------------------------------- MINEUR 12 : compteurs justes


def test_les_compteurs_du_rapport_comptent_des_fichiers(
    tmp_path: Path, serveur: ServeurProgrammable
) -> None:
    """Les blocs repris apportent des fichiers absents de `files_selected` : la barre
    de progression affichait 200 % (`files_selected=4 files_done=8`)."""
    csv_path = corpus(tmp_path, petits=8)
    cfg = config(tmp_path, serveur.base_url, block_tokens=1_200, batch_files=2)
    cancel = threading.Event()

    def journal(ligne: str) -> None:
        if ligne.startswith("lot b0002"):
            cancel.set()

    with Database(cfg.db_path) as db:
        prepare(db, cfg, csv_path)
        run1 = run_pipeline(db, cfg, progress=journal, cancel=cancel)
        # Construction et envoi se recouvrent : le premier lot a pu partir avant
        # l'annulation ; le reste est repris par le run 2.
        restants = 8 - run1.files_done
        run2 = run_pipeline(db, cfg)
        assert run2.files_done == restants
        assert run2.files_selected == restants, "les fichiers des blocs repris sont comptés"
        assert run2.files_done <= run2.files_selected
        assert run2.blocks_resumed == run1.blocks_built - run1.blocks_done
