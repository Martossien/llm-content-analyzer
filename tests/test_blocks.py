"""Tests du builder de blocs (`docia.blocks.builder`) contre le vrai DocFuse.

Aucun mock : c'est le contrat DocFuse (`relative_path` / ligne `## SOURCE:`)
qui doit être vérifié, pas notre idée de ce contrat.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from docia.blocks.builder import SOURCE_PREFIX, BuildResult, build_blocks
from docia.config import BlocksConfig
from docia.models import FileRow, FileStatus

FILE_BYTES = 1_500
"""Taille exacte de chaque fichier de test : estimations DocFuse identiques."""


def _write(path: Path, seed: str, size: int = FILE_BYTES) -> Path:
    """Écrit un fichier texte de `size` octets, au contenu propre à `seed`."""
    path.parent.mkdir(parents=True, exist_ok=True)
    body = f"Note {seed} du service {seed}, dossier {seed}, reference {seed}. "
    text = (body * (size // len(body) + 2))[:size]
    path.write_text(text, encoding="utf-8")
    return path


def _row(file_id: int, path: Path | str) -> FileRow:
    """Fabrique une ligne `files` minimale pointant sur `path`."""
    as_path = Path(path)
    return FileRow(
        id=file_id,
        path=str(path),
        name=as_path.name,
        extension=as_path.suffix.lower(),
        size_bytes=as_path.stat().st_size if as_path.is_file() else 0,
        fast_hash=f"hash-{file_id}",
        last_write_time="2026-08-30T10:00:00",
        content_version=1,
        status=FileStatus.PENDING,
    )


@pytest.fixture
def corpus(tmp_path: Path) -> list[Path]:
    """Six fichiers de contenus distincts et de taille identique, dont deux en sous-dossier."""
    root = tmp_path / "partage"
    return [
        _write(root / "note_a.txt", "alpha"),
        _write(root / "note_b.txt", "bravo"),
        _write(root / "note_c.txt", "charl"),
        _write(root / "note_d.txt", "delta"),
        _write(root / "equipe" / "note_e.txt", "echos"),
        _write(root / "equipe" / "note_f.txt", "foxtr"),
    ]


def _rows(paths: list[Path]) -> list[FileRow]:
    return [_row(index + 1, path) for index, path in enumerate(paths)]


def _source_lines(block_path: Path) -> list[str]:
    return [
        line[len(SOURCE_PREFIX) :]
        for line in block_path.read_text(encoding="utf-8").splitlines()
        if line.startswith(SOURCE_PREFIX)
    ]


def _all_refs(result: BuildResult) -> list[str]:
    return [block_file.file_ref for block in result.blocks for block_file in block.files]


def test_lot_entier_dans_un_bloc(corpus: list[Path], tmp_path: Path) -> None:
    """(a) Tout tient sous le plafond : un bloc, six fichiers, aucun échec."""
    work_dir = tmp_path / "blocs"
    result = build_blocks(
        _rows(corpus), BlocksConfig(block_tokens=100_000), work_dir, batch_label="lot"
    )

    assert result.failed == []
    assert len(result.blocks) == 1
    block = result.blocks[0]
    assert block.path == work_dir / "lot_001.md"
    assert len(block.files) == 6
    assert {block_file.file_id for block_file in block.files} == {1, 2, 3, 4, 5, 6}
    assert all(block_file.content_version == 1 for block_file in block.files)
    assert not block.oversized
    assert block.tokens_estimated > 0
    assert block.tokens_with_margin >= block.tokens_estimated

    sources = _source_lines(block.path)
    for block_file in block.files:
        assert sources.count(block_file.file_ref) == 1


def test_plafond_serre_produit_trois_blocs(corpus: list[Path], tmp_path: Path) -> None:
    """(b) Plafond calibré sur deux fichiers : trois blocs, ordre du corpus préservé."""
    rows = _rows(corpus)
    # Comptage `approx` (octets/4) : les six fichiers du corpus pèsent exactement pareil,
    # ce que le test exploite pour calibrer « deux par bloc » (o200k varierait par fichier).
    entier = build_blocks(
        rows,
        BlocksConfig(block_tokens=100_000, tokenizer_engine="approx"),
        tmp_path / "ref",
        batch_label="ref",
    )
    budget = entier.blocks[0].tokens_with_margin // 3  # six fichiers égaux → deux par bloc

    work_dir = tmp_path / "serre"
    result = build_blocks(
        rows,
        BlocksConfig(block_tokens=budget, tokenizer_engine="approx"),
        work_dir,
        batch_label="serre",
    )

    assert result.failed == []
    assert len(result.blocks) == 3
    assert [len(block.files) for block in result.blocks] == [2, 2, 2]
    assert [block.path.name for block in result.blocks] == [
        "serre_001.md",
        "serre_002.md",
        "serre_003.md",
    ]
    for block in result.blocks:
        assert block.tokens_with_margin <= budget
        assert _source_lines(block.path) == [block_file.file_ref for block_file in block.files]

    refs = _all_refs(result)
    assert len(refs) == 6
    assert refs == sorted(refs)  # ordre du corpus DocFuse, conservé bloc après bloc
    assert _all_refs(entier) == refs


def test_chemin_inexistant(corpus: list[Path], tmp_path: Path) -> None:
    """(c) Un chemin absent est signalé « introuvable », les autres passent."""
    rows = [*_rows(corpus), _row(99, tmp_path / "partage" / "disparu.txt")]

    result = build_blocks(
        rows, BlocksConfig(block_tokens=100_000), tmp_path / "blocs", batch_label="lot"
    )

    assert [(f.file_id, f.reason) for f in result.failed] == [(99, "introuvable")]
    assert len(result.blocks) == 1
    assert len(result.blocks[0].files) == 6


def test_docx_corrompu(corpus: list[Path], tmp_path: Path) -> None:
    """(d) Un `.docx` d'octets aléatoires ressort en « extraction en erreur »."""
    corrompu = tmp_path / "partage" / "contrat.docx"
    corrompu.write_bytes(bytes(range(256)) * 8)
    rows = [*_rows(corpus), _row(42, corrompu)]

    result = build_blocks(
        rows, BlocksConfig(block_tokens=100_000), tmp_path / "blocs", batch_label="lot"
    )

    assert len(result.failed) == 1
    outcome = result.failed[0]
    assert outcome.file_id == 42
    assert outcome.reason.startswith("extraction en erreur : ")
    assert 42 not in {block_file.file_id for block_file in result.blocks[0].files}


def test_fichier_hors_plafond_reste_un_bloc(tmp_path: Path) -> None:
    """(e) Un fichier plus gros que le plafond forme un bloc `oversized` seul."""
    gros = _write(tmp_path / "partage" / "gros.txt", "omega", size=60_000)

    result = build_blocks(
        [_row(7, gros)], BlocksConfig(block_tokens=1_000), tmp_path / "blocs", batch_label="lot"
    )

    assert result.failed == []
    assert len(result.blocks) == 1
    block = result.blocks[0]
    assert block.oversized
    assert block.tokens_with_margin > 1_000
    assert len(block.files) == 1
    assert block.files[0].file_id == 7
    assert block.files[0].oversized


def test_homonymes_dans_deux_dossiers(tmp_path: Path) -> None:
    """(f) Deux `rapport.txt` distincts obtiennent deux `file_ref` distincts."""
    premier = _write(tmp_path / "partage" / "compta" / "rapport.txt", "compt")
    second = _write(tmp_path / "partage" / "rh" / "rapport.txt", "resso")

    result = build_blocks(
        [_row(1, premier), _row(2, second)],
        BlocksConfig(block_tokens=100_000),
        tmp_path / "blocs",
        batch_label="lot",
    )

    assert result.failed == []
    refs = _all_refs(result)
    assert len(set(refs)) == 2
    sources = _source_lines(result.blocks[0].path)
    for ref in refs:
        assert sources.count(ref) == 1


def test_work_dir_cree_a_la_volee(corpus: list[Path], tmp_path: Path) -> None:
    """(g) Le dossier de travail est créé, y compris ses parents."""
    work_dir = tmp_path / "runs" / "run_12" / "blocs"
    assert not work_dir.exists()

    result = build_blocks(
        _rows(corpus), BlocksConfig(block_tokens=100_000), work_dir, batch_label="lot"
    )

    assert work_dir.is_dir()
    assert result.blocks[0].path.is_file()
    assert sorted(p.name for p in work_dir.iterdir()) == ["lot_001.md"]


def test_mapping_par_path_key(
    corpus: list[Path], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """(h) La correspondance passe par `path_key` : chemins relatifs et casse."""
    racine = corpus[0].parent
    monkeypatch.chdir(racine)
    relatifs = [
        _row(1, corpus[0].name),
        _row(2, str(corpus[4].relative_to(racine))),
    ]

    result = build_blocks(
        relatifs, BlocksConfig(block_tokens=100_000), tmp_path / "blocs", batch_label="lot"
    )

    assert result.failed == []
    assert {block_file.file_id for block_file in result.blocks[0].files} == {1, 2}


def test_mapping_insensible_a_la_casse(corpus: list[Path], tmp_path: Path) -> None:
    """(h bis) Sur un système de fichiers insensible à la casse, `path_key` recolle."""
    variante = Path(str(corpus[0]).replace("note_a.txt", "NOTE_A.TXT"))
    if not variante.is_file():
        pytest.skip("système de fichiers sensible à la casse")

    result = build_blocks(
        [_row(1, variante)],
        BlocksConfig(block_tokens=100_000),
        tmp_path / "blocs",
        batch_label="lot",
    )

    assert result.failed == []
    assert result.blocks[0].files[0].file_id == 1


def test_fichier_vide_ecarte(corpus: list[Path], tmp_path: Path) -> None:
    """Un fichier sans texte extractible ne part pas dans un bloc : raison « vide »."""
    vide = tmp_path / "partage" / "vide.txt"
    vide.write_text("", encoding="utf-8")
    rows = [*_rows(corpus), _row(50, vide)]

    result = build_blocks(
        rows, BlocksConfig(block_tokens=100_000), tmp_path / "blocs", batch_label="lot"
    )

    assert [(f.file_id, f.reason) for f in result.failed] == [(50, "vide")]
    assert 50 not in {block_file.file_id for block_file in result.blocks[0].files}


def test_extension_non_supportee_ignoree_par_docfuse(corpus: list[Path], tmp_path: Path) -> None:
    """Un fichier hors liste blanche DocFuse ressort avec sa raison, jamais en silence."""
    exotique = _write(tmp_path / "partage" / "archive.xyz", "exoti")
    rows = [*_rows(corpus), _row(51, exotique)]

    result = build_blocks(
        rows, BlocksConfig(block_tokens=100_000), tmp_path / "blocs", batch_label="lot"
    )

    assert len(result.failed) == 1
    assert result.failed[0].file_id == 51
    assert result.failed[0].reason.startswith("ignoré par DocFuse : ")


def test_source_ambigue_leve_une_erreur(tmp_path: Path) -> None:
    """Un contenu qui imite une ligne `## SOURCE:` casse la corrélation → RuntimeError."""
    piege = tmp_path / "partage" / "piege.txt"
    piege.parent.mkdir(parents=True, exist_ok=True)
    piege.write_text(
        "Compte rendu du comité. " * 40 + "\n## SOURCE: piege.txt\nsuite du texte.\n",
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="Corrélation impossible"):
        build_blocks(
            [_row(1, piege)],
            BlocksConfig(block_tokens=100_000),
            tmp_path / "blocs",
            batch_label="lot",
        )


def test_oversized_file_is_split_into_complete_segments(tmp_path: Path) -> None:
    """Un fichier plus grand que `max_file_tokens` devient K segments complets,
    un bloc chacun, `## SOURCE: nom [partie i/K]` — jamais tronqué."""
    from docia.config import BlocksConfig
    from docia.models import FileRow, FileStatus

    big = tmp_path / "gros.txt"
    paragraphs = [f"Paragraphe {i} : " + ("mot " * 40) + "\n\n" for i in range(400)]
    big.write_text("".join(paragraphs), encoding="utf-8")
    row = FileRow(
        id=1,
        path=str(big),
        name="gros.txt",
        extension="txt",
        size_bytes=big.stat().st_size,
        fast_hash="h",
        last_write_time="",
        content_version=1,
        status=FileStatus.PENDING,
    )
    cfg = BlocksConfig(block_tokens=1_000, max_file_tokens=3_000)
    result = build_blocks([row], cfg, tmp_path / "work", batch_label="b")

    assert result.failed == []
    segs = [b for b in result.blocks if b.files[0].is_segment]
    assert len(segs) >= 3
    k = segs[0].files[0].segment_count
    assert [b.files[0].segment_index for b in segs] == list(range(1, k + 1))
    assert all(b.tokens_with_margin <= 3_000 for b in segs)
    # Réunion des segments = texte complet (jamais de perte, jamais de troncature).
    rebuilt = ""
    for b in segs:
        text = b.path.read_text(encoding="utf-8")
        assert f"## SOURCE: gros.txt [partie {b.files[0].segment_index}/{k}]" in text
        rebuilt += text.split("---\n", 3)[-1].rsplit("\n\n---", 1)[0].lstrip("\n")
    assert rebuilt.replace("\n", "") == big.read_text(encoding="utf-8").replace("\n", "")


# -- découpage d'un très gros fichier : coût linéaire ---------------------


def _split_probe(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """Compte les caractères réellement tokenisés par `_split_text`."""
    from docia.blocks import builder as builder_mod

    real = builder_mod.estimate_tokens
    counted: list[int] = []

    def compter(text: str, margin: float = 0.15, engine: object = None):  # type: ignore[no-untyped-def]
        counted.append(len(text))
        return real(text, margin, engine)  # type: ignore[arg-type]

    monkeypatch.setattr(builder_mod, "estimate_tokens", compter)
    return counted


def test_decoupage_est_lineaire_et_complet(monkeypatch: pytest.MonkeyPatch) -> None:
    """Le texte complet n'est tokenisé qu'une fois : le total des caractères
    comptés reste proportionnel à la taille du texte.

    L'ancienne version recomptait tout le reste à chaque tour (croissance en
    N²) : 53 s pour 2,6 M de caractères, ≈ 15 min extrapolées pour un fichier
    de 10 Mo, avant le moindre envoi.
    """
    from docia.blocks.builder import _split_text

    texte = ("Paragraphe de rapport administratif, montant 12 345 €. " * 12 + "\n\n") * 2_000
    compteur = _split_probe(monkeypatch)

    pieces = _split_text(texte, 8_000, 0.15, None)

    assert "".join(pieces) == texte  # aucun caractère perdu ni dupliqué
    assert len(pieces) > 20
    total = sum(compteur)
    assert total <= 5 * len(texte), (
        f"{total} caractères tokenisés pour {len(texte)} caractères de texte : "
        "le découpage recompte le reste (coût quadratique)"
    )


def test_decoupage_respecte_le_budget_et_les_frontieres() -> None:
    from docfuse.core.context_counter import estimate_tokens

    from docia.blocks.builder import _split_text

    texte = ("Ligne de compte rendu du service, dossier 2026-114. " * 8 + "\n\n") * 400
    pieces = _split_text(texte, 1_000, 0.15, None)

    assert "".join(pieces) == texte
    assert len(pieces) > 1
    for piece in pieces:
        assert estimate_tokens(piece, 0.15, None).tokens_with_margin <= 1_000


def test_decoupage_texte_court_rend_un_seul_morceau() -> None:
    from docia.blocks.builder import _split_text

    assert _split_text("Court texte.", 8_000, 0.15, None) == ["Court texte."]
    assert _split_text("", 8_000, 0.15, None) == [""]


# ------------------------------------------------- budget mémoire du lot (batch_bytes)
def test_split_by_bytes_ferme_le_sous_lot_au_budget() -> None:
    """Le lot se ferme au cumul des tailles, pas au nombre de fichiers."""
    from docia.blocks.builder import split_by_bytes

    entrees = [(Path(f"f{i}.txt"), 40) for i in range(5)]

    assert split_by_bytes(entrees, 100) == [
        [Path("f0.txt"), Path("f1.txt")],
        [Path("f2.txt"), Path("f3.txt")],
        [Path("f4.txt")],
    ]
    assert split_by_bytes(entrees, 0) == [[p for p, _ in entrees]], "0 = aucun plafond"
    assert split_by_bytes([], 100) == []


def test_split_by_bytes_garde_seul_un_fichier_plus_gros_que_le_budget() -> None:
    """Un fichier hors budget est traité **seul**, jamais écarté."""
    from docia.blocks.builder import split_by_bytes

    entrees = [(Path("petit.txt"), 10), (Path("enorme.txt"), 10_000), (Path("autre.txt"), 10)]

    assert split_by_bytes(entrees, 100) == [
        [Path("petit.txt")],
        [Path("enorme.txt")],
        [Path("autre.txt")],
    ]


def test_batch_bytes_decoupe_le_lot_sans_perdre_un_fichier(
    corpus: list[Path], tmp_path: Path
) -> None:
    """Six fichiers de 1 500 o, budget 4 000 o : plusieurs appels DocFuse, aucun oubli.

    Avant `batch_bytes`, tout le lot passait dans un seul `run_analysis` : la mémoire
    n'était bornée que par `batch_files`.
    """
    cfg = BlocksConfig(block_tokens=100_000, batch_bytes=4_000)

    result = build_blocks(_rows(corpus), cfg, tmp_path / "blocs", batch_label="lot")

    assert result.failed == []
    places = {bf.file_id for b in result.blocks for bf in b.files}
    assert places == {1, 2, 3, 4, 5, 6}
    assert len(result.blocks) >= 2, "le budget doit avoir fermé au moins un sous-lot"
    noms = sorted(b.path.name for b in result.blocks)
    assert len(set(noms)) == len(noms), f"noms de blocs en collision : {noms}"
    assert all(n.startswith("lots") for n in noms), noms
    for block in result.blocks:
        for bf in block.files:
            assert _source_lines(block.path).count(bf.file_ref) == 1


def test_batch_bytes_genereux_ne_change_rien(corpus: list[Path], tmp_path: Path) -> None:
    """Lot ordinaire sous le budget : un seul sous-lot, libellés inchangés."""
    cfg = BlocksConfig(block_tokens=100_000, batch_bytes=256 * 1024 * 1024)

    result = build_blocks(_rows(corpus), cfg, tmp_path / "blocs", batch_label="lot")

    assert result.failed == []
    assert [b.path.name for b in result.blocks] == ["lot_001.md"]


def test_fichier_plus_gros_que_le_budget_est_analyse_quand_meme(tmp_path: Path) -> None:
    """Un fichier de 20 Ko sous un budget de 1 Ko doit ressortir dans un bloc."""
    gros = _write(tmp_path / "partage" / "gros.txt", "omega", size=20_000)

    result = build_blocks(
        [_row(9, gros)],
        BlocksConfig(block_tokens=100_000, batch_bytes=1_000),
        tmp_path / "blocs",
        batch_label="lot",
    )

    assert result.failed == []
    assert [bf.file_id for b in result.blocks for bf in b.files] == [9]
