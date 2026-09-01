"""Construction des blocs `.md` envoyés à la LLM, via DocFuse (`docs/DESIGN_V3.md` §5).

Le builder ne touche pas à la base : il reçoit des `FileRow`, appelle DocFuse
(inventaire → extraction → comptage → découpage), écrit un `.md` par partie
dans `work_dir` et rend des `BlockSpec`. Tout fichier qui n'entre dans aucun
bloc ressort dans `BuildResult.failed` avec une raison en français : la règle
« jamais de perte silencieuse » (§2) s'applique ici en premier.

La clé de corrélation avec la réponse LLM est `BlockFile.file_ref`, égale à
`ExtractedFile.relative_path` et donc à la valeur exacte de la ligne
`## SOURCE:` du bloc. Elle est revérifiée dans le fichier écrit.
"""

from __future__ import annotations

import logging
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path

from docfuse.core.context_counter import estimate_tokens
from docfuse.core.orchestrator import run_analysis
from docfuse.core.splitter import split_by_budget
from docfuse.i18n import set_language
from docfuse.models.extraction_result import ExtractedFile
from docfuse.output.markdown_writer import write_markdown_corpus
from docfuse.output.source_header import build_source_header

from docia.config import BlocksConfig
from docia.models import BlockFile, BlockSpec, FileRow, path_key

logger = logging.getLogger(__name__)

SOURCE_PREFIX = "## SOURCE: "
"""Préfixe de la ligne d'en-tête DocFuse qui porte le `file_ref`."""


@dataclass(frozen=True)
class FileOutcome:
    """Un fichier qui n'a pas pu entrer dans un bloc, et pourquoi."""

    file_id: int
    reason: str


@dataclass
class BuildResult:
    """Blocs construits et fichiers écartés du lot."""

    blocks: list[BlockSpec] = field(default_factory=list)
    failed: list[FileOutcome] = field(default_factory=list)
    duplicates: list[tuple[int, int]] = field(default_factory=list)
    """(file_id, original_file_id) : contenu strictement identique à un autre fichier
    du lot (détection DocFuse) — pas envoyé, hérite de l'analyse de l'original."""


def split_by_bytes(entries: Sequence[tuple[Path, int]], budget_bytes: int) -> list[list[Path]]:
    """Découpe un lot en sous-lots dont le cumul des tailles tient dans `budget_bytes`.

    C'est le **seul** garde-fou mémoire de l'extraction : `blocks.batch_files` ne
    compte que des fichiers, or DocFuse garde en RAM le texte extrait de tout le lot
    avant de le découper. Sur un partage où quelques fichiers pèsent 100 Mo (plafond
    par défaut de `filter.max_size_bytes`), un lot de 50 fichiers demandait plusieurs
    gigaoctets sur un serveur Windows qui n'en a que 8.

    Un fichier seul plus gros que le budget forme son propre sous-lot : il est traité,
    jamais écarté — le budget borne le cumul, il ne filtre rien.

    Args:
        entries: `(chemin, taille en octets)` dans l'ordre de sélection.
        budget_bytes: Plafond du cumul par sous-lot ; `0` ou moins = aucun plafond.

    Returns:
        Les sous-lots, dans l'ordre ; liste vide si `entries` est vide.
    """
    if not entries:
        return []
    if budget_bytes <= 0:
        return [[path for path, _ in entries]]
    chunks: list[list[Path]] = []
    current: list[Path] = []
    total = 0
    for path, size in entries:
        if current and total + size > budget_bytes:
            chunks.append(current)
            current, total = [], 0
        current.append(path)
        total += size
    if current:
        chunks.append(current)
    return chunks


def build_blocks(
    files: Sequence[FileRow],
    cfg: BlocksConfig,
    work_dir: Path,
    *,
    batch_label: str,
    lang: str = "fr",
) -> BuildResult:
    """Construit les blocs `.md` d'un lot de fichiers.

    Le lot est découpé en sous-lots par `cfg.batch_bytes` (voir `split_by_bytes`) et
    DocFuse est appelé une fois par sous-lot : c'est ce qui borne la mémoire. Deux
    conséquences assumées quand le budget mord — donc jamais sur un lot ordinaire de
    documents bureautiques : les blocs portent un libellé `<label>sNN`, et la détection
    des doublons exacts par DocFuse ne joue qu'à l'intérieur d'un sous-lot (deux copies
    séparées par une coupure sont analysées deux fois — un coût, pas une perte).

    Args:
        files: Fichiers du lot, dans l'ordre de sélection.
        cfg: Plafond, marge, moteur de comptage et budget mémoire du lot.
        work_dir: Dossier des `.md` (créé au besoin) ; rien n'est écrit ailleurs.
        batch_label: Préfixe des noms de blocs (`<label>_001.md`).
        lang: Langue DocFuse (en-têtes et raisons d'exclusion).

    Returns:
        Les blocs prêts à envoyer et les fichiers écartés avec leur raison.

    Raises:
        RuntimeError: un `file_ref` n'apparaît pas exactement une fois dans le
            `.md` écrit — la corrélation avec la réponse LLM serait ambiguë.
    """
    outcomes: dict[int, str] = {}
    rows_by_key: dict[str, FileRow] = {}
    inputs: list[tuple[Path, int]] = []

    for row in files:
        path = Path(row.path)
        if not path.is_file():
            outcomes.setdefault(row.id, "introuvable")
            continue
        try:
            size = path.stat().st_size
        except OSError:  # disparu entre les deux appels : la taille du scan fera foi
            size = 0
        keys = _path_keys(row.path)
        if any(key in rows_by_key for key in keys):
            outcomes.setdefault(row.id, "chemin en double dans le lot")
            continue
        for key in keys:
            rows_by_key[key] = row
        inputs.append((path, max(size, row.size_bytes)))

    blocks: list[BlockSpec] = []
    duplicates: dict[int, int] = {}
    chunks = split_by_bytes(inputs, cfg.batch_bytes)
    if len(chunks) > 1:
        logger.info(
            "Lot %s : %d fichier(s), %.0f Mo — découpé en %d sous-lot(s) "
            "pour tenir le budget mémoire blocks.batch_bytes (%.0f Mo)",
            batch_label,
            len(inputs),
            sum(size for _, size in inputs) / 1e6,
            len(chunks),
            cfg.batch_bytes / 1e6,
        )
    for number, chunk in enumerate(chunks, 1):
        # Un seul sous-lot : le libellé — donc les noms de blocs — ne change pas.
        label = batch_label if len(chunks) == 1 else f"{batch_label}s{number:02d}"
        blocks.extend(
            _run_docfuse(chunk, cfg, work_dir, rows_by_key, outcomes, duplicates, label, lang)
        )

    placed = {block_file.file_id for block in blocks for block_file in block.files}
    placed |= set(duplicates)
    for row in files:
        if row.id not in placed:
            outcomes.setdefault(row.id, "absent du résultat DocFuse")
    failed = [FileOutcome(row.id, outcomes[row.id]) for row in files if row.id not in placed]

    logger.info(
        "Lot %s : %d bloc(s), %d fichier(s) placé(s), %d écarté(s)",
        batch_label,
        len(blocks),
        len(placed),
        len(failed),
    )
    for outcome in failed:
        logger.debug(
            "Lot %s : fichier %d écarté (%s)", batch_label, outcome.file_id, outcome.reason
        )
    return BuildResult(blocks=blocks, failed=failed, duplicates=sorted(duplicates.items()))


def _run_docfuse(
    inputs: list[Path],
    cfg: BlocksConfig,
    work_dir: Path,
    rows_by_key: dict[str, FileRow],
    outcomes: dict[int, str],
    duplicates: dict[int, int],
    batch_label: str,
    lang: str,
) -> list[BlockSpec]:
    """Extrait, découpe et écrit les blocs ; alimente `outcomes` et `duplicates`."""
    set_language(lang)
    result = run_analysis(
        input_path=inputs,
        context_limit=cfg.block_tokens,
        margin=cfg.margin,
        recursive=False,
        tokenizer_engine=cfg.tokenizer_engine,
        split_context=True,
    )

    for ignored_path, reason in result.ignored:
        row = _row_for(rows_by_key, ignored_path)
        if row is not None:
            outcomes.setdefault(row.id, f"ignoré par DocFuse : {reason}")

    # Fichiers exploitables, par indice dans `result.files` (aligné sur les
    # `file_indices` des parties).
    rows_by_index: dict[int, FileRow] = {}
    index_by_ref = {f.relative_path: i for i, f in enumerate(result.files)}
    for index, extracted in enumerate(result.files):
        row = _row_for(rows_by_key, extracted.path)
        if row is None:
            logger.warning("Fichier rendu par DocFuse sans ligne connue : %s", extracted.path)
            continue
        original_ref = extracted.extra_metadata.get("duplicate_of")
        if original_ref:
            # Doublon exact (D-064 DocFuse) : le texte est remplacé par un renvoi.
            # On n'envoie pas le renvoi à la LLM ; le pipeline copie l'analyse
            # de l'original.
            original = rows_by_index.get(index_by_ref.get(original_ref, -1))
            if original is None:
                orig_idx = index_by_ref.get(original_ref)
                original = (
                    _row_for(rows_by_key, result.files[orig_idx].path)
                    if orig_idx is not None
                    else None
                )
            if original is not None:
                duplicates[row.id] = original.id
                continue
            outcomes.setdefault(row.id, f"doublon de {original_ref} (original hors lot)")
            continue
        if not extracted.status.is_extracted():
            detail = extracted.error_message or str(extracted.status)
            outcomes.setdefault(row.id, f"extraction en erreur : {detail}")
            continue
        if not extracted.text.strip():
            outcomes.setdefault(row.id, "vide")
            continue
        rows_by_index[index] = row

    parts = split_by_budget(result)
    work_dir.mkdir(parents=True, exist_ok=True)

    blocks: list[BlockSpec] = []
    for part in parts:
        if (
            part.oversized
            and cfg.max_file_tokens > 0
            and part.tokens_with_margin > cfg.max_file_tokens
            and len(part.file_indices) == 1
            and part.file_indices[0] in rows_by_index
        ):
            # Fichier plus grand que le contexte du modèle : segments complets,
            # un bloc par segment, agrégés ensuite par le pipeline (jamais tronqué).
            index = part.file_indices[0]
            blocks.extend(
                _segment_blocks(
                    result.files[index],
                    rows_by_index[index],
                    cfg,
                    work_dir,
                    f"{batch_label}_{part.index:03d}",
                    engine=result.engine,
                )
            )
            continue
        block_files = [
            BlockFile(
                file_id=rows_by_index[index].id,
                file_ref=result.files[index].relative_path,
                content_version=rows_by_index[index].content_version,
                oversized=part.oversized,
            )
            for index in part.file_indices
            if index in rows_by_index
        ]
        if not block_files:
            # Partie ne contenant que des fichiers vides ou inconnus : pas de
            # bloc, chaque fichier a déjà sa raison dans `outcomes`.
            continue
        path = work_dir / f"{batch_label}_{part.index:03d}.md"
        write_markdown_corpus(result, path, cfg.margin, "lf", part=part, parts_total=len(parts))
        _verify_source_lines(path, block_files)
        blocks.append(
            BlockSpec(
                path=path,
                files=block_files,
                tokens_estimated=part.tokens_estimated,
                tokens_with_margin=part.tokens_with_margin,
                oversized=part.oversized,
            )
        )
    return blocks


def _segment_blocks(
    extracted: ExtractedFile,
    row: FileRow,
    cfg: BlocksConfig,
    work_dir: Path,
    label: str,
    *,
    engine: object,
) -> list[BlockSpec]:
    """Découpe le texte d'un fichier en K segments ≤ `cfg.max_file_tokens` (tokens
    avec marge), aux limites de paragraphes, et écrit un bloc par segment avec
    `## SOURCE: <ref> [partie i/K]`. Chaque segment est complet : la réunion des
    K segments est exactement le texte extrait."""
    from copy import copy

    from docfuse.core.tokenizers.base import TokenizerEngine

    text: str = extracted.text
    relative_path: str = extracted.relative_path
    eng = engine if isinstance(engine, TokenizerEngine) else None
    # Budget de texte par segment : le plafond moins l'en-tête SOURCE (~80 tokens).
    budget = max(500, cfg.max_file_tokens - 200)
    pieces = _split_text(text, budget, cfg.margin, eng)
    k = len(pieces)
    specs: list[BlockSpec] = []
    for i, piece in enumerate(pieces, 1):
        seg = copy(extracted)
        seg.text = piece
        seg.relative_path = f"{relative_path} [partie {i}/{k}]"
        est = estimate_tokens(piece, cfg.margin, eng)
        header = build_source_header(seg, cfg.margin, est.tokens_estimated, est.tokens_with_margin)
        path = work_dir / f"{label}_seg{i:03d}.md"
        body = f"# Corpus DocFuse — segment {i}/{k}\n\n---\n\n{header}\n\n{piece}\n\n---\n"
        path.write_bytes(body.encode("utf-8"))
        block_file = BlockFile(
            file_id=row.id,
            file_ref=f"{relative_path} [partie {i}/{k}]",
            content_version=row.content_version,
            oversized=True,
            segment_index=i,
            segment_count=k,
        )
        _verify_source_lines(path, [block_file])
        specs.append(
            BlockSpec(
                path=path,
                files=[block_file],
                tokens_estimated=est.tokens_estimated,
                tokens_with_margin=est.tokens_with_margin,
                oversized=True,
            )
        )
    logger.info(
        "%s : fichier découpé en %d segments (%d tokens)",
        relative_path,
        k,
        sum(s.tokens_with_margin for s in specs),
    )
    return specs


def _split_text(text: str, budget_tokens: int, margin: float, engine: object) -> list[str]:
    """Découpe `text` en morceaux ≤ `budget_tokens` (avec marge), en coupant de
    préférence sur une ligne vide, sinon une fin de ligne, sinon un espace.

    Coût LINÉAIRE : le texte complet n'est tokenisé qu'UNE fois, converti en un
    ratio caractères/token, puis on avance par fenêtre — chaque comptage porte
    sur le morceau candidat, jamais sur tout le reste du texte. La version qui
    recomptait le reste à chaque tour était quadratique : 53 s pour 2,6 M de
    caractères (≈ 15 min extrapolées pour un fichier texte de 10 Mo, avant le
    moindre envoi et sans aucune trace d'avancement).
    """
    from docfuse.core.tokenizers.base import TokenizerEngine

    eng = engine if isinstance(engine, TokenizerEngine) else None

    def tokens(s: str) -> int:
        return estimate_tokens(s, margin, eng).tokens_with_margin

    total_chars = len(text)
    if total_chars == 0:
        return [text]
    total_tokens = tokens(text)  # LE seul comptage sur le texte entier
    if total_tokens <= budget_tokens:
        return [text]
    chars_per_token = total_chars / max(1, total_tokens)
    # Fenêtre visée : 95 % du budget, pour laisser l'ajustement descendant converger vite.
    window = max(1, int(budget_tokens * chars_per_token * 0.95))

    pieces: list[str] = []
    pos = 0
    while pos < total_chars:
        remaining = total_chars - pos
        # Ne compter le reste que s'il est plausiblement dans le budget : ce
        # comptage porte alors sur une longueur bornée, pas sur tout le texte.
        if remaining <= int(window * 1.5) and tokens(text[pos:]) <= budget_tokens:
            pieces.append(text[pos:])
            break
        cut = min(total_chars, pos + window)
        while cut > pos + 1 and tokens(text[pos:cut]) > budget_tokens:
            cut = pos + max(1, int((cut - pos) * 0.9))
        # Reculer jusqu'à une frontière naturelle (dans les 20 % précédents).
        floor = pos + int((cut - pos) * 0.8)
        for sep in ("\n\n", "\n", " "):
            found = text.rfind(sep, floor, cut)
            if found > pos:
                cut = found + len(sep)
                break
        pieces.append(text[pos:cut])
        pos = cut
    return [p for p in pieces if p.strip()] or [text]


def _path_keys(raw: str) -> list[str]:
    """Clés de correspondance d'un chemin : tel quel, et absolu (DocFuse absolutise)."""
    keys = [path_key(raw)]
    try:
        absolute = path_key(Path(raw).absolute())
    except OSError:  # pragma: no cover — cwd supprimé sous les pieds du process
        return keys
    if absolute not in keys:
        keys.append(absolute)
    return keys


def _row_for(rows_by_key: dict[str, FileRow], path: Path) -> FileRow | None:
    """Retrouve la ligne d'origine d'un chemin rendu par DocFuse."""
    for key in _path_keys(str(path)):
        row = rows_by_key.get(key)
        if row is not None:
            return row
    return None


def _verify_source_lines(path: Path, block_files: Sequence[BlockFile]) -> None:
    """Contrôle qu'un `file_ref` = une et une seule ligne `## SOURCE:` du bloc."""
    counts = Counter(
        line[len(SOURCE_PREFIX) :]
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.startswith(SOURCE_PREFIX)
    )
    for block_file in block_files:
        found = counts[block_file.file_ref]
        if found != 1:
            raise RuntimeError(
                f"Corrélation impossible dans {path} : la ligne "
                f"« {SOURCE_PREFIX}{block_file.file_ref} » apparaît {found} fois "
                f"(attendu : exactement une)"
            )
