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

from docfuse.core.orchestrator import run_analysis
from docfuse.core.splitter import split_by_budget
from docfuse.i18n import set_language
from docfuse.output.markdown_writer import write_markdown_corpus

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


def build_blocks(
    files: Sequence[FileRow],
    cfg: BlocksConfig,
    work_dir: Path,
    *,
    batch_label: str,
    lang: str = "fr",
) -> BuildResult:
    """Construit les blocs `.md` d'un lot de fichiers.

    Args:
        files: Fichiers du lot, dans l'ordre de sélection.
        cfg: Plafond, marge et moteur de comptage.
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
    inputs: list[Path] = []

    for row in files:
        if not Path(row.path).is_file():
            outcomes.setdefault(row.id, "introuvable")
            continue
        keys = _path_keys(row.path)
        if any(key in rows_by_key for key in keys):
            outcomes.setdefault(row.id, "chemin en double dans le lot")
            continue
        for key in keys:
            rows_by_key[key] = row
        inputs.append(Path(row.path))

    blocks: list[BlockSpec] = []
    if inputs:
        blocks = _run_docfuse(inputs, cfg, work_dir, rows_by_key, outcomes, batch_label, lang)

    placed = {block_file.file_id for block in blocks for block_file in block.files}
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
    return BuildResult(blocks=blocks, failed=failed)


def _run_docfuse(
    inputs: list[Path],
    cfg: BlocksConfig,
    work_dir: Path,
    rows_by_key: dict[str, FileRow],
    outcomes: dict[int, str],
    batch_label: str,
    lang: str,
) -> list[BlockSpec]:
    """Extrait, découpe et écrit les blocs ; alimente `outcomes` au passage."""
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
    for index, extracted in enumerate(result.files):
        row = _row_for(rows_by_key, extracted.path)
        if row is None:
            logger.warning("Fichier rendu par DocFuse sans ligne connue : %s", extracted.path)
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
