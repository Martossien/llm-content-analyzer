"""Banc de vitesse LLM (`docia bench`, `docs/DESIGN_V3.md` §10 lot C).

L'utilisateur comme l'administrateur doivent pouvoir répondre à « combien de
fichiers par heure ce serveur tient-il ? » sans toucher au partage réel : le
banc fabrique des blocs synthétiques **en français** au format DocFuse, les
envoie en parallèle comme le pipeline le ferait, et mesure débit de prefill,
débit de décodage, latence, JSON valides et surcoût du raisonnement.

Aucune base n'est écrite : les blocs vivent dans un dossier temporaire supprimé
en fin de mesure.
"""

from __future__ import annotations

import asyncio
import logging
import random
import shutil
import sqlite3
import statistics
import tempfile
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path

from docia.config import Config, LLMConfig
from docia.db import Database
from docia.llm.client import LLMClient, LLMError
from docia.llm.parse import ParseError, parse_block_response
from docia.llm.schema import load_system_prompt
from docia.models import BlockFile, BlockSpec

logger = logging.getLogger(__name__)

CHARS_PER_TOKEN = 3.3
"""Français : ≈ 3,3 caractères par token (banc du 30/08). Sert à viser une taille de bloc."""

MIN_DOC_CHARS = 400
"""Un document de banc plus court n'aurait plus rien d'un document administratif."""

ProgressCallback = Callable[[str], None]


# --------------------------------------------------------------- corpus


_TEMPLATES: tuple[str, ...] = (
    "Le conseil municipal de {ville}, réuni en séance ordinaire le {jour} {mois} 2026, a délibéré "
    "sur le budget supplémentaire de l'exercice. Après examen du rapport de la commission des "
    "finances, les crédits d'investissement sont arrêtés à {montant} € et les crédits de "
    "fonctionnement à {montant2} €. La délibération est transmise au contrôle de légalité. ",
    "La direction des ressources humaines rappelle que les entretiens annuels d'évaluation "
    "doivent être conduits avant le {jour} {mois}. Les données collectées (appréciations, "
    "souhaits de mobilité, formations suivies) sont conservées trois ans au registre des "
    "traitements, conformément au RGPD ; le délégué à la protection des données en est informé. ",
    "Facture n° {numero} — prestation de maintenance du parc informatique du site de {ville}. "
    "Montant HT : {montant} € — TVA 20 % : {montant2} € — échéance à trente jours fin de mois. "
    "Tout retard de paiement entraîne une pénalité égale à trois fois le taux d'intérêt légal, "
    "outre l'indemnité forfaitaire de recouvrement de 40 €. ",
    "Contrat de prestation de services conclu entre la société {ville} Services et le service "
    "acheteur. Durée : douze mois reconductibles une fois. Le prestataire s'engage à respecter "
    "la confidentialité des informations transmises et à restituer l'intégralité des supports "
    "au terme du contrat. Résiliation possible avec un préavis de trois mois. ",
    "Incident n° {numero} : interruption de connectivité sur le site de {ville} pendant "
    "{duree} minutes. Cause identifiée : commutateur défaillant en salle technique. Un matériel "
    "de remplacement a été installé le lendemain. Aucune donnée à caractère personnel n'a été "
    "exposée ; l'incident n'appelle pas de notification à la CNIL. ",
    "Compte rendu de la réunion du comité de pilotage du {jour} {mois}. Points abordés : "
    "avancement du chantier de dématérialisation, plan de classement des archives "
    "intermédiaires, durées de conservation des pièces comptables (dix ans) et des bulletins de "
    "paie (cinquante ans). Prochaine réunion fixée au mois suivant. ",
    "Procédure de sauvegarde : copie quotidienne des serveurs de fichiers à deux heures du "
    "matin vers la baie de {ville}, rétention de trente jours, copie hebdomadaire hors site "
    "chiffrée. Le compte de service dispose de droits étendus ; son mot de passe est déposé dans "
    "le coffre de l'équipe infrastructure. Un test de restauration est réalisé chaque mois. ",
    "Note de service relative aux frais de déplacement. Les demandes de remboursement sont "
    "déposées avant le {jour} {mois} accompagnées des justificatifs originaux. Le barème "
    "kilométrique retenu est celui publié par l'administration fiscale. Les avances sur frais "
    "supérieures à {montant} € font l'objet d'un accord préalable du directeur. ",
)

_VILLES: tuple[str, ...] = (
    "Amiens",
    "Abbeville",
    "Péronne",
    "Doullens",
    "Albert",
    "Montdidier",
    "Roye",
    "Corbie",
)
_MOIS: tuple[str, ...] = (
    "janvier",
    "février",
    "mars",
    "avril",
    "mai",
    "juin",
    "juillet",
    "août",
    "septembre",
    "octobre",
    "novembre",
    "décembre",
)


def synthetic_french(target_chars: int, seed: int) -> str:
    """Texte administratif français d'environ `target_chars` caractères (déterministe)."""
    rng = random.Random(seed)
    parts: list[str] = []
    size = 0
    while size < max(MIN_DOC_CHARS, target_chars):
        text = rng.choice(_TEMPLATES).format(
            ville=rng.choice(_VILLES),
            jour=rng.randint(1, 28),
            mois=rng.choice(_MOIS),
            montant=f"{rng.randint(1_000, 400_000):,}".replace(",", " "),
            montant2=f"{rng.randint(100, 40_000):,}".replace(",", " "),
            numero=rng.randint(1_000, 9_999),
            duree=rng.randint(5, 240),
        )
        parts.append(text)
        size += len(text)
        if len(parts) % 4 == 0:
            parts.append("\n\n")
            size += 2
    return "".join(parts)


def build_bench_blocks(
    work_dir: Path, *, blocks: int, block_tokens: int, files_per_block: int
) -> list[BlockSpec]:
    """Écrit `blocks` blocs `.md` au format DocFuse et rend leurs `BlockSpec`.

    Chaque bloc porte `files_per_block` documents `## SOURCE: bench/doc_i.txt`
    dont la somme vise `block_tokens` tokens.
    """
    work_dir.mkdir(parents=True, exist_ok=True)
    doc_chars = max(MIN_DOC_CHARS, int(block_tokens * CHARS_PER_TOKEN / max(1, files_per_block)))
    specs: list[BlockSpec] = []
    doc_number = 0
    for index in range(1, blocks + 1):
        body = [f"# Corpus DocFuse — bloc {index}/{blocks}\n"]
        block_files: list[BlockFile] = []
        for _ in range(files_per_block):
            doc_number += 1
            ref = f"bench/doc_{doc_number:03d}.txt"
            text = synthetic_french(doc_chars, seed=doc_number)
            body.append(
                f"---\n## SOURCE: {ref}\n- type: txt\n"
                f"- taille_octets: {len(text.encode('utf-8'))}\n---\n\n{text}\n\n---\n"
            )
            block_files.append(BlockFile(file_id=doc_number, file_ref=ref, content_version=1))
        path = work_dir / f"bench_{index:03d}.md"
        path.write_text("\n".join(body), encoding="utf-8")
        estimated = int(len(path.read_text(encoding="utf-8")) / CHARS_PER_TOKEN)
        specs.append(
            BlockSpec(
                path=path,
                files=block_files,
                tokens_estimated=estimated,
                tokens_with_margin=estimated,
            )
        )
    return specs


# --------------------------------------------------------------- rapport


@dataclass
class BenchBlockResult:
    """Mesure d'un bloc envoyé."""

    name: str
    ok: bool = False
    latency_ms: int = 0
    prompt_tokens: int = 0
    completion_tokens: int = 0
    reasoning_chars: int = 0
    files: int = 0
    analyses: int = 0
    missing: int = 0
    json_valid: bool = False
    error: str = ""


@dataclass
class BenchReport:
    """Résultat complet d'un `run_bench`. `ok is False` ⇒ `message` dit pourquoi."""

    model: str = ""
    base_url: str = ""
    transport: str = ""
    ok: bool = True
    message: str = ""
    blocks_sent: int = 0
    files_per_block: int = 0
    block_tokens: int = 0
    in_flight: int = 0
    wall_s: float = 0.0
    prompt_tokens: int = 0
    completion_tokens: int = 0
    prefill_tok_s: float = 0.0
    decode_tok_s: float = 0.0
    latency_min_ms: int = 0
    latency_median_ms: int = 0
    latency_max_ms: int = 0
    json_valid: int = 0
    files_expected: int = 0
    files_analyzed: int = 0
    files_missing: int = 0
    files_per_hour: float = 0.0
    thinking_enabled: bool = False
    thinking_budget: int = 0
    """Budget de raisonnement imposé par requête (`llm.thinking_budget_tokens`)."""
    thinking_completion_tokens: int = 0
    """Tokens de sortie du bloc témoin, raisonnement activé (0 si non mesuré)."""
    plain_completion_tokens: int = 0
    """Tokens de sortie du même bloc, `enable_thinking=False` (0 si non mesuré)."""
    thinking_overhead_pct: float = 0.0
    reasoning_tokens_est: int = 0
    """Tokens de raisonnement estimés (longueur de `reasoning_content` / 3,5), tous blocs."""
    reasoning_tokens_max: int = 0
    """Même estimation pour le bloc qui a le plus raisonné — à comparer au budget imposé."""
    blocks: list[BenchBlockResult] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def thinking_measured(self) -> bool:
        """Les deux mesures (avec et sans raisonnement) sont disponibles."""
        return self.thinking_completion_tokens > 0 and self.plain_completion_tokens > 0

    def as_dict(self) -> dict[str, object]:
        return asdict(self)

    def as_lines(self) -> list[str]:
        """Résumé lisible (une phrase par ligne, ≤ 120 colonnes)."""
        head = f"banc LLM : modèle {self.model} sur {self.base_url} (transport {self.transport})"
        if not self.ok:
            # Sans le détail des erreurs, « aucun bloc exploitable » ne dit pas POURQUOI :
            # le message du serveur (contexte dépassé, modèle inconnu, clé refusée…) est
            # la seule chose exploitable pour l'utilisateur comme pour le support.
            lines = [head, f"ÉCHEC : {self.message}"]
            lines.extend(f"  erreur : {error}" for error in self.errors[:5])
            if len(self.errors) > 5:
                lines.append(f"  … et {len(self.errors) - 5} autre(s) erreur(s) identiques")
            return lines
        lines = [
            head,
            f"{self.blocks_sent} bloc(s) de ~{self.block_tokens} tokens, "
            f"{self.files_per_block} fichier(s) par bloc, {self.in_flight} en vol",
            f"durée totale {self.wall_s:.1f} s — {self.prompt_tokens} tokens d'entrée, "
            f"{self.completion_tokens} tokens de sortie",
            f"{self.prefill_tok_s:.0f} tokens/s prefill, {self.decode_tok_s:.0f} decode",
            f"latence par bloc : min {self.latency_min_ms / 1000:.1f} s, "
            f"médiane {self.latency_median_ms / 1000:.1f} s, max {self.latency_max_ms / 1000:.1f} s",
            f"JSON valides {self.json_valid}/{self.blocks_sent} — fichiers rendus "
            f"{self.files_analyzed}/{self.files_expected} ({self.files_missing} absent(s))",
        ]
        if self.thinking_measured:
            lines.append(
                f"thinking : +{self.thinking_overhead_pct:.0f} % de tokens de sortie "
                f"({self.thinking_completion_tokens} avec, {self.plain_completion_tokens} sans)"
            )
        elif self.thinking_enabled:
            lines.append("thinking : activé, surcoût non mesuré (bloc témoin en échec)")
        else:
            lines.append("thinking : désactivé dans la configuration")
        if self.reasoning_tokens_est:
            lines.append(
                f"raisonnement ≈ {self.reasoning_tokens_est} tokens au total, "
                f"{self.reasoning_tokens_max} au maximum pour un bloc "
                f"(budget imposé : {self.thinking_budget})"
            )
        per_hour = f"{self.files_per_hour:,.0f}".replace(",", " ")
        lines.append(f"débit estimé : ≈ {per_hour} fichiers/heure à ce réglage")
        lines.extend(f"  erreur : {error}" for error in self.errors[:5])
        return lines


# --------------------------------------------------------------- mesure


def _system_prompt(cfg: Config) -> str:
    """Prompt effectif, sans créer de base : fichier, profil actif si la base
    existe déjà, sinon prompt embarqué."""
    if cfg.prompt_path:
        return load_system_prompt(Path(cfg.prompt_path))
    db_file = Path(cfg.db_path)
    if db_file.is_file():
        try:
            with Database(db_file) as db:
                active = db.active_prompt()
        except sqlite3.Error as exc:  # base illisible : le banc ne doit pas échouer pour ça
            logger.warning("banc : profil de prompt illisible (%s), prompt embarqué", exc)
            active = None
        if active is not None:
            return active[1]
    return load_system_prompt(None)


def run_bench(
    cfg: Config,
    *,
    blocks: int = 6,
    block_tokens: int = 8_000,
    files_per_block: int = 4,
    in_flight: int | None = None,
    progress: ProgressCallback | None = None,
) -> BenchReport:
    """Mesure le débit du serveur LLM sur des blocs synthétiques.

    Args:
        cfg: Configuration (section `llm` uniquement ; la base n'est pas modifiée).
        blocks: Nombre de blocs envoyés.
        block_tokens: Taille visée d'un bloc, en tokens.
        files_per_block: Documents par bloc (comme un vrai bloc DocFuse).
        in_flight: Requêtes en parallèle ; défaut `cfg.llm.max_in_flight`.
        progress: Rappel d'avancement (une ligne lisible).

    Returns:
        Le rapport ; `ok is False` avec un `message` si le serveur est
        injoignable — aucune exception n'est levée pour un serveur absent.
    """
    return asyncio.run(
        _bench(
            cfg,
            blocks=max(1, blocks),
            block_tokens=max(200, block_tokens),
            files_per_block=max(1, files_per_block),
            in_flight=in_flight,
            say=progress or (lambda _m: None),
        )
    )


async def _bench(
    cfg: Config,
    *,
    blocks: int,
    block_tokens: int,
    files_per_block: int,
    in_flight: int | None,
    say: ProgressCallback,
) -> BenchReport:
    llm: LLMConfig = replace(cfg.llm, max_in_flight=max(1, in_flight or cfg.llm.max_in_flight))
    report = BenchReport(
        model=llm.model,
        base_url=llm.base_url,
        transport=llm.transport,
        blocks_sent=blocks,
        files_per_block=files_per_block,
        block_tokens=block_tokens,
        in_flight=llm.max_in_flight,
        thinking_enabled=llm.enable_thinking,
        thinking_budget=llm.thinking_budget_tokens,
        files_expected=blocks * files_per_block,
    )
    work_dir = Path(tempfile.mkdtemp(prefix="docia_bench_"))
    try:
        say(f"fabrication de {blocks} bloc(s) de ~{block_tokens} tokens…")
        specs = build_bench_blocks(
            work_dir, blocks=blocks, block_tokens=block_tokens, files_per_block=files_per_block
        )
        prompt = _system_prompt(cfg)
        async with LLMClient(llm, prompt) as client:
            if not await client.health():
                report.ok = False
                report.message = (
                    f"serveur LLM injoignable : {llm.base_url} (transport {llm.transport})"
                )
                say(report.message)
                return report
            say(f"envoi de {blocks} bloc(s), {llm.max_in_flight} en vol…")
            started = time.perf_counter()
            results = await asyncio.gather(*(_one_block(client, spec) for spec in specs))
            report.wall_s = max(time.perf_counter() - started, 1e-6)
        _aggregate(report, results, say)
        if llm.enable_thinking and results and results[0].ok:
            await _measure_thinking(report, llm, prompt, specs[0], results[0], say)
        return report
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


async def _one_block(client: LLMClient, spec: BlockSpec) -> BenchBlockResult:
    """Envoie un bloc et mesure sa latence, ses tokens et la validité de sa réponse."""
    outcome = BenchBlockResult(name=spec.path.name, files=len(spec.files))
    started = time.perf_counter()
    try:
        result = await client.analyze_block(spec)
    except LLMError as exc:
        outcome.latency_ms = int((time.perf_counter() - started) * 1000)
        outcome.error = str(exc)
        return outcome
    outcome.ok = True
    outcome.latency_ms = result.usage.latency_ms or int((time.perf_counter() - started) * 1000)
    outcome.prompt_tokens = result.usage.prompt_tokens
    outcome.completion_tokens = result.usage.completion_tokens
    outcome.reasoning_chars = result.reasoning_chars
    try:
        parsed = parse_block_response(result.content, spec.files)
    except ParseError as exc:
        outcome.error = f"réponse illisible : {exc}"
        outcome.missing = len(spec.files)
        return outcome
    outcome.json_valid = True
    outcome.analyses = len(parsed.analyses)
    outcome.missing = len(parsed.missing)
    if parsed.invalid:
        outcome.error = f"{len(parsed.invalid)} entrée(s) rejetée(s) : {parsed.invalid[0][1]}"
    return outcome


def _aggregate(report: BenchReport, results: list[BenchBlockResult], say: ProgressCallback) -> None:
    """Remplit les agrégats du rapport à partir des mesures par bloc."""
    report.blocks = results
    report.prompt_tokens = sum(r.prompt_tokens for r in results)
    report.completion_tokens = sum(r.completion_tokens for r in results)
    report.prefill_tok_s = report.prompt_tokens / report.wall_s
    report.decode_tok_s = report.completion_tokens / report.wall_s
    report.json_valid = sum(1 for r in results if r.json_valid)
    report.reasoning_tokens_est = int(sum(r.reasoning_chars for r in results) / 3.5)
    report.reasoning_tokens_max = int(max((r.reasoning_chars for r in results), default=0) / 3.5)
    report.files_analyzed = sum(r.analyses for r in results)
    report.files_missing = report.files_expected - report.files_analyzed
    report.files_per_hour = report.files_analyzed / report.wall_s * 3600.0
    latencies = [r.latency_ms for r in results if r.ok]
    if latencies:
        report.latency_min_ms = min(latencies)
        report.latency_median_ms = int(statistics.median(latencies))
        report.latency_max_ms = max(latencies)
    report.errors = [f"{r.name} : {r.error}" for r in results if r.error]
    if report.errors:
        report.ok = report.json_valid > 0
        if not report.ok:
            first = results[0].error if results and results[0].error else report.errors[0]
            report.message = (
                f"aucun bloc exploitable ({len(report.errors)} erreur(s)) — {first}"
                if first
                else f"aucun bloc exploitable ({len(report.errors)} erreur(s))"
            )
    say(
        f"{report.json_valid}/{len(results)} bloc(s) valides en {report.wall_s:.1f} s "
        f"({report.prefill_tok_s:.0f} tok/s prefill, {report.decode_tok_s:.0f} decode)"
    )


async def _measure_thinking(
    report: BenchReport,
    llm: LLMConfig,
    prompt: str,
    spec: BlockSpec,
    with_thinking: BenchBlockResult,
    say: ProgressCallback,
) -> None:
    """Renvoie un bloc témoin avec `enable_thinking=False` pour chiffrer le surcoût."""
    say("bloc témoin sans raisonnement (mesure du surcoût)…")
    plain = replace(llm, enable_thinking=False, max_in_flight=1)
    async with LLMClient(plain, prompt) as client:
        outcome = await _one_block(client, spec)
    if not outcome.ok:
        logger.warning("banc : bloc témoin sans thinking en échec (%s)", outcome.error)
        return
    report.thinking_completion_tokens = with_thinking.completion_tokens
    report.plain_completion_tokens = outcome.completion_tokens
    if outcome.completion_tokens:
        report.thinking_overhead_pct = (
            (with_thinking.completion_tokens - outcome.completion_tokens)
            / outcome.completion_tokens
            * 100.0
        )
