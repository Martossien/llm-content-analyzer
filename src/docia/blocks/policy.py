"""Politique de découpage d'un fichier seul : plafond par fichier et comptage exact.

Découper un fichier coûte en qualité — chaque segment est classé sans le reste du
document — et ne doit donc se faire que quand il ne tient **réellement** pas sous
le plafond. Or le builder ne dispose que d'une *estimation* locale des tokens
(`blocks.tokenizer_engine`), que le pipeline dévalue par un facteur de sécurité
(`pipeline.SEGMENT_SAFETY` : 0,6 en `approx`, 0,85 en `openai`) pour absorber son
erreur. Avant de découper, on demande au serveur le compte exact quand il sait
compter (`POST /tokenize`, vLLM) : un fichier qui tient part entier, un fichier
trop long est découpé en segments calibrés sur le rapport mesuré estimation/réel
plutôt que sur le facteur de sécurité forfaitaire.

Sans serveur capable de compter (open-webui, `--dry-run`), on retombe sur le
comportement historique : plafond estimé, facteur de sécurité.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass

logger = logging.getLogger(__name__)

TokenCounter = Callable[[str], int | None]
"""Compte exact d'un texte par le serveur ; None quand il ne sait pas compter."""

PIECE_SAFETY = 0.9
"""Part du plafond exact visée par segment quand le rapport estimation/réel est
mesuré sur le fichier entier : le rapport varie un peu d'un passage à l'autre
(tableaux vs prose) et le contrôle exact avant envoi (`LLMClient.check_fits`)
ne doit pas renvoyer le segment en seconde passe pour quelques tokens."""

MIN_PIECE_BUDGET = 500
"""Plancher du budget de texte d'un segment (tokens estimés avec marge)."""


@dataclass(frozen=True)
class SegmentPolicy:
    """Plafond **exact** par fichier et compteur du serveur.

    `cap_exact` : tokens réels au-delà desquels un fichier seul est découpé
    (`pipeline.file_cap` : part `blocks.max_file_share` du contexte servi, moins
    la réserve prompt + réponse). Le plafond en tokens *estimés* reste
    `BlocksConfig.max_file_tokens` (le même, dévalué par le facteur de sécurité).
    """

    cap_exact: int
    count_exact: TokenCounter | None = None


@dataclass(frozen=True)
class SegmentPlan:
    """Décision pour un fichier candidat au découpage."""

    piece_budget: int | None
    """Budget de texte par segment (tokens estimés avec marge) ; None = le fichier
    part entier."""
    exact_tokens: int | None
    """Compte exact rendu par le serveur, None si indisponible."""
    reason: str
    """Une ligne de journal, en français, qui dit pourquoi."""

    @property
    def whole(self) -> bool:
        return self.piece_budget is None


def plan_file(
    text: str, estimated: int, cap_estimated: int, policy: SegmentPolicy | None
) -> SegmentPlan:
    """Décide si un fichier estimé à `estimated` tokens (avec marge), au-dessus
    du plafond estimé `cap_estimated`, part entier ou en segments — et de quelle
    taille.

    - sans politique ou sans compteur : segments de `cap_estimated` (historique) ;
    - compte exact ≤ `policy.cap_exact` : entier, l'estimation se trompait ;
    - compte exact au-dessus : segments calibrés sur le rapport `estimated / exact`
      mesuré sur ce fichier, à `PIECE_SAFETY` près.
    """
    fallback = max(MIN_PIECE_BUDGET, cap_estimated)
    if policy is None or policy.count_exact is None:
        return SegmentPlan(fallback, None, f"{estimated} tokens estimés > plafond {cap_estimated}")
    exact = policy.count_exact(text)
    if exact is None:
        return SegmentPlan(
            fallback,
            None,
            f"{estimated} tokens estimés > plafond {cap_estimated} (serveur muet sur le compte)",
        )
    if exact <= policy.cap_exact:
        return SegmentPlan(
            None,
            exact,
            f"{estimated} tokens estimés mais {exact} comptés par le serveur "
            f"≤ plafond {policy.cap_exact} : envoyé entier",
        )
    ratio = estimated / max(1, exact)
    budget = max(MIN_PIECE_BUDGET, int(policy.cap_exact * ratio * PIECE_SAFETY))
    return SegmentPlan(
        budget,
        exact,
        f"{exact} tokens comptés par le serveur > plafond {policy.cap_exact} "
        f"(rapport estimation/réel {ratio:.2f}, budget par segment {budget})",
    )
