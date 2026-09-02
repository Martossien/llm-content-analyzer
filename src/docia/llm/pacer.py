"""Alimentation adaptative : combien de tokens laisser en vol vers le serveur.

Un serveur vLLM ne rend pas un débit fixe. Il sert d'autres usages, son cache KV
se remplit (une requête de 200 K tokens ≈ 26 Go sur Qwen3.8-27B) et, saturé, il
**préempte** — recalcule des requêtes déjà entamées — et le débit s'effondre ;
en deçà, plus de requêtes en vol = plus de débit. Le point d'équilibre dépend du
matériel (2×5090 NVFP4 avec MTP : ~70 000 tokens en lot et 5 requêtes ;
4×3090 FP8 : plus stable, plus haut), du modèle, du contenu (prose ou tableaux)
et de l'heure. Un réglage fixe est faux la moitié du temps.

Le régulateur ne connaît que ce que le pipeline observe : quand un bloc part,
quand il revient, combien de tokens il pesait. Il cherche le **budget de tokens
en vol** qui maximise le débit total (tokens rendus par seconde, pas la latence
d'un bloc) par montée progressive tant que le débit suit, recul quand il baisse,
et sonde périodique en palier pour suivre un serveur qui se libère. Un signal de
détresse (coupure réseau ou délai dépassé après les tentatives, préemptions
vLLM) divise le budget par deux sans attendre.

Pur : pas d'horloge réelle ni de réseau (`clock` injectable), testable à froid.
`llm.max_in_flight` reste le plafond en nombre de requêtes.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

logger = logging.getLogger(__name__)

UP = 1.25
"""Pas de montée du budget quand le débit suit."""
DOWN = 0.8
"""Pas de descente quand le débit baisse (le serveur ralentit, ou on l'a saturé)."""
PROBE = 1.10
"""Pas de la sonde périodique en palier : petite, pour ne pas décrocher un serveur stable."""
STRAIN = 0.5
"""Division du budget sur un signal de détresse."""
GAIN = 0.10
"""Hystérésis : un écart de débit sous ±10 % est un palier, pas une tendance."""
SMOOTHING = 0.5
"""Le débit de référence est une moyenne glissante (poids de la dernière fenêtre) :
mesuré le 02/09 sur 4×3090, le débit brut d'une fenêtre variait de 579 à 3 797
tok/s selon le contenu des blocs (tableaux de chiffres vs prose) et le moment
où ils se terminaient — comparer deux fenêtres brutes, c'est comparer du bruit."""
CONFIRM_DOWN = 2
"""Fenêtres consécutives en baisse avant de descendre en palier (un ralentissement
réel dure ; une fenêtre de blocs coûteux, non)."""
LATENCY_SPAN = 2.0
"""Une fenêtre doit durer au moins ce multiple de la latence médiane de ses blocs :
plus courte que la latence d'un bloc, elle mesure l'instant où des blocs lancés
bien avant se terminent en rafale, pas le débit du serveur au budget courant."""
SATURATION = 0.8
"""Part du budget effectivement en vol pour que la fenêtre dise quelque chose du
budget : en dessous (file vide, `max_in_flight` atteint avant les tokens), le
débit mesuré ne dépend pas de lui et la fenêtre est non concluante."""
HOLD_WINDOWS = 3
"""Fenêtres de palier avant la première sonde à la hausse."""
MAX_PATIENCE = 24
"""La patience double à chaque palier (3, 6, 12, 24 fenêtres) tant que rien ne
change : on sonde de moins en moins souvent un serveur stable, et la patience
retombe à `HOLD_WINDOWS` dès qu'il ralentit ou lâche."""
WINDOW = 5
"""Blocs rendus par décision : assez pour lisser, assez peu pour suivre le serveur."""


@dataclass(frozen=True)
class PacerStats:
    """Ce que voit une barre d'avancement."""

    budget_tokens: int
    tokens_in_flight: int
    requests_in_flight: int
    throughput_tok_s: float | None
    """Débit de référence (moyenne glissante des fenêtres décidées, tokens de
    prompt rendus par seconde)."""
    decisions: int


class Pacer:
    """Régulateur du nombre de tokens en vol (voir le module)."""

    def __init__(
        self,
        *,
        budget_tokens: int,
        min_tokens: int,
        max_tokens: int,
        window: int = WINDOW,
        clock: Callable[[], float] = time.monotonic,
        on_decision: Callable[[str], None] | None = None,
    ) -> None:
        self.min_tokens = max(1, min_tokens)
        self.max_tokens = max(self.min_tokens, max_tokens)
        self.budget = min(self.max_tokens, max(self.min_tokens, budget_tokens))
        self.window = max(1, window)
        self._clock = clock
        self._on_decision = on_decision
        self.tokens_in_flight = 0
        self.requests_in_flight = 0
        self._freed = asyncio.Condition()
        # Fenêtre courante : tokens rendus, latences, instant d'ouverture, et si le
        # budget a réellement contraint pendant la fenêtre.
        self._window_tokens = 0
        self._window_samples = 0
        self._window_latencies: list[float] = []
        self._window_busy = False
        self._window_started = clock()
        self._reference: float | None = None
        """Débit de référence (moyenne glissante des fenêtres décidées)."""
        self._worse_streak = 0
        self._better_streak = 0
        self._discard_window = False
        """La fenêtre qui vient de faire reculer n'entre pas dans la référence :
        elle décrit un budget qu'on a quitté, pas celui où l'on revient."""
        self.inconclusive = 0
        """Fenêtres écartées faute de budget atteint."""
        self._direction = 0
        """+1 en montée, -1 en descente, 0 en palier."""
        self._hold_left = 0
        self._patience = HOLD_WINDOWS
        self.decisions = 0

    # ------------------------------------------------------------- en vol
    async def acquire(self, tokens: int) -> None:
        """Attend que `tokens` tiennent dans le budget. Un bloc plus gros que le
        budget passe seul, quand plus rien n'est en vol : jamais de blocage."""
        async with self._freed:
            if not self._fits(tokens):
                self.mark_busy()
                await self._freed.wait_for(lambda: self._fits(tokens))
            self.tokens_in_flight += tokens
            self.requests_in_flight += 1
            if self.tokens_in_flight >= self.budget * SATURATION:
                self.mark_busy()

    def mark_busy(self) -> None:
        """Le budget a contraint pendant la fenêtre courante : elle sera concluante."""
        self._window_busy = True

    def _fits(self, tokens: int) -> bool:
        return self.requests_in_flight == 0 or self.tokens_in_flight + tokens <= self.budget

    def release(
        self, tokens: int, *, ok: bool, strain: bool = False, latency_s: float = 0.0
    ) -> None:
        """Un bloc est revenu. `ok` : rendu par le modèle (il compte dans le débit) ;
        `strain` : le serveur a lâché (coupure, délai dépassé) — détresse ;
        `latency_s` : durée du bloc, qui fixe la durée minimale d'une fenêtre."""
        self.tokens_in_flight = max(0, self.tokens_in_flight - tokens)
        self.requests_in_flight = max(0, self.requests_in_flight - 1)
        if strain:
            self.distress("le serveur a lâché un bloc")
        elif ok:
            self._window_tokens += tokens
            self._window_samples += 1
            if latency_s > 0:
                self._window_latencies.append(latency_s)
            if self._window_complete():
                self._decide()
        self._wake()

    def _window_complete(self) -> bool:
        if self._window_samples < self.window:
            return False
        if not self._window_latencies:
            return True
        ordered = sorted(self._window_latencies)
        median = ordered[len(ordered) // 2]
        return self._clock() - self._window_started >= LATENCY_SPAN * median

    def _wake(self) -> None:
        # Réveil des travailleurs en attente ; `release` est appelé depuis la boucle.
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        loop.create_task(self._notify())

    async def _notify(self) -> None:
        async with self._freed:
            self._freed.notify_all()

    # ------------------------------------------------------------ décisions
    def distress(self, why: str) -> None:
        """Signal de détresse : budget divisé, descente, fenêtre remise à zéro."""
        before = self.budget
        self.budget = max(self.min_tokens, int(self.budget * STRAIN))
        self._direction = -1
        self._patience = HOLD_WINDOWS
        self._reset_window()
        self._say(f"détresse ({why}) : budget {before} → {self.budget} tokens en vol")

    def _decide(self) -> None:
        if not self._window_busy:
            # Le budget n'a pas contraint (file vide, plafond de requêtes atteint
            # avant les tokens) : le débit mesuré ne dit rien de lui.
            self.inconclusive += 1
            logger.debug("alimentation adaptative : fenêtre non concluante, budget non atteint")
            self._reset_window()
            return
        elapsed = max(1e-6, self._clock() - self._window_started)
        throughput = self._window_tokens / elapsed
        reference = self._reference
        before = self.budget
        if reference is None:
            self._direction = 1
            self.budget = self._step(UP)
            verdict = "premier relevé, montée"
        else:
            verdict = self._steer(throughput, reference)
        if reference is None:
            self._reference = throughput
        elif not self._discard_window:
            self._reference = (1 - SMOOTHING) * reference + SMOOTHING * throughput
        self._discard_window = False
        self.decisions += 1
        self._reset_window()
        if self.budget != before:
            rate = f"{throughput:,.0f}".replace(",", " ")
            self._say(
                f"{verdict} : budget {before} → {self.budget} tokens en vol (débit {rate} tok/s)"
            )

    def _steer(self, throughput: float, last: float) -> str:
        better = throughput >= last * (1 + GAIN)
        worse = throughput <= last * (1 - GAIN)
        if self._direction > 0:
            # Montée : on continue tant que le débit ne BAISSE pas. Attendre qu'il
            # monte franchement à chaque cran calait sur les paliers de quelques
            # pour cent, bien avant le coude ; c'est la baisse qui dit qu'on l'a passé.
            if worse:
                self.budget = self._step(1 / UP)
                self._hold()
                self._discard_window = True
                return "le débit a baissé en montant : retour au cran d'avant, palier"
            if self.budget >= self.max_tokens:
                self._hold()
                return "plafond atteint, palier"
            self.budget = self._step(UP)
            return "le débit ne baisse pas, montée"
        if self._direction < 0:
            if better:
                self.budget = self._step(DOWN)
                return "le débit remonte en descendant, on continue"
            if worse:
                self.budget = self._step(1 / DOWN)
                self._hold()
                self._discard_window = True
                return "descendu trop bas : un cran de plus, palier"
            self._hold()
            return "palier"
        # Palier : on veille au ralentissement (confirmé sur plusieurs fenêtres),
        # on suit un serveur qui se libère, et on sonde de temps en temps — de
        # moins en moins souvent.
        self._worse_streak = self._worse_streak + 1 if worse else 0
        self._better_streak = self._better_streak + 1 if better else 0
        if self._worse_streak >= CONFIRM_DOWN:
            self._worse_streak = 0
            self._direction = -1
            self._patience = HOLD_WINDOWS
            self.budget = self._step(DOWN)
            return "le serveur ralentit, descente"
        if self._better_streak >= CONFIRM_DOWN and self.budget < self.max_tokens:
            self._better_streak = 0
            self._direction = 1
            self.budget = self._step(PROBE)
            return "le serveur se libère, sonde à la hausse"
        self._hold_left -= 1
        if self._hold_left <= 0 and self.budget < self.max_tokens:
            self._direction = 1
            self.budget = self._step(PROBE)
            return "sonde à la hausse"
        return "palier"

    def _hold(self) -> None:
        self._direction = 0
        self._hold_left = self._patience
        self._patience = min(MAX_PATIENCE, self._patience * 2)
        self._worse_streak = 0
        self._better_streak = 0

    def _step(self, factor: float) -> int:
        target = int(self.budget * factor)
        return min(self.max_tokens, max(self.min_tokens, target))

    def _reset_window(self) -> None:
        self._window_tokens = 0
        self._window_samples = 0
        self._window_latencies = []
        self._window_busy = False
        self._window_started = self._clock()

    def _say(self, message: str) -> None:
        logger.info("alimentation adaptative : %s", message)
        if self._on_decision is not None:
            self._on_decision(message)

    def stats(self) -> PacerStats:
        return PacerStats(
            budget_tokens=self.budget,
            tokens_in_flight=self.tokens_in_flight,
            requests_in_flight=self.requests_in_flight,
            throughput_tok_s=self._reference,
            decisions=self.decisions,
        )


# ---------------------------------------------------------------- mémoire
PACER_FILE = "pacer.json"
"""Budgets appris, par serveur et modèle, dans le dossier de configuration."""


class PacerMemory:
    """Le budget trouvé pendant un run sert de point de départ au suivant, sur le
    même serveur et le même modèle : une campagne reprise n'a pas à réapprendre.
    Fichier absent ou illisible : on repart de zéro, sans un mot de plus."""

    def __init__(self, path: Path) -> None:
        self.path = path

    @staticmethod
    def key(base_url: str, model: str) -> str:
        return f"{base_url.rstrip('/')}|{model}"

    def load(self, key: str) -> int | None:
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None
        entry = data.get(key) if isinstance(data, dict) else None
        budget = entry.get("budget_tokens") if isinstance(entry, dict) else None
        return budget if isinstance(budget, int) and budget > 0 else None

    def save(self, key: str, budget_tokens: int) -> None:
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            data = {}
        if not isinstance(data, dict):
            data = {}
        data[key] = {
            "budget_tokens": int(budget_tokens),
            "updated": datetime.now(UTC).isoformat(timespec="seconds"),
        }
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        except OSError as exc:
            logger.warning("budget adaptatif non mémorisé (%s) : %s", self.path, exc)
