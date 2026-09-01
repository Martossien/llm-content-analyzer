"""Patron commun des écrans qui calculent hors du thread Tk (`LazyScreen`).

Accueil, Résultats et Statistiques répétaient le même enchaînement — marquer l'écran
« à recalculer », ne calculer que s'il est visible, lancer le calcul dans un thread,
appliquer le résultat dans le thread Tk — en **trois variantes divergentes**, et
chaque divergence était un défaut : un résultat périmé qui écrasait un résultat
récent (jeton absent), un calcul lancé pour une campagne qui en lisait une autre
(chemin relu dans le thread), un écran figé sur « chargement… » pour de bon (`_dirty`
remis à False avant le calcul, donc jamais réessayé après un échec).

Le patron vit désormais ici, une seule fois, et se teste **sans écran** : il suffit
d'un objet `app` factice dont `run_background(compute, apply)` appelle directement
`apply(compute())`.

Trois garanties :

* **jeton** — seul le résultat du dernier calcul demandé est appliqué ;
* **campagne capturée** — le chemin de la base est lu une seule fois, dans le thread
  Tk, et le calcul reçoit une `Database` ouverte sur *ce* chemin : changer de campagne
  pendant un calcul ne le fait pas basculer sur l'autre base, et aucun thread de fond
  ne relit l'état de la fenêtre ;
* **`_dirty` remis à False dans `apply`** — un calcul qui échoue laisse l'écran à
  recalculer, donc y revenir réessaie.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any, TypeVar

from docia.db import Database

_T = TypeVar("_T")


class LazyScreen:
    """Écran qui ne calcule que s'il est visible, dans un thread, avec jeton."""

    #: nom de l'onglet dans `DociaApp.tabs` — l'écran ne calcule que s'il est dessus.
    TAB_NAME: str = ""
    #: vrai pour l'onglet affiché à l'ouverture : tant qu'aucun onglet n'est posé,
    #: `current_tab()` rend "" et c'est pourtant celui-là qui est à l'écran.
    FIRST_TAB: bool = False

    app: Any
    parent: Any

    def _lazy_setup(self) -> None:
        """À appeler dans `__init__` de l'écran (avant tout `refresh`)."""
        self._dirty = True
        self._token = 0
        self._disposed = False

    # ------------------------------------------------------------------ visibilité
    def visible(self) -> bool:
        current = self.app.current_tab()
        return bool(current == self.TAB_NAME or (current == "" and self.FIRST_TAB))

    def refresh(self) -> None:
        """Marque l'écran à recalculer ; le calcul n'a lieu que s'il est visible."""
        self._dirty = True
        self.refresh_if_needed()

    def refresh_if_needed(self) -> None:
        """Rattrape le calcul sauté (changement d'onglet, de filtre, de campagne)."""
        raise NotImplementedError

    # ------------------------------------------------------------------ calcul
    def _start(
        self,
        compute: Callable[[Database], _T],
        apply: Callable[[_T], None],
        *,
        name: str,
    ) -> Path:
        """Calcule `compute(db)` hors du thread Tk, applique `apply(résultat)` dedans.

        Rend le chemin de campagne capturé — les écrans le font entrer dans leur clé
        de fraîcheur pour ne pas confondre deux campagnes.
        """
        db_path = Path(self.app.db_path())  # lu ici, dans le thread Tk, une seule fois
        self._token += 1
        token = self._token

        def work() -> _T:
            with Database(db_path) as db:
                return compute(db)

        def done(result: _T) -> None:
            if self._disposed or token != self._token:
                return  # un calcul plus récent a été demandé, ou l'écran a disparu
            apply(result)
            self._dirty = False  # après `apply` : un échec laisse l'écran à réessayer

        self.app.run_background(work, done, name=name)
        return db_path

    # ------------------------------------------------------------------ fin de vie
    def dispose(self) -> None:
        """Retire les rappels de l'écran et périme ses calculs en vol.

        Appelé avant la destruction des widgets (onglets administrateur refermés) :
        un rappel survivant à son widget fait lever `TclError` à `_set_busy`, et la
        boucle `_poll` meurt avec lui.
        """
        self._disposed = True
        self._token += 1
        self.app.off_refresh(self.refresh)
        busy = getattr(self, "_busy", None)
        if busy is not None:
            self.app.off_busy(busy)
