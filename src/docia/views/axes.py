"""Axes d'agrégation (partage, propriétaire, répertoire, extension) : regroupement SQL et libellés."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from docia.db import Database
from docia.views._common import _FROM_LATEST, _IS_LATEST, directory_label, share_label

AXES: tuple[str, ...] = ("share", "owner", "directory", "extension")
"""Axes acceptés par `classification_matrix`."""

_SIMPLE_AXES: dict[str, tuple[str, str]] = {
    "owner": ("f.owner", "(inconnu)"),
    "extension": ("f.extension", "(sans extension)"),
}
"""Axes dont l'étiquette est la colonne SQL elle-même : (colonne, libellé si vide)."""


_BASE_UNNAMED = "TRIM(TRIM(f.base), '\\/') = ''"
"""`base` ne nomme pas son partage : vide, blancs ou séparateurs seuls.

Transcription SQL de `share_from_base(base) == ''` : `TRIM` retire les blancs,
le second retire les séparateurs. Rogner les deux bouts au lieu de la seule fin
ne change pas le *test de vacuité* — seule une valeur réduite à des blancs et à
des séparateurs est vide dans les deux cas."""


def _all_shares_named(db: Database) -> bool:
    """Vrai si toute valeur de `base` nomme déjà son partage.

    Dans ce cas — celui de tout scan SMBeagle — `share_label` ne regarde jamais
    `unc_directory` : le regroupement SQL peut l'ignorer et rendre un groupe par
    partage au lieu d'un groupe par répertoire. Test d'existence borné, et non
    `SELECT DISTINCT base` : la réponse tient au premier enregistrement fautif.
    """
    return not db.query_values(f"SELECT 1 FROM files f WHERE {_BASE_UNNAMED} LIMIT 1")


_FILLER = "''"
"""Clé d'axe inutilisée : garde la largeur des lignes sans peser sur le regroupement."""

_SHARE_FALLBACK = f"CASE WHEN {_BASE_UNNAMED} THEN f.unc_directory ELSE '' END"
"""Second niveau de regroupement de l'axe « partage », **seulement** quand `base` est vide.

Un unique enregistrement à `base` vide suffisait à faire retomber tout l'axe sur
`f.unc_directory` : le regroupement passait d'un groupe par partage à un groupe
par répertoire — 6 groupes contre 521 718 sur un parc de 934 028 fichiers, soit
19 Mo de mémoire contre 462 (× 24). Le repli est ici **borné aux seules lignes
concernées** : les fichiers dont la `base` nomme le partage restent regroupés
par partage, quoi qu'il arrive ailleurs dans la campagne."""


def _axis_group(db: Database, axis: str) -> list[str]:
    """Expressions SQL identifiant un groupe pour cet axe, dans l'ordre des colonnes."""
    if axis in _SIMPLE_AXES:
        return [_SIMPLE_AXES[axis][0]]
    if axis == "share":
        return ["f.base", _FILLER if _all_shares_named(db) else _SHARE_FALLBACK]
    return ["f.base", "f.unc_directory"]


def _group_by(keys: list[str], extra: int) -> str:
    """Clause `GROUP BY` positionnelle : les clés réelles, puis `extra` colonnes."""
    positions = [i + 1 for i, key in enumerate(keys) if key != _FILLER]
    positions += [len(keys) + i + 1 for i in range(extra)]
    return ", ".join(str(position) for position in positions)


def _axis_volumes(db: Database, keys: list[str]) -> list[tuple[Any, ...]]:
    """Clés d'axe, puis nombre de fichiers et octets (tous les fichiers)."""
    return db.query_values(
        f"SELECT {', '.join(keys)}, COUNT(*), COALESCE(SUM(f.size_bytes),0)"
        f" FROM files f GROUP BY {_group_by(keys, 0)}"
    )


def _axis_risk(db: Database, keys: list[str]) -> list[tuple[Any, ...]]:
    """Clés d'axe, puis sécurité, RGPD et nombre de fichiers (fichiers analysés)."""
    return db.query_values(
        f"SELECT {', '.join(keys)}, a.security_classification, a.rgpd_risk_level, COUNT(*)"
        f"{_FROM_LATEST} WHERE {_IS_LATEST} GROUP BY {_group_by(keys, 2)}"
    )


def _run_prefix(text: str, segments: int) -> str:
    """Préfixe de `text` couvrant ses `segments` premiers niveaux **non vides**
    (`''` s'il en manque).

    Les niveaux sont comptés comme `share_label` et `directory_label` les comptent :
    les séparateurs vides ne comptent pas (`\\\\srv\\part` a deux niveaux, pas trois).
    Compter les antislashs bruts décalait le compte dès qu'un chemin portait un
    séparateur doublé, et le préfixe couvrait alors moins de niveaux que l'étiquette
    n'en consomme : deux partages distincts se retrouvaient sous la même étiquette.
    """
    seen = 0
    position = 0
    length = len(text)
    while position < length:
        if text[position] == "\\":
            position += 1
            continue
        end = text.find("\\", position)
        if end == -1:
            return ""
        seen += 1
        if seen == segments:
            return text[: end + 1]
        position = end + 1
    return ""


def _path_levels(text: str) -> int:
    """Nombre de niveaux non vides d'un chemin (`\\\\srv\\part` → 2, `\\\\srv\\part\\a` → 3).

    Compté comme `_run_prefix` compte : les séparateurs vides ne font pas un niveau."""
    return sum(1 for part in text.replace("/", "\\").split("\\") if part)


def _axis_labeller(axis: str, depth: int) -> Callable[[tuple[Any, ...]], str]:
    """Rend la fonction qui étiquette une ligne d'axe (clés d'axe en tête de ligne).

    `share_label` ne lit que les deux premiers niveaux d'un répertoire, et
    `directory_label` `depth` de plus **à partir du partage** : deux répertoires
    qui partagent ces niveaux portent la même étiquette. Les lignes arrivant
    groupées par répertoire, l'étiquette n'est donc recalculée qu'au changement
    d'arborescence — sur un parc où presque chaque fichier a son répertoire,
    c'est l'essentiel du coût de la vue.

    Le nombre de niveaux couverts par le préfixe est tiré de la **profondeur
    réelle du partage**, celle de la colonne `base`. Le supposer égal à deux
    (`\\\\serveur\\partage`) était juste pour un scan SMB, faux dès qu'une campagne
    porte sur un sous-arbre : avec `base = \\\\srv\\part\\Direction\\RH`, le préfixe
    couvrait deux niveaux de moins que l'étiquette n'en consomme, et
    `\\\\srv\\part\\Direction\\RH\\Paie` réutilisait l'étiquette de
    `\\\\srv\\part\\Direction\\RH\\Contrats` : deux répertoires distincts additionnés
    sur une même ligne du tableau « Répertoires », le second disparaissant.
    Un préfixe trop long ne fait, lui, que recalculer inutilement.
    """
    if axis in _SIMPLE_AXES:
        empty = _SIMPLE_AXES[axis][1]

        def by_column(row: tuple[Any, ...]) -> str:
            return str(row[0] or empty)

        return by_column

    previous_base: Any = None
    prefix = ""
    label = ""

    def by_path(row: tuple[Any, ...]) -> str:
        nonlocal previous_base, prefix, label
        base, directory = row[0], row[1]  # colonnes TEXT NOT NULL : toujours des chaînes
        # Le préfixe est tiré et comparé sur le texte normalisé, celui-là même dont
        # les étiquettes sont tirées : sinon `/` et `\` ne se comparent pas entre eux,
        # et un chemin à slashs ne se compte pas dans les mêmes niveaux.
        text = directory.replace("/", "\\").strip()
        if base != previous_base or not prefix or not text.startswith(prefix):
            share = share_label(base, directory)
            if axis == "share":
                label = share
                segments = 2  # `share_label` ne lit jamais plus de deux niveaux
            else:
                label = directory_label(base, directory, depth)
                segments = _path_levels(share) + depth
            prefix = _run_prefix(text, segments)
            previous_base = base
        return label

    return by_path


RiskTally = tuple[dict[str, int], dict[str, int], list[int]]
"""Compteurs d'une étiquette d'axe : (sécurité, RGPD, [analysés])."""
