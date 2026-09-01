"""Défense commune aux sorties tabulaires : les données ne deviennent pas des formules.

Un classeur `.xlsx` et un CSV `;` en UTF-8 avec BOM sont faits pour être ouverts
d'un double-clic — c'est même la raison du BOM et du point-virgule. Or Excel
interprète comme une **formule** toute cellule *texte* dont le premier caractère
est `=`, `+`, `-` ou `@` (ainsi qu'une tabulation ou un retour chariot, qu'il
supprime avant d'analyser la suite).

Les vecteurs ne sont pas exotiques :

- un nom de fichier du partage : `- copie.docx`, `-- ancien devis.pdf`,
  `+33 1 23 45 67 89.pdf`, `@relire.docx` s'affichent `#NOM ?` au lieu du nom ;
- le `resume` et les justifications rendus par la LLM, sans contrainte de
  caractères, qui traversent jusqu'aux onglets « Fichiers » et « Sensibles » et
  jusqu'à `analyses.csv` : `=cmd|'/c calc.exe'!A1` (DDE) ou
  `=HYPERLINK("http://mechant";"cliquez")` y devenaient de vraies formules,
  devant des lecteurs qui ne sont pas informaticiens.

Seul le **texte** est assaini. Les nombres, dates et booléens passent intacts —
ils doivent rester calculables dans Excel comme dans Power BI — et un texte qui
*est* un nombre écrit en toutes lettres (`-1500`, `+3,5`, `1e6`, tel que le rend
`powerbi._age_days` pour une date future) est laissé tel quel : Excel n'y voit
pas une formule mais une valeur.
"""

from __future__ import annotations

import re
from typing import Any

FORMULA_PREFIXES: tuple[str, ...] = ("=", "+", "-", "@", "\t", "\r")
"""Premiers caractères qui font d'une cellule texte une formule pour Excel."""

_NUMBER = re.compile(r"[+-]?(\d+([.,]\d*)?|[.,]\d+)([eE][+-]?\d+)?\Z")
"""Texte qui n'est qu'un nombre : `-1500`, `+3,5`, `1e6` — sans danger, et utile tel quel."""


def is_formula_text(value: object) -> bool:
    """Vrai si `value` est un texte qu'Excel prendrait pour une formule.

    Faux pour tout ce qui n'est pas une chaîne (nombres, dates, booléens, `None`)
    et pour une chaîne qui n'est qu'un nombre.
    """
    if not isinstance(value, str) or value[:1] not in FORMULA_PREFIXES:
        return False
    return _NUMBER.match(value) is None


def csv_cell(value: Any) -> Any:
    """Valeur prête pour un CSV destiné à Excel : texte à risque préfixé d'une apostrophe.

    L'apostrophe est la neutralisation habituelle : Excel affiche la valeur telle
    quelle sans l'évaluer. Elle fait partie de la chaîne côté Power BI, qui n'a
    pas ce comportement — c'est le prix d'un CSV lisible par les deux, et c'est
    documenté dans `README_powerbi.md`.
    """
    return f"'{value}" if is_formula_text(value) else value


__all__ = ["FORMULA_PREFIXES", "csv_cell", "is_formula_text"]
