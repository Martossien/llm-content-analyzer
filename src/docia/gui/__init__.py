"""Interface graphique CustomTkinter (extra `docia[gui]`).

`launch()` construit la fenêtre (`docia.gui.app`) ; les fonctions pures de
`docia.gui.helpers` sont réexportées pour les tests et la compatibilité.
"""

from __future__ import annotations

from docia.gui.helpers import (
    config_to_toml,
    estimate_prompt_tokens,
    parse_int,
    result_rows,
    result_rows_v31,
    status_lines,
)


def launch(config_path: object = None, *, smoke: bool = False) -> None:
    """Ouvre la fenêtre principale (import différé : le cœur s'importe sans Tk)."""
    from pathlib import Path

    from docia.gui.app import launch as _launch

    _launch(Path(str(config_path)) if config_path else None, smoke=smoke)


__all__ = [
    "config_to_toml",
    "estimate_prompt_tokens",
    "launch",
    "parse_int",
    "result_rows",
    "result_rows_v31",
    "status_lines",
]
