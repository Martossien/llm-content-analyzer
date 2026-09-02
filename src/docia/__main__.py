"""Point d'entrée : `python -m docia`.

Sans argument → interface graphique (extra `docia[gui]`) ; avec arguments → CLI.
"""

from __future__ import annotations

import multiprocessing
import sys


def main() -> None:
    # DocFuse ≥ 0.2.2 extrait dans un pool de processus : dans `Docia.exe` (PyInstaller,
    # onefile) chaque travailleur est un relancement de l'exe avec ses propres
    # arguments, que `freeze_support` intercepte ici — avant tout aiguillage.
    multiprocessing.freeze_support()
    if len(sys.argv) <= 1:
        try:
            from docia.gui import launch

            launch()
        except ImportError as exc:
            print(
                "L'interface graphique n'est pas installée : pip install \"docia[gui]\" "
                f"— ou utilisez la ligne de commande (docia --help). ({exc})",
                file=sys.stderr,
            )
            sys.exit(1)
    else:
        from docia.cli import main as cli_main

        sys.exit(cli_main())


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrompu", file=sys.stderr)
        sys.exit(1)
