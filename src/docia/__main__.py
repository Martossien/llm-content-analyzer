"""Point d'entrée : `python -m docia`.

Sans argument → interface graphique (extra `docia[gui]`) ; avec arguments → CLI.
"""

from __future__ import annotations

import sys


def main() -> None:
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
