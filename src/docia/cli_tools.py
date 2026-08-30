"""Sous-commandes « outils » de la CLI : `docia bench` et `docia quick`.

Le module s'enregistre dans le parseur principal (`cli.py`) via `register()`,
qui rend les gestionnaires associés — même contrat que les autres commandes :
`(args, cfg) -> code retour` (0 OK, 1 erreur, 2 erreurs partielles).
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable
from pathlib import Path

from docia.config import Config

Handler = Callable[[argparse.Namespace, Config], int]


def register(
    sub: argparse._SubParsersAction[argparse.ArgumentParser],
) -> dict[str, Handler]:
    """Ajoute `bench` et `quick` au parseur et rend leurs gestionnaires."""
    p = sub.add_parser("bench", help="mesure la vitesse du serveur LLM (blocs synthétiques)")
    p.add_argument("--blocks", type=int, default=6, help="nombre de blocs envoyés (défaut 6)")
    p.add_argument(
        "--block-tokens", type=int, default=8_000, help="taille visée d'un bloc (défaut 8000)"
    )
    p.add_argument("--files-per-block", type=int, default=4, help="documents par bloc (défaut 4)")
    p.add_argument(
        "--in-flight",
        type=int,
        default=None,
        help="requêtes en parallèle (défaut llm.max_in_flight)",
    )
    p.add_argument("--json", action="store_true", help="rapport JSON au lieu du résumé")

    p = sub.add_parser("quick", help="analyse immédiate de fichiers ou dossiers, sans CSV")
    p.add_argument("paths", type=Path, nargs="+", metavar="PATH")
    p.add_argument(
        "--keep-db", type=Path, default=None, help="base à conserver (historique et reprise)"
    )
    p.add_argument("--json", action="store_true", help="rapport JSON au lieu du tableau")
    return {"bench": cmd_bench, "quick": cmd_quick}


def cmd_bench(args: argparse.Namespace, cfg: Config) -> int:
    """`docia bench` : débit du serveur LLM, en tokens/s et en fichiers/heure."""
    from docia.bench import run_bench

    report = run_bench(
        cfg,
        blocks=args.blocks,
        block_tokens=args.block_tokens,
        files_per_block=args.files_per_block,
        in_flight=args.in_flight,
        progress=None if args.json else print,
    )
    if args.json:
        print(json.dumps(report.as_dict(), ensure_ascii=False, indent=2))
    else:
        for line in report.as_lines():
            print(line)
    if not report.ok:
        print(report.message, file=sys.stderr)
        return 1
    return 2 if report.errors else 0


def cmd_quick(args: argparse.Namespace, cfg: Config) -> int:
    """`docia quick` : analyse immédiate de fichiers ou dossiers locaux."""
    from docia.quick import quick_analyze

    report = quick_analyze(
        cfg,
        args.paths,
        db_path=args.keep_db,
        progress=None if args.json else print,
    )
    if args.json:
        print(json.dumps(report.as_dict(), ensure_ascii=False, indent=2))
    else:
        for line in report.as_lines():
            print(line)
    if not report.ok:
        print(report.message, file=sys.stderr)
        return 1
    return 2 if report.errors or report.llm_errors else 0
