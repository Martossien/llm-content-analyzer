"""CLI `docia` : init | ingest | plan | run | status | export | retry.

Codes retour : 0 OK, 1 erreur (config, base, LLM injoignable), 2 erreurs partielles
(des blocs ou fichiers en erreur — les résultats obtenus sont persistés).
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
from pathlib import Path

from docia import __version__
from docia.config import DEFAULT_CONFIG_NAME, Config, default_toml, load_config
from docia.db import Database
from docia.models import FileStatus


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="docia", description="Doc-IA analyzer v3")
    parser.add_argument(
        "--config", "-c", type=Path, default=Path(DEFAULT_CONFIG_NAME), help="docia.toml"
    )
    parser.add_argument("--db", type=Path, default=None, help="base SQLite (prime sur la config)")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--version", action="version", version=f"docia {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("init", help="écrit un docia.toml commenté et crée la base")
    p.add_argument("--force", action="store_true", help="écrase un docia.toml existant")

    p = sub.add_parser("ingest", help="importe un CSV SMBeagle (19 colonnes)")
    p.add_argument("csv", type=Path)
    p.add_argument(
        "--lenient", action="store_true", help="tolère les lignes invalides (comptées, ignorées)"
    )

    sub.add_parser("plan", help="applique exclusions et score de priorité")

    p = sub.add_parser("run", help="blocs DocFuse → LLM → analyses (reprend si interrompu)")
    p.add_argument("--limit", type=int, default=None, help="nombre max de fichiers à traiter")
    p.add_argument("--dry-run", action="store_true", help="construit les blocs sans appeler la LLM")

    p = sub.add_parser("status", help="compteurs et répartition des classifications")
    p.add_argument("--json", action="store_true")

    p = sub.add_parser("export", help="exporte la dernière analyse de chaque fichier")
    p.add_argument("--format", choices=["csv", "json"], default="csv")
    p.add_argument("--out", "-o", type=Path, required=True)

    p = sub.add_parser("retry", help="remet les fichiers en erreur à « à analyser »")
    return parser


def _load(args: argparse.Namespace) -> Config:
    try:
        cfg = load_config(args.config if args.config.exists() else None)
    except (ValueError, OSError) as exc:
        print(f"config invalide ({args.config}) : {exc}", file=sys.stderr)
        raise SystemExit(1) from None
    if args.db is not None:
        cfg.db_path = str(args.db)
    errors = cfg.validate()
    if errors:
        for e in errors:
            print(f"config invalide : {e}", file=sys.stderr)
        raise SystemExit(1)
    return cfg


def cmd_init(args: argparse.Namespace) -> int:
    target: Path = args.config
    if target.exists() and not args.force:
        print(f"{target} existe déjà (--force pour écraser)", file=sys.stderr)
        return 1
    target.write_text(default_toml(), encoding="utf-8")
    cfg = _load(args)
    with Database(cfg.db_path) as db:
        print(f"config écrite : {target} — base : {db.path} (schéma {db.schema_version})")
    return 0


def cmd_ingest(args: argparse.Namespace, cfg: Config) -> int:
    from docia.ingest.smbeagle_csv import import_csv

    if not args.csv.exists():
        print(f"CSV introuvable : {args.csv}", file=sys.stderr)
        return 1
    with Database(cfg.db_path) as db:
        report = import_csv(db, args.csv, strict=not args.lenient)
    print(
        f"scan {report.scan_id} : {report.total} lignes — {report.new} nouveaux, "
        f"{report.updated} modifiés, {report.unchanged} inchangés, {report.invalid} invalides"
    )
    for err in report.errors[:10]:
        print(f"  ligne {err.line_number} : {err.reason}", file=sys.stderr)
    return 0 if report.invalid == 0 else 2


def cmd_plan(_args: argparse.Namespace, cfg: Config) -> int:
    from docia.filter import plan_files

    with Database(cfg.db_path) as db:
        report = plan_files(db, cfg.filter)
    print(f"à analyser : {report.pending} — exclus : {report.excluded}")
    for reason, n in sorted(report.by_reason.items(), key=lambda kv: -kv[1]):
        print(f"  {n:>7}  {reason}")
    return 0


def cmd_run(args: argparse.Namespace, cfg: Config) -> int:
    from docia.pipeline import run_pipeline

    with Database(cfg.db_path) as db:
        report = run_pipeline(db, cfg, limit=args.limit, dry_run=args.dry_run, progress=print)
    print(
        f"run {report.run_id} : {report.files_selected} sélectionnés, {report.files_done} analysés, "
        f"{report.files_error} en erreur — blocs {report.blocks_done}/"
        f"{report.blocks_built + report.blocks_resumed} "
        f"(erreurs {report.blocks_error}) — tokens {report.prompt_tokens} prompt / {report.completion_tokens} sortie"
    )
    for e in report.errors[:10]:
        print(f"  {e}", file=sys.stderr)
    if report.errors and report.files_done == 0 and report.blocks_built and not args.dry_run:
        return 1
    return 2 if report.errors or report.files_error else 0


def cmd_status(args: argparse.Namespace, cfg: Config) -> int:
    with Database(cfg.db_path) as db:
        counts = db.counts()
        classes = db.classification_summary()
    if args.json:
        print(
            json.dumps({"counts": counts, "classifications": classes}, ensure_ascii=False, indent=2)
        )
        return 0
    print(f"base : {cfg.db_path}")
    print(
        f"fichiers : {counts['files']}  ("
        + ", ".join(f"{s.value} {counts[s.value]}" for s in FileStatus)
        + ")"
    )
    print(
        f"blocs : built {counts['blocks_built']}, sent {counts['blocks_sent']}, "
        f"done {counts['blocks_done']}, error {counts['blocks_error']} — analyses : {counts['analyses']}"
    )
    for domain, dist in classes.items():
        if dist:
            print(f"{domain:>9} : " + ", ".join(f"{k} {v}" for k, v in sorted(dist.items())))
    return 0


def cmd_export(args: argparse.Namespace, cfg: Config) -> int:
    with Database(cfg.db_path) as db:
        rows = [dict(r) for r in db.latest_analyses()]
    args.out.parent.mkdir(parents=True, exist_ok=True)
    if args.format == "json":
        for r in rows:
            for key in ("rgpd_data_types", "finance_amounts", "legal_parties"):
                if r.get(key):
                    r[key] = json.loads(r[key])
        args.out.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    else:
        with args.out.open("w", newline="", encoding="utf-8-sig") as fh:
            writer = csv.DictWriter(
                fh, fieldnames=list(rows[0].keys()) if rows else ["path"], delimiter=";"
            )
            writer.writeheader()
            writer.writerows(rows)
    print(f"{len(rows)} ligne(s) → {args.out}")
    return 0


def cmd_retry(_args: argparse.Namespace, cfg: Config) -> int:
    with Database(cfg.db_path) as db:
        n = db.reset_errors()
    print(f"{n} fichier(s) remis à analyser")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    if args.command == "init":
        return cmd_init(args)
    cfg = _load(args)
    handlers = {
        "ingest": cmd_ingest,
        "plan": cmd_plan,
        "run": cmd_run,
        "status": cmd_status,
        "export": cmd_export,
        "retry": cmd_retry,
    }
    return handlers[args.command](args, cfg)


if __name__ == "__main__":
    sys.exit(main())
