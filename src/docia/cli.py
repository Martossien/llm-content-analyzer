"""CLI `docia` : init | ingest | plan | run | status | export | report | retry
| backup | restore | reanalyze | campaigns.

Codes retour : 0 OK, 1 erreur (config, base, LLM injoignable), 2 erreurs partielles
(des blocs ou fichiers en erreur — les résultats obtenus sont persistés).
"""

from __future__ import annotations

import argparse
import contextlib
import csv
import json
import logging
import sys
from pathlib import Path

from docia import __version__
from docia.config import DEFAULT_CONFIG_NAME, Config, default_toml, load_config
from docia.db import Database
from docia.models import FileStatus

DEFAULT_KEEP_BACKUPS = 10
"""Sauvegardes conservées par `docia backup` (voir `service.DEFAULT_KEEP_BACKUPS`)."""


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
    p.add_argument(
        "--format",
        choices=["csv", "json", "xlsx", "powerbi"],
        default="csv",
        help="csv/json : un fichier ; xlsx : classeur Excel ; powerbi : dossier de CSV",
    )
    p.add_argument("--out", "-o", type=Path, required=True)

    p = sub.add_parser("report", help="rapport de restitution (HTML autonome ou Markdown)")
    p.add_argument("--format", choices=["html", "md"], default="html")
    p.add_argument(
        "--out",
        "-o",
        type=Path,
        default=None,
        help="défaut : <base>_rapport.html à côté de la base",
    )
    p.add_argument(
        "--cleanup-years",
        type=int,
        default=5,
        help="seuil d'ancienneté des candidats au nettoyage (défaut 5)",
    )

    p = sub.add_parser("retry", help="remet les fichiers en erreur à « à analyser »")
    p = sub.add_parser("gui", help="ouvre l'interface graphique (extra docia[gui])")
    p.add_argument(
        "--smoke",
        action="store_true",
        help="construit la fenêtre puis la ferme (contrôle d'un exécutable empaqueté)",
    )

    p = sub.add_parser("backup", help="sauvegarde horodatée de la base (rotation)")
    p.add_argument("--out", type=Path, default=None, help="dossier (défaut : <base>.backups)")
    p.add_argument("--label", default="", help="étiquette ajoutée au nom du fichier")
    p.add_argument("--keep", type=int, default=DEFAULT_KEEP_BACKUPS, help="copies conservées")

    p = sub.add_parser("restore", help="restaure une sauvegarde par-dessus la base")
    p.add_argument("backup", type=Path, help="fichier de sauvegarde à restaurer")
    p.add_argument("--yes", action="store_true", help="confirme le remplacement de la base")

    p = sub.add_parser("reanalyze", help="force la réanalyse de fichiers déjà traités")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--all", action="store_true", help="toute la campagne (hors exclus)")
    g.add_argument("--errors", action="store_true", help="seulement les fichiers en erreur")
    g.add_argument("--pending-only", action="store_true", help="seulement les fichiers à analyser")
    p.add_argument(
        "--where",
        action="append",
        default=None,
        metavar="CLÉ=VALEUR",
        help="critère répétable : security, rgpd, owner, extension, path_like",
    )
    p.add_argument(
        "--no-backup", action="store_true", help="ne pas sauvegarder avant (déconseillé)"
    )

    sub.add_parser("campaigns", help="campagnes récentes et leur avancement")

    p = sub.add_parser("prompt", help="profils de prompt (le prompt est une variable)")
    ps = p.add_subparsers(dest="prompt_cmd", required=True)
    ps.add_parser("list", help="liste les profils (l'actif est marqué *)")
    q = ps.add_parser("show", help="affiche un profil (défaut : le prompt effectif)")
    q.add_argument("name", nargs="?")
    q = ps.add_parser("save", help="enregistre un profil depuis un fichier texte")
    q.add_argument("name")
    q.add_argument("file", type=Path)
    q.add_argument("--use", action="store_true", help="et l'active")
    q = ps.add_parser("use", help="active un profil")
    q.add_argument("name")
    ps.add_parser("reset", help="désactive tout profil : prompt embarqué")
    q = ps.add_parser("export", help="écrit un profil (ou le prompt embarqué) dans un fichier")
    q.add_argument("file", type=Path)
    q.add_argument("--name", default=None)
    q = ps.add_parser("delete", help="supprime un profil")
    q.add_argument("name")

    from docia.cli_tools import register as register_tools

    register_tools(sub)

    p = sub.add_parser("review", help="statut de vérification humaine d'un fichier")
    p.add_argument("file_id", type=int)
    p.add_argument("--status", choices=["to_review", "validated", "corrected"], required=True)
    p.add_argument("--comment", default="")
    p.add_argument("--reviewer", default="")
    p.add_argument("--security", default=None, help="classe corrigée (C0..C3)")
    p.add_argument("--rgpd", default=None, help="niveau RGPD corrigé")
    p.add_argument("--retention-years", type=int, default=None)
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
    from dataclasses import asdict

    from docia.service import campaign_status

    with Database(cfg.db_path) as db:
        counts = db.counts()
        classes = db.classification_summary()
        state = campaign_status(db)
    if args.json:
        print(
            json.dumps(
                {
                    "counts": counts,
                    "classifications": classes,
                    "active_prompt": state.active_prompt,
                    "schema_version": state.schema_version,
                    "reviews": {"reviewed": state.reviewed, "to_review": state.to_review},
                    "last_run": asdict(state.last_run) if state.last_run else None,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0
    print(f"base : {cfg.db_path}  (schéma {state.schema_version})")
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
    print(f"prompt actif : {state.active_prompt}")
    print(f"revues : {state.reviewed} vérifiée(s), {state.to_review} à vérifier")
    run = state.last_run
    if run is not None:
        print(
            f"dernier run : {run.run_id} ({run.status}) — {run.files} fichier(s), "
            f"{run.blocks_done}/{run.blocks} bloc(s), {run.duration_s:.0f} s, modèle {run.model}"
        )
    else:
        print("dernier run : aucun")
    return 0


def cmd_export(args: argparse.Namespace, cfg: Config) -> int:
    if args.format in ("xlsx", "powerbi"):
        return _cmd_export_workbook(args, cfg)
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


def _cmd_export_workbook(args: argparse.Namespace, cfg: Config) -> int:
    """`export --format xlsx|powerbi` : classeur Excel ou dossier Power BI."""
    try:
        from docia.report import export_powerbi, write_workbook
    except ImportError as exc:  # pragma: no cover - openpyxl fourni par DocFuse
        print(f"export indisponible ({exc})", file=sys.stderr)
        return 1
    with Database(cfg.db_path) as db:
        if args.format == "xlsx":
            path = write_workbook(db, args.out)
            print(f"classeur Excel → {path}")
            return 0
        written = export_powerbi(db, args.out)
    print(f"export Power BI → {args.out}")
    for path in written:
        print(f"  {path.name}")
    return 0


def cmd_report(args: argparse.Namespace, cfg: Config) -> int:
    """`report` : rapport HTML autonome ou Markdown, à partir des vues partagées."""
    from docia.report import collect, render_html, render_markdown

    out: Path | None = args.out
    if out is None:
        base = Path(cfg.db_path)
        suffix = "html" if args.format == "html" else "md"
        out = base.with_name(f"{base.stem}_rapport.{suffix}")
    with Database(cfg.db_path) as db:
        data = collect(db, cleanup_years=args.cleanup_years)
        text = (
            render_html(db, data=data) if args.format == "html" else render_markdown(db, data=data)
        )
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(text, encoding="utf-8")
    o = data.overview
    print(
        f"rapport → {out} — {o.total_files} fichier(s), {o.analyzed} analysé(s), "
        f"{o.sensitive_files} sensible(s), {o.duplicate_families} famille(s) de doublons"
    )
    return 0


def cmd_prompt(args: argparse.Namespace, cfg: Config) -> int:
    from docia.llm.schema import load_system_prompt, prompt_hash

    with Database(cfg.db_path) as db:
        if args.prompt_cmd == "list":
            active = db.active_prompt()
            print(
                f"  {'*' if active is None else ' '} (embarqué)  {prompt_hash(load_system_prompt(None), cfg.llm.model)}"
            )
            for r in db.list_prompts():
                mark = "*" if r["active"] else " "
                print(
                    f"  {mark} {r['name']:<24} {r['hash']}  {r['chars']} car.  maj {r['updated_at']}"
                )
            return 0
        if args.prompt_cmd == "show":
            if args.name:
                text = db.get_prompt(args.name)
                if text is None:
                    print(f"profil inconnu : {args.name}", file=sys.stderr)
                    return 1
            else:
                from docia.pipeline import resolve_system_prompt

                text = resolve_system_prompt(db, cfg)
            print(text)
            return 0
        if args.prompt_cmd == "save":
            if not args.file.exists():
                print(f"fichier introuvable : {args.file}", file=sys.stderr)
                return 1
            text = args.file.read_text(encoding="utf-8")
            if len(text.strip()) < 50:
                print("prompt trop court (< 50 caractères)", file=sys.stderr)
                return 1
            db.save_prompt(args.name, text, activate=args.use)
            print(f"profil « {args.name} » enregistré" + (" et activé" if args.use else ""))
            return 0
        if args.prompt_cmd == "use":
            if not db.set_active_prompt(args.name):
                print(f"profil inconnu : {args.name}", file=sys.stderr)
                return 1
            print(
                f"profil actif : {args.name} — les fichiers déjà analysés avec un autre prompt seront réanalysés au prochain run"
            )
            return 0
        if args.prompt_cmd == "reset":
            db.set_active_prompt(None)
            print("prompt embarqué actif")
            return 0
        if args.prompt_cmd == "export":
            text = db.get_prompt(args.name) if args.name else load_system_prompt(None)
            if text is None:
                print(f"profil inconnu : {args.name}", file=sys.stderr)
                return 1
            args.file.write_text(text, encoding="utf-8")
            print(f"→ {args.file}")
            return 0
        if args.prompt_cmd == "delete":
            if not db.delete_prompt(args.name):
                print(f"profil inconnu : {args.name}", file=sys.stderr)
                return 1
            print(f"profil « {args.name} » supprimé")
            return 0
    return 1


def cmd_review(args: argparse.Namespace, cfg: Config) -> int:
    with Database(cfg.db_path) as db:
        if db.get_file(args.file_id) is None:
            print(f"fichier inconnu : {args.file_id}", file=sys.stderr)
            return 1
        db.set_review(
            args.file_id,
            args.status,
            comment=args.comment,
            reviewer=args.reviewer,
            corrected_security=args.security,
            corrected_rgpd=args.rgpd,
            corrected_retention_years=args.retention_years,
        )
        counts = db.review_counts()
    print(
        f"fichier {args.file_id} : {args.status} — revues : "
        + ", ".join(f"{k} {v}" for k, v in counts.items())
    )
    return 0


def cmd_backup(args: argparse.Namespace, cfg: Config) -> int:
    """`backup` : copie horodatée de la base, avec rotation."""
    from docia.service import ServiceError, backup_database, list_backups

    try:
        path = backup_database(
            Path(cfg.db_path), out_dir=args.out, label=args.label, keep=args.keep
        )
    except ServiceError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(f"sauvegarde → {path}")
    kept = list_backups(Path(cfg.db_path))
    if kept:
        print(f"{len(kept)} sauvegarde(s) conservée(s) dans {kept[0].parent}")
    return 0


def cmd_restore(args: argparse.Namespace, cfg: Config) -> int:
    """`restore` : remplace la base par une sauvegarde (`--yes` obligatoire)."""
    from docia.service import ServiceError, restore_database

    target = Path(cfg.db_path)
    if not args.yes:
        print(f"remplacerait {target} par {args.backup}")
        print("aucune modification : relancer avec --yes pour confirmer", file=sys.stderr)
        return 1
    try:
        path = restore_database(target, args.backup)
    except ServiceError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    with Database(path) as db:
        counts = db.counts()
    print(
        f"base restaurée : {path} — {counts['files']} fichier(s), {counts['analyses']} analyse(s)"
    )
    return 0


def _parse_where(items: list[str] | None) -> dict[str, str]:
    """`["security=C3", "owner=X"]` → dictionnaire ; lève `ValueError` si mal formé."""
    out: dict[str, str] = {}
    for item in items or []:
        key, sep, value = item.partition("=")
        if not sep or not key.strip() or not value.strip():
            raise ValueError(f"critère mal formé : « {item} » (attendu clé=valeur)")
        out[key.strip()] = value.strip()
    return out


def cmd_reanalyze(args: argparse.Namespace, cfg: Config) -> int:
    """`reanalyze` : remet des fichiers à analyser après une sauvegarde automatique."""
    from docia.service import ServiceError, reanalyze

    try:
        where = _parse_where(args.where)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    if args.all:
        scope = "all"
    elif args.errors:
        scope = "errors"
    elif args.pending_only:
        scope = "pending_only"
    elif where:
        scope = "filter"
    else:
        print(
            "préciser --all, --errors, --pending-only ou au moins un --where clé=valeur",
            file=sys.stderr,
        )
        return 1
    if where and scope != "filter":
        print("--where ne se combine pas avec --all/--errors/--pending-only", file=sys.stderr)
        return 1
    try:
        with Database(cfg.db_path) as db:
            count = reanalyze(db, cfg, scope=scope, where=where or None, backup=not args.no_backup)
    except ServiceError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(f"{count} fichier(s) remis à analyser — lancer « docia run » pour les traiter")
    return 0


def cmd_campaigns(_args: argparse.Namespace, _cfg: Config) -> int:
    """`campaigns` : campagnes récentes, avec l'avancement des bases encore présentes."""
    from docia.service import campaign_status, recent_campaigns

    entries = recent_campaigns()
    if not entries:
        print("aucune campagne récente")
        return 0
    for entry in entries:
        if not entry.db_path.exists():
            print(f"  {entry.db_path}  (base absente)")
            continue
        try:
            with Database(entry.db_path) as db:
                state = campaign_status(db)
        except Exception as exc:  # noqa: BLE001 - une base illisible ne doit pas tout arrêter
            print(f"  {entry.db_path}  (illisible : {exc})")
            continue
        label = f" « {entry.label} »" if entry.label else ""
        print(
            f"  {entry.db_path}{label}  {state.done}/{state.files} analysé(s), "
            f"{state.pending} à analyser, {state.error} en erreur — "
            f"prompt {state.active_prompt} — ouvert {entry.last_opened}"
        )
    return 0


def cmd_retry(_args: argparse.Namespace, cfg: Config) -> int:
    with Database(cfg.db_path) as db:
        n = db.reset_errors()
    print(f"{n} fichier(s) remis à analyser")
    return 0


def _utf8_console() -> None:
    """Console Windows en cp1252 par défaut : les accents des messages sortent en `�`.
    On passe stdout/stderr en UTF-8 (sans jamais planter si le flux ne le permet pas)."""
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is not None:
            with contextlib.suppress(ValueError, OSError):  # flux fermé ou redirigé
                reconfigure(encoding="utf-8", errors="replace")


class _ConsoleFormatter(logging.Formatter):
    """Console : une ligne par événement, **sans la pile d'appels**.

    Une campagne réelle rencontre toujours quelques fichiers illisibles (mails
    corrompus, PDF protégés…). Déverser une trace Python de vingt lignes par fichier
    rend la console inutilisable et alarme l'utilisateur pour un incident bénin :
    la pile complète part dans le fichier journal, la console garde le message.
    """

    def format(self, record: logging.LogRecord) -> str:
        saved = (record.exc_info, record.exc_text, record.stack_info)
        record.exc_info = record.exc_text = record.stack_info = None
        try:
            return super().format(record)
        finally:
            record.exc_info, record.exc_text, record.stack_info = saved


def _log_file(config_path: Path | None) -> Path:
    """`docia.log` à côté du fichier de configuration (donc à côté de `Docia.exe`)."""
    base = Path(config_path).resolve().parent if config_path else Path.cwd()
    return base / "docia.log"


_JOURNAL: Path | None = None
"""Fichier journal en cours (configuré une seule fois par processus)."""

_LOGGING_CONFIGURED = False
"""Vrai dès que `_setup_logging` a posé ses gestionnaires."""


def _setup_logging(args: argparse.Namespace) -> Path | None:
    """Console lisible + journal détaillé sur disque (traces complètes, rotation).

    Idempotent : la fenêtre rappelle `main()` pour produire ses documents, il ne faut
    ni doubler les messages ni rouvrir le journal.
    """
    global _JOURNAL, _LOGGING_CONFIGURED
    if _LOGGING_CONFIGURED:
        return _JOURNAL
    _LOGGING_CONFIGURED = True
    root = logging.getLogger()
    root.setLevel(logging.DEBUG if args.verbose else logging.INFO)
    console = logging.StreamHandler(stream=sys.stderr)
    console.setLevel(logging.DEBUG if args.verbose else logging.INFO)
    console.setFormatter(_ConsoleFormatter("%(levelname)s %(name)s : %(message)s"))
    root.addHandler(console)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    target = _log_file(getattr(args, "config", None))
    try:
        from logging.handlers import RotatingFileHandler

        journal = RotatingFileHandler(target, maxBytes=4_000_000, backupCount=3, encoding="utf-8")
    except OSError as exc:  # dossier en lecture seule, disque plein : la console suffit
        logging.getLogger(__name__).warning("journal sur disque impossible (%s) : %s", target, exc)
        return None
    journal.setLevel(logging.DEBUG)
    journal.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    root.addHandler(journal)
    _JOURNAL = target
    return target


def main(argv: list[str] | None = None) -> int:
    _utf8_console()
    parser = build_parser()
    args = parser.parse_args(argv)
    journal = _setup_logging(args)
    if journal is not None:
        logging.getLogger(__name__).info("journal détaillé : %s", journal)
    if args.command == "init":
        return cmd_init(args)
    if args.command == "gui":
        try:
            from docia.gui import launch
        except ImportError as exc:
            print(
                f'interface graphique non installée (pip install "docia[gui]") : {exc}',
                file=sys.stderr,
            )
            return 1
        launch(args.config, smoke=bool(getattr(args, "smoke", False)))
        return 0
    cfg = _load(args)
    handlers = {
        "ingest": cmd_ingest,
        "plan": cmd_plan,
        "run": cmd_run,
        "status": cmd_status,
        "export": cmd_export,
        "report": cmd_report,
        "retry": cmd_retry,
        "prompt": cmd_prompt,
        "review": cmd_review,
        "backup": cmd_backup,
        "restore": cmd_restore,
        "reanalyze": cmd_reanalyze,
        "campaigns": cmd_campaigns,
    }
    from docia.cli_tools import cmd_bench, cmd_doctor, cmd_quick, cmd_scan

    handlers["bench"] = cmd_bench
    handlers["quick"] = cmd_quick
    handlers["scan"] = cmd_scan
    handlers["doctor"] = cmd_doctor
    return handlers[args.command](args, cfg)


if __name__ == "__main__":
    sys.exit(main())
