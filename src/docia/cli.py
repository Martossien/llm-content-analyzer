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
import logging.handlers
import os
import sqlite3
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
        cfg = load_config(args.config, on_missing=lambda line: print(line, file=sys.stderr))
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
    """`ingest` : importe un CSV SMBeagle **par la couche service**.

    Passer par `service.import_scan` et non par `ingest.import_csv` en direct
    n'est pas une élégance : c'est ce qui inscrit la campagne dans les récentes
    (sans quoi `docia campaigns` répondait « aucune campagne récente » après un
    import réussi en ligne de commande, alors que la fenêtre, elle, la retenait)
    et ce qui traduit un `OSError` en message français au lieu d'une trace Python.
    """
    from docia.service import (
        ServiceError,
        format_import_report,
        import_progress_logger,
        import_scan,
    )

    if not args.csv.exists():
        print(f"CSV introuvable : {args.csv}", file=sys.stderr)
        return 1
    # Progression sur stderr : un CSV de plusieurs centaines de Mo prend des minutes,
    # la sortie standard reste réservée au bilan.
    progress = import_progress_logger(lambda line: print(line, file=sys.stderr))
    try:
        with Database(cfg.db_path) as db:
            report = import_scan(db, args.csv, strict=not args.lenient, progress=progress)
    except ServiceError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(format_import_report(report, prefix=f"scan {report.scan_id}"))
    for err in report.errors[:10]:
        print(f"  ligne {err.line_number} : {err.reason}", file=sys.stderr)
    return 0 if report.invalid == 0 else 2


def cmd_plan(_args: argparse.Namespace, cfg: Config) -> int:
    from docia.filter import plan_files, plan_progress_logger

    # Progression sur stderr, comme `ingest` : une préparation d'un million de
    # fichiers dure une minute, la sortie standard reste réservée au bilan.
    progress = plan_progress_logger(lambda line: print(line, file=sys.stderr))
    with Database(cfg.db_path) as db:
        report = plan_files(db, cfg.filter, progress=progress)
    print(f"à analyser : {report.pending} — exclus : {report.excluded}")
    # Bilan borné : les raisons sont stables (voir `filter.TOO_SMALL`), mais les
    # marqueurs de dossier et les extensions d'un partage réel peuvent en faire
    # des dizaines — la console n'est pas un export.
    top = 20
    reasons = sorted(report.by_reason.items(), key=lambda kv: (-kv[1], kv[0]))
    for reason, n in reasons[:top]:
        print(f"  {n:>7}  {reason}")
    if len(reasons) > top:
        print(f"  … et {len(reasons) - top} autre(s) raison(s) — détail complet dans les exports")
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
    """`export` : dernière analyse de chaque fichier, écrite **au fil du curseur**.

    Rien n'est accumulé en mémoire : ni la liste des lignes, ni la chaîne JSON
    complète (plusieurs gigaoctets sur une campagne d'un million de fichiers).
    Le CSV prend ses en-têtes sur la première ligne lue ; le tableau JSON est
    écrit élément par élément (indentation de deux espaces, `ensure_ascii=False`).

    **Le CSV passe par `report.tabular.csv_cell`.** Il est écrit en `utf-8-sig`
    avec `;` pour être ouvert d'un double-clic dans Excel, c'est le bouton
    « CSV des fichiers » de la fenêtre, et c'est le format vers lequel le message
    de troncature du classeur (`report.excel`) renvoie l'utilisateur pour
    récupérer ses données complètes. Excel évalue comme une **formule** toute
    cellule texte commençant par `=`, `+`, `-`, `@`, une tabulation ou un retour
    chariot : le nom d'un fichier du partage (`- copie.docx`), le `resume` et les
    justifications rendus par le modèle (`=cmd|'/c calc.exe'!A1`,
    `=HYPERLINK("http://…")`) y arrivaient tels quels. Le JSON, lui, n'est pas
    concerné : aucun lecteur JSON n'évalue une chaîne — il est écrit inchangé.
    """
    if args.format in ("xlsx", "powerbi"):
        return _cmd_export_workbook(args, cfg)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with Database(cfg.db_path) as db:
        records = db.latest_analyses()
        if args.format == "json":
            with args.out.open("w", encoding="utf-8") as fh:
                fh.write("[")
                for record in records:
                    r = dict(record)
                    for key in ("rgpd_data_types", "finance_amounts", "legal_parties"):
                        if r.get(key):
                            r[key] = json.loads(r[key])
                    fh.write("\n" if count == 0 else ",\n")
                    # `json.dumps` d'un tableau indente chaque objet de deux espaces
                    # de plus : décaler toutes les lignes rend exactement le même texte.
                    fh.write(
                        "  " + json.dumps(r, ensure_ascii=False, indent=2).replace("\n", "\n  ")
                    )
                    count += 1
                fh.write("\n]" if count else "]")
        else:
            from docia.report.tabular import csv_cell

            with args.out.open("w", newline="", encoding="utf-8-sig") as fh:
                writer: csv.DictWriter[str] | None = None
                for record in records:
                    r = dict(record)
                    if writer is None:
                        writer = csv.DictWriter(fh, fieldnames=list(r), delimiter=";")
                        writer.writeheader()
                    writer.writerow({key: csv_cell(value) for key, value in r.items()})
                    count += 1
                if writer is None:
                    csv.DictWriter(fh, fieldnames=["path"], delimiter=";").writeheader()
    print(f"{count} ligne(s) → {args.out}")
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


JOURNAL_ONLY = "journal_seul"
"""Attribut d'enregistrement (`extra={JOURNAL_ONLY: True}`) qui réserve un message au
fichier : le garde-fou de `main()` y dépose la pile complète, mais parle à
l'utilisateur par un `print`, sans le préfixe `ERROR docia.cli :` ni doublon."""


def _log_file(config_path: Path | None) -> Path:
    """Emplacement de `docia.log` : à côté de `Docia.exe`, sinon à côté du `--config`.

    Le guide promet « `docia.log`, à côté de `Docia.exe` ». Dériver le chemin du
    seul `--config` ne tenait pas cette promesse : sa valeur par défaut est le
    **relatif** `docia.toml`, donc le journal atterrissait dans le répertoire
    courant — celui d'où l'utilisateur a lancé l'exe, pas celui de l'exe.
    """
    if getattr(sys, "frozen", False):  # exécutable empaqueté (PyInstaller)
        return Path(sys.executable).resolve().parent / "docia.log"
    base = Path(config_path).resolve().parent if config_path else Path.cwd()
    return base / "docia.log"


_JOURNAL: Path | None = None
"""Fichier journal en cours (configuré une seule fois par processus)."""

_LOGGING_CONFIGURED = False
"""Vrai dès que `_setup_logging` a posé ses gestionnaires."""

_LOG_MAX_BYTES = 4_000_000
"""Taille de `docia.log` au-delà de laquelle il est mis en rotation."""

_LOG_BACKUPS = 3
"""Sauvegardes de rotation conservées (`docia.log.1` … `docia.log.3`)."""

NOISY_DEBUG_LOGGERS: tuple[str, ...] = (
    "httpx",
    "httpcore",
    "openai",
    "urllib3",
    "asyncio",
    "PIL",
    "matplotlib",
    "charset_normalizer",
    "filelock",
)
"""Bibliothèques tierces muselées à `INFO` : en `DEBUG` elles écrivent une ligne
par requête HTTP, par image ouverte ou par police chargée, et noieraient le
journal — qui doit rester lisible pour diagnostiquer *notre* code."""


class _RotatingFileHandler(logging.handlers.RotatingFileHandler):
    """`RotatingFileHandler` qui survit à une rotation impossible.

    Sous Windows, un fichier ouvert par un autre processus ne peut pas être
    renommé : deux `Docia.exe` (la fenêtre et un `scan` en parallèle, ou deux
    sessions RDS) et `doRollover()` lève `PermissionError` à *chaque*
    enregistrement une fois les 4 Mo atteints. Le gestionnaire standard déverse
    alors une trace par message sur stderr — exactement ce que
    `_ConsoleFormatter` a été écrit pour empêcher — et **perd** tous les
    enregistrements suivants.

    Ici, le premier échec est signalé une seule fois, la rotation est abandonnée
    pour ce processus (`maxBytes = 0` : le journal continue de grossir, ce qui
    vaut infiniment mieux que de ne plus rien écrire) et le flux est rouvert.
    """

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)  # type: ignore[arg-type]
        self.rollover_failed = False

    def doRollover(self) -> None:  # noqa: N802 - nom imposé par logging
        try:
            super().doRollover()
        except OSError as exc:
            self.maxBytes = 0  # plus de tentative : un journal trop gros > un journal muet
            if self.stream is None:  # `doRollover` ferme avant de renommer
                self.stream = self._open()
            if not self.rollover_failed:
                self.rollover_failed = True
                logging.getLogger(__name__).warning(
                    "rotation de %s impossible (%s) — journal poursuivi sans rotation "
                    "(une autre instance de Docia le tient ouvert ?)",
                    self.baseFilename,
                    exc,
                )


def _open_journal(target: Path) -> _RotatingFileHandler | None:
    """Ouvre `docia.log`, ou `docia-<pid>.log` si le premier est inaccessible.

    Sous Windows, `docia.log` peut être verrouillé par une autre instance : plutôt
    que de renoncer au journal, on en ouvre un par processus, au même endroit.
    """
    per_process = target.with_name(f"{target.stem}-{os.getpid()}{target.suffix}")
    for candidate in (target, per_process):
        try:
            return _RotatingFileHandler(
                candidate, maxBytes=_LOG_MAX_BYTES, backupCount=_LOG_BACKUPS, encoding="utf-8"
            )
        except OSError as exc:  # dossier en lecture seule, disque plein, fichier verrouillé
            logging.getLogger(__name__).warning(
                "journal sur disque impossible (%s) : %s", candidate, exc
            )
    return None


def _setup_logging(args: argparse.Namespace) -> Path | None:
    """Console lisible + journal détaillé sur disque (traces complètes, rotation).

    Les niveaux sont distincts, sans quoi le fichier ne contiendrait rien de plus
    que l'écran alors qu'il s'annonce « journal détaillé » : la racine est en
    `DEBUG` (elle laisse tout passer), la console filtre à `INFO` (`DEBUG` avec
    `-v`) et le fichier prend tout. Les bibliothèques tierces bavardes sont
    muselées nommément (`NOISY_DEBUG_LOGGERS`).

    Idempotent : la fenêtre rappelle `main()` pour produire ses documents, il ne faut
    ni doubler les messages ni rouvrir le journal.
    """
    global _JOURNAL, _LOGGING_CONFIGURED
    if _LOGGING_CONFIGURED:
        return _JOURNAL
    _LOGGING_CONFIGURED = True
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    console = logging.StreamHandler(stream=sys.stderr)
    console.setLevel(logging.DEBUG if args.verbose else logging.INFO)
    console.setFormatter(_ConsoleFormatter("%(levelname)s %(name)s : %(message)s"))
    console.addFilter(lambda record: not getattr(record, JOURNAL_ONLY, False))
    root.addHandler(console)
    for name in NOISY_DEBUG_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING if name == "httpx" else logging.INFO)
    journal = _open_journal(_log_file(getattr(args, "config", None)))
    if journal is None:  # la console suffit : jamais de plantage pour un journal
        return None
    journal.setLevel(logging.DEBUG)
    journal.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    root.addHandler(journal)
    _JOURNAL = Path(journal.baseFilename)
    return _JOURNAL


def _silence_third_party_warnings() -> None:
    """Coupe les avertissements openpyxl sans conséquence (« Data Validation extension
    is not supported »), qui inquiétaient l'utilisateur dans la console de l'exe.

    DocFuse ne les filtre volontairement plus à l'import : une bibliothèque n'a pas à
    modifier la politique d'avertissements de son hôte. C'est donc au point d'entrée
    applicatif de le faire — ici, une fois, au démarrage.
    """
    try:
        from docfuse.extractors.xlsx import silence_openpyxl_warnings
    except (ImportError, AttributeError):  # DocFuse absent ou antérieur à cette API
        return
    with contextlib.suppress(Exception):
        silence_openpyxl_warnings()


def _expected_failure(exc: BaseException) -> bool:
    """Vrai pour les pannes que l'utilisateur peut comprendre et corriger seul.

    L'import de `docia.service` n'a lieu qu'ici, sur le chemin d'erreur : `docia
    init` n'a pas à payer le chargement du pipeline pour un garde-fou.
    """
    from docia.service import ServiceError

    return isinstance(exc, OSError | sqlite3.Error | ServiceError)


def _report_failure(command: str, exc: BaseException) -> None:
    """Une ligne pour l'utilisateur, la trace complète pour `docia.log` — et rien d'autre."""
    logging.getLogger(__name__).error(
        "échec de « docia %s »", command, exc_info=exc, extra={JOURNAL_ONLY: True}
    )
    detail = str(exc).strip().splitlines()
    message = detail[0] if detail else type(exc).__name__
    suffix = f" — détail dans {_JOURNAL}" if _JOURNAL is not None else ""
    print(f"échec de « docia {command} » : {message}{suffix}", file=sys.stderr)


def main(argv: list[str] | None = None) -> int:
    """Point d'entrée de la CLI.

    Garde-fou : une panne prévisible — base en lecture seule (`Docia.exe` posé
    dans `C:\\Program Files`), base verrouillée, disque plein, fichier
    inaccessible — sort en **une ligne** et en code 1 ; la trace complète part
    dans `docia.log`. Sans lui, l'utilisateur recevait un
    `sqlite3.OperationalError: attempt to write a readonly database` de vingt
    lignes venu d'un `PRAGMA journal_mode=WAL`. Tout le reste (une vraie
    anomalie) remonte intact : on ne masque pas ce qu'on ne comprend pas.
    """
    _utf8_console()
    _silence_third_party_warnings()
    parser = build_parser()
    args = parser.parse_args(argv)
    journal = _setup_logging(args)
    if journal is not None:
        logging.getLogger(__name__).info("journal détaillé : %s", journal)
    try:
        return _dispatch(args)
    except KeyboardInterrupt:
        print("interrompu", file=sys.stderr)
        return 1
    except Exception as exc:
        if not _expected_failure(exc):
            raise
        _report_failure(str(args.command), exc)
        return 1


def _dispatch(args: argparse.Namespace) -> int:
    """Exécute la sous-commande demandée (voir `main` pour le garde-fou)."""
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
