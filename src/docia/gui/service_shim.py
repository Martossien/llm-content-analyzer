"""Pont entre la fenêtre et la couche service (`docia.service`).

La fenêtre n'appelle jamais `db`/`pipeline` directement pour les opérations de
campagne : elle passe par `GuiService`, qui délègue à `docia.service` — la même
couche que la CLI et, demain, le serveur web distant (v4). Ici : uniquement la
mise en forme des messages pour le journal.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from docia import service
from docia.config import Config
from docia.db import Database, backup_dir_for
from docia.service import RecentCampaign, RunEvent

Log = Callable[[str], None]


def load_recent() -> list[RecentCampaign]:
    return service.recent_campaigns()


def remember_recent(db_path: str, csv_path: str | None = None) -> None:
    service.remember_campaign(Path(db_path), Path(csv_path) if csv_path else None)


def default_backup_dir(db_path: Path) -> Path:
    return backup_dir_for(db_path)


def list_backups(db_path: Path) -> list[Path]:
    return service.list_backups(db_path)


def produce_document(
    app: Any,
    fmt: str,
    kind: str,
    *,
    on_done: Callable[[Path], None] | None = None,
) -> None:
    """Demande l'emplacement, produit le document, puis **ouvre le rapport HTML**.

    Partagé par l'onglet Rapports et par le bouton « Rapport HTML… » des Statistiques :
    un bouton qui promet un document doit produire ce document, pas seulement changer
    d'onglet. Le chemin complet est écrit au journal — c'est la réponse à « où est le
    fichier ? ».
    """
    from tkinter import filedialog

    if not app.db_path().exists():
        app.log("aucune campagne ouverte")
        return
    stem = app.db_path().stem
    if fmt == "powerbi":
        chosen = filedialog.askdirectory(title="Dossier de sortie Power BI")
    else:
        chosen = filedialog.asksaveasfilename(
            title=f"Enregistrer le document {fmt.upper()}",
            defaultextension=f".{fmt}",
            initialfile=f"{stem}-rapport.{fmt}",
            filetypes=[(fmt.upper(), f"*.{fmt}")],
        )
    if not chosen:
        return
    out = Path(chosen)
    db_path = str(app.db_path())

    def work() -> None:
        from docia.cli import main as cli_main

        try:
            code = cli_main(["--db", db_path, kind, "--format", fmt, "--out", str(out)])
        except SystemExit as exc:  # `_load` sort par SystemExit sur une config invalide
            app.log(f"{fmt} : configuration refusée ({exc.code})")
            return
        if code != 0:
            app.log(f"{fmt} : échec de la production du document")
            return
        app.log(f"document {fmt} écrit : {out}")
        if fmt == "html":
            import webbrowser

            webbrowser.open(out.as_uri())
            app.log("le rapport s'ouvre dans le navigateur")
        if on_done is not None:
            app.ui(lambda: on_done(out))

    app.run_in_thread(work, f"document {fmt}")


class GuiService:
    """Opérations de campagne exposées à la fenêtre, indépendantes de Tk."""

    def __init__(self, open_db: Callable[[], Database]) -> None:
        self._open_db = open_db

    # ---- import / plan / run
    def import_scan(self, csv_path: Path, *, strict: bool, log: Log) -> None:
        with self._open_db() as db:
            rep = service.import_scan(db, csv_path, strict=strict)
        log(
            f"import : {rep.total} lignes — {rep.new} nouveaux, {rep.updated} modifiés, "
            f"{rep.unchanged} inchangés, {rep.invalid} invalides"
        )
        for err in rep.errors[:5]:
            log(f"   ligne {err.line_number} : {err.reason}")

    def plan(self, cfg: Config, log: Log) -> None:
        with self._open_db() as db:
            rep = service.plan(db, cfg)
        log(f"préparation : {rep.pending} fichier(s) à analyser, {rep.excluded} exclu(s)")
        for reason, n in sorted(rep.by_reason.items(), key=lambda kv: -kv[1])[:6]:
            log(f"   {n:>7}  {reason}")

    def run(
        self,
        cfg: Config,
        *,
        limit: int | None,
        dry_run: bool,
        log: Log,
        cancel: Any,
        on_event: Callable[[RunEvent], None] | None = None,
    ) -> None:
        def forward(event: RunEvent) -> None:
            if event.message:
                log(event.message)
            if on_event is not None:
                on_event(event)

        with self._open_db() as db:
            rep = service.run_campaign(
                db, cfg, limit=limit, dry_run=dry_run, on_event=forward, cancel=cancel
            )
        log(
            f"run {rep.run_id} : {rep.files_done} analysé(s) sur {rep.files_selected} "
            f"({rep.files_duplicates} doublons hérités, {rep.files_segmented} découpés), "
            f"{rep.files_error} en erreur — tokens {rep.prompt_tokens} entrée / "
            f"{rep.completion_tokens} sortie"
        )
        for e in rep.errors[:10]:
            log(f"   {e}")

    def quick(self, cfg: Config, target: Path, db_path: Path, log: Log, cancel: Any) -> None:
        from docia.quick import quick_analyze

        rep = quick_analyze(cfg, [target], db_path=db_path, progress=log, cancel=cancel)
        for line in rep.as_lines():
            log(line)

    # ---- relancer
    def reanalyze(self, cfg: Config, scope: str, log: Log) -> int:
        """`errors` : remet les erreurs à analyser ; `all` : sauvegarde puis efface les analyses."""
        with self._open_db() as db:
            n = service.reanalyze(db, cfg, scope=scope, backup=scope == "all")
        log(
            f"{n} fichier(s) en erreur remis à analyser"
            if scope == "errors"
            else f"réanalyse complète : {n} fichier(s) remis à analyser (sauvegarde faite)"
        )
        return n

    # ---- sauvegarde
    def backup(self, db_path: Path, out_dir: Path | None = None) -> Path:
        with self._open_db() as db:
            return service.backup_database(db_path, out_dir=out_dir, db=db)

    def restore(self, db_path: Path, source: Path) -> Path:
        return service.restore_database(db_path, source)
