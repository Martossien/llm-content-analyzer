"""Pont entre la fenêtre et la couche service (`docia.service`).

Toute **écriture** de campagne (import, préparation, run, réanalyse, sauvegarde,
vérification humaine) passe par `GuiService`, qui délègue à `docia.service` — la même
couche que la CLI et, demain, le serveur web distant (v4). Ici : uniquement la mise en
forme des messages pour le journal.

Ce module ne connaît **rien** de Tk : pas de `filedialog`, pas de `webbrowser`, pas
d'appel à la CLI. Ce qui demande une fenêtre vit dans `docia.gui.dialogs`.
"""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from docia import service
from docia.config import Config
from docia.db import Database, backup_dir_for
from docia.gui.theme import format_duration
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


class GuiService:
    """Opérations de campagne exposées à la fenêtre, indépendantes de Tk."""

    def __init__(self, open_db: Callable[[], Database]) -> None:
        self._open_db = open_db

    # ---- import / plan / run
    def import_scan(self, csv_path: Path, *, strict: bool, log: Log) -> None:
        """Importe un CSV et rend compte de l'avancement au journal.

        Un CSV de 250 Mo demande une minute et plus : sans les lignes
        d'avancement, la fenêtre reste muette et l'utilisateur croit à un blocage.
        `import_progress_logger` espace ces lignes (2 s ou 50 000 lignes).
        """
        with self._open_db() as db:
            rep = service.import_scan(
                db, csv_path, strict=strict, progress=service.import_progress_logger(log)
            )
        # Le bilan d'import est formulé une seule fois, dans `service` : la CLI, `scan`
        # et la fenêtre l'écrivaient chacun de leur côté, et les trois divergeaient.
        log(service.format_import_report(rep))
        for err in rep.errors[:5]:
            log(f"   ligne {err.line_number} : {err.reason}")

    def plan(self, cfg: Config, log: Log) -> None:
        """Prépare la campagne et rend compte de l'avancement au journal.

        Une préparation d'un million de fichiers dure une minute : sans les
        lignes d'avancement, la fenêtre reste muette et l'utilisateur croit à un
        blocage. `plan_progress_logger` les espace (2 s ou 50 000 fichiers),
        exactement comme l'import.
        """
        from docia.filter import plan_progress_logger

        with self._open_db() as db:
            rep = service.plan(db, cfg, progress=plan_progress_logger(log))
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

        # Le bilan disait tout sauf **combien de temps** : il a fallu estimer le temps
        # mur d'un run par ordonnancement de ses blocs, faute de la mesure. C'est
        # pourtant le premier chiffre qu'on regarde pour comparer deux réglages.
        debut = time.time()
        log(f"run démarré à {time.strftime('%H:%M:%S', time.localtime(debut))}")
        with self._open_db() as db:
            rep = service.run_campaign(
                db, cfg, limit=limit, dry_run=dry_run, on_event=forward, cancel=cancel
            )
        fin = time.time()
        log(
            f"run {rep.run_id} : {rep.files_done} analysé(s) sur {rep.files_selected} "
            f"({rep.files_duplicates} doublons hérités, {rep.files_segmented} découpés), "
            f"{rep.files_error} en erreur — tokens {rep.prompt_tokens} entrée / "
            f"{rep.completion_tokens} sortie"
        )
        log(
            f"run {rep.run_id} : {time.strftime('%H:%M:%S', time.localtime(debut))} → "
            f"{time.strftime('%H:%M:%S', time.localtime(fin))} "
            f"({format_duration(fin - debut)})"
            + (
                f" — {rep.files_done / (fin - debut) * 3600:.0f} fichiers/h, "
                f"{rep.prompt_tokens / (fin - debut):.0f} tokens d'entrée/s"
                if fin > debut and rep.files_done
                else ""
            )
        )
        for e in rep.errors[:10]:
            log(f"   {e}")

    def quick(self, cfg: Config, target: Path, db_path: Path, log: Log, cancel: Any) -> None:
        from docia.quick import quick_analyze

        rep = quick_analyze(cfg, [target], db_path=db_path, progress=log, cancel=cancel)
        for line in rep.as_lines():
            log(line)

    # ---- vérification humaine (onglet Résultats)
    def latest_analysis(self, file_id: int) -> sqlite3.Row | None:
        """Fiche complète d'un fichier, par son identifiant — None s'il a disparu."""
        with self._open_db() as db:
            return next(iter(db.latest_analyses(file_id=file_id)), None)

    def set_review(self, file_id: int, status: str, **kwargs: Any) -> sqlite3.Row | None:
        """Enregistre la vérification humaine (`to_review` / `validated` / `corrected`).

        Délègue à `service.set_review`, comme toutes les autres écritures. Ce fut
        longtemps la seule qui appelait `Database` en direct depuis ce pont : la
        doctrine « toute écriture par le service » qu'annonce le docstring du module
        n'a de valeur que si elle n'a pas d'exception, et la v4 n'aurait eu aucune
        revue à exposer dans son API.

        Rend la fiche **relue après écriture** (None si le fichier a disparu) : l'écran
        s'en sert pour réécrire la seule ligne concernée au lieu de relire toute la
        campagne, et il l'obtient sans rouvrir la base une seconde fois.
        """
        with self._open_db() as db:
            return service.set_review(db, file_id, status, **kwargs)

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
