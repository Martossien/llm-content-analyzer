"""Run d'une campagne (cadence, ETA), réanalyse ciblée, vérification humaine."""

from __future__ import annotations

import sqlite3
import threading
import time
from collections.abc import Callable

from docia.config import Config
from docia.db import Database, latest_analysis_sql
from docia.pipeline import RunReport, run_pipeline
from docia.service._common import (
    REANALYZE_SCOPES,
    WHERE_KEYS,
    RunEvent,
    ServiceError,
    _effective_keys,
    logger,
)
from docia.service.backups import backup_database


class _Pace:
    """Cadence observée d'un run : débit à partir du premier bloc terminé, et ETA."""

    def __init__(self) -> None:
        self.started = time.monotonic()
        self.first_done_at: float | None = None
        self.files_at_first_done = 0

    def note_block_done(self, files_done: int) -> None:
        """Premier bloc terminé : début du régime établi pour le débit."""
        if self.first_done_at is None:
            self.first_done_at = time.monotonic()
            self.files_at_first_done = files_done

    def rate_per_s(self, files_done: int) -> float | None:
        """Fichiers par seconde depuis le premier bloc terminé (régime établi)."""
        if self.first_done_at is not None:
            span = time.monotonic() - self.first_done_at
            produced = files_done - self.files_at_first_done
            if span > 0.0 and produced > 0:
                return produced / span
        span = time.monotonic() - self.started
        if span > 0.0 and files_done > 0:
            return files_done / span
        return None

    def eta_s(self, files_done: int, files_error: int, files_total: int) -> float | None:
        """Secondes restantes estimées, ou None faute de débit ou de reste."""
        rate = self.rate_per_s(files_done)
        remaining = files_total - files_done - files_error
        if rate is None or rate <= 0.0 or remaining <= 0:
            return None
        return round(remaining / rate, 1)

    def elapsed_s(self) -> float:
        """Secondes écoulées depuis le début du run."""
        return round(time.monotonic() - self.started, 3)


def _as_int(payload: dict[str, object], key: str) -> int:
    """Entier d'un événement du pipeline (0 si absent ou inattendu)."""
    value = payload.get(key)
    return value if isinstance(value, int) else 0


def _as_float(payload: dict[str, object], key: str) -> float | None:
    """Réel d'un événement du pipeline (None si absent ou inattendu)."""
    value = payload.get(key)
    return float(value) if isinstance(value, (int, float)) else None


_EVENT_KINDS = {
    "start": "info",
    "info": "info",
    "block_done": "block_done",
    "block_error": "block_error",
    "file_error": "file_error",
    "cancelled": "cancelled",
    "finished": "finished",
}


def run_campaign(
    db: Database,
    cfg: Config,
    *,
    limit: int | None = None,
    dry_run: bool = False,
    on_event: Callable[[RunEvent], None] | None = None,
    cancel: threading.Event | None = None,
) -> RunReport:
    """Lance un run et rend un `RunEvent` enrichi (durée, reste à faire, débit) par étape.

    Enveloppe `pipeline.run_pipeline` : le pipeline reste la seule implémentation,
    ce service n'ajoute que la mesure de cadence. Une erreur de configuration ou
    d'accès à la base devient un `ServiceError`.
    """
    pace = _Pace()

    def forward(payload: dict[str, object]) -> None:
        if on_event is None:
            return
        raw_kind = str(payload.get("event", "info"))
        files_done = _as_int(payload, "files_done")
        files_total = _as_int(payload, "files_total")
        files_error = _as_int(payload, "files_error")
        if raw_kind == "block_done":
            pace.note_block_done(files_done)
        event = RunEvent(
            kind=_EVENT_KINDS.get(raw_kind, "info"),
            message=str(payload.get("message", "")),
            files_done=files_done,
            files_total=files_total,
            files_error=files_error,
            blocks_done=_as_int(payload, "blocks_done"),
            blocks_total=_as_int(payload, "blocks_total"),
            elapsed_s=pace.elapsed_s(),
            eta_s=pace.eta_s(files_done, files_error, files_total),
            files_per_hour=_per_hour(pace.rate_per_s(files_done)),
            tokens_in_flight=_as_int(payload, "tokens_in_flight"),
            budget_tokens=_as_int(payload, "budget_tokens"),
            throughput_tok_s=_as_float(payload, "throughput_tok_s"),
        )
        try:
            on_event(event)
        except Exception:  # pragma: no cover - un afficheur ne doit jamais casser le run
            logger.exception("callback de progression en erreur (ignoré)")

    try:
        return run_pipeline(
            db,
            cfg,
            limit=limit,
            dry_run=dry_run,
            on_progress=forward if on_event is not None else None,
            cancel=cancel,
        )
    except sqlite3.Error as exc:
        raise ServiceError(f"base inutilisable pendant le run : {exc}") from exc
    except OSError as exc:
        raise ServiceError(f"run impossible (accès fichier) : {exc}") from exc


def _per_hour(rate_per_s: float | None) -> float | None:
    return round(rate_per_s * 3600.0, 1) if rate_per_s else None


# ------------------------------------------------------------------ réanalyse


def _where_clauses(where: dict[str, str]) -> tuple[list[str], list[object]]:
    """Traduit `where` en conditions SQL sur `files f` (+ dernière analyse `a`)."""
    clauses: list[str] = []
    params: list[object] = []
    for key, value in where.items():
        if key not in WHERE_KEYS:
            raise ServiceError(
                f"critère de sélection inconnu : « {key} » (attendu : {', '.join(WHERE_KEYS)})"
            )
        text = str(value).strip()
        if not text:
            raise ServiceError(f"critère « {key} » sans valeur")
        if key == "security":
            clauses.append("a.security_classification = ?")
            params.append(text)
        elif key == "rgpd":
            clauses.append("a.rgpd_risk_level = ?")
            params.append(text)
        elif key == "owner":
            clauses.append("f.owner = ?")
            params.append(text)
        elif key == "extension":
            clauses.append("LOWER(f.extension) = ?")
            params.append(text.lower().lstrip("."))
        else:  # path_like
            clauses.append("f.path LIKE ?")
            params.append(text)
    return clauses, params


def _targets(db: Database, scope: str, where: dict[str, str] | None) -> list[int]:
    """Identifiants des fichiers visés par une réanalyse (jamais les exclus)."""
    # La même règle « l'analyse qui fait foi » que partout ailleurs : la plus
    # récente **du contenu actuel**. Une copie locale de la règle, sans la
    # condition sur `content_version`, ciblait un fichier modifié depuis son
    # analyse sur une classification périmée — et en laissait passer un autre.
    latest = f" LEFT JOIN analyses a ON {latest_analysis_sql('f.id')}"
    clauses = ["f.status <> 'excluded'"]
    params: list[object] = []
    if scope == "pending_only":
        clauses = ["f.status = 'pending'"]
    elif scope == "filter":
        if not where:
            raise ServiceError(
                "réanalyse ciblée : préciser au moins un critère (--where clé=valeur)"
            )
        extra, extra_params = _where_clauses(where)
        clauses.extend(extra)
        params.extend(extra_params)
    sql = f"SELECT f.id AS id FROM files f{latest} WHERE {' AND '.join(clauses)} ORDER BY f.id"  # noqa: S608
    return [int(r["id"]) for r in db.query(sql, tuple(params))]


def reanalyze(
    db: Database,
    cfg: Config,
    *,
    scope: str,
    where: dict[str, str] | None = None,
    backup: bool = True,
) -> int:
    """Force la réanalyse de fichiers déjà traités et rend leur nombre.

    `scope` : `all` (toute la campagne, hors exclus), `errors` (fichiers en
    erreur remis à analyser, sans rien supprimer), `pending_only` (nettoie les
    analyses des fichiers déjà à analyser), `filter` (sélection `where` :
    `security`, `rgpd`, `owner`, `extension`, `path_like`).

    Les analyses supprimées sont celles de la clé courante — empreinte du prompt
    effectif et modèle configuré : changer de prompt ou de modèle provoque déjà
    une réanalyse sans rien effacer. Une sauvegarde est prise avant l'opération
    (`backup=False` pour la désactiver, à réserver aux tests).

    **Atomicité.** Remettre les fichiers `pending` et supprimer leurs analyses sont
    deux écritures : faites séparément, une coupure entre les deux laissait un état
    intermédiaire — au mieux visible et réparable, jamais souhaitable.
    `Database.reset_for_reanalysis` les enchaîne dans **une seule transaction**, et
    c'est elle qui est appelée ici. La fenêtre est fermée : une coupure rend la
    campagne exactement telle qu'elle était.

    Cette méthode existait, testée, et **personne ne l'appelait** — pendant que cette
    docstring affirmait qu'elle n'existait pas. C'est le travers qu'une relecture
    extérieure a nommé sur ce projet : « le correctif est écrit, le test est écrit,
    le branchement est oublié, et la documentation raconte la version corrigée ».
    """
    if scope not in REANALYZE_SCOPES:
        raise ServiceError(
            f"portée de réanalyse inconnue : « {scope} » (attendu : {', '.join(REANALYZE_SCOPES)})"
        )
    if backup:
        backup_database(db.path, label=f"avant_reanalyse_{scope}", db=db)
    if scope == "errors":
        count = db.reset_errors()
        logger.info("réanalyse (erreurs) : %s fichier(s) remis à analyser", count)
        return count

    file_ids = _targets(db, scope, where)
    if not file_ids:
        logger.info("réanalyse (%s) : aucun fichier ciblé", scope)
        return 0
    phash, model = _effective_keys(db, cfg)
    # Une seule transaction pour les deux écritures : ni ordre à ruser, ni état
    # intermédiaire à rendre réparable. Une coupure au milieu laisse la campagne
    # exactement telle qu'elle était.
    deleted = db.reset_for_reanalysis(file_ids, prompt_hash=phash, model=model)
    logger.info(
        "réanalyse (%s) : %s fichier(s) remis à analyser, %s analyse(s) supprimée(s)",
        scope,
        len(file_ids),
        deleted,
    )
    return len(file_ids)


# -------------------------------------------------------- vérification humaine


def set_review(
    db: Database,
    file_id: int,
    status: str,
    *,
    comment: str = "",
    reviewer: str = "",
    corrected_security: str | None = None,
    corrected_rgpd: str | None = None,
    corrected_retention_years: int | None = None,
) -> sqlite3.Row | None:
    """Enregistre la vérification humaine d'un fichier et rend sa fiche relue.

    `status` : `to_review`, `validated` ou `corrected`. Rend la ligne telle qu'elle
    est **après** écriture (`None` si le fichier a disparu) : l'appelant réaffiche
    la seule ligne concernée sans rouvrir la base.

    Cette fonction n'ajoute rien à `Database.set_review` — c'est justement le point.
    La doctrine du module annonce que toute écriture de campagne passe par le
    service ; l'onglet Résultats était la seule exception, et une doctrine avec une
    exception ne protège plus rien (l'API REST de la v4 n'aurait pas eu de revue à
    exposer). `cli.cmd_review` passe par ici depuis la 3.0.
    """
    try:
        db.set_review(
            file_id,
            status,
            comment=comment,
            reviewer=reviewer,
            corrected_security=corrected_security,
            corrected_rgpd=corrected_rgpd,
            corrected_retention_years=corrected_retention_years,
        )
    except ValueError as exc:
        raise ServiceError(f"statut de vérification inconnu : « {status} »") from exc
    except sqlite3.Error as exc:
        raise ServiceError(f"vérification non enregistrée : {exc}") from exc
    return next(iter(db.latest_analyses(file_id=file_id)), None)


# ----------------------------------------------------------------- sauvegarde
