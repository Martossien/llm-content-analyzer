"""Campagnes : état d'une campagne et liste des campagnes récentes (`recent.json`)."""

from __future__ import annotations

import json
import os
from collections.abc import Sequence
from pathlib import Path

from docia.db import Database
from docia.models import FileStatus
from docia.service._common import (
    MAX_RECENT,
    RECENT_FILE,
    CampaignStatus,
    RecentCampaign,
    _now_iso,
    docia_home,
    logger,
)
from docia.views import runs_summary


def campaign_status(db: Database) -> CampaignStatus:
    """Compteurs, classifications, revues, prompt actif et dernier run d'une campagne."""
    counts = db.counts()
    classes = db.classification_summary()
    reviews = db.review_counts()
    runs = runs_summary(db)
    active = db.active_prompt()
    last_run = max(runs, key=lambda r: r.run_id) if runs else None
    return CampaignStatus(
        db_path=db.path,
        files=counts.get("files", 0),
        pending=counts.get(FileStatus.PENDING.value, 0),
        queued=counts.get(FileStatus.QUEUED.value, 0),
        done=counts.get(FileStatus.DONE.value, 0),
        error=counts.get(FileStatus.ERROR.value, 0),
        excluded=counts.get(FileStatus.EXCLUDED.value, 0),
        analyses=counts.get("analyses", 0),
        blocks_built=counts.get("blocks_built", 0),
        blocks_sent=counts.get("blocks_sent", 0),
        blocks_done=counts.get("blocks_done", 0),
        blocks_error=counts.get("blocks_error", 0),
        reviewed=reviews.get("validated", 0) + reviews.get("corrected", 0),
        to_review=reviews.get("to_review", 0),
        security=dict(classes.get("security", {})),
        rgpd=dict(classes.get("rgpd", {})),
        active_prompt=active[0] if active else "(embarqué)",
        last_run=last_run,
        schema_version=db.schema_version,
    )


# ------------------------------------------------------------------ ingestion


def _recent_path() -> Path:
    return docia_home() / RECENT_FILE


def _read_recent() -> list[dict[str, str]]:
    """Contenu de `recent.json`, ou une liste vide si absent ou illisible."""
    path = _recent_path()
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return []
    entries = raw.get("campaigns") if isinstance(raw, dict) else raw
    if not isinstance(entries, list):
        return []
    out: list[dict[str, str]] = []
    for item in entries:
        if isinstance(item, dict) and str(item.get("db_path", "")).strip():
            out.append({str(k): str(v) for k, v in item.items() if v is not None})
    return out


def _write_recent(entries: Sequence[dict[str, str]]) -> None:
    """Écrit `recent.json` (fichier temporaire puis `os.replace`) sans jamais lever."""
    path = _recent_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(path.name + ".tmp")
        temporary.write_text(
            json.dumps({"campaigns": list(entries)}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        os.replace(temporary, path)
    except OSError as exc:
        logger.warning("liste des campagnes récentes non enregistrée (%s) : %s", path, exc)


def _same_db(left: str, right: str) -> bool:
    """Deux chemins désignent la même base (comparaison insensible à la casse sous Windows)."""
    if os.name == "nt":
        return left.casefold() == right.casefold()
    return left == right


def remember_campaign(db_path: Path, csv_path: Path | None = None, label: str = "") -> None:
    """Place une campagne en tête des récentes (20 au plus, chemins absolus)."""
    key = str(Path(db_path).resolve())
    entry = {
        "db_path": key,
        "csv_path": str(Path(csv_path).resolve()) if csv_path is not None else "",
        "last_opened": _now_iso(),
        "label": label,
    }
    existing = _read_recent()
    kept = [e for e in existing if not _same_db(str(e.get("db_path", "")), key)]
    previous = next((e for e in existing if _same_db(str(e.get("db_path", "")), key)), None)
    if previous is not None:
        if not entry["csv_path"]:
            entry["csv_path"] = str(previous.get("csv_path", ""))
        if not entry["label"]:
            entry["label"] = str(previous.get("label", ""))
    _write_recent([entry, *kept][:MAX_RECENT])


def recent_campaigns() -> list[RecentCampaign]:
    """Campagnes récemment ouvertes, de la plus récente à la plus ancienne."""
    out: list[RecentCampaign] = []
    for entry in _read_recent():
        csv_text = str(entry.get("csv_path", ""))
        out.append(
            RecentCampaign(
                db_path=Path(entry["db_path"]),
                csv_path=Path(csv_text) if csv_text else None,
                last_opened=str(entry.get("last_opened", "")),
                label=str(entry.get("label", "")),
            )
        )
    return out


def forget_campaign(db_path: Path) -> None:
    """Retire une campagne de la liste des récentes (la base n'est pas touchée)."""
    key = str(Path(db_path).resolve())
    _write_recent([e for e in _read_recent() if not _same_db(str(e.get("db_path", "")), key)])
