"""Classeur Excel : un onglet par vue, en-têtes gras, filtres et volets figés.

`openpyxl` est déjà présent (dépendance de DocFuse) ; aucun ajout requis.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

from openpyxl import Workbook  # type: ignore[import-untyped]
from openpyxl.styles import Alignment, Font, PatternFill  # type: ignore[import-untyped]
from openpyxl.utils import get_column_letter  # type: ignore[import-untyped]

from docia import views
from docia.db import Database
from docia.report.data import ReportData, collect

SHEETS: tuple[str, ...] = (
    "Synthèse",
    "Fichiers",
    "Doublons",
    "Ancienneté",
    "Sensibles",
    "Conservation",
    "Nettoyage",
    "Revues",
    "Erreurs",
)
"""Onglets du classeur, dans l'ordre."""

_HEADER_FILL = PatternFill("solid", fgColor="D9E2EC")
_HEADER_FONT = Font(bold=True, color="1F2933")
_MAX_WIDTH = 70


def _add_sheet(
    workbook: Workbook,
    title: str,
    headers: list[str],
    rows: list[list[Any]],
    *,
    formats: dict[int, str] | None = None,
) -> None:
    """Ajoute un onglet formaté (en-tête figé, filtre auto, colonnes dimensionnées)."""
    sheet = workbook.create_sheet(title=title[:31])
    sheet.append(headers)
    for cell in sheet[1]:
        cell.font = _HEADER_FONT
        cell.fill = _HEADER_FILL
        cell.alignment = Alignment(horizontal="left", vertical="center")
    for row in rows:
        sheet.append(row)
    widths = [len(h) + 2 for h in headers]
    for row in rows:
        for index, value in enumerate(row):
            widths[index] = max(widths[index], min(len(str(value)) + 2, _MAX_WIDTH))
    for index, width in enumerate(widths, start=1):
        sheet.column_dimensions[get_column_letter(index)].width = width
    if headers:
        last = get_column_letter(len(headers))
        sheet.auto_filter.ref = f"A1:{last}{max(len(rows) + 1, 2)}"
    sheet.freeze_panes = "A2"
    for column, number_format in (formats or {}).items():
        letter = get_column_letter(column)
        for row_index in range(2, len(rows) + 2):
            sheet[f"{letter}{row_index}"].number_format = number_format


def _date_cell(value: date | None) -> date | str:
    return value if value is not None else ""


def write_workbook(
    db: Database, path: Path, *, today: date | None = None, data: ReportData | None = None
) -> Path:
    """Écrit le classeur `path` et le rend."""
    report = data if data is not None else collect(db, today=today)
    workbook = Workbook()
    workbook.remove(workbook.active)
    o = report.overview

    _add_sheet(
        workbook,
        "Synthèse",
        ["Indicateur", "Valeur", "Détail"],
        [
            ["Base", o.db_path, ""],
            ["Généré le", o.generated_at, ""],
            ["Modèle", o.model, ""],
            ["Prompt", o.prompt_name, o.prompt_hash],
            ["Fichiers inventoriés", o.total_files, views.format_bytes(o.total_bytes)],
            ["Volume total (octets)", o.total_bytes, ""],
            ["Analysés", o.analyzed, f"{views.percent(o.analyzed, o.total_files)} %"],
            ["À analyser", o.pending, ""],
            ["Exclus", o.excluded, ""],
            ["En erreur", o.errors, ""],
            ["Familles de doublons", o.duplicate_families, ""],
            [
                "Octets récupérables (doublons)",
                o.duplicate_reclaimable_bytes,
                views.format_bytes(o.duplicate_reclaimable_bytes),
            ],
            [f"Fichiers non accédés depuis {o.stale_years} ans", o.stale_files, ""],
            ["Octets non accédés", o.stale_bytes, views.format_bytes(o.stale_bytes)],
            ["Fichiers sensibles (C2/C3)", o.sensitive_files, ""],
            ["RGPD élevé ou critique", o.rgpd_at_risk, ""],
            ["Fichiers à conserver", o.retention_files, ""],
            ["Candidats au nettoyage", o.cleanup_files, views.format_bytes(o.cleanup_bytes)],
            ["Octets libérables", o.cleanup_bytes, ""],
            ["Vérifiés par un humain", o.reviewed, ""],
        ],
    )

    file_rows = [dict(r) for r in db.latest_analyses()]
    headers = list(file_rows[0].keys()) if file_rows else ["path"]
    _add_sheet(
        workbook,
        "Fichiers",
        headers,
        [[r.get(h, "") if r.get(h) is not None else "" for h in headers] for r in file_rows],
    )

    _add_sheet(
        workbook,
        "Doublons",
        [
            "Famille",
            "Empreinte",
            "Taille unitaire (o)",
            "Copies",
            "Octets récupérables",
            "Chemins",
        ],
        [
            [
                f.family_id,
                f.fast_hash,
                f.size_bytes,
                f.copies,
                f.reclaimable_bytes,
                " | ".join(f.paths),
            ]
            for f in report.duplicates.families
        ],
        formats={3: "#,##0", 5: "#,##0"},
    )

    _add_sheet(
        workbook,
        "Ancienneté",
        [
            "Seuil (ans)",
            "Antérieur au",
            "Fichiers non accédés",
            "Octets non accédés",
            "Fichiers non modifiés",
            "Octets non modifiés",
        ],
        [
            [
                b.years,
                b.cutoff,
                b.not_accessed_files,
                b.not_accessed_bytes,
                b.not_modified_files,
                b.not_modified_bytes,
            ]
            for b in report.stale
        ],
        formats={2: "DD/MM/YYYY", 4: "#,##0", 6: "#,##0"},
    )

    _add_sheet(
        workbook,
        "Sensibles",
        [
            "file_id",
            "Chemin",
            "Propriétaire",
            "Taille (o)",
            "Sécurité",
            "Confiance sécurité",
            "RGPD",
            "Confiance RGPD",
            "Résumé",
            "Justification",
            "Revue",
        ],
        [
            [
                f.file_id,
                f.path,
                f.owner,
                f.size_bytes,
                f.security,
                f.security_confidence,
                f.rgpd,
                f.rgpd_confidence,
                f.resume,
                f.justification,
                f.review_status,
            ]
            for f in report.sensitive
        ],
        formats={4: "#,##0"},
    )

    _add_sheet(
        workbook,
        "Conservation",
        [
            "file_id",
            "Chemin",
            "Propriétaire",
            "Taille (o)",
            "Durée (ans)",
            "Fondement",
            "Dernière écriture",
            "Fin de conservation",
            "Échu",
            "Justification",
        ],
        [
            [
                r.file_id,
                r.path,
                r.owner,
                r.size_bytes,
                r.years,
                views.RETENTION_BASIS_LABELS.get(r.basis, r.basis),
                r.last_write_time,
                _date_cell(r.end_date),
                "oui" if r.expired else "non",
                r.justification,
            ]
            for r in report.retention.rows
        ],
        formats={4: "#,##0", 8: "DD/MM/YYYY"},
    )

    _add_sheet(
        workbook,
        "Nettoyage",
        ["file_id", "Chemin", "Propriétaire", "Taille (o)", "Dernier accès", "Sécurité"],
        [
            [r.file_id, r.path, r.owner, r.size_bytes, r.access_time, r.security]
            for r in report.cleanup.rows
        ],
        formats={4: "#,##0"},
    )

    reviews = report.reviews
    _add_sheet(
        workbook,
        "Revues",
        ["Indicateur", "Valeur"],
        [
            ["À vérifier", reviews.to_review],
            ["Validés", reviews.validated],
            ["Corrigés", reviews.corrected],
            ["Non revus", reviews.not_reviewed],
            ["Analysés", reviews.analyzed],
            ["Avancement (%)", reviews.percent_reviewed],
        ]
        + [
            [f"Écart — {d.path}", f"{d.llm_security}→{d.corrected_security or '—'}"]
            for d in reviews.discrepancies
        ],
    )

    _add_sheet(
        workbook,
        "Erreurs",
        ["Raison", "Fichiers", "Octets"],
        [[g.label, g.files, g.bytes] for g in report.status.reasons]
        + [
            [f"statut : {k}", v, report.status.bytes.get(k, 0)]
            for k, v in report.status.counts.items()
        ],
        formats={3: "#,##0"},
    )

    path.parent.mkdir(parents=True, exist_ok=True)
    workbook.save(str(path))
    return path


__all__ = ["SHEETS", "write_workbook"]
