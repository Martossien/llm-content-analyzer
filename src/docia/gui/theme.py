"""Thème et fonctions pures d'affichage : couleurs par sévérité, chemins abrégés, formats.

Thème **clair** par défaut (lisible en réunion, imprimable), police 13. Les couleurs
de sévérité sont les mêmes que dans le rapport HTML, pour que l'utilisateur
reconnaisse un C3 d'un écran à l'autre.
"""

from __future__ import annotations

from pathlib import PurePath

FONT_FAMILY = "Segoe UI"
FONT_SIZE = 13
FONT_SIZE_SMALL = 11
FONT_SIZE_TITLE = 18
FONT_SIZE_KPI = 26

SEVERITY_COLORS: dict[str, str] = {
    "C3": "#b91c1c",
    "C2": "#c2410c",
    "C1": "#a16207",
    "C0": "#15803d",
    "N/A": "#6b7280",
    "critical": "#b91c1c",
    "high": "#c2410c",
    "medium": "#a16207",
    "low": "#0369a1",
    "none": "#15803d",
    "error": "#b91c1c",
    "done": "#15803d",
    "pending": "#2563eb",
    "queued": "#7c3aed",
    "excluded": "#6b7280",
}
"""Couleur de texte/badge d'une classe de sécurité, d'un niveau RGPD ou d'un statut."""

ACCENT = "#2563eb"
ACCENT_OK = "#15803d"
ACCENT_STOP = "#b91c1c"
ACCENT_ADMIN = "#0e7490"
CARD_BG_LIGHT = "#f3f4f6"
CARD_BG_DARK = "#1f2937"

SECURITY_LABELS: dict[str, str] = {
    "C0": "C0 public",
    "C1": "C1 interne",
    "C2": "C2 confidentiel",
    "C3": "C3 secret",
    "N/A": "non évalué",
}
RGPD_LABELS: dict[str, str] = {
    "none": "aucune donnée",
    "low": "faible",
    "medium": "moyen",
    "high": "élevé",
    "critical": "critique",
    "N/A": "non évalué",
}
STATUS_LABELS: dict[str, str] = {
    "pending": "à analyser",
    "queued": "en cours",
    "done": "analysé",
    "error": "en erreur",
    "excluded": "exclu",
}
REVIEW_LABELS: dict[str, str] = {
    "": "non vérifié",
    "to_review": "à vérifier",
    "validated": "validé",
    "corrected": "corrigé",
}


def severity_color(value: str | None) -> str:
    """Couleur associée à une classe / un niveau / un statut, gris si inconnu."""
    return SEVERITY_COLORS.get(value or "", "#374151")


def shorten_path(path: str, max_len: int = 60) -> str:
    """Abrège un chemin long en gardant le début (serveur/partage) et la fin (fichier).

    `\\\\srv\\part\\a\\b\\c\\d\\fichier.pdf` → `\\\\srv\\part\\…\\d\\fichier.pdf`.
    """
    if len(path) <= max_len:
        return path
    sep = "\\" if "\\" in path and "/" not in path else "/"
    parts = [p for p in path.replace("/", sep).split(sep) if p]
    if len(parts) < 4:
        return path[: max_len - 1] + "…"
    head = sep.join(parts[:2])
    tail = sep.join(parts[-2:])
    candidate = f"{head}{sep}…{sep}{tail}"
    if len(candidate) > max_len:
        tail = parts[-1]
        candidate = f"{head}{sep}…{sep}{tail}"
    if len(candidate) > max_len:
        candidate = "…" + candidate[-(max_len - 1) :]
    return candidate


def folder_of(path: str) -> str:
    """Dossier parent d'un chemin (Windows ou POSIX), sans le nom du fichier."""
    if "\\" in path and "/" not in path:
        return path.rsplit("\\", 1)[0] if "\\" in path else ""
    return str(PurePath(path).parent)


def format_bytes(value: int | float) -> str:
    """Taille lisible en français (Ko, Mo, Go…)."""
    size = float(value)
    for unit in ("o", "Ko", "Mo", "Go", "To"):
        if size < 1024 or unit == "To":
            return (
                f"{size:,.0f} {unit}".replace(",", " ")
                if unit == "o"
                else f"{size:,.1f} {unit}".replace(",", " ")
            )
        size /= 1024
    return f"{size:.1f} To"


def format_duration(seconds: float | None) -> str:
    """Durée lisible : `45 s`, `12 min`, `1 h 05`."""
    if seconds is None or seconds < 0:
        return "—"
    s = int(seconds)
    if s < 60:
        return f"{s} s"
    if s < 3600:
        return f"{s // 60} min"
    return f"{s // 3600} h {(s % 3600) // 60:02d}"


def format_int(value: int | float | None) -> str:
    if value is None:
        return "—"
    return f"{int(value):,}".replace(",", " ")
