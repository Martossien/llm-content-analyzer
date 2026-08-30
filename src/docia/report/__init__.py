"""Restitution : rapport HTML autonome, Markdown, classeur Excel, export Power BI.

Tous les rendus lisent les mêmes vues (`docia.views`) via `collect()` : une
seule source de vérité pour la CLI, la GUI et le rapport.
"""

from __future__ import annotations

from docia.report.data import ReportData, collect
from docia.report.excel import write_workbook
from docia.report.html import render_html
from docia.report.markdown import render_markdown
from docia.report.powerbi import POWERBI_COLUMNS, export_powerbi

__all__ = [
    "POWERBI_COLUMNS",
    "ReportData",
    "collect",
    "export_powerbi",
    "render_html",
    "render_markdown",
    "write_workbook",
]
