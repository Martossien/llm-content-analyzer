"""Helpers pour la création de graphiques matplotlib."""

from __future__ import annotations

import matplotlib

matplotlib.use("Agg")

from matplotlib.figure import Figure
from typing import Any, List


def create_simple_bar_chart(data: List[int], labels: List[str]) -> Figure:
    fig = Figure(figsize=(4, 3), dpi=100)
    ax = fig.add_subplot(111)
    ax.bar(labels, data, color="steelblue")
    return fig
