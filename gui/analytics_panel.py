from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import List, Dict

from content_analyzer.modules import (
    AgeAnalyzer,
    SizeAnalyzer,
    DuplicateDetector,
    FileInfo,
)


class AnalyticsPanel:
    """Simple analytics panel with tabs for age, size and cross analysis."""

    def __init__(self, parent: tk.Widget) -> None:
        self.parent = parent
        self.notebook = ttk.Notebook(parent)
        self.notebook.pack(fill="both", expand=True)

        self.age_analyzer = AgeAnalyzer()
        self.size_analyzer = SizeAnalyzer()
        self.duplicate_detector = DuplicateDetector()

        self.tabs: Dict[str, ttk.Frame] = {}
        self._build_tabs()

    def _build_tabs(self) -> None:
        self.tabs["age"] = self._create_age_tab()
        self.tabs["size"] = self._create_size_tab()
        self.tabs["cross"] = self._create_cross_tab()

    def _create_age_tab(self) -> ttk.Frame:
        frame = ttk.Frame(self.notebook)
        self.notebook.add(frame, text="Âge")
        self.age_label = ttk.Label(frame, text="No data")
        self.age_label.pack()
        return frame

    def _create_size_tab(self) -> ttk.Frame:
        frame = ttk.Frame(self.notebook)
        self.notebook.add(frame, text="Taille")
        self.size_label = ttk.Label(frame, text="No data")
        self.size_label.pack()
        return frame

    def _create_cross_tab(self) -> ttk.Frame:
        frame = ttk.Frame(self.notebook)
        self.notebook.add(frame, text="Croisée")
        self.cross_label = ttk.Label(frame, text="No data")
        self.cross_label.pack()
        return frame

    def refresh(self, files: List[FileInfo]) -> None:
        age_dist = self.age_analyzer.analyze_age_distribution(files)
        self.age_label.config(text=str(age_dist.get("distribution_by_year")))

        size_dist = self.size_analyzer.analyze_size_distribution(files)
        self.size_label.config(text=str(size_dist.get("distribution")))

        dup_fams = self.duplicate_detector.detect_duplicate_family(files)
        self.cross_label.config(text=f"{len(dup_fams)} duplicate families")
