from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Any, Dict, List

from content_analyzer.modules.age_analyzer import AgeAnalyzer
from content_analyzer.modules.size_analyzer import SizeAnalyzer
from content_analyzer.modules.duplicate_detector import DuplicateDetector, FileInfo


class AnalyticsPanel:
    """Panel d'analytics avec onglets et visualisations intégrées"""

    def __init__(self, parent_frame: tk.Widget) -> None:
        self.parent = parent_frame
        self.notebook = ttk.Notebook(parent_frame)
        self.age_analyzer = AgeAnalyzer()
        self.size_analyzer = SizeAnalyzer()
        self.duplicate_detector = DuplicateDetector()

        self.tabs = {
            'duplicates': self.create_duplicates_tab(),
            'age': self.create_age_analysis_tab(),
            'size': self.create_size_analysis_tab(),
            'cross': self.create_cross_analysis_tab(),
        }

        self.notebook.pack(fill='both', expand=True)

    # ------------------------------------------------------------------
    def create_duplicates_tab(self) -> ttk.Frame:
        frame = ttk.Frame(self.notebook)
        self.notebook.add(frame, text='🔁 Doublons')
        info = ttk.Label(frame, text='Fonctionnalité à venir')
        info.pack()
        return frame

    # ------------------------------------------------------------------
    def create_age_analysis_tab(self) -> ttk.Frame:
        frame = ttk.Frame(self.notebook)
        self.notebook.add(frame, text='📅 Âge')
        self.create_metrics_cards(frame, [
            ('Fichiers Anciens', 'age_stale_count', '🗓️'),
            ('Âge Moyen', 'age_average_days', '📊'),
            ('Archivage Possible', 'archival_space_mb', '📦'),
            ('Distribution', 'age_distribution_info', '📈'),
        ])
        chart_frame = ttk.LabelFrame(frame, text='Distribution par Année')
        chart_frame.pack(fill='both', expand=True, padx=5, pady=5)
        self.age_chart = self.create_matplotlib_chart(chart_frame, 'histogram')
        controls_frame = ttk.Frame(frame)
        controls_frame.pack(fill='x', padx=5, pady=5)
        ttk.Label(controls_frame, text='Seuil archivage:').pack(side='left')
        self.age_threshold_var = tk.StringVar(value='730')
        ttk.Entry(controls_frame, textvariable=self.age_threshold_var, width=10).pack(side='left', padx=5)
        ttk.Button(controls_frame, text='Actualiser', command=self.refresh_age_analysis).pack(side='left')
        return frame

    # ------------------------------------------------------------------
    def create_size_analysis_tab(self) -> ttk.Frame:
        frame = ttk.Frame(self.notebook)
        self.notebook.add(frame, text='💾 Taille')
        self.create_metrics_cards(frame, [
            ('Gros Fichiers', 'large_files_count', '🗂️'),
            ('Taille Moyenne', 'size_average_mb', '📏'),
            ('Optimisation', 'optimization_space_mb', '🎯'),
            ('Distribution', 'size_distribution_info', '📊'),
        ])
        chart_frame = ttk.LabelFrame(frame, text='Distribution par Tranches de Taille')
        chart_frame.pack(fill='both', expand=True, padx=5, pady=5)
        self.size_chart = self.create_matplotlib_chart(chart_frame, 'pie')
        return frame

    # ------------------------------------------------------------------
    def create_cross_analysis_tab(self) -> ttk.Frame:
        frame = ttk.Frame(self.notebook)
        self.notebook.add(frame, text='🔍 Analyse Croisée')
        recommendations_frame = ttk.LabelFrame(frame, text='Recommandations Intelligentes')
        recommendations_frame.pack(fill='x', padx=5, pady=5)
        self.recommendations_text = tk.Text(recommendations_frame, height=8, wrap='word')
        self.recommendations_text.pack(fill='both', expand=True, padx=5, pady=5)
        scatter_frame = ttk.LabelFrame(frame, text='Corrélation Âge vs Taille')
        scatter_frame.pack(fill='both', expand=True, padx=5, pady=5)
        self.cross_chart = self.create_matplotlib_chart(scatter_frame, 'scatter')
        return frame

    # ------------------------------------------------------------------
    def create_metrics_cards(self, parent: tk.Widget, cards_config: List[tuple]) -> ttk.Frame:
        frame = ttk.Frame(parent)
        frame.pack(fill='x', padx=5, pady=5)
        for i, (title, key, icon) in enumerate(cards_config):
            card = ttk.LabelFrame(frame, text=f'{icon} {title}', padding=10)
            card.grid(row=0, column=i, padx=5, pady=5, sticky='nsew')
            frame.columnconfigure(i, weight=1)
            value_label = ttk.Label(card, text='0', font=('Arial', 16, 'bold'))
            value_label.pack()
            setattr(self, f'card_{key}', value_label)
        return frame

    # ------------------------------------------------------------------
    def create_matplotlib_chart(self, parent: tk.Widget, chart_type: str):
        from matplotlib.figure import Figure
        from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

        fig = Figure(figsize=(5, 3), dpi=100)
        ax = fig.add_subplot(111)
        if chart_type == 'histogram':
            ax.set_title('Distribution par Année')
            ax.set_xlabel('Année')
            ax.set_ylabel('Nombre de Fichiers')
        elif chart_type == 'pie':
            ax.set_title('Répartition par Taille')
        elif chart_type == 'scatter':
            ax.set_title('Âge vs Taille')
            ax.set_xlabel('Âge (jours)')
            ax.set_ylabel('Taille (MB)')
        canvas = FigureCanvasTkAgg(fig, parent)
        canvas.get_tk_widget().pack(fill='both', expand=True)
        return canvas

    # ------------------------------------------------------------------
    def refresh_age_analysis(self) -> None:
        threshold_days = int(self.age_threshold_var.get())
        files = []
        distribution = self.age_analyzer.analyze_age_distribution(files)
        stale_files = self.age_analyzer.identify_stale_files(files, threshold_days)
        archival_stats = self.age_analyzer.calculate_archival_candidates(files, threshold_days)
        self.card_age_stale_count.config(text=f"{len(stale_files):,}")
        self.card_archival_space_mb.config(text=f"{archival_stats['total_size_mb']:.1f} MB")
        self._update_age_chart(distribution)

    # ------------------------------------------------------------------
    def _update_age_chart(self, distribution: Dict) -> None:
        ax = self.age_chart.figure.axes[0]
        ax.clear()
        years = list(distribution.get('distribution_by_year', {}).keys())
        percentages = list(distribution.get('distribution_by_year', {}).values())
        ax.bar(years, percentages, color='steelblue', alpha=0.7)
        ax.set_title('Distribution par Année')
        ax.set_xlabel('Année')
        ax.set_ylabel('Pourcentage (%)')
        self.age_chart.draw()

    # ------------------------------------------------------------------
    def generate_cross_analysis_recommendations(self, files: List[FileInfo]) -> str:
        recommendations: List[str] = []
        large_old_files = [f for f in files if f.file_size > 100 * 1024 * 1024]
        if large_old_files:
            total_space = sum(f.file_size for f in large_old_files) / (1024 * 1024)
            recommendations.append(
                f"🎯 PRIORITÉ 1: {len(large_old_files)} gros fichiers anciens ({total_space:.1f} MB) - Candidates archivage immédiat"
            )
        duplicate_families = self.duplicate_detector.detect_duplicate_family(files)
        if duplicate_families:
            recommendations.append(f"⚠️ PRIORITÉ 2: {len(duplicate_families)} familles de doublons")
        if not recommendations:
            return "✅ Aucune optimisation urgente détectée"
        return '\n\n'.join(recommendations)

