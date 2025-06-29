from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Any, Dict, List

from content_analyzer.modules.age_analyzer import AgeAnalyzer
from content_analyzer.modules.size_analyzer import SizeAnalyzer
from content_analyzer.modules.duplicate_detector import DuplicateDetector, FileInfo
from content_analyzer.modules.db_manager import DBManager
from datetime import datetime


class AnalyticsPanel:
    """Panel d'analytics avec onglets et visualisations intégrées"""

    def __init__(self, parent_frame: tk.Widget, db_manager: DBManager | None = None) -> None:
        self.parent = parent_frame
        self.notebook = ttk.Notebook(parent_frame)
        self.age_analyzer = AgeAnalyzer()
        self.size_analyzer = SizeAnalyzer()
        self.duplicate_detector = DuplicateDetector()
        self.db_manager = db_manager

        self.tabs = {
            'duplicates': self.create_duplicates_tab(),
            'age': self.create_age_analysis_tab(),
            'size': self.create_size_analysis_tab(),
            'cross': self.create_cross_analysis_tab(),
        }

        self.notebook.pack(fill='both', expand=True)

    def set_db_manager(self, db_manager: DBManager | None) -> None:
        """Attach a DB manager after initialization."""
        self.db_manager = db_manager

    def _get_all_files(self) -> List[FileInfo]:
        if self.db_manager is None:
            return []
        try:
            return self.db_manager.get_all_files_basic()
        except Exception:
            return []

    # ------------------------------------------------------------------
    def create_duplicates_tab(self) -> ttk.Frame:
        frame = ttk.Frame(self.notebook)
        self.notebook.add(frame, text='🔁 Doublons')
        self.dup_info_label = ttk.Label(frame, text='No data')
        self.dup_info_label.pack(pady=5)
        ttk.Button(frame, text='Analyser', command=self.refresh_duplicates).pack(pady=5)
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
        files = self._get_all_files()
        threshold_days = int(self.age_threshold_var.get())
        distribution = self.age_analyzer.analyze_age_distribution(files)
        stale_files = self.age_analyzer.identify_stale_files(files, threshold_days)
        archival_stats = self.age_analyzer.calculate_archival_candidates(files, threshold_days)
        stats = self.age_analyzer.get_age_statistics(files)
        self.card_age_stale_count.config(text=f"{len(stale_files):,}")
        self.card_age_average_days.config(text=f"{stats['average_days']:.1f}")
        self.card_archival_space_mb.config(text=f"{archival_stats['total_size_mb']:.1f} MB")
        self.card_age_distribution_info.config(text=f"{distribution['total_files']:,} fichiers")
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
    def refresh_size_analysis(self) -> None:
        files = self._get_all_files()
        distribution = self.size_analyzer.analyze_size_distribution(files)
        large_files = self.size_analyzer.identify_large_files(files, 100)
        optimization = self.size_analyzer.calculate_space_optimization(files, 100)
        stats = self.size_analyzer.get_size_statistics(files)
        self.card_large_files_count.config(text=f"{len(large_files):,}")
        self.card_size_average_mb.config(text=f"{stats['average_mb']:.1f} MB")
        self.card_optimization_space_mb.config(text=f"{optimization['size_mb']:.1f} MB")
        self.card_size_distribution_info.config(text=f"{distribution['total_files']:,} fichiers")
        self._update_size_chart(distribution['distribution'])

    def _update_size_chart(self, distribution: Dict[str, float]) -> None:
        ax = self.size_chart.figure.axes[0]
        ax.clear()
        labels = list(distribution.keys())
        sizes = list(distribution.values())
        if sizes:
            ax.pie(sizes, labels=labels, autopct='%1.1f%%')
        ax.set_title('Répartition par Taille')
        self.size_chart.draw()

    # ------------------------------------------------------------------
    def refresh_cross_analysis(self) -> None:
        files = self._get_all_files()
        data = []
        now = datetime.now()
        for f in files:
            dt = self.age_analyzer._get_reliable_time(f)
            if dt == datetime.max:
                continue
            age_days = (now - dt).days
            size_mb = f.file_size / (1024 * 1024)
            data.append((age_days, size_mb))
        self._update_cross_chart(data)
        recommendations = self.generate_cross_analysis_recommendations(files)
        self.recommendations_text.delete('1.0', tk.END)
        self.recommendations_text.insert('1.0', recommendations)

    def _update_cross_chart(self, data: List[tuple]) -> None:
        ax = self.cross_chart.figure.axes[0]
        ax.clear()
        if data:
            ages, sizes = zip(*data)
            ax.scatter(ages, sizes, alpha=0.6)
        ax.set_title('Âge vs Taille')
        ax.set_xlabel('Âge (jours)')
        ax.set_ylabel('Taille (MB)')
        self.cross_chart.draw()

    # ------------------------------------------------------------------
    def refresh_duplicates(self) -> None:
        files = self._get_all_files()
        families = self.duplicate_detector.detect_duplicate_family(files)
        stats = self.duplicate_detector.get_duplicate_statistics(families)
        text = (
            f"Familles: {stats['total_families']} | Copies: {stats['total_copies']} | "
            f"Espace gaspillé: {stats['space_wasted_mb']:.1f} MB"
        )
        self.dup_info_label.config(text=text)

    def refresh_all(self) -> None:
        self.refresh_age_analysis()
        self.refresh_size_analysis()
        self.refresh_cross_analysis()
        self.refresh_duplicates()

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

