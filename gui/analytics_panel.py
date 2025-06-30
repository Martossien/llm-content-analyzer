from __future__ import annotations

import json
import logging
import time
import tkinter as tk
from datetime import datetime, timedelta
from pathlib import Path
from tkinter import messagebox, ttk
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

from content_analyzer.modules.age_analyzer import AgeAnalyzer
from content_analyzer.modules.size_analyzer import SizeAnalyzer
from content_analyzer.modules.duplicate_detector import DuplicateDetector, FileInfo
from content_analyzer.modules.db_manager import DBManager


class AnalyticsPanel:
    """Dashboard de supervision business."""

    def __init__(self, parent_frame: tk.Widget, db_manager: DBManager | None = None) -> None:
        self.parent = parent_frame
        self.db_manager = db_manager
        self.age_analyzer = AgeAnalyzer()
        self.size_analyzer = SizeAnalyzer()
        self.duplicate_detector = DuplicateDetector()

        self.threshold_age_years = tk.StringVar(value="2")
        self.threshold_size_mb = tk.StringVar(value="100")
        self.classification_filter = tk.StringVar(value="Tous")
        self.use_last_modified = tk.BooleanVar(value=False)
        self.years_modified = tk.StringVar(value="1")

        # caching for performance
        self._metrics_cache: Dict[str, Any] = {}
        self._cache_timestamp = 0.0
        self.CACHE_DURATION = 30

        self._build_ui()
        self.tabs: Dict[str, ttk.Frame] = {"age": self.security_tab}
        self.update_alert_cards()
        self.update_thematic_tabs()

    def set_db_manager(self, db_manager: DBManager | None) -> None:
        self.db_manager = db_manager

    def _build_ui(self) -> None:
        params_frame = ttk.LabelFrame(self.parent, text="⚙️ PARAMÈTRES UTILISATEUR")
        params_frame.pack(fill="x", padx=5, pady=5)

        ttk.Label(params_frame, text="Âge fichiers (années):").grid(row=0, column=0, padx=5, pady=2, sticky="w")
        ttk.Entry(params_frame, textvariable=self.threshold_age_years, width=5).grid(row=0, column=1, padx=5, pady=2)

        ttk.Label(params_frame, text="Taille fichiers (MB):").grid(row=0, column=2, padx=5, pady=2, sticky="w")
        ttk.Entry(params_frame, textvariable=self.threshold_size_mb, width=6).grid(row=0, column=3, padx=5, pady=2)

        ttk.Label(params_frame, text="Filtres:").grid(row=0, column=4, padx=5, pady=2, sticky="w")
        class_cb = ttk.Combobox(params_frame, textvariable=self.classification_filter, values=["Tous", "C0+", "C1+", "C2+", "C3"], width=5, state="readonly")
        class_cb.grid(row=0, column=5, padx=5, pady=2)

        chk = ttk.Checkbutton(params_frame, text="Modifier depuis", variable=self.use_last_modified)
        chk.grid(row=0, column=6, padx=5, pady=2, sticky="w")
        ttk.Entry(params_frame, textvariable=self.years_modified, width=4).grid(row=0, column=7, padx=5, pady=2)

        ttk.Button(params_frame, text="🔄 Recalculer", command=self.recalculate_all_metrics).grid(row=0, column=8, padx=5)
        ttk.Button(params_frame, text="💾 Sauver", command=self.save_user_preferences).grid(row=0, column=9, padx=5)
        ttk.Button(params_frame, text="📥 Restaurer", command=self.load_user_preferences).grid(row=0, column=10, padx=5)

        alerts_frame = ttk.LabelFrame(self.parent, text="📊 SUPERVISION BUSINESS - MÉTRIQUES CLÉS")
        alerts_frame.pack(fill="x", padx=5, pady=5)
        cards_container = ttk.Frame(alerts_frame)
        cards_container.pack(fill="x", padx=5, pady=5)

        self.super_critical_card = ttk.LabelFrame(cards_container, text="🔴 SUPER CRITIQUES")
        self.super_critical_card.grid(row=0, column=0, padx=5, pady=5, sticky="nsew")
        self.super_critical_line1 = ttk.Label(self.super_critical_card, text="0 C3+RGPD+Legal", font=("Arial", 12, "bold"))
        self.super_critical_line1.pack()
        self.super_critical_line2 = ttk.Label(self.super_critical_card, text="0% | 0 fichiers | 0GB", font=("Arial", 10))
        self.super_critical_line2.pack()
        self.super_critical_line3 = ttk.Label(self.super_critical_card, text="Cumul risques max", font=("Arial", 10))
        self.super_critical_line3.pack()

        self.critical_card = ttk.LabelFrame(cards_container, text="🟠 CRITIQUES")
        self.critical_card.grid(row=0, column=1, padx=5, pady=5, sticky="nsew")
        self.critical_line1 = ttk.Label(self.critical_card, text="0 C3 OU RGPD OU Legal", font=("Arial", 12, "bold"))
        self.critical_line1.pack()
        self.critical_line2 = ttk.Label(self.critical_card, text="0% | 0 fichiers | 0GB", font=("Arial", 10))
        self.critical_line2.pack()
        self.critical_line3 = ttk.Label(self.critical_card, text="Un critère fort", font=("Arial", 10))
        self.critical_line3.pack()

        self.duplicates_card = ttk.LabelFrame(cards_container, text="🟡 DOUBLONS")
        self.duplicates_card.grid(row=0, column=2, padx=5, pady=5, sticky="nsew")
        self.duplicates_line1 = ttk.Label(self.duplicates_card, text="0 fichiers dupliqués 2 fois", font=("Arial", 12, "bold"))
        self.duplicates_line1.pack()
        self.duplicates_line2 = ttk.Label(self.duplicates_card, text="0% | 0 groupes | 0GB gaspillé", font=("Arial", 10))
        self.duplicates_line2.pack()
        self.duplicates_line3 = ttk.Label(self.duplicates_card, text="Top: 0 copies max", font=("Arial", 10))
        self.duplicates_line3.pack()

        self.size_age_card = ttk.LabelFrame(cards_container, text="🔵 TAILLE/ÂGE")
        self.size_age_card.grid(row=0, column=3, padx=5, pady=5, sticky="nsew")
        self.size_age_line1 = ttk.Label(self.size_age_card, text="0% gros + 0% dormants", font=("Arial", 12, "bold"))
        self.size_age_line1.pack()
        self.size_age_line2 = ttk.Label(self.size_age_card, text="0 fichiers | 0GB archivage", font=("Arial", 10))
        self.size_age_line2.pack()
        self.size_age_line3 = ttk.Label(self.size_age_card, text="Seuils utilisateur", font=("Arial", 10))
        self.size_age_line3.pack()

        for i in range(4):
            cards_container.columnconfigure(i, weight=1)

        notebook_frame = ttk.LabelFrame(self.parent, text="🔍 ANALYSE DÉTAILLÉE PAR DOMAINE")
        notebook_frame.pack(fill="both", expand=True, padx=5, pady=5)

        self.thematic_notebook = ttk.Notebook(notebook_frame)
        self.thematic_notebook.pack(fill="both", expand=True, padx=5, pady=5)

        security_frame = ttk.Frame(self.thematic_notebook)
        self.thematic_notebook.add(security_frame, text="🛡️ Security")
        self._build_security_tab(security_frame)

        rgpd_frame = ttk.Frame(self.thematic_notebook)
        self.thematic_notebook.add(rgpd_frame, text="🔒 RGPD")
        self._build_rgpd_tab(rgpd_frame)

        finance_frame = ttk.Frame(self.thematic_notebook)
        self.thematic_notebook.add(finance_frame, text="💰 Finance")
        self._build_finance_tab(finance_frame)

        legal_frame = ttk.Frame(self.thematic_notebook)
        self.thematic_notebook.add(legal_frame, text="⚖️ Legal")
        self._build_legal_tab(legal_frame)

        actions_frame = ttk.Frame(self.parent)
        actions_frame.pack(fill="x", padx=5, pady=5)

        self.progress_frame = ttk.Frame(actions_frame)
        self.progress_frame.pack(side="left", padx=5)
        self.progress_label = ttk.Label(self.progress_frame, text="✅ Prêt")
        self.progress_label.pack()

        ttk.Button(actions_frame, text="📄 Export Rapport Business", command=self.export_business_report).pack(side="left", padx=5)
        ttk.Button(actions_frame, text="👥 Voir Fichiers Concernés", command=self.show_affected_files).pack(side="left", padx=5)
        ttk.Button(actions_frame, text="📥 Restaurer Préférences", command=self.load_user_preferences).pack(side="right", padx=5)

    def _connect_files(self) -> List[FileInfo]:
        if self.db_manager is None:
            return []
        try:
            return self.db_manager.get_all_files_basic()
        except Exception:
            return []

    def _filter_files_by_classification(self, files: List[FileInfo], level: str) -> List[FileInfo]:
        class_map = self._get_classification_map()
        mapping = {
            "C0+": {"C0", "C1", "C2", "C3"},
            "C1+": {"C1", "C2", "C3"},
            "C2+": {"C2", "C3"},
            "C3": {"C3"},
        }
        allowed = mapping.get(level, set())
        return [f for f in files if class_map.get(f.id) in allowed]

    def _count_files_duplicated_n_times(self, families: Dict[str, List[FileInfo]], copies: int) -> int:
        return sum(len(fam) for fam in families.values() if len(fam) == copies)

    def _parse_time(self, value: str | None) -> datetime:
        if not value:
            return datetime.max
        for fmt in ("%d/%m/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(value.strip(), fmt)
            except ValueError:
                continue
        return datetime.max

    def _get_old_files_creation(self, files: List[FileInfo], threshold_days: int) -> List[FileInfo]:
        cutoff = datetime.now() - timedelta(days=threshold_days)
        result: List[FileInfo] = []
        for f in files:
            dt = self._parse_time(f.creation_time)
            if dt != datetime.max and dt <= cutoff:
                result.append(f)
        return result

    def _query_distribution(self, column: str) -> Dict[str, Dict[str, Any]]:
        result: Dict[str, Dict[str, Any]] = {}
        if self.db_manager is None:
            return result
        try:
            with self.db_manager._connect() as conn:
                cur = conn.cursor()
                cur.execute(
                    f"SELECT COALESCE(r.{column}, 'none'), COUNT(*), SUM(f.file_size)"
                    " FROM fichiers f LEFT JOIN reponses_llm r ON f.id = r.fichier_id"
                    f" GROUP BY r.{column}"
                )
                for name, count, size in cur.fetchall():
                    result[str(name)] = {"count": count or 0, "size": size or 0}
        except Exception:
            pass
        return result

    def _get_classification_distribution_optimized(self) -> List[tuple]:
        if self.db_manager is None:
            return []
        with self.db_manager._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT
                    COALESCE(r.security_classification_cached, 'none') as security,
                    COALESCE(r.rgpd_risk_cached, 'none') as rgpd,
                    COALESCE(r.finance_type_cached, 'none') as finance,
                    COALESCE(r.legal_type_cached, 'none') as legal,
                    COUNT(*) as count,
                    SUM(f.file_size) as total_size
                FROM fichiers f
                LEFT JOIN reponses_llm r ON f.id = r.fichier_id
                WHERE f.status = 'completed'
                GROUP BY r.security_classification_cached, r.rgpd_risk_cached,
                         r.finance_type_cached, r.legal_type_cached
                ORDER BY count DESC
                """
            )
            return cursor.fetchall()

    def _get_super_critical_files_optimized(self) -> List[int]:
        if self.db_manager is None:
            return []
        with self.db_manager._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT f.id
                FROM fichiers f
                JOIN reponses_llm r ON f.id = r.fichier_id
                WHERE r.security_classification_cached = 'C3'
                  AND r.rgpd_risk_cached = 'critical'
                  AND r.legal_type_cached IN ('nda', 'litigation')
                  AND f.status = 'completed'
                """
            )
            return [row[0] for row in cursor.fetchall()]

    def _get_classification_map(self) -> Dict[int, str]:
        mapping: Dict[int, str] = {}
        if self.db_manager is None:
            return mapping
        try:
            with self.db_manager._connect() as conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT f.id, r.security_classification_cached FROM fichiers f LEFT JOIN reponses_llm r ON f.id = r.fichier_id"
                )
                for fid, cls in cur.fetchall():
                    mapping[int(fid)] = cls or ""
        except Exception:
            pass
        return mapping

    def _get_rgpd_map(self) -> Dict[int, str]:
        mapping: Dict[int, str] = {}
        if self.db_manager is None:
            return mapping
        try:
            with self.db_manager._connect() as conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT f.id, r.rgpd_risk_cached FROM fichiers f LEFT JOIN reponses_llm r ON f.id = r.fichier_id"
                )
                for fid, lvl in cur.fetchall():
                    mapping[int(fid)] = lvl or "none"
        except Exception:
            pass
        return mapping

    def _get_legal_map(self) -> Dict[int, str]:
        mapping: Dict[int, str] = {}
        if self.db_manager is None:
            return mapping
        try:
            with self.db_manager._connect() as conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT f.id, r.legal_type_cached FROM fichiers f LEFT JOIN reponses_llm r ON f.id = r.fichier_id"
                )
                for fid, typ in cur.fetchall():
                    mapping[int(fid)] = typ or "none"
        except Exception:
            pass
        return mapping

    # ------------------------------------------------------------------
    # Metrics caching helpers
    # ------------------------------------------------------------------

    def _invalidate_cache(self) -> None:
        self._metrics_cache.clear()
        self._cache_timestamp = 0.0

    def _save_metrics_to_disk(self, metrics: Dict[str, Any]) -> None:
        try:
            cache_file = Path("analytics_cache.json")
            cache_data = {
                "timestamp": time.time(),
                "metrics": metrics,
                "parameters": {
                    "age_years": self.threshold_age_years.get(),
                    "size_mb": self.threshold_size_mb.get(),
                    "filter": self.classification_filter.get(),
                },
            }
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump(cache_data, f, indent=2)
        except Exception as exc:  # pragma: no cover - disk issues
            logger.warning("Impossible de sauvegarder le cache: %s", exc)

    def _load_metrics_from_disk(self) -> Dict[str, Any] | None:
        try:
            cache_file = Path("analytics_cache.json")
            if not cache_file.exists():
                return None
            with open(cache_file, "r", encoding="utf-8") as f:
                cache_data = json.load(f)
            if time.time() - cache_data.get("timestamp", 0) > 300:
                return None
            return cache_data.get("metrics")
        except Exception:  # pragma: no cover - disk issues
            return None

    def _calculate_metrics_core(self) -> Dict[str, Any]:
        if self.db_manager is None:
            return {}
        files = self._connect_files()
        if not files:
            return {}
        age_threshold_days = int(self.threshold_age_years.get()) * 365
        size_threshold_mb = int(self.threshold_size_mb.get())
        classification_filter = self.classification_filter.get()
        if classification_filter != "Tous":
            files = self._filter_files_by_classification(files, classification_filter)
        age_stats = self.age_analyzer.calculate_archival_candidates(files, age_threshold_days)
        size_stats = self.size_analyzer.calculate_space_optimization(files, size_threshold_mb)
        dup_families = self.duplicate_detector.detect_duplicate_family(files)
        dup_stats = self.duplicate_detector.get_duplicate_statistics(dup_families)
        class_map = self._get_classification_map()
        rgpd_map = self._get_rgpd_map()
        legal_map = self._get_legal_map()
        super_critical_files = [
            f for f in files if (class_map.get(f.id) == "C3" and rgpd_map.get(f.id) == "critical" and legal_map.get(f.id) in ["nda", "litigation"])
        ]
        critical_files = [
            f for f in files if (class_map.get(f.id) == "C3" or rgpd_map.get(f.id) == "critical" or legal_map.get(f.id) in ["nda", "litigation"]) and f not in super_critical_files
        ]
        duplicates_2x = self._count_files_duplicated_n_times(dup_families, 2)
        duplicates_3x = self._count_files_duplicated_n_times(dup_families, 3)
        duplicates_4x = self._count_files_duplicated_n_times(dup_families, 4)
        max_duplicates = max((len(fam) for fam in dup_families.values()), default=0)
        large_files = self.size_analyzer.identify_large_files(files, size_threshold_mb)
        old_files = self._get_old_files_creation(files, age_threshold_days)
        dormant_files = self.age_analyzer.identify_stale_files(files, age_threshold_days)
        total_files = len(files)
        total_size = sum(f.file_size for f in files)
        large_file_ids = {f.id for f in large_files}
        dormant_file_ids = {f.id for f in dormant_files}
        total_affected_count = len(large_file_ids.union(dormant_file_ids))

        metrics = {
            'super_critical': {
                'count': len(super_critical_files),
                'percentage': round(len(super_critical_files) / total_files * 100, 1) if total_files else 0,
                'size_gb': sum(f.file_size for f in super_critical_files) / (1024**3),
            },
            'critical': {
                'count': len(critical_files),
                'percentage': round(len(critical_files) / total_files * 100, 1) if total_files else 0,
                'size_gb': sum(f.file_size for f in critical_files) / (1024**3),
            },
            'duplicates': {
                'files_2x': duplicates_2x,
                'files_3x': duplicates_3x,
                'files_4x': duplicates_4x,
                'max_copies': max_duplicates,
                'total_groups': len(dup_families),
                'wasted_space_gb': dup_stats.get('space_wasted_bytes', 0) / (1024**3),
                'percentage': round(dup_stats.get('total_duplicates', 0) / total_files * 100, 1) if total_files else 0,
            },
            'size_age': {
                'large_files_pct': round(len(large_files) / total_files * 100, 1) if total_files else 0,
                'old_files_pct': round(len(old_files) / total_files * 100, 1) if total_files else 0,
                'dormant_files_pct': round(len(dormant_files) / total_files * 100, 1) if total_files else 0,
                'archival_size_gb': age_stats.get('total_size_bytes', 0) / (1024**3),
                'total_affected': total_affected_count,
            },
            'global': {
                'total_files': total_files,
                'total_size_gb': total_size / (1024**3),
            }
        }
        return metrics

    def calculate_business_metrics(self) -> Dict[str, Any]:
        cache_key = f"{self.threshold_age_years.get()}_{self.threshold_size_mb.get()}_{self.classification_filter.get()}"
        current_time = time.time()

        if not self._metrics_cache:
            disk_metrics = self._load_metrics_from_disk()
            if disk_metrics:
                self._metrics_cache[cache_key] = disk_metrics
                self._cache_timestamp = current_time

        if (
            cache_key in self._metrics_cache
            and current_time - self._cache_timestamp < self.CACHE_DURATION
        ):
            return self._metrics_cache[cache_key]

        metrics = self._calculate_metrics_core()
        self._metrics_cache = {cache_key: metrics}
        self._cache_timestamp = current_time
        self._save_metrics_to_disk(metrics)
        return metrics

    def update_alert_cards(self) -> None:
        self.progress_label.config(text="⏳ Calcul en cours...")
        self.parent.update_idletasks()
        metrics = self.calculate_business_metrics()
        if not metrics:
            self.progress_label.config(text="❌ Erreur calcul")
            return
        super_crit = metrics.get('super_critical', {})
        count = super_crit.get('count', 0)
        pct = super_crit.get('percentage', 0)
        size_gb = super_crit.get('size_gb', 0)
        self.super_critical_line1.config(text=f"{count} C3+RGPD+Legal")
        self.super_critical_line2.config(text=f"{pct}% | {count} fichiers | {size_gb:.1f}GB")
        self.super_critical_line3.config(text="Cumul risques max")
        self.super_critical_line1.config(foreground="darkred" if count > 0 else "green")
        crit = metrics.get('critical', {})
        count = crit.get('count', 0)
        pct = crit.get('percentage', 0)
        size_gb = crit.get('size_gb', 0)
        self.critical_line1.config(text=f"{count} C3 OU RGPD OU Legal")
        self.critical_line2.config(text=f"{pct}% | {count} fichiers | {size_gb:.1f}GB")
        self.critical_line3.config(text="Un critère fort")
        self.critical_line1.config(foreground="darkorange" if count > 0 else "green")
        dup = metrics.get('duplicates', {})
        files_2x = dup.get('files_2x', 0)
        groups = dup.get('total_groups', 0)
        wasted_gb = dup.get('wasted_space_gb', 0)
        pct = dup.get('percentage', 0)
        max_copies = dup.get('max_copies', 0)
        self.duplicates_line1.config(text=f"{files_2x} fichiers dupliqués 2 fois")
        self.duplicates_line2.config(text=f"{pct}% | {groups} groupes | {wasted_gb:.1f}GB gaspillé")
        self.duplicates_line3.config(text=f"Top: {max_copies} copies max")
        self.duplicates_line1.config(foreground="orange" if wasted_gb > 0.5 else "green")
        size_age = metrics.get('size_age', {})
        large_pct = size_age.get('large_files_pct', 0)
        dormant_pct = size_age.get('dormant_files_pct', 0)
        affected = size_age.get('total_affected', 0)
        archival_gb = size_age.get('archival_size_gb', 0)
        self.size_age_line1.config(text=f"{large_pct}% gros + {dormant_pct}% dormants")
        self.size_age_line2.config(text=f"{affected} fichiers | {archival_gb:.1f}GB archivage")
        self.size_age_line3.config(text="Seuils utilisateur")
        self.size_age_line1.config(foreground="blue" if affected > 0 else "green")
        try:
            self.update_thematic_tabs()
        except Exception as e:  # pragma: no cover - UI issues
            logger.error("Erreur mise à jour onglets: %s", e)
        self.progress_label.config(text="✅ Métriques à jour")

    def _build_security_tab(self, parent_frame: ttk.Frame) -> None:
        title_label = ttk.Label(parent_frame, text="🛡️ ANALYSE SÉCURITÉ", font=("Arial", 14, 'bold'))
        title_label.pack(pady=10)
        help_label = ttk.Label(parent_frame, text="Répartition des fichiers par niveau de classification sécurité")
        help_label.pack(pady=5)
        main_container = ttk.Frame(parent_frame)
        main_container.pack(fill="both", expand=True, padx=10, pady=10)
        left_frame = ttk.LabelFrame(main_container, text="RÉPARTITION SÉCURITÉ")
        left_frame.pack(side="left", fill="both", expand=True, padx=5)
        self.security_labels = {}
        for level in ["C0", "C1", "C2", "C3", "Autres"]:
            label = ttk.Label(left_frame, text=f"{level}: 0% | 0 fichiers | 0GB", font=("Arial", 12))
            label.pack(anchor="w", pady=3, padx=10)
            self.security_labels[level] = label
        right_frame = ttk.LabelFrame(main_container, text="FOCUS CRITIQUE")
        right_frame.pack(side="right", fill="both", expand=True, padx=5)
        self.security_focus_labels = {}
        for item in ["C3 Total", "C3 + RGPD", "C3 + Legal", "Recommandations"]:
            label = ttk.Label(right_frame, text=f"{item}: --", font=("Arial", 11))
            label.pack(anchor="w", pady=3, padx=10)
            self.security_focus_labels[item] = label

    def _build_rgpd_tab(self, parent_frame: ttk.Frame) -> None:
        title_label = ttk.Label(parent_frame, text="🔒 ANALYSE RGPD", font=("Arial", 14, 'bold'))
        title_label.pack(pady=10)
        help_label = ttk.Label(parent_frame, text="Répartition des fichiers par niveau de risque RGPD")
        help_label.pack(pady=5)
        container = ttk.LabelFrame(parent_frame, text="NIVEAUX RGPD")
        container.pack(fill="both", expand=True, padx=10, pady=10)
        self.rgpd_labels = {}
        for level in ["none", "low", "medium", "high", "critical", "Autres"]:
            label = ttk.Label(container, text=f"{level}: 0% | 0 fichiers | 0GB", font=("Arial", 12))
            label.pack(anchor="w", pady=3, padx=10)
            self.rgpd_labels[level] = label

    def _build_finance_tab(self, parent_frame: ttk.Frame) -> None:
        title_label = ttk.Label(parent_frame, text="💰 ANALYSE FINANCE", font=("Arial", 14, 'bold'))
        title_label.pack(pady=10)
        help_label = ttk.Label(parent_frame, text="Répartition des documents par type financier")
        help_label.pack(pady=5)
        container = ttk.LabelFrame(parent_frame, text="TYPES FINANCIERS")
        container.pack(fill="both", expand=True, padx=10, pady=10)
        self.finance_labels = {}
        for doc_type in ["none", "invoice", "contract", "budget", "accounting", "payment", "Autres"]:
            label = ttk.Label(container, text=f"{doc_type}: 0% | 0 fichiers | 0GB", font=("Arial", 12))
            label.pack(anchor="w", pady=3, padx=10)
            self.finance_labels[doc_type] = label

    def _build_legal_tab(self, parent_frame: ttk.Frame) -> None:
        title_label = ttk.Label(parent_frame, text="⚖️ ANALYSE LEGAL", font=("Arial", 14, 'bold'))
        title_label.pack(pady=10)
        help_label = ttk.Label(parent_frame, text="Répartition des documents par type légal")
        help_label.pack(pady=5)
        container = ttk.LabelFrame(parent_frame, text="TYPES LÉGAUX")
        container.pack(fill="both", expand=True, padx=10, pady=10)
        self.legal_labels = {}
        for doc_type in ["none", "employment", "lease", "sale", "nda", "compliance", "litigation", "Autres"]:
            label = ttk.Label(container, text=f"{doc_type}: 0% | 0 fichiers | 0GB", font=("Arial", 12))
            label.pack(anchor="w", pady=3, padx=10)
            self.legal_labels[doc_type] = label

    def recalculate_all_metrics(self) -> None:
        try:
            age_years = int(self.threshold_age_years.get())
            size_mb = int(self.threshold_size_mb.get())
            if age_years < 0 or age_years > 99:
                messagebox.showerror("Erreur", "Âge doit être entre 0 et 99 ans", parent=self.parent)
                return
            if size_mb < 0 or size_mb > 999999:
                messagebox.showerror("Erreur", "Taille doit être entre 0 et 999999 MB", parent=self.parent)
                return
            self.progress_label.config(text="⏳ Recalcul en cours...")
            self.parent.update_idletasks()
            self._invalidate_cache()
            self.update_alert_cards()
            self.progress_label.config(text="✅ Terminé")
            success_window = tk.Toplevel(self.parent)
            success_window.title("Succès")
            success_window.geometry("300x100")
            success_window.transient(self.parent)
            success_window.lift()
            success_window.focus_set()
            success_window.grab_set()
            ttk.Label(success_window, text="✅ Métriques recalculées avec succès!", font=("Arial", 12)).pack(pady=20)
            ttk.Button(success_window, text="OK", command=success_window.destroy).pack()
        except ValueError:
            messagebox.showerror("Erreur", "Paramètres invalides", parent=self.parent)
            self.progress_label.config(text="❌ Erreur")
        except Exception as e:
            self._handle_analytics_error("recalcul métriques", e)

    def save_user_preferences(self) -> None:
        prefs = {
            "age_years": self.threshold_age_years.get(),
            "size_mb": self.threshold_size_mb.get(),
            "classification_filter": self.classification_filter.get(),
            "years_modified": self.years_modified.get(),
            "saved_timestamp": datetime.now().isoformat(),
            "version": "2.0",
        }
        try:
            with open("user_prefs.json", "w", encoding="utf-8") as f:
                json.dump(prefs, f, indent=2, ensure_ascii=False)
            success_window = tk.Toplevel(self.parent)
            success_window.title("Préférences sauvegardées")
            success_window.geometry("350x120")
            success_window.transient(self.parent)
            success_window.lift()
            success_window.focus_set()
            success_window.grab_set()
            ttk.Label(success_window, text="💾 Préférences sauvegardées", font=("Arial", 12, "bold")).pack(pady=10)
            ttk.Label(success_window, text="Fichier: user_prefs.json").pack()
            ttk.Button(success_window, text="OK", command=success_window.destroy).pack(pady=10)
        except Exception as exc:
            messagebox.showerror("Erreur Sauvegarde", f"Échec: {str(exc)}", parent=self.parent)

    def load_user_preferences(self) -> None:
        try:
            if not Path("user_prefs.json").exists():
                messagebox.showinfo("Info", "Aucun fichier de préférences trouvé", parent=self.parent)
                return
            with open("user_prefs.json", "r", encoding="utf-8") as f:
                prefs = json.load(f)
            self.threshold_age_years.set(prefs.get("age_years", "2"))
            self.threshold_size_mb.set(prefs.get("size_mb", "100"))
            self.classification_filter.set(prefs.get("classification_filter", "Tous"))
            self.years_modified.set(prefs.get("years_modified", "1"))
            self.recalculate_all_metrics()
            messagebox.showinfo("Succès", "Préférences restaurées et métriques recalculées!", parent=self.parent)
        except Exception as exc:
            messagebox.showerror("Erreur Restauration", f"Échec: {str(exc)}", parent=self.parent)

    def show_affected_files(self) -> None:
        try:
            metrics = self.calculate_business_metrics()
            results_window = tk.Toplevel(self.parent)
            results_window.title("👥 Fichiers Concernés par les Alertes")
            results_window.geometry("800x600")
            results_window.transient(self.parent)
            results_window.lift()
            results_window.focus_set()
            notebook = ttk.Notebook(results_window)
            notebook.pack(fill="both", expand=True, padx=10, pady=10)
            if metrics.get('super_critical', {}).get('count', 0) > 0:
                super_frame = ttk.Frame(notebook)
                notebook.add(super_frame, text=f"🔴 Super Critiques ({metrics['super_critical']['count']})")
                self._populate_files_list(super_frame, 'super_critical')
            if metrics.get('critical', {}).get('count', 0) > 0:
                crit_frame = ttk.Frame(notebook)
                notebook.add(crit_frame, text=f"🟠 Critiques ({metrics['critical']['count']})")
                self._populate_files_list(crit_frame, 'critical')
            if metrics.get('duplicates', {}).get('total_groups', 0) > 0:
                dup_frame = ttk.Frame(notebook)
                notebook.add(dup_frame, text=f"🟡 Doublons ({metrics['duplicates']['total_groups']} groupes)")
                self._populate_files_list(dup_frame, 'duplicates')
            ttk.Button(results_window, text="Fermer", command=results_window.destroy).pack(pady=5)
        except Exception as exc:
            self._handle_analytics_error("affichage des fichiers", exc)

    def _populate_files_list(self, frame: ttk.Frame, category: str) -> None:
        files = self._connect_files()
        class_map = self._get_classification_map()
        rgpd_map = self._get_rgpd_map()
        legal_map = self._get_legal_map()
        dup_families = self.duplicate_detector.detect_duplicate_family(files)
        items: List[FileInfo] = []
        if category == 'super_critical':
            for f in files:
                if class_map.get(f.id) == 'C3' and rgpd_map.get(f.id) == 'critical' and legal_map.get(f.id) in ['nda', 'litigation']:
                    items.append(f)
        elif category == 'critical':
            for f in files:
                if (class_map.get(f.id) == 'C3' or rgpd_map.get(f.id) == 'critical' or legal_map.get(f.id) in ['nda', 'litigation']) and not (class_map.get(f.id) == 'C3' and rgpd_map.get(f.id) == 'critical' and legal_map.get(f.id) in ['nda', 'litigation']):
                    items.append(f)
        elif category == 'duplicates':
            for fam in dup_families.values():
                items.extend(fam)
        listbox = tk.Listbox(frame)
        listbox.pack(fill='both', expand=True, padx=5, pady=5)
        for f in items:
            listbox.insert(tk.END, f.path)

    def update_thematic_tabs(self) -> None:
        security_dist = self._query_distribution('security_classification_cached')
        total = sum(v['count'] for v in security_dist.values())
        for level in ['C0', 'C1', 'C2', 'C3']:
            info = security_dist.get(level, {'count': 0, 'size': 0})
            pct = round(info['count'] / total * 100, 1) if total else 0
            size_gb = info['size'] / (1024 ** 3)
            self.security_labels[level].config(text=f"{level}: {pct}% | {info['count']} fichiers | {size_gb:.1f}GB")
        others_count = total - sum(security_dist.get(l, {'count': 0})['count'] for l in ['C0', 'C1', 'C2', 'C3'])
        others_size = sum(security_dist.get(k, {'size': 0})['size'] for k in security_dist.keys() if k not in {'C0', 'C1', 'C2', 'C3'})
        pct_others = round(others_count / total * 100, 1) if total else 0
        self.security_labels['Autres'].config(text=f"Autres: {pct_others}% | {others_count} fichiers | {others_size/(1024**3):.1f}GB")
        rgpd_dist = self._query_distribution('rgpd_risk_cached')
        total_r = sum(v['count'] for v in rgpd_dist.values())
        levels_rgpd = ['none', 'low', 'medium', 'high', 'critical']
        for lvl in levels_rgpd:
            info = rgpd_dist.get(lvl, {'count': 0, 'size': 0})
            pct = round(info['count'] / total_r * 100, 1) if total_r else 0
            self.rgpd_labels[lvl].config(text=f"{lvl}: {pct}% | {info['count']} fichiers | {info['size']/(1024**3):.1f}GB")
        others_rgpd = total_r - sum(rgpd_dist.get(l, {'count': 0})['count'] for l in levels_rgpd)
        size_rgpd = sum(rgpd_dist.get(k, {'size': 0})['size'] for k in rgpd_dist.keys() if k not in levels_rgpd)
        pct_rgpd_oth = round(others_rgpd / total_r * 100, 1) if total_r else 0
        self.rgpd_labels['Autres'].config(text=f"Autres: {pct_rgpd_oth}% | {others_rgpd} fichiers | {size_rgpd/(1024**3):.1f}GB")
        fin_dist = self._query_distribution('finance_type_cached')
        total_f = sum(v['count'] for v in fin_dist.values())
        fin_types = ['none', 'invoice', 'contract', 'budget', 'accounting', 'payment']
        for typ in fin_types:
            info = fin_dist.get(typ, {'count': 0, 'size': 0})
            pct = round(info['count'] / total_f * 100, 1) if total_f else 0
            self.finance_labels[typ].config(text=f"{typ}: {pct}% | {info['count']} fichiers | {info['size']/(1024**3):.1f}GB")
        others_f = total_f - sum(fin_dist.get(t, {'count': 0})['count'] for t in fin_types)
        size_f = sum(fin_dist.get(k, {'size': 0})['size'] for k in fin_dist.keys() if k not in fin_types)
        pct_f_oth = round(others_f / total_f * 100, 1) if total_f else 0
        self.finance_labels['Autres'].config(text=f"Autres: {pct_f_oth}% | {others_f} fichiers | {size_f/(1024**3):.1f}GB")
        legal_dist = self._query_distribution('legal_type_cached')
        total_l = sum(v['count'] for v in legal_dist.values())
        legal_types = ['none', 'employment', 'lease', 'sale', 'nda', 'compliance', 'litigation']
        for typ in legal_types:
            info = legal_dist.get(typ, {'count': 0, 'size': 0})
            pct = round(info['count'] / total_l * 100, 1) if total_l else 0
            self.legal_labels[typ].config(text=f"{typ}: {pct}% | {info['count']} fichiers | {info['size']/(1024**3):.1f}GB")
        others_l = total_l - sum(legal_dist.get(t, {'count': 0})['count'] for t in legal_types)
        size_l = sum(legal_dist.get(k, {'size': 0})['size'] for k in legal_dist.keys() if k not in legal_types)
        pct_l_oth = round(others_l / total_l * 100, 1) if total_l else 0
        self.legal_labels['Autres'].config(text=f"Autres: {pct_l_oth}% | {others_l} fichiers | {size_l/(1024**3):.1f}GB")
        metrics = self.calculate_business_metrics()
        c3_total = metrics.get('critical', {}).get('count', 0) + metrics.get('super_critical', {}).get('count', 0)
        self.security_focus_labels['C3 Total'].config(text=f"C3 Total: {c3_total}")
        self.security_focus_labels['C3 + RGPD'].config(text=f"C3 + RGPD: {metrics.get('super_critical', {}).get('count', 0)}")
        self.security_focus_labels['C3 + Legal'].config(text=f"C3 + Legal: {metrics.get('critical', {}).get('count', 0)}")
        self.security_focus_labels['Recommandations'].config(text=self.generate_recommendations(metrics))

    def export_business_report(self) -> None:
        try:
            metrics = self.calculate_business_metrics()
            from tkinter import filedialog
            filename = filedialog.asksaveasfilename(title="Exporter le rapport business", defaultextension=".json", filetypes=[("JSON files", "*.json"), ("All files", "*.*")])
            if filename:
                report = {
                    'timestamp': datetime.now().isoformat(),
                    'parameters': {
                        'age_threshold_years': self.threshold_age_years.get(),
                        'size_threshold_mb': self.threshold_size_mb.get(),
                        'classification_filter': self.classification_filter.get(),
                    },
                    'metrics': metrics,
                    'recommendations': self.generate_recommendations(metrics),
                }
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(report, f, indent=2, ensure_ascii=False)
                messagebox.showinfo('Succès', f'Rapport exporté : {filename}')
        except Exception as e:
            self._handle_analytics_error("export du rapport", e)

    def generate_recommendations(self, metrics: Dict[str, Any]) -> str:
        recs: List[str] = []
        attention = metrics.get('duplicates', {})
        if attention.get('wasted_space_gb', 0) > 0.5:
            recs.append("➡️ Réduire les doublons pour économiser de l'espace")
        surveillance = metrics.get('size_age', {})
        if surveillance.get('archival_size_gb', 0) > 1:
            recs.append("📦 Envisager l'archivage des fichiers anciens volumineux")
        if not recs:
            return "✅ Aucune recommandation particulière"
        return "\n".join(recs)

    def _handle_analytics_error(self, operation: str, error: Exception) -> None:
        error_msg = f"Analytics {operation}: {str(error)}"
        self.progress_label.config(text=f"❌ {operation} échoué")
        logging.error("Analytics Dashboard - %s", error_msg, exc_info=True)
        messagebox.showerror(
            f"Erreur Analytics - {operation}",
            (
                f"Une erreur est survenue lors de {operation}.\n\n"
                f"Détails: {str(error)}\n\n"
                "Le dashboard continue de fonctionner avec les données en cache."
            ),
            parent=self.parent,
        )
