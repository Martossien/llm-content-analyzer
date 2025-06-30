from __future__ import annotations

import json
import tkinter as tk
from datetime import datetime
from pathlib import Path
from tkinter import messagebox, ttk
from typing import Any, Dict, List

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
        self.threshold_duplicates = tk.StringVar(value="3")
        self.classification_filter = tk.StringVar(value="C2+")

        self._build_ui()
        # Minimal tabs dict for tests
        self.tabs: Dict[str, ttk.Frame] = {"age": self.security_tab}
        self.update_alert_cards()
        self.update_thematic_tabs()

    # ------------------------------------------------------------------
    def set_db_manager(self, db_manager: DBManager | None) -> None:
        self.db_manager = db_manager

    # ------------------------------------------------------------------
    def _build_ui(self) -> None:
        params_frame = ttk.LabelFrame(self.parent, text="⚙️ PARAMÈTRES UTILISATEUR")
        params_frame.pack(fill="x", padx=5, pady=5)
        ttk.Label(params_frame, text="Anciens:").grid(row=0, column=0, padx=5, pady=2)
        ttk.Entry(params_frame, textvariable=self.threshold_age_years, width=5).grid(row=0, column=1, padx=5, pady=2)
        ttk.Label(params_frame, text="Gros:").grid(row=0, column=2, padx=5, pady=2)
        ttk.Entry(params_frame, textvariable=self.threshold_size_mb, width=6).grid(row=0, column=3, padx=5, pady=2)
        ttk.Label(params_frame, text="Doublons:").grid(row=0, column=4, padx=5, pady=2)
        ttk.Entry(params_frame, textvariable=self.threshold_duplicates, width=4).grid(row=0, column=5, padx=5, pady=2)
        ttk.Label(params_frame, text="Classification:").grid(row=0, column=6, padx=5, pady=2)
        class_cb = ttk.Combobox(params_frame, textvariable=self.classification_filter, values=["C0+", "C1+", "C2+", "C3"], width=5, state="readonly")
        class_cb.grid(row=0, column=7, padx=5, pady=2)
        ttk.Button(params_frame, text="🔄 Recalculer", command=self.recalculate_all_metrics).grid(row=0, column=8, padx=5)
        ttk.Button(params_frame, text="💾 Sauver", command=self.save_user_preferences).grid(row=0, column=9, padx=5)

        alerts_frame = ttk.LabelFrame(self.parent, text="📊 ALERTES & KPIs GLOBAUX")
        alerts_frame.pack(fill="x", padx=5, pady=5)
        cards_container = ttk.Frame(alerts_frame)
        cards_container.pack(fill="x", padx=5, pady=5)

        self.critical_card = ttk.LabelFrame(cards_container, text="🔴 CRITIQUE")
        self.critical_card.grid(row=0, column=0, padx=5, pady=5, sticky="nsew")
        self.critical_line1 = ttk.Label(self.critical_card, text="0GB secrets", font=("Arial", 12, "bold"))
        self.critical_line1.pack()
        self.critical_line2 = ttk.Label(self.critical_card, text="dupliqués", font=("Arial", 10))
        self.critical_line2.pack()
        self.critical_line3 = ttk.Label(self.critical_card, text="0 fichiers C3", font=("Arial", 10))
        self.critical_line3.pack()

        self.attention_card = ttk.LabelFrame(cards_container, text="🟠 ATTENTION")
        self.attention_card.grid(row=0, column=1, padx=5, pady=5, sticky="nsew")
        self.attention_line1 = ttk.Label(self.attention_card, text="0 doublons", font=("Arial", 12, "bold"))
        self.attention_line1.pack()
        self.attention_line2 = ttk.Label(self.attention_card, text="= 0GB gaspillé", font=("Arial", 10))
        self.attention_line2.pack()
        self.attention_line3 = ttk.Label(self.attention_card, text="0 familles", font=("Arial", 10))
        self.attention_line3.pack()

        self.surveillance_card = ttk.LabelFrame(cards_container, text="🟡 SURVEILLER")
        self.surveillance_card.grid(row=0, column=2, padx=5, pady=5, sticky="nsew")
        self.surveillance_line1 = ttk.Label(self.surveillance_card, text="0 gros anciens", font=("Arial", 12, "bold"))
        self.surveillance_line1.pack()
        self.surveillance_line2 = ttk.Label(self.surveillance_card, text="= 0GB à archiver", font=("Arial", 10))
        self.surveillance_line2.pack()
        self.surveillance_line3 = ttk.Label(self.surveillance_card, text="Moyenne: 0GB", font=("Arial", 10))
        self.surveillance_line3.pack()

        self.status_card = ttk.LabelFrame(cards_container, text="✅ STATUS")
        self.status_card.grid(row=0, column=3, padx=5, pady=5, sticky="nsew")
        self.status_line1 = ttk.Label(self.status_card, text="0 fichiers", font=("Arial", 12, "bold"))
        self.status_line1.pack()
        self.status_line2 = ttk.Label(self.status_card, text="= 0GB analysés", font=("Arial", 10))
        self.status_line2.pack()
        self.status_line3 = ttk.Label(self.status_card, text="Gain: 0% espace", font=("Arial", 10))
        self.status_line3.pack()

        for i in range(4):
            cards_container.columnconfigure(i, weight=1)

        notebook_frame = ttk.LabelFrame(self.parent, text="Navigation Thématique")
        notebook_frame.pack(fill="both", expand=True, padx=5, pady=5)
        self.thematic_notebook = ttk.Notebook(notebook_frame)
        self.thematic_notebook.pack(fill="both", expand=True, padx=5, pady=5)

        self.security_tab = ttk.Frame(self.thematic_notebook)
        self.thematic_notebook.add(self.security_tab, text="🛡️ Security")
        sec_left = ttk.LabelFrame(self.security_tab, text="RÉPARTITION")
        sec_left.pack(side="left", fill="both", expand=True, padx=5, pady=5)
        self.security_distribution_labels: Dict[str, ttk.Label] = {}
        for cls in ["C0+", "C1+", "C2+", "C3"]:
            lbl = ttk.Label(sec_left, text=f"{cls}: 0 (0GB)")
            lbl.pack(anchor="w")
            self.security_distribution_labels[cls] = lbl
        sec_right = ttk.LabelFrame(self.security_tab, text="FOCUS CRITIQUE")
        sec_right.pack(side="left", fill="both", expand=True, padx=5, pady=5)
        self.security_focus1 = ttk.Label(sec_right, text="")
        self.security_focus1.pack(anchor="w")
        self.security_focus2 = ttk.Label(sec_right, text="")
        self.security_focus2.pack(anchor="w")
        self.security_focus3 = ttk.Label(sec_right, text="")
        self.security_focus3.pack(anchor="w")

        self.rgpd_tab = ttk.Frame(self.thematic_notebook)
        self.thematic_notebook.add(self.rgpd_tab, text="🔒 RGPD")
        self.rgpd_labels: Dict[str, ttk.Label] = {}
        for lvl in ["none", "low", "medium", "high"]:
            lbl = ttk.Label(self.rgpd_tab, text=f"{lvl}: 0 (0GB)")
            lbl.pack(anchor="w", padx=5, pady=2)
            self.rgpd_labels[lvl] = lbl

        self.finance_tab = ttk.Frame(self.thematic_notebook)
        self.thematic_notebook.add(self.finance_tab, text="💰 Finance")
        self.finance_labels: Dict[str, ttk.Label] = {}
        for typ in ["none", "invoice", "contract", "budget", "accounting", "payment"]:
            lbl = ttk.Label(self.finance_tab, text=f"{typ}: 0")
            lbl.pack(anchor="w", padx=5, pady=2)
            self.finance_labels[typ] = lbl

        self.legal_tab = ttk.Frame(self.thematic_notebook)
        self.thematic_notebook.add(self.legal_tab, text="⚖️ Legal")
        self.legal_labels: Dict[str, ttk.Label] = {}
        for typ in ["none", "employment", "lease", "sale", "nda", "compliance", "litigation"]:
            lbl = ttk.Label(self.legal_tab, text=f"{typ}: 0")
            lbl.pack(anchor="w", padx=5, pady=2)
            self.legal_labels[typ] = lbl

        self.global_tab = ttk.Frame(self.thematic_notebook)
        self.thematic_notebook.add(self.global_tab, text="📊 Vue Globale")
        self.global_text = tk.Text(self.global_tab, height=10, wrap="word")
        self.global_text.pack(fill="both", expand=True, padx=5, pady=5)

        actions_frame = ttk.Frame(self.parent)
        actions_frame.pack(fill="x", padx=5, pady=5)
        ttk.Button(actions_frame, text="📄 Export ce Rapport", command=self.export_business_report).pack(side="left", padx=5)
        ttk.Button(actions_frame, text="🔍 Drill-down Détaillé", command=self.open_detailed_analysis).pack(side="left", padx=5)
        ttk.Button(actions_frame, text="⚙️ Paramètres Avancés", command=self.open_advanced_settings).pack(side="right", padx=5)

    # ------------------------------------------------------------------
    def _get_all_files(self) -> List[FileInfo]:
        if self.db_manager is None:
            return []
        try:
            return self.db_manager.get_all_files_basic()
        except Exception:
            return []

    # ------------------------------------------------------------------
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

    # ------------------------------------------------------------------
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

    # ------------------------------------------------------------------
    def calculate_business_metrics(self) -> Dict[str, Any]:
        if self.db_manager is None:
            return {}
        files = self._get_all_files()
        if not files:
            return {}
        age_threshold_days = int(self.threshold_age_years.get()) * 365
        size_threshold_mb = int(self.threshold_size_mb.get())

        age_stats = self.age_analyzer.calculate_archival_candidates(files, age_threshold_days)
        size_stats = self.size_analyzer.calculate_space_optimization(files, size_threshold_mb)
        dup_families = self.duplicate_detector.detect_duplicate_family(files)
        dup_stats = self.duplicate_detector.get_duplicate_statistics(dup_families)

        class_map = self._get_classification_map()
        c3_families = [fam for fam in dup_families.values() if class_map.get(fam[0].id) == "C3"]
        secrets_wasted = sum((len(fam) - 1) * fam[0].file_size for fam in c3_families)
        c3_files = [f for f in files if class_map.get(f.id) == "C3"]

        total_size = sum(f.file_size for f in files)
        potential_reclaim = dup_stats.get("space_wasted_bytes", 0) + age_stats.get("total_size_bytes", 0)
        gain_pct = round(potential_reclaim / total_size * 100, 1) if total_size else 0

        metrics = {
            "critical": {
                "secrets_duplicated_gb": secrets_wasted / (1024 ** 3),
                "critical_files_count": len(c3_files),
            },
            "attention": {
                "duplicates_count": dup_stats.get("total_families", 0),
                "wasted_space_gb": dup_stats.get("space_wasted_bytes", 0) / (1024 ** 3),
                "duplicate_families": dup_stats.get("total_families", 0),
            },
            "surveillance": {
                "large_old_files": len(self.age_analyzer.identify_stale_files(files, age_threshold_days)),
                "archival_space_gb": age_stats.get("total_size_bytes", 0) / (1024 ** 3),
                "average_size_gb": size_stats.get("size_bytes", 0) / (1024 ** 3),
            },
            "status": {
                "total_files": len(files),
                "total_analyzed_gb": total_size / (1024 ** 3),
                "space_gain_percent": gain_pct,
            },
        }
        return metrics

    # ------------------------------------------------------------------
    def update_alert_cards(self) -> None:
        metrics = self.calculate_business_metrics()
        if not metrics:
            return
        critical = metrics.get("critical", {})
        secrets_gb = critical.get("secrets_duplicated_gb", 0)
        critical_files = critical.get("critical_files_count", 0)
        self.critical_line1.config(text=f"{secrets_gb:.1f}GB secrets")
        self.critical_line2.config(text="dupliqués")
        self.critical_line3.config(text=f"{critical_files} fichiers C3")
        if secrets_gb > 1.0:
            self.critical_line1.config(foreground="red")
        else:
            self.critical_line1.config(foreground="darkred")

        attention = metrics.get("attention", {})
        duplicates = attention.get("duplicates_count", 0)
        wasted_gb = attention.get("wasted_space_gb", 0)
        families = attention.get("duplicate_families", 0)
        self.attention_line1.config(text=f"{duplicates} doublons")
        self.attention_line2.config(text=f"= {wasted_gb:.1f}GB gaspillé")
        self.attention_line3.config(text=f"{families} familles")
        if wasted_gb > 0.5:
            self.attention_line1.config(foreground="darkorange")
        else:
            self.attention_line1.config(foreground="orange")

        surveillance = metrics.get("surveillance", {})
        large_old = surveillance.get("large_old_files", 0)
        archival_gb = surveillance.get("archival_space_gb", 0)
        avg_gb = surveillance.get("average_size_gb", 0)
        self.surveillance_line1.config(text=f"{large_old} gros anciens")
        self.surveillance_line2.config(text=f"= {archival_gb:.1f}GB à archiver")
        self.surveillance_line3.config(text=f"Moyenne: {avg_gb:.1f}GB")

        status = metrics.get("status", {})
        total_files = status.get("total_files", 0)
        analyzed_gb = status.get("total_analyzed_gb", 0)
        gain_pct = status.get("space_gain_percent", 0)
        self.status_line1.config(text=f"{total_files:,} fichiers")
        self.status_line2.config(text=f"= {analyzed_gb:.1f}GB analysés")
        self.status_line3.config(text=f"Gain: {gain_pct}% espace")

    # ------------------------------------------------------------------
    def update_thematic_tabs(self) -> None:
        security_dist = self._query_distribution("security_classification_cached")
        for cls, lbl in self.security_distribution_labels.items():
            info = security_dist.get(cls, {"count": 0, "size": 0})
            lbl.config(text=f"{cls}: {info['count']} ({info['size']/(1024**3):.1f}GB)")
        metrics = self.calculate_business_metrics()
        crit = metrics.get("critical", {})
        self.security_focus1.config(text=f"C3 fichiers: {crit.get('critical_files_count', 0)}")
        self.security_focus2.config(text=f"Secrets dupliqués: {crit.get('secrets_duplicated_gb', 0):.1f}GB")
        self.security_focus3.config(text="")

        rgpd_dist = self._query_distribution("rgpd_risk_cached")
        for lvl, lbl in self.rgpd_labels.items():
            info = rgpd_dist.get(lvl, {"count": 0, "size": 0})
            lbl.config(text=f"{lvl}: {info['count']} ({info['size']/(1024**3):.1f}GB)")

        fin_dist = self._query_distribution("finance_type_cached")
        for typ, lbl in self.finance_labels.items():
            info = fin_dist.get(typ, {"count": 0, "size": 0})
            lbl.config(text=f"{typ}: {info['count']}")

        legal_dist = self._query_distribution("legal_type_cached")
        for typ, lbl in self.legal_labels.items():
            info = legal_dist.get(typ, {"count": 0, "size": 0})
            lbl.config(text=f"{typ}: {info['count']}")

        self.global_text.delete("1.0", tk.END)
        self.global_text.insert("1.0", self.generate_recommendations(metrics))

    # ------------------------------------------------------------------
    def recalculate_all_metrics(self) -> None:
        try:
            age_years = int(self.threshold_age_years.get())
            size_mb = int(self.threshold_size_mb.get())
            if age_years < 1 or age_years > 10:
                messagebox.showerror("Erreur", "Âge doit être entre 1 et 10 ans")
                return
            if size_mb < 1 or size_mb > 10000:
                messagebox.showerror("Erreur", "Taille doit être entre 1 et 10000 MB")
                return
            self.update_alert_cards()
            self.update_thematic_tabs()
            messagebox.showinfo("Succès", "Métriques recalculées avec succès")
        except ValueError:
            messagebox.showerror("Erreur", "Paramètres invalides")

    # ------------------------------------------------------------------
    def export_business_report(self) -> None:
        try:
            metrics = self.calculate_business_metrics()
            from tkinter import filedialog
            filename = filedialog.asksaveasfilename(
                title="Exporter le rapport business",
                defaultextension=".json",
                filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
            )
            if filename:
                report = {
                    "timestamp": datetime.now().isoformat(),
                    "parameters": {
                        "age_threshold_years": self.threshold_age_years.get(),
                        "size_threshold_mb": self.threshold_size_mb.get(),
                        "classification_filter": self.classification_filter.get(),
                    },
                    "metrics": metrics,
                    "recommendations": self.generate_recommendations(metrics),
                }
                with open(filename, "w", encoding="utf-8") as f:
                    json.dump(report, f, indent=2, ensure_ascii=False)
                messagebox.showinfo("Succès", f"Rapport exporté : {filename}")
        except Exception as e:
            messagebox.showerror("Erreur", f"Échec export : {str(e)}")

    # ------------------------------------------------------------------
    def generate_recommendations(self, metrics: Dict[str, Any]) -> str:
        recs: List[str] = []
        attention = metrics.get("attention", {})
        if attention.get("wasted_space_gb", 0) > 0.5:
            recs.append("➡️ Réduire les doublons pour économiser de l'espace")
        surveillance = metrics.get("surveillance", {})
        if surveillance.get("archival_space_gb", 0) > 1:
            recs.append("📦 Envisager l'archivage des fichiers anciens volumineux")
        if not recs:
            return "✅ Aucune recommandation particulière"
        return "\n".join(recs)

    # ------------------------------------------------------------------
    def save_user_preferences(self) -> None:
        prefs = {
            "age_years": self.threshold_age_years.get(),
            "size_mb": self.threshold_size_mb.get(),
            "duplicates": self.threshold_duplicates.get(),
            "classification": self.classification_filter.get(),
        }
        try:
            Path("user_prefs.json").write_text(json.dumps(prefs, indent=2), encoding="utf-8")
            messagebox.showinfo("Succès", "Préférences sauvegardées")
        except Exception as exc:
            messagebox.showerror("Erreur", str(exc))

    # ------------------------------------------------------------------
    def open_detailed_analysis(self) -> None:
        win = tk.Toplevel(self.parent)
        win.title("Drill-down Détaillé")
        txt = tk.Text(win, width=80, height=20)
        txt.pack(fill="both", expand=True)
        metrics = self.calculate_business_metrics()
        txt.insert("1.0", json.dumps(metrics, indent=2, ensure_ascii=False))

    # ------------------------------------------------------------------
    def open_advanced_settings(self) -> None:
        win = tk.Toplevel(self.parent)
        win.title("Paramètres Avancés")
        ttk.Label(win, text="Ajustez les paramètres avancés ici").pack(padx=10, pady=10)

    # ------------------------------------------------------------------
    def refresh_all(self) -> None:
        self.update_alert_cards()
        self.update_thematic_tabs()

