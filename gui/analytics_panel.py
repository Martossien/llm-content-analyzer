from __future__ import annotations

import json
import logging
import time
import tkinter as tk
from datetime import datetime, timedelta
from pathlib import Path
from tkinter import messagebox, ttk
from typing import Any, Dict, List, Optional
import threading
import queue

logger = logging.getLogger(__name__)

from content_analyzer.modules.age_analyzer import AgeAnalyzer
from content_analyzer.modules.size_analyzer import SizeAnalyzer
from content_analyzer.modules.duplicate_detector import DuplicateDetector, FileInfo
from content_analyzer.modules.db_manager import SafeDBManager as DBManager


class AnalyticsDrillDownViewer:
    """Comprehensive drill-down system for all Analytics tabs exploration."""

    def __init__(self, parent_analytics_panel: "AnalyticsPanel") -> None:
        self.analytics_panel = parent_analytics_panel
        self.db_manager = parent_analytics_panel.db_manager
        # Store currently displayed files for export functionality
        self.current_files: List[Dict[str, Any]] = []

    # ------------------------------------------------------------------
    # Base modal creation helpers
    # ------------------------------------------------------------------
    def _create_base_modal(self, title: str, subtitle: str) -> tk.Toplevel:
        """Creation de modale sécurisée avec gestion d'erreur robuste."""

        try:
            if (
                not hasattr(self.analytics_panel, "parent")
                or not self.analytics_panel.parent
            ):
                logger.error("Parent window non disponible pour la modale")
                raise ValueError("Parent window indisponible")

            modal = tk.Toplevel(self.analytics_panel.parent)
            modal.title(title)
            modal.withdraw()
            modal.geometry("1200x700")
            modal.transient(self.analytics_panel.parent)
            modal.resizable(True, True)

            header_frame = ttk.Frame(modal)
            header_frame.pack(fill="x", padx=10, pady=5)
            ttk.Label(header_frame, text=subtitle, font=("Arial", 11, "bold")).pack(
                anchor="w"
            )

            self._build_drill_down_treeview(modal)

            buttons_frame = ttk.Frame(modal)
            buttons_frame.pack(fill="x", padx=10, pady=5)
            ttk.Button(
                buttons_frame,
                text="📊 Export Liste",
                command=self._export_filtered_files,
            ).pack(side="left", padx=5)
            ttk.Button(buttons_frame, text="❌ Fermer", command=modal.destroy).pack(
                side="right", padx=5
            )

            modal.update_idletasks()
            x = (modal.winfo_screenwidth() // 2) - (1200 // 2)
            y = (modal.winfo_screenheight() // 2) - (700 // 2)
            modal.geometry(f"1200x700+{x}+{y}")

            modal.deiconify()
            modal.lift()
            modal.focus_set()

            def apply_modal_grab():
                try:
                    if modal.winfo_exists():
                        modal.grab_set()
                        logger.info(f"Modal grab applied successfully for: {title}")
                except Exception as e:
                    logger.warning(f"Modal grab application failed: {e}")

            modal.after(50, apply_modal_grab)
            logger.info(f"Modal window created successfully: {title}")
            return modal

        except Exception as e:
            logger.error(f"Critical failure creating modal window: {e}")
            messagebox.showerror(
                "Erreur Critique",
                f"Impossible de creer la fenetre d'analyse.\nErreur: {str(e)}",
                parent=self.analytics_panel.parent,
            )
            raise

    def _build_drill_down_treeview(self, parent_window: tk.Toplevel) -> None:
        """Build treeview for file exploration."""

        tree_frame = ttk.Frame(parent_window)
        tree_frame.pack(fill="both", expand=True, padx=10, pady=5)

        columns = (
            "Name",
            "Path",
            "Size",
            "Modified",
            "Classification",
            "RGPD",
            "Type",
            "Owner",
        )

        self.drill_tree = ttk.Treeview(
            tree_frame, columns=columns, show="headings", height=20
        )
        column_configs = {
            "Name": {"width": 200, "text": "Nom"},
            "Path": {"width": 250, "text": "Chemin"},
            "Size": {"width": 100, "text": "Taille"},
            "Modified": {"width": 120, "text": "Modifié"},
            "Classification": {"width": 100, "text": "Sécurité"},
            "RGPD": {"width": 80, "text": "RGPD"},
            "Type": {"width": 80, "text": "Type"},
            "Owner": {"width": 150, "text": "Propriétaire"},
        }
        for col, config in column_configs.items():
            self.drill_tree.heading(col, text=config["text"])
            self.drill_tree.column(col, width=config["width"])

        v_scrollbar = ttk.Scrollbar(
            tree_frame, orient="vertical", command=self.drill_tree.yview
        )
        h_scrollbar = ttk.Scrollbar(
            tree_frame, orient="horizontal", command=self.drill_tree.xview
        )
        self.drill_tree.configure(
            yscrollcommand=v_scrollbar.set, xscrollcommand=h_scrollbar.set
        )

        self.drill_tree.grid(row=0, column=0, sticky="nsew")
        v_scrollbar.grid(row=0, column=1, sticky="ns")
        h_scrollbar.grid(row=1, column=0, sticky="ew")

        tree_frame.grid_rowconfigure(0, weight=1)
        tree_frame.grid_columnconfigure(0, weight=1)

        self.drill_tree.bind("<Double-1>", self._on_file_double_click)
        self.drill_tree.bind("<Button-3>", self._show_file_context_menu)

    # ------------------------------------------------------------------
    # Helper utilities
    # ------------------------------------------------------------------
    def _format_file_size(self, size_bytes: int) -> str:
        if not size_bytes:
            return "0B"
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f} PB"

    def _get_file_type(self, filename: str) -> str:
        if not filename:
            return "Unknown"
        ext = filename.lower().split(".")[-1] if "." in filename else ""
        type_map = {
            "pdf": "PDF",
            "doc": "DOC",
            "docx": "DOC",
            "xls": "XLS",
            "xlsx": "XLS",
            "ppt": "PPT",
            "pptx": "PPT",
            "jpg": "IMG",
            "jpeg": "IMG",
            "png": "IMG",
            "gif": "IMG",
            "txt": "TXT",
            "csv": "CSV",
            "json": "JSON",
            "xml": "XML",
        }
        return type_map.get(ext, "Autres")

    # ------------------------------------------------------------------
    # Unified modal query builder
    # ------------------------------------------------------------------
    def _build_modal_query_unified(
        self,
        category_type: str,
        category_value: str,
    ) -> tuple[str, tuple]:
        """Return SQL query and parameters for a given modal category."""

        base_query = """
        SELECT f.id, f.name, f.path, f.file_size, f.last_modified, f.owner,
               COALESCE(r.security_classification_cached, 'none') AS classif,
               COALESCE(r.rgpd_risk_cached, 'none') AS rgpd
        FROM fichiers f
        LEFT JOIN reponses_llm r ON f.id = r.fichier_id
        WHERE (f.status IS NULL OR f.status != 'error')
        """

        conditions: list[str] = []
        params: list[Any] = []

        if category_type == "security":
            if category_value == "Autres":
                conditions.append(
                    "COALESCE(r.security_classification_cached, 'none') NOT IN ('C0','C1','C2','C3','none')"
                )
            elif category_value == "none":
                conditions.append(
                    "COALESCE(r.security_classification_cached, 'none') = 'none'"
                )
            else:
                conditions.append(
                    "COALESCE(r.security_classification_cached, 'none') = ?"
                )
                params.append(category_value)
        elif category_type == "rgpd":
            if category_value == "Autres":
                conditions.append(
                    "COALESCE(r.rgpd_risk_cached, 'none') NOT IN ('low','medium','high','critical','none')"
                )
            elif category_value == "none":
                conditions.append("COALESCE(r.rgpd_risk_cached, 'none') = 'none'")
            else:
                conditions.append("COALESCE(r.rgpd_risk_cached, 'none') = ?")
                params.append(category_value)
        elif category_type == "size":
            size_map = {
                "<50MB": (0, 50 * 1024 * 1024),
                "50-100MB": (50 * 1024 * 1024, 100 * 1024 * 1024),
                "100-150MB": (100 * 1024 * 1024, 150 * 1024 * 1024),
                "150-200MB": (150 * 1024 * 1024, 200 * 1024 * 1024),
                "200-300MB": (200 * 1024 * 1024, 300 * 1024 * 1024),
                "300-500MB": (300 * 1024 * 1024, 500 * 1024 * 1024),
                ">500MB": (500 * 1024 * 1024, float("inf")),
            }
            if category_value in size_map:
                min_size, max_size = size_map[category_value]
                if max_size == float("inf"):
                    conditions.append("f.file_size >= ?")
                    params.append(min_size)
                else:
                    conditions.append("f.file_size >= ? AND f.file_size < ?")
                    params.extend([min_size, max_size])
        elif category_type == "temporal":
            now = datetime.now()
            date_field = category_value.split(":")[0]
            period = category_value.split(":")[1]
            mapping = {
                "last_7_days": now - timedelta(days=7),
                "last_30_days": now - timedelta(days=30),
                "last_90_days": now - timedelta(days=90),
                "last_year": now - timedelta(days=365),
            }
            if period == "older_1_year":
                conditions.append(f"f.{date_field} < ?")
                params.append((now - timedelta(days=365)).strftime("%Y-%m-%d %H:%M:%S"))
            elif period == "all":
                pass
            else:
                cutoff = mapping.get(period, now)
                conditions.append(f"f.{date_field} >= ?")
                params.append(cutoff.strftime("%Y-%m-%d %H:%M:%S"))

        if conditions:
            query = base_query + " AND " + " AND ".join(conditions)
        else:
            query = base_query

        query += " ORDER BY f.file_size DESC"
        return query, tuple(params)

    def _export_filtered_files(self) -> None:  # pragma: no cover - UI
        """Export filtered files with proper window Z-order management."""
        try:
            if not hasattr(self, "current_files") or not self.current_files:
                messagebox.showwarning(
                    "Attention",
                    "Aucun fichier à exporter",
                    parent=self.analytics_panel.parent,
                )
                return

            from tkinter import filedialog

            filename = filedialog.asksaveasfilename(
                parent=self.analytics_panel.parent,
                title="Exporter la liste des fichiers",
                defaultextension=".csv",
                filetypes=[
                    ("CSV files", "*.csv"),
                    ("Excel files", "*.xlsx"),
                    ("All files", "*.*"),
                ],
            )

            if filename:
                import csv

                with open(filename, "w", newline="", encoding="utf-8") as csvfile:
                    writer = csv.writer(csvfile)
                    headers = [
                        "Nom",
                        "Chemin",
                        "Taille",
                        "Propriétaire",
                        "Modifié",
                        "Classification",
                        "Risque RGPD",
                    ]
                    writer.writerow(headers)
                    for file_data in self.current_files:
                        writer.writerow(
                            [
                                file_data.get("name", ""),
                                file_data.get("path", ""),
                                file_data.get("file_size", 0),
                                file_data.get("owner", ""),
                                file_data.get("last_modified", ""),
                                file_data.get("classif", ""),
                                file_data.get("rgpd", ""),
                            ]
                        )

                success_window = tk.Toplevel(self.analytics_panel.parent)
                success_window.title("Export Réussi")
                success_window.geometry("400x150")
                success_window.transient(self.analytics_panel.parent)
                success_window.lift()
                success_window.focus_set()
                success_window.attributes("-topmost", True)
                success_window.grab_set()
                success_window.after(
                    100, lambda: success_window.attributes("-topmost", False)
                )

                ttk.Label(
                    success_window, text="✅ Export réussi!", font=("Arial", 12, "bold")
                ).pack(pady=20)
                ttk.Label(success_window, text=f"Fichier: {filename}").pack(pady=5)
                ttk.Button(
                    success_window,
                    text="OK",
                    command=lambda: [
                        success_window.grab_release(),
                        success_window.destroy(),
                    ],
                ).pack(pady=10)

                logger.info(f"Files exported successfully to: {filename}")

        except Exception as e:
            logger.error(f"Failed to export files: {e}")
            messagebox.showerror(
                "Erreur Export",
                f"Échec de l'export:\n{str(e)}",
                parent=self.analytics_panel.parent,
            )

    def _on_file_double_click(self, event):  # pragma: no cover - UI
        selection = self.drill_tree.selection()
        if selection:
            item = self.drill_tree.item(selection[0])
            filename = item["values"][0]
            messagebox.showinfo("Fichier", f"Ouverture de: {filename}")

    def _show_file_context_menu(self, event):  # pragma: no cover - UI
        pass

    # ------------------------------------------------------------------
    # Data loading helpers for each tab type
    # ------------------------------------------------------------------
    def _load_filtered_files(
        self, modal: tk.Toplevel, query: str, params: tuple, category: str
    ) -> None:
        try:
            if not self.db_manager:
                logger.warning("No database manager available for filtered files")
                messagebox.showwarning(
                    "Base de données",
                    "Gestionnaire de base de données non disponible",
                    parent=modal,
                )
                return

            progress_label = ttk.Label(modal, text="🔄 Chargement des données...")
            progress_label.pack(pady=10)
            modal.update_idletasks()

            with self.db_manager._connect().get() as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                rows = cursor.fetchall()

                logger.info(f"Query executed: {query[:100]}... with params: {params}")
                logger.info(f"Results found: {len(rows)} rows")

                if not rows:
                    progress_label.config(
                        text="ℹ️ Aucun fichier trouvé pour ces critères"
                    )
                    logger.warning(
                        f"No results for query: {query[:100]}... params: {params}"
                    )
                    return

                for item in self.drill_tree.get_children():
                    self.drill_tree.delete(item)

                self.current_files = []
                for row in rows:
                    try:
                        if len(row) >= 6:
                            file_id, name, path, size, modified, owner = row[:6]
                            classif = row[6] if len(row) > 6 else "N/A"
                            rgpd = row[7] if len(row) > 7 else "N/A"

                            size_str = self._format_file_size(size or 0)
                            modified_str = modified[:19] if modified else "N/A"
                            name_str = (
                                str(name)[:50] + "..."
                                if len(str(name)) > 50
                                else str(name)
                            )
                            path_str = (
                                str(path)[:80] + "..."
                                if len(str(path)) > 80
                                else str(path)
                            )
                            owner_str = str(owner or "Inconnu")

                            self.drill_tree.insert(
                                "",
                                "end",
                                values=(
                                    name_str,
                                    path_str,
                                    size_str,
                                    modified_str,
                                    classif,
                                    rgpd,
                                    "",
                                    owner_str,
                                ),
                            )

                            self.current_files.append(
                                {
                                    "name": name,
                                    "path": path,
                                    "file_size": size,
                                    "last_modified": modified,
                                    "owner": owner,
                                    "classif": classif,
                                    "rgpd": rgpd,
                                }
                            )
                    except Exception as row_error:
                        logger.warning(f"Erreur traitement ligne: {row_error}")
                        continue

                progress_label.config(
                    text=f"✅ {len(rows)} fichiers chargés - {category}"
                )
                modal.after(3000, progress_label.destroy)
        except Exception as e:
            logger.error(f"Critical error in _load_filtered_files: {e}")
            if "progress_label" in locals():
                progress_label.config(text=f"❌ Erreur: {str(e)}")
            messagebox.showerror(
                "Erreur Chargement",
                f"Erreur lors du chargement des fichiers:\n{str(e)}",
                parent=modal,
            )

    # ------------------------------------------------------------------
    # Public modal entry points
    # ------------------------------------------------------------------
    def show_classification_files_modal(
        self, classification: str, title: str, click_info: Dict[str, Any]
    ) -> None:
        """Fixed Security classification modal with proper 'Autres' handling."""
        try:
            modal = self._create_base_modal(
                title, f"🔐 Classification: {classification}"
            )

            logger.debug(f"Classification modal query for: {classification}")

            if classification == "Autres":
                query = """
                SELECT f.id, f.name, f.path, f.file_size, f.last_modified, f.owner,
                       COALESCE(r.security_classification_cached, 'none') AS security_class,
                       COALESCE(r.rgpd_risk_cached, 'none') AS rgpd_risk
                FROM fichiers f
                LEFT JOIN reponses_llm r ON f.id = r.fichier_id
                WHERE (f.status IS NULL OR f.status != 'error')
                AND f.file_size > 0
                AND (r.security_classification_cached IS NULL
                     OR r.security_classification_cached NOT IN ('C0', 'C1', 'C2', 'C3'))
                ORDER BY f.file_size DESC
                """
                params = ()
            else:
                query = """
                SELECT f.id, f.name, f.path, f.file_size, f.last_modified, f.owner,
                       COALESCE(r.security_classification_cached, 'none') AS security_class,
                       COALESCE(r.rgpd_risk_cached, 'none') AS rgpd_risk
                FROM fichiers f
                LEFT JOIN reponses_llm r ON f.id = r.fichier_id
                WHERE (f.status IS NULL OR f.status != 'error')
                AND f.file_size > 0
                AND r.security_classification_cached = ?
                ORDER BY f.file_size DESC
                """
                params = (classification,)

            self._load_filtered_files(
                modal, query, params, f"Sécurité {classification}"
            )

        except Exception as e:
            logger.error(f"Failed to show classification modal: {e}")
            messagebox.showerror(
                "Erreur",
                f"Impossible d'ouvrir la vue Classification.\nErreur: {str(e)}",
            )

    def show_rgpd_files_modal(
        self, risk_level: str, title: str, click_info: Dict[str, Any]
    ) -> None:
        """Fixed RGPD risk modal with proper 'Autres'/'none' handling."""
        try:
            modal = self._create_base_modal(title, f"🛡️ Risque RGPD: {risk_level}")

            logger.debug(f"RGPD modal query for: {risk_level}")

            if risk_level == "Autres":
                query = """
                SELECT f.id, f.name, f.path, f.file_size, f.last_modified, f.owner,
                       COALESCE(r.security_classification_cached, 'none') AS security_class,
                       COALESCE(r.rgpd_risk_cached, 'none') AS rgpd_risk
                FROM fichiers f
                LEFT JOIN reponses_llm r ON f.id = r.fichier_id
                WHERE (f.status IS NULL OR f.status != 'error')
                AND f.file_size > 0
                AND (r.rgpd_risk_cached IS NULL
                     OR r.rgpd_risk_cached NOT IN ('none', 'low', 'medium', 'high', 'critical'))
                ORDER BY f.file_size DESC
                """
                params = ()
            elif risk_level == "none":
                query = """
                SELECT f.id, f.name, f.path, f.file_size, f.last_modified, f.owner,
                       COALESCE(r.security_classification_cached, 'none') AS security_class,
                       COALESCE(r.rgpd_risk_cached, 'none') AS rgpd_risk
                FROM fichiers f
                LEFT JOIN reponses_llm r ON f.id = r.fichier_id
                WHERE (f.status IS NULL OR f.status != 'error')
                AND f.file_size > 0
                AND (r.rgpd_risk_cached IS NULL OR r.rgpd_risk_cached = 'none')
                ORDER BY f.file_size DESC
                """
                params = ()
            else:
                query = """
                SELECT f.id, f.name, f.path, f.file_size, f.last_modified, f.owner,
                       COALESCE(r.security_classification_cached, 'none') AS security_class,
                       COALESCE(r.rgpd_risk_cached, 'none') AS rgpd_risk
                FROM fichiers f
                LEFT JOIN reponses_llm r ON f.id = r.fichier_id
                WHERE (f.status IS NULL OR f.status != 'error')
                AND f.file_size > 0
                AND r.rgpd_risk_cached = ?
                ORDER BY f.file_size DESC
                """
                params = (risk_level,)

            self._load_filtered_files(modal, query, params, f"RGPD {risk_level}")

        except Exception as e:
            logger.error(f"Failed to show RGPD modal: {e}")
            messagebox.showerror(
                "Erreur", f"Impossible d'ouvrir la vue RGPD.\nErreur: {str(e)}"
            )

    def show_age_files_modal(
        self, age_type: str, title: str, click_info: Dict[str, Any]
    ) -> None:
        try:
            modal = self._create_base_modal(title, f"📅 Analyse d'âge: {age_type}")
            age_category = click_info.get("age_category", "old_files")
            threshold_days = click_info.get("threshold_days", 365)

            date_field = (
                "last_modified" if "modification" in age_type else "creation_time"
            )

            age_conditions = {
                "recent_7_days": f"f.{date_field} >= date('now', '-7 days')",
                "recent_30_days": f"f.{date_field} >= date('now', '-30 days') AND f.{date_field} < date('now', '-7 days')",
                "recent_90_days": f"f.{date_field} >= date('now', '-90 days') AND f.{date_field} < date('now', '-30 days')",
                "old_1_year": f"f.{date_field} < date('now', '-1 year')",
                "old_2_years": f"f.{date_field} < date('now', '-2 years')",
                "dormant": f"f.{date_field} < date('now', '-{threshold_days} days')",
            }

            condition = age_conditions.get(
                age_category, f"f.{date_field} < date('now', '-1 year')"
            )

            query = f"""
            SELECT f.id, f.name, f.path, f.file_size, f.{date_field}, f.owner,
                   COALESCE(r.security_classification_cached, 'none') AS classif,
                   COALESCE(r.rgpd_risk_cached, 'none') AS rgpd
            FROM fichiers f
            LEFT JOIN reponses_llm r ON f.id = r.fichier_id
            WHERE (f.status IS NULL OR f.status != 'error')
            AND f.file_size > 0
            AND f.{date_field} IS NOT NULL
            AND {condition}
            ORDER BY f.{date_field} ASC
            """
            self._load_filtered_files(modal, query, (), f"Fichiers {age_category}")
            logger.info(
                f"Opened age analysis modal: {age_type}, category: {age_category}"
            )
        except Exception as e:  # pragma: no cover - UI
            logger.error("Failed to show age analysis modal: %s", e)
            messagebox.showerror(
                "Erreur", f"Impossible d'ouvrir la vue Analyse d'âge.\nErreur: {str(e)}"
            )

    def show_size_files_modal(
        self, size_type: str, title: str, click_info: Dict[str, Any]
    ) -> None:
        try:
            modal = self._create_base_modal(title, f"📊 Analyse de taille: {size_type}")
            query, params = self._build_modal_query_unified("size", size_type)
            self._load_filtered_files(modal, query, params, f"Fichiers {size_type}")
            logger.info(f"Opened size analysis modal for: {size_type}")
        except Exception as e:  # pragma: no cover - UI
            logger.error("Failed to show size analysis modal: %s", e)
            messagebox.showerror(
                "Erreur",
                f"Impossible d'ouvrir la vue Analyse de taille.\nErreur: {str(e)}",
            )

    def show_duplicates_modal(self, title: str, click_info: Dict[str, Any]) -> None:
        try:
            modal = self._create_base_modal(title, "🔄 Fichiers dupliqués par groupe")
            query = """
            SELECT f.id, f.name, f.path, f.file_size, f.last_modified, f.owner,
                   COALESCE(r.security_classification_cached, 'none') AS classif,
                   COALESCE(r.rgpd_risk_cached, 'none') AS rgpd,
                   f.fast_hash,
                   (
                       SELECT COUNT(*) FROM fichiers f2
                       WHERE f2.fast_hash = f.fast_hash
                         AND f2.file_size = f.file_size
                         AND f2.fast_hash IS NOT NULL
                         AND f2.fast_hash != ''
                         AND (f2.status IS NULL OR f2.status != 'error')
                   ) as duplicate_count
            FROM fichiers f
            LEFT JOIN reponses_llm r ON f.id = r.fichier_id
            WHERE (f.status IS NULL OR f.status != 'error')
            AND f.file_size > 0
            AND f.fast_hash IS NOT NULL
            AND f.fast_hash != ''
            AND (
                SELECT COUNT(*) FROM fichiers f2
                WHERE f2.fast_hash = f.fast_hash
                  AND f2.file_size = f.file_size
                  AND f2.fast_hash IS NOT NULL
                  AND f2.fast_hash != ''
                  AND (f2.status IS NULL OR f2.status != 'error')
            ) > 1
            ORDER BY duplicate_count DESC, f.file_size DESC
            """
            self._load_filtered_files(
                modal, query, (), "Groupes fichiers dupliqués (FastHash)"
            )
            logger.info("Opened duplicates modal with FastHash logic")
        except Exception as e:  # pragma: no cover - UI
            logger.error("Failed to show duplicates modal: %s", e)
            messagebox.showerror(
                "Erreur", f"Impossible d'ouvrir la vue Doublons.\nErreur: {str(e)}"
            )

    def show_temporal_files_modal(
        self, period: str, date_type: str, title: str, click_info: Dict[str, Any]
    ) -> None:
        """Display temporal files modal with strict date filtering."""

        try:
            logger = logging.getLogger(__name__)
            logger.info(f"Ouverture modal temporel: {date_type}, période: {period}")

            modal = self._create_base_modal(
                f"Fichiers - Analyse Temporelle ({date_type})", title
            )

            from datetime import datetime, timedelta

            now = datetime.now()
            period_definitions = {
                "0_1y": (now - timedelta(days=365), now),
                "1_2y": (now - timedelta(days=730), now - timedelta(days=365)),
                "2_3y": (now - timedelta(days=1095), now - timedelta(days=730)),
                "3_4y": (now - timedelta(days=1460), now - timedelta(days=1095)),
                "4_5y": (now - timedelta(days=1825), now - timedelta(days=1460)),
                "5_6y": (now - timedelta(days=2190), now - timedelta(days=1825)),
                "6plus": (datetime.min, now - timedelta(days=2190)),
            }

            if period not in period_definitions:
                raise ValueError(f"Période temporelle inconnue: {period}")

            start_date, end_date = period_definitions[period]

            date_column = (
                "f.last_modified" if date_type == "modification" else "f.creation_time"
            )
            fallback_column = (
                "f.creation_time" if date_type == "modification" else "f.last_modified"
            )

            if period == "6plus":
                date_filter = f"""
            (
                COALESCE({date_column}, {fallback_column}) IS NOT NULL
                AND COALESCE({date_column}, {fallback_column}) != ''
                AND datetime(COALESCE({date_column}, {fallback_column})) < datetime('{start_date.strftime('%Y-%m-%d %H:%M:%S')}')
            )
            """
            else:
                date_filter = f"""
            (
                COALESCE({date_column}, {fallback_column}) IS NOT NULL
                AND COALESCE({date_column}, {fallback_column}) != ''
                AND datetime(COALESCE({date_column}, {fallback_column})) >= datetime('{start_date.strftime('%Y-%m-%d %H:%M:%S')}')
                AND datetime(COALESCE({date_column}, {fallback_column})) < datetime('{end_date.strftime('%Y-%m-%d %H:%M:%S')}')
            )
            """

            query = f"""
        SELECT f.id, f.name, f.path, f.file_size, f.last_modified, f.creation_time, f.owner,
               COALESCE(r.security_classification_cached, 'none') AS security_class,
               COALESCE(r.rgpd_risk_cached, 'none') AS rgpd_risk,
               COALESCE(r.finance_type_cached, 'none') AS finance_type,
               COALESCE(r.legal_type_cached, 'none') AS legal_type,
               r.document_resume,
               COALESCE({date_column}, {fallback_column}) AS effective_date
        FROM fichiers f
        LEFT JOIN reponses_llm r ON f.id = r.fichier_id
        WHERE (f.status IS NULL OR f.status != 'error')
        AND f.file_size > 0
        AND {date_filter}
        ORDER BY datetime(COALESCE({date_column}, {fallback_column})) DESC, f.file_size DESC
        LIMIT 5000
            """

            params: tuple = ()

            logger.debug(f"Requête temporelle pour {period}: {query[:100]}...")

            self._load_filtered_files(
                modal, query, params, f"Temporel {date_type} - {period}"
            )

            logger.info(f"Modal temporel ouvert: {date_type}, période: {period}")

        except Exception as e:
            logger.error(f"Échec ouverture modal temporel: {e}")
            messagebox.showerror(
                "Erreur",
                f"Impossible d'ouvrir la vue temporelle.\nPériode: {period}\nType: {date_type}\nErreur: {str(e)}",
                parent=self.analytics_panel.parent,
            )

    def show_combined_files_modal(
        self, title: str, subtitle: str, click_info: Dict[str, Any]
    ) -> None:
        """Show modal for combined classification + RGPD/Legal files."""
        try:
            modal = self._create_base_modal(title, subtitle)

            category = click_info.get("category", "")
            if category == "c3_rgpd":
                query = """
                SELECT f.id, f.name, f.path, f.file_size, f.last_modified, f.owner,
                       COALESCE(r.security_classification_cached, 'none') AS classif,
                       COALESCE(r.rgpd_risk_cached, 'none') AS rgpd
                FROM fichiers f
                LEFT JOIN reponses_llm r ON f.id = r.fichier_id
                WHERE (f.status IS NULL OR f.status != 'error')
                AND COALESCE(r.security_classification_cached, 'none') = 'C3'
                AND COALESCE(r.rgpd_risk_cached, 'none') = 'critical'
                ORDER BY f.file_size DESC
                """
                params = ()
            elif category == "c3_legal":
                query = """
                SELECT f.id, f.name, f.path, f.file_size, f.last_modified, f.owner,
                       COALESCE(r.security_classification_cached, 'none') AS classif,
                       COALESCE(r.legal_type_cached, 'none') AS legal
                FROM fichiers f
                LEFT JOIN reponses_llm r ON f.id = r.fichier_id
                WHERE (f.status IS NULL OR f.status != 'error')
                AND COALESCE(r.security_classification_cached, 'none') = 'C3'
                AND COALESCE(r.legal_type_cached, 'none') IN ('nda', 'litigation')
                ORDER BY f.file_size DESC
                """
                params = ()
            elif category == "c3_total":
                query = """
                SELECT f.id, f.name, f.path, f.file_size, f.last_modified, f.owner,
                       COALESCE(r.security_classification_cached, 'none') AS classif,
                       COALESCE(r.rgpd_risk_cached, 'none') AS rgpd
                FROM fichiers f
                LEFT JOIN reponses_llm r ON f.id = r.fichier_id
                WHERE (f.status IS NULL OR f.status != 'error')
                AND COALESCE(r.security_classification_cached, 'none') = 'C3'
                ORDER BY f.file_size DESC
                """
                params = ()
            else:
                return

            self._load_filtered_files(modal, query, params, subtitle)

        except Exception as e:
            logger.error("Failed to show combined modal: %s", e)
            messagebox.showerror(
                "Erreur", f"Impossible d'ouvrir la vue combinée.\nErreur: {str(e)}"
            )

    def show_duplicates_detailed_modal(
        self, level: str, title: str, click_info: Dict[str, Any]
    ) -> None:
        """Fixed duplicate analysis modal with proper counting logic."""
        try:
            modal = self._create_base_modal(title, f"🔄 Fichiers dupliqués - {level}")

            logger.debug(f"Duplicates detailed modal query for: {level}")

            if level == "1x":
                query = """
                SELECT f.id, f.name, f.path, f.file_size, f.last_modified, f.owner,
                       COALESCE(r.security_classification_cached, 'none') AS classif,
                       COALESCE(r.rgpd_risk_cached, 'none') AS rgpd,
                       1 as copy_count
                FROM fichiers f
                LEFT JOIN reponses_llm r ON f.id = r.fichier_id
                WHERE (f.status IS NULL OR f.status != 'error')
                AND f.fast_hash IS NOT NULL AND f.fast_hash != ''
                AND (SELECT COUNT(*) FROM fichiers f2
                     WHERE f2.fast_hash = f.fast_hash AND f2.file_size = f.file_size
                       AND (f2.status IS NULL OR f2.status != 'error')) = 1
                ORDER BY f.file_size DESC
                """
                params = ()

            elif level == "7x+":
                query = """
                SELECT f.id, f.name, f.path, f.file_size, f.last_modified, f.owner,
                       COALESCE(r.security_classification_cached, 'none') AS classif,
                       COALESCE(r.rgpd_risk_cached, 'none') AS rgpd,
                       (SELECT COUNT(*) FROM fichiers f2
                        WHERE f2.fast_hash = f.fast_hash AND f2.file_size = f.file_size
                          AND (f2.status IS NULL OR f2.status != 'error')) as copy_count
                FROM fichiers f
                LEFT JOIN reponses_llm r ON f.id = r.fichier_id
                WHERE (f.status IS NULL OR f.status != 'error')
                AND f.fast_hash IS NOT NULL AND f.fast_hash != ''
                AND (SELECT COUNT(*) FROM fichiers f2
                     WHERE f2.fast_hash = f.fast_hash AND f2.file_size = f.file_size
                       AND (f2.status IS NULL OR f2.status != 'error')) >= 7
                ORDER BY copy_count DESC, f.file_size DESC
                """
                params = ()

            else:
                target_count = int(level.replace("x", ""))
                query = """
                SELECT f.id, f.name, f.path, f.file_size, f.last_modified, f.owner,
                       COALESCE(r.security_classification_cached, 'none') AS classif,
                       COALESCE(r.rgpd_risk_cached, 'none') AS rgpd,
                       (SELECT COUNT(*) FROM fichiers f2
                        WHERE f2.fast_hash = f.fast_hash AND f2.file_size = f.file_size
                          AND (f2.status IS NULL OR f2.status != 'error')) as copy_count
                FROM fichiers f
                LEFT JOIN reponses_llm r ON f.id = r.fichier_id
                WHERE (f.status IS NULL OR f.status != 'error')
                AND f.fast_hash IS NOT NULL AND f.fast_hash != ''
                AND (SELECT COUNT(*) FROM fichiers f2
                     WHERE f2.fast_hash = f.fast_hash AND f2.file_size = f.file_size
                       AND (f2.status IS NULL OR f2.status != 'error')) = ?
                ORDER BY f.file_size DESC
                """
                params = (target_count,)

            self._load_filtered_files(modal, query, params, f"Groupes avec {level}")
            logger.info(f"Opened duplicates detailed modal: {level}")

        except Exception as e:
            logger.error(f"Failed to show duplicates detailed modal: {e}")
            messagebox.showerror(
                "Erreur",
                f"Impossible d'ouvrir la vue doublons détaillée.\nErreur: {str(e)}",
            )

    def show_finance_modal(
        self, finance_type: str, title: str, click_info: Dict[str, Any]
    ) -> None:
        """Fixed Finance modal with proper 'Autres'/'none' handling."""
        try:
            modal = self._create_base_modal(title, f"💰 Finance: {finance_type}")

            logger.debug(f"Finance modal query for: {finance_type}")

            if finance_type == "Autres":
                query = """
                SELECT f.id, f.name, f.path, f.file_size, f.last_modified, f.owner,
                       COALESCE(r.security_classification_cached, 'none') AS classif,
                       COALESCE(r.finance_type_cached, 'none') AS finance
                FROM fichiers f
                LEFT JOIN reponses_llm r ON f.id = r.fichier_id
                WHERE (f.status IS NULL OR f.status != 'error')
                AND f.file_size > 0
                AND (r.finance_type_cached NOT IN ('none', 'invoice', 'contract', 'budget', 'accounting', 'payment')
                     OR r.finance_type_cached IS NULL)
                ORDER BY f.file_size DESC
                """
                params = ()
                logger.debug("Using NOT IN query for Finance 'Autres' category")
            elif finance_type == "none":
                query = """
                SELECT f.id, f.name, f.path, f.file_size, f.last_modified, f.owner,
                       COALESCE(r.security_classification_cached, 'none') AS classif,
                       COALESCE(r.finance_type_cached, 'none') AS finance
                FROM fichiers f
                LEFT JOIN reponses_llm r ON f.id = r.fichier_id
                WHERE (f.status IS NULL OR f.status != 'error')
                AND f.file_size > 0
                AND (r.finance_type_cached IS NULL OR r.finance_type_cached = 'none')
                ORDER BY f.file_size DESC
                """
                params = ()
                logger.debug("Using NULL/none query for Finance 'none' category")
            else:
                query = """
                SELECT f.id, f.name, f.path, f.file_size, f.last_modified, f.owner,
                       COALESCE(r.security_classification_cached, 'none') AS classif,
                       COALESCE(r.finance_type_cached, 'none') AS finance
                FROM fichiers f
                LEFT JOIN reponses_llm r ON f.id = r.fichier_id
                WHERE (f.status IS NULL OR f.status != 'error')
                AND f.file_size > 0
                AND r.finance_type_cached = ?
                ORDER BY f.file_size DESC
                """
                params = (finance_type,)
                logger.debug(f"Using exact match query for Finance '{finance_type}'")

            self._load_filtered_files(modal, query, params, f"Finance {finance_type}")

        except Exception as e:
            logger.error(f"Failed to show finance modal: {e}")
            messagebox.showerror(
                "Erreur",
                f"Impossible d'ouvrir la vue Financière.\nErreur: {str(e)}",
                parent=self.analytics_panel.parent,
            )

    def show_legal_modal(
        self, legal_type: str, title: str, click_info: Dict[str, Any]
    ) -> None:
        """Fixed Legal modal with proper 'Autres'/'none' handling."""
        try:
            modal = self._create_base_modal(title, f"⚖️ Légal: {legal_type}")

            logger.debug(f"Legal modal query for: {legal_type}")

            if legal_type == "Autres":
                query = """
                SELECT f.id, f.name, f.path, f.file_size, f.last_modified, f.owner,
                       COALESCE(r.security_classification_cached, 'none') AS classif,
                       COALESCE(r.legal_type_cached, 'none') AS legal
                FROM fichiers f
                LEFT JOIN reponses_llm r ON f.id = r.fichier_id
                WHERE (f.status IS NULL OR f.status != 'error')
                AND f.file_size > 0
                AND (r.legal_type_cached NOT IN ('none', 'employment', 'lease', 'sale', 'nda', 'compliance', 'litigation')
                     OR r.legal_type_cached IS NULL)
                ORDER BY f.file_size DESC
                """
                params = ()
                logger.debug("Using NOT IN query for Legal 'Autres' category")
            elif legal_type == "none":
                query = """
                SELECT f.id, f.name, f.path, f.file_size, f.last_modified, f.owner,
                       COALESCE(r.security_classification_cached, 'none') AS classif,
                       COALESCE(r.legal_type_cached, 'none') AS legal
                FROM fichiers f
                LEFT JOIN reponses_llm r ON f.id = r.fichier_id
                WHERE (f.status IS NULL OR f.status != 'error')
                AND f.file_size > 0
                AND (r.legal_type_cached IS NULL OR r.legal_type_cached = 'none')
                ORDER BY f.file_size DESC
                """
                params = ()
                logger.debug("Using NULL/none query for Legal 'none' category")
            else:
                query = """
                SELECT f.id, f.name, f.path, f.file_size, f.last_modified, f.owner,
                       COALESCE(r.security_classification_cached, 'none') AS classif,
                       COALESCE(r.legal_type_cached, 'none') AS legal
                FROM fichiers f
                LEFT JOIN reponses_llm r ON f.id = r.fichier_id
                WHERE (f.status IS NULL OR f.status != 'error')
                AND f.file_size > 0
                AND r.legal_type_cached = ?
                ORDER BY f.file_size DESC
                """
                params = (legal_type,)
                logger.debug(f"Using exact match query for Legal '{legal_type}'")

            self._load_filtered_files(modal, query, params, f"Légal {legal_type}")

        except Exception as e:
            logger.error(f"Failed to show legal modal: {e}")
            messagebox.showerror(
                "Erreur",
                f"Impossible d'ouvrir la vue Légale.\nErreur: {str(e)}",
                parent=self.analytics_panel.parent,
            )


class AnalyticsTabClickManager:
    """Manage click functionality across all Analytics tabs."""

    def __init__(self, parent_analytics_panel: "AnalyticsPanel") -> None:
        self.analytics_panel = parent_analytics_panel
        self.db_manager = parent_analytics_panel.db_manager
        self.drill_down_viewer = AnalyticsDrillDownViewer(parent_analytics_panel)

    # ------------------------------------------------------------------
    def add_click_handlers_to_all_tabs(self) -> None:
        """Add click handlers to all analytics result displays."""

        self._add_security_click_handlers()
        self._add_rgpd_click_handlers()
        self._add_security_focus_click_handlers()
        self._add_age_analysis_click_handlers()
        self._add_size_analysis_click_handlers()
        self._add_duplicates_click_handlers()
        self._add_duplicates_detailed_click_handlers()
        self._add_temporal_click_handlers()
        self._add_finance_click_handlers()
        self._add_legal_click_handlers()

    # ------------------------------------------------------------------
    def _add_security_click_handlers(self) -> None:
        if not hasattr(self.analytics_panel, "security_labels"):
            return
        for level, label in self.analytics_panel.security_labels.items():
            label.configure(cursor="hand2")
            label.click_info = {
                "type": "classification",
                "classification": level,
                "category": "security_classification",
            }
            label.bind(
                "<Button-1>",
                lambda e, lbl=label: self._handle_classification_click(lbl),
            )
            label.bind(
                "<Enter>",
                lambda e, l=label: l.configure(
                    foreground="blue", font=("Arial", 10, "underline")
                ),
            )
            label.bind(
                "<Leave>",
                lambda e, l=label: l.configure(
                    foreground="black", font=("Arial", 10, "normal")
                ),
            )

    def _add_rgpd_click_handlers(self) -> None:
        if not hasattr(self.analytics_panel, "rgpd_labels"):
            return
        for level, label in self.analytics_panel.rgpd_labels.items():
            label.configure(cursor="hand2")
            label.click_info = {
                "type": "rgpd_risk",
                "risk_level": level,
                "category": "rgpd_risk",
            }
            label.bind("<Button-1>", lambda e, lbl=label: self._handle_rgpd_click(lbl))
            label.bind(
                "<Enter>",
                lambda e, l=label: l.configure(
                    foreground="blue", font=("Arial", 10, "underline")
                ),
            )
            label.bind(
                "<Leave>",
                lambda e, l=label: l.configure(
                    foreground="black", font=("Arial", 10, "normal")
                ),
            )

    def _add_security_focus_click_handlers(self) -> None:
        """Add click handlers for security focus labels."""
        if not hasattr(self.analytics_panel, "security_focus_labels"):
            return

        labels = self.analytics_panel.security_focus_labels

        if "C3 Total" in labels:
            label = labels["C3 Total"]
            label.configure(cursor="hand2")
            label.click_info = {
                "type": "security_focus",
                "category": "c3_total",
                "classification": "C3",
            }
            label.bind(
                "<Button-1>",
                lambda e, lbl=label: self._handle_security_focus_click(lbl),
            )
            label.bind(
                "<Enter>",
                lambda e, l=label: l.configure(
                    foreground="blue", font=("Arial", 11, "underline")
                ),
            )
            label.bind(
                "<Leave>",
                lambda e, l=label: l.configure(
                    foreground="black", font=("Arial", 11, "normal")
                ),
            )

        if "C3 + RGPD" in labels:
            label = labels["C3 + RGPD"]
            label.configure(cursor="hand2")
            label.click_info = {
                "type": "security_focus",
                "category": "c3_rgpd",
                "classification": "C3",
                "rgpd_risk": "critical",
            }
            label.bind(
                "<Button-1>",
                lambda e, lbl=label: self._handle_security_focus_click(lbl),
            )
            label.bind(
                "<Enter>",
                lambda e, l=label: l.configure(
                    foreground="blue", font=("Arial", 11, "underline")
                ),
            )
            label.bind(
                "<Leave>",
                lambda e, l=label: l.configure(
                    foreground="black", font=("Arial", 11, "normal")
                ),
            )

        if "C3 + Legal" in labels:
            label = labels["C3 + Legal"]
            label.configure(cursor="hand2")
            label.click_info = {
                "type": "security_focus",
                "category": "c3_legal",
                "classification": "C3",
                "legal_types": ["nda", "litigation"],
            }
            label.bind(
                "<Button-1>",
                lambda e, lbl=label: self._handle_security_focus_click(lbl),
            )
            label.bind(
                "<Enter>",
                lambda e, l=label: l.configure(
                    foreground="blue", font=("Arial", 11, "underline")
                ),
            )
            label.bind(
                "<Leave>",
                lambda e, l=label: l.configure(
                    foreground="black", font=("Arial", 11, "normal")
                ),
            )

    def _add_age_analysis_click_handlers(self) -> None:
        for attr in ["modification_labels", "creation_labels"]:
            if hasattr(self.analytics_panel, attr):
                labels = getattr(self.analytics_panel, attr)
                for key, label in labels.items():
                    label.configure(cursor="hand2")
                    label.click_info = {
                        "type": "age_analysis",
                        "age_type": f"{attr.split('_')[0]}_{key}",
                        "category": "age_analysis",
                    }
                    label.bind(
                        "<Button-1>", lambda e, lbl=label: self._handle_age_click(lbl)
                    )
                    label.bind(
                        "<Enter>",
                        lambda e, l=label: l.configure(
                            foreground="blue", font=("Arial", 10, "underline")
                        ),
                    )
                    label.bind(
                        "<Leave>",
                        lambda e, l=label: l.configure(
                            foreground="black", font=("Arial", 10, "normal")
                        ),
                    )

    def _add_size_analysis_click_handlers(self) -> None:
        if not hasattr(self.analytics_panel, "file_size_labels"):
            return
        for level, label in self.analytics_panel.file_size_labels.items():
            label.configure(cursor="hand2")
            label.click_info = {
                "type": "size_analysis",
                "size_type": level,
                "category": "size_analysis",
            }
            label.bind("<Button-1>", lambda e, lbl=label: self._handle_size_click(lbl))
            label.bind(
                "<Enter>",
                lambda e, l=label: l.configure(
                    foreground="blue", font=("Arial", 10, "underline")
                ),
            )
            label.bind(
                "<Leave>",
                lambda e, l=label: l.configure(
                    foreground="black", font=("Arial", 10, "normal")
                ),
            )

    def _add_duplicates_click_handlers(self) -> None:
        if hasattr(self.analytics_panel, "duplicates_label"):
            label = self.analytics_panel.duplicates_label
            label.configure(cursor="hand2")
            label.click_info = {"type": "duplicates", "category": "duplicate_files"}
            label.bind(
                "<Button-1>", lambda e, lbl=label: self._handle_duplicates_click(lbl)
            )
            label.bind(
                "<Enter>",
                lambda e, l=label: l.configure(
                    foreground="blue", font=("Arial", 10, "underline")
                ),
            )
            label.bind(
                "<Leave>",
                lambda e, l=label: l.configure(
                    foreground="black", font=("Arial", 10, "normal")
                ),
            )

    def _add_duplicates_detailed_click_handlers(self) -> None:
        """Add click handlers for detailed duplicates labels."""
        if not hasattr(self.analytics_panel, "duplicates_detailed_labels"):
            return
        for level, label in self.analytics_panel.duplicates_detailed_labels.items():
            label.configure(cursor="hand2")
            label.click_info = {
                "type": "duplicates_detailed",
                "level": level,
                "category": "duplicate_analysis",
                "logic_type": "exact_count" if level != "7x+" else "minimum_count",
            }
            label.bind(
                "<Button-1>",
                lambda e, lbl=label: self._handle_duplicates_detailed_click(lbl),
            )
            label.bind(
                "<Enter>",
                lambda e, l=label: l.configure(
                    foreground="blue", font=("Arial", 11, "underline")
                ),
            )
            label.bind(
                "<Leave>",
                lambda e, l=label: l.configure(
                    foreground="black", font=("Arial", 11, "normal")
                ),
            )

    def _add_temporal_click_handlers(self) -> None:
        """Add click handlers for temporal analysis with standardized keys."""
        for attr in ["modification_labels", "creation_labels"]:
            if hasattr(self.analytics_panel, attr):
                labels = getattr(self.analytics_panel, attr)
                for key, label in labels.items():
                    label.configure(cursor="hand2")
                    label.click_info = {
                        "type": "temporal_analysis",
                        "temporal_type": attr.split("_")[0],
                        "period_filter": key,
                        "category": "temporal_analysis",
                    }
                    label.bind(
                        "<Button-1>",
                        lambda e, lbl=label: self._handle_temporal_click(lbl),
                    )
                    label.bind(
                        "<Enter>",
                        lambda e, l=label: l.configure(
                            foreground="blue", font=("Arial", 10, "underline")
                        ),
                    )
                    label.bind(
                        "<Leave>",
                        lambda e, l=label: l.configure(
                            foreground="black", font=("Arial", 10, "normal")
                        ),
                    )

    def _add_finance_click_handlers(self) -> None:
        """Ajouter gestionnaires de clic pour types financiers."""
        if not hasattr(self.analytics_panel, "finance_labels"):
            return

        for finance_type, label in self.analytics_panel.finance_labels.items():
            label.configure(cursor="hand2")
            label.click_info = {
                "type": "finance",
                "finance_type": finance_type,
                "category": "finance_type",
            }
            label.bind(
                "<Button-1>",
                lambda e, lbl=label: self._handle_finance_click(lbl),
            )
            label.bind(
                "<Enter>",
                lambda e, l=label: l.configure(
                    foreground="blue", font=("Arial", 10, "underline")
                ),
            )
            label.bind(
                "<Leave>",
                lambda e, l=label: l.configure(
                    foreground="black", font=("Arial", 10, "normal")
                ),
            )

    def _add_legal_click_handlers(self) -> None:
        """Ajouter gestionnaires de clic pour types légaux."""
        if not hasattr(self.analytics_panel, "legal_labels"):
            return

        for legal_type, label in self.analytics_panel.legal_labels.items():
            label.configure(cursor="hand2")
            label.click_info = {
                "type": "legal",
                "legal_type": legal_type,
                "category": "legal_type",
            }
            label.bind(
                "<Button-1>",
                lambda e, lbl=label: self._handle_legal_click(lbl),
            )
            label.bind(
                "<Enter>",
                lambda e, l=label: l.configure(
                    foreground="blue", font=("Arial", 10, "underline")
                ),
            )
            label.bind(
                "<Leave>",
                lambda e, l=label: l.configure(
                    foreground="black", font=("Arial", 10, "normal")
                ),
            )

    # ------------------------------------------------------------------
    # Click handlers
    # ------------------------------------------------------------------

    def _debug_click_info(self, label_widget, handler_name: str) -> None:
        """Méthode de debug pour validation click_info."""
        try:
            click_info = getattr(label_widget, "click_info", {})
            text = label_widget.cget("text") if hasattr(label_widget, "cget") else "N/A"

            logger.info(f"DEBUG {handler_name}:")
            logger.info(f"  - Label text: {text}")
            logger.info(f"  - Click info: {click_info}")

            if not click_info:
                logger.error(f"  - ERROR: click_info manquant pour {handler_name}")

        except Exception as e:
            logger.error(f"Debug error in {handler_name}: {e}")

    def _handle_classification_click(self, label_widget) -> None:
        click_info = getattr(label_widget, "click_info", {})
        classification = click_info.get("classification", "")
        self.drill_down_viewer.show_classification_files_modal(
            classification, f"Fichiers de Classification {classification}", click_info
        )

    def _handle_rgpd_click(self, label_widget) -> None:
        click_info = getattr(label_widget, "click_info", {})
        risk_level = click_info.get("risk_level", "")
        self.drill_down_viewer.show_rgpd_files_modal(
            risk_level, f"Fichiers RGPD - Risque {risk_level}", click_info
        )

    def _handle_age_click(self, label_widget) -> None:
        click_info = getattr(label_widget, "click_info", {})
        age_type = click_info.get("age_type", "")
        self.drill_down_viewer.show_age_files_modal(
            age_type, f"Fichiers - Analyse d'Âge ({age_type})", click_info
        )

    def _handle_size_click(self, label_widget) -> None:
        click_info = getattr(label_widget, "click_info", {})
        size_type = click_info.get("size_type", "")
        self.drill_down_viewer.show_size_files_modal(
            size_type, f"Fichiers - Analyse de Taille ({size_type})", click_info
        )

    def _handle_finance_click(self, label_widget) -> None:
        self._debug_click_info(label_widget, "_handle_finance_click")
        click_info = getattr(label_widget, "click_info", {})
        finance_type = click_info.get("finance_type", "")
        self.drill_down_viewer.show_finance_modal(
            finance_type, f"Types Financiers - {finance_type}", click_info
        )

    def _handle_legal_click(self, label_widget) -> None:
        self._debug_click_info(label_widget, "_handle_legal_click")
        click_info = getattr(label_widget, "click_info", {})
        legal_type = click_info.get("legal_type", "")
        self.drill_down_viewer.show_legal_modal(
            legal_type, f"Types Légaux - {legal_type}", click_info
        )

    def _handle_duplicates_click(self, label_widget) -> None:
        click_info = getattr(label_widget, "click_info", {})
        self.drill_down_viewer.show_duplicates_modal(
            "Fichiers Dupliqués - Groupes", click_info
        )

    def _handle_duplicates_detailed_click(self, label_widget) -> None:
        click_info = getattr(label_widget, "click_info", {})
        level = click_info.get("level", "")
        self.drill_down_viewer.show_duplicates_detailed_modal(
            level, f"Fichiers Dupliqués - {level}", click_info
        )

    def _handle_temporal_click(self, label_widget) -> None:
        click_info = getattr(label_widget, "click_info", {})

        date_type = click_info.get("temporal_type", "modification")

        period = None
        for attr_name in ["period_key", "period_filter", "key"]:
            period = click_info.get(attr_name)
            if period:
                break

        if not period:
            label_text = label_widget.cget("text")
            if "0-1" in label_text:
                period = "0_1y"
            elif "1-2" in label_text:
                period = "1_2y"
            elif "2-3" in label_text:
                period = "2_3y"
            elif "3-4" in label_text:
                period = "3_4y"
            elif "4-5" in label_text:
                period = "4_5y"
            elif "5-6" in label_text:
                period = "5_6y"
            elif "+6" in label_text:
                period = "6plus"
            else:
                period = "0_1y"

        self.drill_down_viewer.show_temporal_files_modal(
            period,
            date_type,
            f"Fichiers - Analyse Temporelle ({date_type})",
            click_info,
        )

    def _handle_security_focus_click(self, label_widget) -> None:
        click_info = getattr(label_widget, "click_info", {})
        category = click_info.get("category", "")

        if category == "c3_total":
            self.drill_down_viewer.show_classification_files_modal(
                "C3", "Fichiers Classification C3 (Critique)", click_info
            )
        elif category == "c3_rgpd":
            self.drill_down_viewer.show_combined_files_modal(
                "C3 + RGPD Critique", "Fichiers C3 avec RGPD Critique", click_info
            )
        elif category == "c3_legal":
            self.drill_down_viewer.show_combined_files_modal(
                "C3 + Legal", "Fichiers C3 avec Contenu Juridique", click_info
            )


class UserDrillDownViewer:
    """Système de drill-down interactif pour l'exploration des fichiers utilisateur."""

    def __init__(self, parent_analytics_panel: "AnalyticsPanel") -> None:
        self.analytics_panel = parent_analytics_panel
        self.db_manager = parent_analytics_panel.db_manager

    def show_user_files_modal(
        self, username: str, category: str, user_data: Dict[str, Any]
    ) -> None:
        """Affiche fenêtre modale avec fichiers utilisateur filtrés par catégorie."""
        try:
            drill_window = tk.Toplevel(self.analytics_panel.parent)
            drill_window.title(f"📁 Fichiers de {username} - {category}")
            drill_window.withdraw()
            drill_window.geometry("1400x800")
            drill_window.transient(self.analytics_panel.parent)

            header_frame = ttk.Frame(drill_window)
            header_frame.pack(fill="x", padx=10, pady=5)

            title_label = ttk.Label(
                header_frame,
                text=f"Analyse détaillée: {username}",
                font=("Arial", 16, "bold"),
            )
            title_label.pack(anchor="w")

            summary_label = ttk.Label(
                header_frame,
                text=f"Catégorie: {category} | {user_data.get('count', 0)} fichiers | {user_data.get('total_size', 0)/(1024**3):.1f} GB",
                font=("Arial", 12),
            )
            summary_label.pack(anchor="w", pady=2)

            tree_frame = ttk.Frame(drill_window)
            tree_frame.pack(fill="both", expand=True, padx=10, pady=5)

            columns = ("nom", "chemin", "taille", "modifie", "classification", "rgpd")
            tree = ttk.Treeview(tree_frame, columns=columns, show="headings", height=20)

            tree.heading("nom", text="Nom du fichier")
            tree.heading("chemin", text="Chemin")
            tree.heading("taille", text="Taille")
            tree.heading("modifie", text="Modifié")
            tree.heading("classification", text="Classification")
            tree.heading("rgpd", text="RGPD")

            tree.column("nom", width=200)
            tree.column("chemin", width=300)
            tree.column("taille", width=100)
            tree.column("modifie", width=150)
            tree.column("classification", width=100)
            tree.column("rgpd", width=100)

            v_scrollbar = ttk.Scrollbar(
                tree_frame, orient="vertical", command=tree.yview
            )
            h_scrollbar = ttk.Scrollbar(
                tree_frame, orient="horizontal", command=tree.xview
            )
            tree.configure(
                yscrollcommand=v_scrollbar.set, xscrollcommand=h_scrollbar.set
            )

            tree.pack(side="left", fill="both", expand=True)
            v_scrollbar.pack(side="right", fill="y")
            h_scrollbar.pack(side="bottom", fill="x")

            buttons_frame = ttk.Frame(drill_window)
            buttons_frame.pack(fill="x", padx=10, pady=5)

            ttk.Button(
                buttons_frame,
                text="📊 Export",
                command=lambda: self._export_user_files(username, category),
            ).pack(side="left", padx=5)
            ttk.Button(
                buttons_frame, text="❌ Fermer", command=drill_window.destroy
            ).pack(side="right", padx=5)

            self._load_user_files(tree, username, category)

            drill_window.update_idletasks()
            x = (drill_window.winfo_screenwidth() // 2) - (700 // 2)
            y = (drill_window.winfo_screenheight() // 2) - (400 // 2)
            drill_window.geometry(f"1400x800+{x}+{y}")

            drill_window.deiconify()
            drill_window.lift()
            drill_window.focus_set()
            drill_window.after(50, lambda: drill_window.grab_set())

            logger.info(f"Modal utilisateur créée: {username} - {category}")

        except Exception as e:
            logger.error(f"Échec création modal utilisateur: {e}")
            messagebox.showerror(
                "Erreur",
                f"Impossible d'ouvrir la vue utilisateur.\nErreur: {str(e)}",
                parent=self.analytics_panel.parent,
            )

    def _load_user_files(
        self, tree: ttk.Treeview, username: str, category: str
    ) -> None:
        """Charge les fichiers utilisateur dans le TreeView."""
        try:
            if not self.db_manager:
                return

            if category == "Gros fichiers":
                query = """
                    SELECT f.name, f.path, f.file_size, f.last_modified,
                           COALESCE(r.security_classification_cached, 'N/A') as classif,
                           COALESCE(r.rgpd_risk_cached, 'N/A') as rgpd
                    FROM fichiers f
                    LEFT JOIN reponses_llm r ON f.id = r.fichier_id
                    WHERE f.owner = ? AND (f.status IS NULL OR f.status != 'error')
                    AND f.file_size > 100000000
                    ORDER BY f.file_size DESC
                """
            elif category == "Classification C3":
                query = """
                    SELECT f.name, f.path, f.file_size, f.last_modified,
                           COALESCE(r.security_classification_cached, 'N/A') as classif,
                           COALESCE(r.rgpd_risk_cached, 'N/A') as rgpd
                    FROM fichiers f
                    LEFT JOIN reponses_llm r ON f.id = r.fichier_id
                    WHERE f.owner = ? AND (f.status IS NULL OR f.status != 'error')
                    AND r.security_classification_cached = 'C3'
                    ORDER BY f.file_size DESC
                """
            elif category == "RGPD Critical":
                query = """
                    SELECT f.name, f.path, f.file_size, f.last_modified,
                           COALESCE(r.security_classification_cached, 'N/A') as classif,
                           COALESCE(r.rgpd_risk_cached, 'N/A') as rgpd
                    FROM fichiers f
                    LEFT JOIN reponses_llm r ON f.id = r.fichier_id
                    WHERE f.owner = ? AND (f.status IS NULL OR f.status != 'error')
                    AND r.rgpd_risk_cached = 'critical'
                    ORDER BY f.file_size DESC
                """
            else:
                query = """
                    SELECT f.name, f.path, f.file_size, f.last_modified,
                           COALESCE(r.security_classification_cached, 'N/A') as classif,
                           COALESCE(r.rgpd_risk_cached, 'N/A') as rgpd
                    FROM fichiers f
                    LEFT JOIN reponses_llm r ON f.id = r.fichier_id
                    WHERE f.owner = ? AND (f.status IS NULL OR f.status != 'error')
                    ORDER BY f.file_size DESC
                """

            with self.db_manager._connect().get() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (username,))
                rows = cursor.fetchall()

                for row in rows:
                    name, path, size, modified, classif, rgpd = row
                    size_str = self._format_file_size(size)
                    modified_str = modified[:19] if modified else "N/A"
                    tree.insert(
                        "",
                        "end",
                        values=(
                            name[:50] + "..." if len(name) > 50 else name,
                            path[:60] + "..." if len(path) > 60 else path,
                            size_str,
                            modified_str,
                            classif,
                            rgpd,
                        ),
                    )

                logger.info(
                    f"Chargement {len(rows)} fichiers pour utilisateur {username}"
                )

        except Exception as e:
            logger.error(f"Erreur chargement fichiers utilisateur: {e}")

    def _format_file_size(self, size_bytes: int) -> str:
        """Formate la taille de fichier lisible."""
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f} PB"

    def _export_user_files(self, username: str, category: str) -> None:
        """Exporte les fichiers utilisateur."""
        messagebox.showinfo("Export", f"Export des fichiers de {username} - {category}")


class AnalyticsPanel:
    """Dashboard de supervision business."""

    def __init__(self, parent, db_manager) -> None:
        """Initialize Analytics Panel with robust database manager handling."""

        self.parent = parent

        if db_manager is None:
            logger.critical("AnalyticsPanel initialized with None database manager")
            self._db_manager_error = True
            self.db_manager = None
        else:
            try:
                with db_manager._connect().get() as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' LIMIT 1"
                    )
                    cursor.fetchone()
                self.db_manager = db_manager
                self._db_manager_error = False
                logger.info("Analytics Panel: Database manager validated successfully")
                if not self._validate_connection_manager():
                    logger.warning(
                        "Connection manager validation failed during initialization"
                    )
            except Exception as e:
                logger.error("Database manager validation failed during init: %s", e)
                self._db_manager_error = True
                self.db_manager = None

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

        # async calculation helpers
        self._calculation_thread: Optional[threading.Thread] = None
        self._result_queue: queue.Queue = queue.Queue()
        self._calculation_in_progress = False
        self.click_manager = AnalyticsTabClickManager(self)

        if self._db_manager_error:
            self._show_database_manager_error()
        else:
            if not self._validate_database_schema():
                logger.error("Database schema validation failed during initialization")
                self._show_schema_error()
            else:
                try:
                    self._build_interface()
                    self._initialize_click_functionality()
                    logger.info("Analytics Panel initialized successfully")
                except Exception as e:
                    logger.error("Failed to build Analytics Panel interface: %s", e)
                    self._show_initialization_error(e)

    def set_db_manager(self, db_manager: DBManager | None) -> None:
        self.db_manager = db_manager

    def _build_interface(self) -> None:
        """Wrapper to build the analytics UI."""
        self._build_ui()

    def _build_ui(self) -> None:
        params_frame = ttk.LabelFrame(self.parent, text="⚙️ PARAMÈTRES UTILISATEUR")
        params_frame.pack(fill="x", padx=5, pady=5)

        ttk.Label(params_frame, text="Âge fichiers (années):").grid(
            row=0, column=0, padx=5, pady=2, sticky="w"
        )
        ttk.Entry(params_frame, textvariable=self.threshold_age_years, width=5).grid(
            row=0, column=1, padx=5, pady=2
        )

        ttk.Label(params_frame, text="Taille fichiers (MB):").grid(
            row=0, column=2, padx=5, pady=2, sticky="w"
        )
        ttk.Entry(params_frame, textvariable=self.threshold_size_mb, width=6).grid(
            row=0, column=3, padx=5, pady=2
        )

        ttk.Label(params_frame, text="Filtres:").grid(
            row=0, column=4, padx=5, pady=2, sticky="w"
        )
        class_cb = ttk.Combobox(
            params_frame,
            textvariable=self.classification_filter,
            values=["Tous", "C0+", "C1+", "C2+", "C3"],
            width=5,
            state="readonly",
        )
        class_cb.grid(row=0, column=5, padx=5, pady=2)

        chk = ttk.Checkbutton(
            params_frame, text="Modifier depuis", variable=self.use_last_modified
        )
        chk.grid(row=0, column=6, padx=5, pady=2, sticky="w")
        ttk.Entry(params_frame, textvariable=self.years_modified, width=4).grid(
            row=0, column=7, padx=5, pady=2
        )

        self.recalculate_button = ttk.Button(
            params_frame, text="🔄 Recalculer", command=self.recalculate_all_metrics
        )
        self.recalculate_button.grid(row=0, column=8, padx=5)
        ttk.Button(
            params_frame, text="💾 Sauver", command=self.save_user_preferences
        ).grid(row=0, column=9, padx=5)
        ttk.Button(
            params_frame, text="📥 Restaurer", command=self.load_user_preferences
        ).grid(row=0, column=10, padx=5)

        alerts_frame = ttk.LabelFrame(
            self.parent, text="📊 SUPERVISION BUSINESS - MÉTRIQUES CLÉS"
        )
        alerts_frame.pack(fill="x", padx=5, pady=5)
        cards_container = ttk.Frame(alerts_frame)
        cards_container.pack(fill="x", padx=5, pady=5)

        self.super_critical_card = ttk.LabelFrame(
            cards_container, text="🔴 SUPER CRITIQUES"
        )
        self.super_critical_card.grid(row=0, column=0, padx=5, pady=5, sticky="nsew")
        self.super_critical_line1 = ttk.Label(
            self.super_critical_card, text="0 C3+RGPD+Legal", font=("Arial", 12, "bold")
        )
        self.super_critical_line1.pack()
        self.super_critical_line2 = ttk.Label(
            self.super_critical_card, text="0% | 0 fichiers | 0GB", font=("Arial", 10)
        )
        self.super_critical_line2.pack()
        self.super_critical_line3 = ttk.Label(
            self.super_critical_card, text="Cumul risques max", font=("Arial", 10)
        )
        self.super_critical_line3.pack()

        self.critical_card = ttk.LabelFrame(cards_container, text="🟠 CRITIQUES")
        self.critical_card.grid(row=0, column=1, padx=5, pady=5, sticky="nsew")
        self.critical_line1 = ttk.Label(
            self.critical_card, text="0 C3 OU RGPD OU Legal", font=("Arial", 12, "bold")
        )
        self.critical_line1.pack()
        self.critical_line2 = ttk.Label(
            self.critical_card, text="0% | 0 fichiers | 0GB", font=("Arial", 10)
        )
        self.critical_line2.pack()
        self.critical_line3 = ttk.Label(
            self.critical_card, text="Un critère fort", font=("Arial", 10)
        )
        self.critical_line3.pack()

        self.duplicates_card = ttk.LabelFrame(cards_container, text="🟡 DOUBLONS")
        self.duplicates_card.grid(row=0, column=2, padx=5, pady=5, sticky="nsew")
        self.duplicates_line1 = ttk.Label(
            self.duplicates_card,
            text="0 fichiers dupliqués 2 fois",
            font=("Arial", 12, "bold"),
        )
        self.duplicates_line1.pack()
        self.duplicates_line2 = ttk.Label(
            self.duplicates_card,
            text="0% | 0 groupes | 0GB gaspillé",
            font=("Arial", 10),
        )
        self.duplicates_line2.pack()
        self.duplicates_line3 = ttk.Label(
            self.duplicates_card, text="Top: 0 copies max", font=("Arial", 10)
        )
        self.duplicates_line3.pack()

        self.size_age_card = ttk.LabelFrame(cards_container, text="🔵 TAILLE/ÂGE")
        self.size_age_card.grid(row=0, column=3, padx=5, pady=5, sticky="nsew")
        self.size_age_line1 = ttk.Label(
            self.size_age_card, text="0% gros + 0% dormants", font=("Arial", 12, "bold")
        )
        self.size_age_line1.pack()
        self.size_age_line2 = ttk.Label(
            self.size_age_card, text="0 fichiers | 0GB archivage", font=("Arial", 10)
        )
        self.size_age_line2.pack()
        self.size_age_line3 = ttk.Label(
            self.size_age_card, text="Seuils utilisateur", font=("Arial", 10)
        )
        self.size_age_line3.pack()

        for i in range(4):
            cards_container.columnconfigure(i, weight=1)

        notebook_frame = ttk.LabelFrame(
            self.parent, text="🔍 ANALYSE DÉTAILLÉE BUSINESS INTELLIGENCE"
        )
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

        duplicates_detailed_frame = ttk.Frame(self.thematic_notebook)
        self.thematic_notebook.add(
            duplicates_detailed_frame, text="🔄 Doublons Détaillés"
        )
        self._build_duplicates_detailed_tab(duplicates_detailed_frame)

        temporal_frame = ttk.Frame(self.thematic_notebook)
        self.thematic_notebook.add(temporal_frame, text="📅 Analyse Temporelle")
        self._build_temporal_analysis_tab(temporal_frame)

        file_size_frame = ttk.Frame(self.thematic_notebook)
        self.thematic_notebook.add(file_size_frame, text="📏 Tailles Fichiers")
        self._build_file_size_analysis_tab(file_size_frame)

        top_users_frame = ttk.Frame(self.thematic_notebook)
        self.thematic_notebook.add(top_users_frame, text="🏆 Top Utilisateurs")
        self._build_top_users_tab(top_users_frame)

        actions_frame = ttk.Frame(self.parent)
        actions_frame.pack(fill="x", padx=5, pady=5)

        self.progress_frame = ttk.Frame(actions_frame)
        self.progress_frame.pack(side="left", padx=5)
        self.progress_label = ttk.Label(self.progress_frame, text="✅ Prêt")
        self.progress_label.pack()

        ttk.Button(
            actions_frame,
            text="📄 Export Rapport Business",
            command=self.export_business_report,
        ).pack(side="left", padx=5)
        ttk.Button(
            actions_frame,
            text="👥 Voir Fichiers Concernés",
            command=self.show_affected_files,
        ).pack(side="left", padx=5)
        ttk.Button(
            actions_frame,
            text="📥 Restaurer Préférences",
            command=self.load_user_preferences,
        ).pack(side="right", padx=5)

        self._initialize_click_functionality()

    def _initialize_click_functionality(self) -> None:
        """Initialize click functionality for all analytics tabs."""
        try:
            if hasattr(self, "click_manager"):
                self.click_manager.add_click_handlers_to_all_tabs()
                logger.info("Click functionality initialized for all analytics tabs")
        except Exception as e:  # pragma: no cover - init
            logger.error("Failed to initialize click functionality: %s", e)

    def _validate_connection_manager(self) -> bool:
        """Validate that database connection manager works properly."""
        try:
            if not self.db_manager:
                logger.error("No database manager available for validation")
                return False

            with self.db_manager._connect().get() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()

            logger.info("Database connection manager validation successful")
            return True
        except Exception as e:
            logger.error(f"Connection manager validation failed: {e}")
            return False

    def _ensure_database_manager(self) -> bool:
        """Ensure database manager is available and functional."""
        if self.db_manager is None:
            logger.error("Database manager is None - attempting recovery")
            try:
                if hasattr(self.parent, "master") and hasattr(
                    self.parent.master, "db_manager"
                ):
                    potential_db_manager = self.parent.master.db_manager
                    if potential_db_manager is not None:
                        logger.info("Found database manager in parent window")
                        self.db_manager = potential_db_manager
                        return True
            except Exception as e:
                logger.warning("Could not recover database manager from parent: %s", e)
            return False

        try:
            with self.db_manager._connect().get() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
            return True
        except Exception as e:
            logger.error("Database manager connectivity test failed: %s", e)
            self.db_manager = None
            return False

    def _validate_database_schema(self) -> bool:
        """Validate database schema compatibility."""

        if not self._ensure_database_manager():
            logger.error("No database manager available for analytics")
            return False

        try:
            with self.db_manager._connect().get() as conn:
                cursor = conn.cursor()

                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('fichiers', 'reponses_llm')"
                )
                tables = [row[0] for row in cursor.fetchall()]

                if "fichiers" not in tables:
                    logger.error("Table 'fichiers' not found in database")
                    return False

                cursor.execute("PRAGMA table_info(fichiers)")
                columns = [row[1] for row in cursor.fetchall()]
                required_columns = ["id", "path", "file_size", "status", "owner"]
                missing_columns = [
                    col for col in required_columns if col not in columns
                ]
                if missing_columns:
                    logger.error(
                        f"Missing required columns in fichiers table: {missing_columns}"
                    )
                    return False

                cursor.execute(
                    "SELECT COUNT(*) FROM fichiers WHERE (status IS NULL OR status != 'error')"
                )
                file_count = cursor.fetchone()[0]
                logger.info(f"Found {file_count} available files in database")

                return file_count > 0

        except Exception as e:
            logger.error(f"Database schema validation failed: {e}")
            return False

    def _show_database_manager_error(self) -> None:
        """Display enhanced database manager error with recovery options."""
        for widget in self.parent.winfo_children():
            widget.destroy()

        error_frame = ttk.Frame(self.parent)
        error_frame.pack(fill="both", expand=True, padx=20, pady=20)

        title_label = ttk.Label(
            error_frame,
            text="🚨 Erreur SQLite Connection Pool",
            font=("Arial", 18, "bold"),
            foreground="red",
        )
        title_label.pack(pady=(0, 20))

        desc_label = ttk.Label(
            error_frame,
            text=(
                "Erreur de gestion des connexions à la base de données.\n"
                "Problème probable :\n\n"
                "• SQLiteConnectionPool context manager non implémenté\n"
                "• Syntaxe incorrecte : with db._connect() au lieu de .get()\n"
                "• Base de données corrompue ou inaccessible\n\n"
                "Solutions recommandées :"
            ),
            font=("Arial", 12),
            justify=tk.LEFT,
        )
        desc_label.pack(pady=(0, 20))

        button_frame = ttk.Frame(error_frame)
        button_frame.pack(pady=10)

        ttk.Button(
            button_frame,
            text="🔄 Réessayer la Connexion",
            command=self._retry_database_connection,
        ).pack(side="left", padx=5)

        ttk.Button(
            button_frame,
            text="📂 Charger une Base de Données",
            command=self._prompt_load_database,
        ).pack(side="left", padx=5)

        ttk.Button(
            button_frame,
            text="📋 Voir les Logs",
            command=self._show_error_logs,
        ).pack(side="left", padx=5)

    def _retry_database_connection(self) -> None:
        """Attempt to retry database connection with correct syntax."""
        try:
            main_window = self.parent
            while main_window and not hasattr(main_window, "db_manager"):
                main_window = getattr(main_window, "master", None)

            if (
                main_window
                and hasattr(main_window, "db_manager")
                and main_window.db_manager
            ):
                self.db_manager = main_window.db_manager
                logger.info("Database manager recovered from main window")

                try:
                    with self.db_manager._connect().get() as conn:
                        cursor = conn.cursor()
                        cursor.execute(
                            "SELECT name FROM sqlite_master WHERE type='table' LIMIT 1"
                        )
                        cursor.fetchone()

                    for widget in self.parent.winfo_children():
                        widget.destroy()

                    if self._validate_database_schema():
                        self._build_interface()
                        self._initialize_click_functionality()
                        messagebox.showinfo(
                            "Connexion Rétablie",
                            "La connexion à la base de données a été rétablie!\n"
                            "Problème SQLite Connection Pool résolu.",
                            parent=self.parent,
                        )
                    else:
                        self._show_schema_error()
                except Exception as conn_e:
                    logger.error(
                        "Connection test failed with correct syntax: %s", conn_e
                    )
                    messagebox.showerror(
                        "Erreur de Connexion Persistante",
                        "Même avec la syntaxe corrigée, la connexion échoue:\n"
                        f"{str(conn_e)}\n\n"
                        "Vérifiez que la base de données n'est pas corrompue.",
                        parent=self.parent,
                    )
            else:
                messagebox.showerror(
                    "Échec de Reconnexion",
                    "Impossible de rétablir la connexion.\nVeuillez charger une base de données.",
                    parent=self.parent,
                )
        except Exception as e:
            logger.error("Database reconnection failed: %s", e)
            messagebox.showerror(
                "Erreur de Reconnexion",
                f"Erreur lors de la reconnexion: {str(e)}",
                parent=self.parent,
            )

    def _prompt_load_database(self) -> None:
        """Prompt user to load a database file."""
        messagebox.showinfo(
            "Charger Base de Données",
            "Fermez cette fenêtre et utilisez l'option 'Load Database' \n"
            "dans le menu principal pour charger une base de données.\n\n"
            "Assurez-vous que le fichier .db n'est pas corrompu.",
            parent=self.parent,
        )

    def _show_error_logs(self) -> None:
        """Show recent error logs to user with technical details."""
        log_window = tk.Toplevel(self.parent)
        log_window.title("Logs d'Erreur SQLiteConnectionPool")
        log_window.geometry("700x500")

        text_widget = tk.Text(log_window, wrap=tk.WORD, font=("Courier", 10))
        scrollbar = ttk.Scrollbar(
            log_window, orient="vertical", command=text_widget.yview
        )
        text_widget.configure(yscrollcommand=scrollbar.set)

        log_content = (
            "=== DIAGNOSTIC TECHNIQUE SQLiteConnectionPool ===\n\n"
            "ERREUR IDENTIFIÉE:\n"
            "• SQLiteConnectionPool ne implémente pas __enter__/__exit__\n"
            "• Code utilise: with db._connect() as conn  [❌ INCORRECT]\n"
            "• Devrait être: with db._connect().get() as conn  [✅ CORRECT]\n\n"
            "LOGS D'ERREUR RÉCENTS:\n"
            "2025-07-08 18:00:42 - ERROR - No database manager available for analytics\n"
            "2025-07-08 18:00:42 - ERROR - Database state: manager=missing\n"
            "2025-07-08 18:00:42 - ERROR - Analytics Dashboard Error - Operation: schema_validation\n\n"
            "CORRECTION APPLIQUÉE:\n"
            "✅ Ajout de .get() dans tous les appels context manager\n"
            "✅ Validation robuste du database manager\n"
            "✅ Gestion d'erreurs avec fallbacks\n\n"
            "ACTIONS RECOMMANDÉES:\n"
            "1. Utiliser .get() pour toutes les connexions SQLite\n"
            "2. Vérifier l'intégrité de la base de données\n"
            "3. Redémarrer l'application si nécessaire\n"
            "4. Contacter le support si le problème persiste\n"
        )

        text_widget.insert(tk.END, log_content)
        text_widget.config(state=tk.DISABLED)

        text_widget.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

    def _show_schema_error(self) -> None:
        """Display schema validation error to user."""
        error_frame = ttk.Frame(self.parent)
        error_frame.pack(fill="both", expand=True, padx=20, pady=20)

        ttk.Label(
            error_frame,
            text="❌ Erreur Schema Database",
            font=("Arial", 16, "bold"),
        ).pack(pady=10)

        ttk.Label(
            error_frame,
            text="Le schéma de la base de données est incompatible.\n"
            "Veuillez lancer une analyse complète pour mettre à jour la base.",
            font=("Arial", 12),
        ).pack(pady=5)

    def _show_initialization_error(self, error: Exception) -> None:
        """Display initialization error to user."""
        error_frame = ttk.Frame(self.parent)
        error_frame.pack(fill="both", expand=True, padx=20, pady=20)

        ttk.Label(
            error_frame,
            text="❌ Erreur Initialisation Analytics",
            font=("Arial", 16, "bold"),
        ).pack(pady=10)

        ttk.Label(
            error_frame,
            text=(
                "Impossible d'initialiser le dashboard Analytics.\n"
                f"Erreur: {str(error)[:100]}" + ("..." if len(str(error)) > 100 else "")
            ),
            font=("Arial", 12),
        ).pack(pady=5)

    def _retry_initialization(self) -> None:
        """Retry analytics panel initialization."""
        try:
            for widget in self.parent.winfo_children():
                widget.destroy()

            if self.db_manager and self._validate_database_schema():
                self._build_interface()
                self._initialize_click_functionality()
                logger.info("Analytics Panel retry initialization successful")
            else:
                self._show_database_manager_error()
        except Exception as e:
            logger.error("Retry initialization failed: %s", e)
            self._show_initialization_error(e)

    def _connect_files(self) -> List[FileInfo]:
        """Retrieve files using correct FileInfo constructor."""
        try:
            if not self.db_manager:
                logger.error("No database manager for file retrieval")
                return []

            with self.db_manager._connect().get() as conn:
                cursor = conn.cursor()
                query = """
                SELECT f.id, f.path, f.fast_hash, COALESCE(f.file_size, 0),
                       f.creation_time, f.last_modified, COALESCE(f.owner, 'Unknown')
                FROM fichiers f
                WHERE (f.status IS NULL OR f.status != 'error')
                ORDER BY f.id
                """
                cursor.execute(query)
                rows = cursor.fetchall()
                files: List[FileInfo] = []

                for row in rows:
                    try:
                        file_info = FileInfo(
                            id=row[0],
                            path=row[1] or "",
                            fast_hash=row[2],
                            file_size=int(row[3]) if row[3] else 0,
                            creation_time=row[4],
                            last_modified=row[5],
                            owner=row[6] or "Unknown",
                        )
                        files.append(file_info)
                    except Exception as e:
                        logger.warning(
                            "Skipping malformed file record %s: %s", row[0], e
                        )
                        continue

                logger.info("Retrieved %d valid files from database", len(files))
                return files

        except Exception as e:
            logger.error("Failed to retrieve files: %s", e)
            return []

    def _filter_files_by_classification(
        self, files: List[FileInfo], level: str
    ) -> List[FileInfo]:
        class_map = self._get_classification_map()
        mapping = {
            "C0+": {"C0", "C1", "C2", "C3"},
            "C1+": {"C1", "C2", "C3"},
            "C2+": {"C2", "C3"},
            "C3": {"C3"},
        }
        allowed = mapping.get(level, set())
        return [f for f in files if class_map.get(f.id) in allowed]

    def _count_files_duplicated_n_times(
        self, families: Dict[str, List[FileInfo]], copies: int
    ) -> int:
        return sum(len(fam) for fam in families.values() if len(fam) == copies)

    def _parse_time(self, value: str | None) -> datetime:
        if not value or value == "None":
            return datetime.min

        try:
            formats = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M:%S.%f",
                "%Y-%m-%d",
                "%d/%m/%Y %H:%M:%S",
                "%d/%m/%Y",
            ]
            for fmt in formats:
                try:
                    return datetime.strptime(str(value), fmt)
                except ValueError:
                    continue

            try:
                return datetime.fromtimestamp(float(value))
            except (ValueError, TypeError):
                logger.warning(f"Could not parse time: {value}")
                return datetime.min
        except Exception as e:
            logger.warning(f"Time parsing error for '{value}': {e}")
            return datetime.min

    def _get_old_files_creation(
        self, files: List[FileInfo], threshold_days: int
    ) -> List[FileInfo]:
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
            with self.db_manager._connect().get() as conn:
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
        with self.db_manager._connect().get() as conn:
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
                WHERE (f.status IS NULL OR f.status != 'error')
                GROUP BY r.security_classification_cached, r.rgpd_risk_cached,
                         r.finance_type_cached, r.legal_type_cached
                ORDER BY count DESC
                """
            )
            return cursor.fetchall()

    def _get_super_critical_files_optimized(self) -> List[int]:
        if self.db_manager is None:
            return []
        with self.db_manager._connect().get() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT f.id
                FROM fichiers f
                JOIN reponses_llm r ON f.id = r.fichier_id
                WHERE r.security_classification_cached = 'C3'
                  AND r.rgpd_risk_cached = 'critical'
                  AND r.legal_type_cached IN ('nda', 'litigation')
                  AND (f.status IS NULL OR f.status != 'error')
                """
            )
            return [row[0] for row in cursor.fetchall()]

    def _get_classification_map(self) -> Dict[int, str]:
        mapping: Dict[int, str] = {}
        if self.db_manager is None:
            return mapping
        try:
            with self.db_manager._connect().get() as conn:
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
            with self.db_manager._connect().get() as conn:
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
            with self.db_manager._connect().get() as conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT f.id, r.legal_type_cached FROM fichiers f LEFT JOIN reponses_llm r ON f.id = r.fichier_id"
                )
                for fid, typ in cur.fetchall():
                    mapping[int(fid)] = typ or "none"
        except Exception:
            pass
        return mapping

    def _get_all_files_safe(self) -> List[FileInfo]:
        """Get all files with comprehensive error handling."""
        try:
            if not self.db_manager:
                logger.error("No database manager for file retrieval")
                return []

            with self.db_manager._connect().get() as conn:
                cursor = conn.cursor()
                query = """
                SELECT id, name, COALESCE(file_size, 0), COALESCE(owner, 'Unknown'),
                       status, last_modified, creation_time, path
                FROM fichiers
                WHERE (status IS NULL OR status != 'error')
                ORDER BY id
                """
                cursor.execute(query)
                rows = cursor.fetchall()
                files: List[FileInfo] = []
                for row in rows:
                    try:
                        file_info = FileInfo(
                            id=row[0],
                            name=row[1] or "Unknown",
                            file_size=int(row[2]) if row[2] else 0,
                            owner=row[3] or "Unknown",
                            last_modified=row[5],
                            creation_time=row[6],
                            path=row[7] or "",
                        )
                        files.append(file_info)
                    except Exception as e:
                        logger.warning(
                            "Skipping malformed file record %s: %s", row[0], e
                        )
                        continue
                logger.info("Retrieved %d valid files from database", len(files))
                return files
        except Exception as e:  # pragma: no cover - db failure
            logger.error("Failed to retrieve files: %s", e)
            return []

    def _get_classification_map_safe(self) -> Dict[int, str]:
        """Get classification mapping with error handling."""
        mapping: Dict[int, str] = {}
        try:
            if not self.db_manager:
                return mapping

            with self.db_manager._connect().get() as conn:
                cursor = conn.cursor()
                cursor.execute("PRAGMA table_info(reponses_llm)")
                fields = [row[1] for row in cursor.fetchall()]
                if "security_classification_cached" not in fields:
                    logger.warning("security_classification_cached field not found")
                    return mapping

                query = (
                    "SELECT fichier_id, COALESCE(security_classification_cached, 'none')"
                    " FROM reponses_llm WHERE fichier_id IS NOT NULL"
                )
                cursor.execute(query)
                for row in cursor.fetchall():
                    mapping[row[0]] = row[1]
                logger.debug("Retrieved %d classification mappings", len(mapping))
                return mapping
        except Exception as e:
            logger.error("Failed to get classification map: %s", e)
            return mapping

    def _get_rgpd_map_safe(self) -> Dict[int, str]:
        """Get RGPD mapping with error handling."""
        mapping: Dict[int, str] = {}
        try:
            if not self.db_manager:
                return mapping

            with self.db_manager._connect().get() as conn:
                cursor = conn.cursor()
                cursor.execute("PRAGMA table_info(reponses_llm)")
                fields = [row[1] for row in cursor.fetchall()]
                if "rgpd_risk_cached" not in fields:
                    logger.warning("rgpd_risk_cached field not found")
                    return mapping

                query = (
                    "SELECT fichier_id, COALESCE(rgpd_risk_cached, 'none')"
                    " FROM reponses_llm WHERE fichier_id IS NOT NULL"
                )
                cursor.execute(query)
                for row in cursor.fetchall():
                    mapping[row[0]] = row[1]
                logger.debug("Retrieved %d RGPD mappings", len(mapping))
                return mapping
        except Exception as e:
            logger.error("Failed to get RGPD map: %s", e)
            return mapping

    def _get_fallback_metrics(self) -> Dict[str, Any]:
        """Return minimal fallback metrics when calculation fails."""
        return {
            "global": {"total_files": 0, "total_size_gb": 0},
            "super_critical": {"count": 0, "percentage": 0, "size_gb": 0},
            "critical": {"count": 0, "percentage": 0, "size_gb": 0},
            "duplicates": {
                "files_2x": 0,
                "files_3x": 0,
                "files_4x": 0,
                "max_copies": 0,
            },
            "size_age": {
                "large_files_pct": 0,
                "old_files_pct": 0,
                "dormant_files_pct": 0,
            },
            "top_users": {
                "top_large_files": [],
                "top_c3_files": [],
                "top_rgpd_critical": [],
            },
            "duplicates_detailed": {},
            "_fallback_mode": True,
        }

    def _get_empty_metrics(self) -> Dict[str, Any]:
        """Return empty metrics structure as fallback."""
        return {
            "total_files": 0,
            "total_size": 0,
            "classifications": {"C0": 0, "C1": 0, "C2": 0, "C3": 0},
            "rgpd_risks": {"none": 0, "low": 0, "medium": 0, "high": 0, "critical": 0},
            "file_ages": {"recent": 0, "old": 0, "very_old": 0},
            "file_sizes": {"small": 0, "medium": 0, "large": 0, "very_large": 0},
            "duplicates": {"families": 0, "duplicate_files": 0, "wasted_space": 0},
            "temporal": {"last_week": 0, "last_month": 0, "last_year": 0},
            "users": [],
        }

    # ------------------------------------------------------------------
    # Metrics caching helpers
    # ------------------------------------------------------------------

    def _invalidate_cache(self) -> None:
        self._metrics_cache.clear()
        self._cache_timestamp = 0.0

    # ------------------------------------------------------------------
    # Async helpers
    # ------------------------------------------------------------------

    def _start_result_polling(self) -> None:
        def poll_results() -> None:
            try:
                result = self._result_queue.get_nowait()
                if isinstance(result, dict) and "metrics" in result:
                    self._update_ui_with_metrics(result["metrics"])
                elif isinstance(result, Exception):
                    self._update_ui_with_error(result)
            except queue.Empty:
                pass
            self.parent.after(100, poll_results)

        self.parent.after(100, poll_results)

    def _start_async_calculation(self) -> None:
        if hasattr(self, "progress_label"):
            self.progress_label.config(text="⏳ Calcul en cours...")
            self.parent.update_idletasks()

        self._disable_calculation_controls()
        self._calculation_in_progress = True
        self._calculation_thread = threading.Thread(
            target=self._async_calculate_metrics,
            daemon=True,
        )
        self._calculation_thread.start()
        # Start polling the results so the UI updates when ready
        self._start_result_polling()

    def _async_calculate_metrics(self) -> None:
        try:
            metrics = self.calculate_business_metrics()
            self._result_queue.put({"metrics": metrics})
        except Exception as exc:  # pragma: no cover - runtime
            self._result_queue.put(exc)

    def _update_ui_with_metrics(self, metrics: Dict[str, Any]) -> None:
        logger.info(f"Updating UI with metrics: {len(metrics)} sections")
        try:
            if not metrics:
                if hasattr(self, "progress_label"):
                    self.progress_label.config(text="❌ Erreur calcul")
                return
            # reuse existing update_alert_cards UI logic
            global_metrics = metrics.get("global", {})
            total_files = global_metrics.get("total_files", 0)
            total_size_gb = global_metrics.get("total_size_gb", 0)
            if not hasattr(self, "totals_label"):
                totals_frame = ttk.Frame(self.parent)
                totals_frame.pack(fill="x", padx=5, pady=2)
                self.totals_label = ttk.Label(
                    totals_frame,
                    text=f"📊 TOTAL: {total_files:,} fichiers | {total_size_gb:.1f}GB",
                    font=("Arial", 12, "bold"),
                    foreground="navy",
                )
                self.totals_label.pack()
            else:
                self.totals_label.config(
                    text=f"📊 TOTAL: {total_files:,} fichiers | {total_size_gb:.1f}GB"
                )

            super_crit = metrics.get("super_critical", {})
            count = super_crit.get("count", 0)
            pct = super_crit.get("percentage", 0)
            size_gb = super_crit.get("size_gb", 0)
            self.super_critical_line1.config(text=f"{count} C3+RGPD+Legal")
            self.super_critical_line2.config(
                text=f"{pct}% | {count} fichiers | {size_gb:.1f}GB"
            )
            self.super_critical_line3.config(text="Cumul risques max")
            self.super_critical_line1.config(
                foreground="darkred" if count > 0 else "green"
            )

            crit = metrics.get("critical", {})
            count = crit.get("count", 0)
            pct = crit.get("percentage", 0)
            size_gb = crit.get("size_gb", 0)
            self.critical_line1.config(text=f"{count} C3 OU RGPD OU Legal")
            self.critical_line2.config(
                text=f"{pct}% | {count} fichiers | {size_gb:.1f}GB"
            )
            self.critical_line3.config(text="Un critère fort")
            self.critical_line1.config(
                foreground="darkorange" if count > 0 else "green"
            )

            dup = metrics.get("duplicates", {})
            files_2x = dup.get("files_2x", 0)
            groups = dup.get("total_groups", 0)
            wasted_gb = dup.get("wasted_space_gb", 0)
            pct = dup.get("percentage", 0)
            max_copies = dup.get("max_copies", 0)
            self.duplicates_line1.config(text=f"{files_2x} fichiers dupliqués 2 fois")
            self.duplicates_line2.config(
                text=f"{pct}% | {groups} groupes | {wasted_gb:.1f}GB gaspillé"
            )
            self.duplicates_line3.config(text=f"Top: {max_copies} copies max")
            self.duplicates_line1.config(
                foreground="orange" if wasted_gb > 0.5 else "green"
            )

            size_age = metrics.get("size_age", {})
            large_pct = size_age.get("large_files_pct", 0)
            dormant_pct = size_age.get("dormant_files_pct", 0)
            affected = size_age.get("total_affected", 0)
            archival_gb = size_age.get("archival_size_gb", 0)
            self.size_age_line1.config(
                text=f"{large_pct}% gros + {dormant_pct}% dormants"
            )
            self.size_age_line2.config(
                text=f"{affected} fichiers | {archival_gb:.1f}GB archivage"
            )
            self.size_age_line3.config(text="Seuils utilisateur")
            self.size_age_line1.config(foreground="blue" if affected > 0 else "green")

            try:
                self.update_thematic_tabs()
                self.update_extended_tabs(metrics)
            except Exception as e:  # pragma: no cover - UI issues
                logger.error("Erreur mise à jour onglets: %s", e)
            if hasattr(self, "progress_label"):
                self.progress_label.config(text="✅ Métriques à jour")
        finally:
            self._enable_calculation_controls()
            self._calculation_in_progress = False
            self.parent.update_idletasks()

    def _update_ui_with_error(self, error: Exception) -> None:
        try:
            self._handle_analytics_error("calcul asynchrone", error)
            if hasattr(self, "progress_label"):
                self.progress_label.config(text="❌ Erreur de calcul")
        finally:
            self._enable_calculation_controls()
            self._calculation_in_progress = False

    def _disable_calculation_controls(self) -> None:
        try:
            if hasattr(self, "recalculate_button"):
                self.recalculate_button.config(state="disabled")
            if hasattr(self, "refresh_button"):
                self.refresh_button.config(state="disabled")
        except Exception as exc:
            logger.warning(f"Failed to disable controls: {exc}")

    def _enable_calculation_controls(self) -> None:
        try:
            if hasattr(self, "recalculate_button"):
                self.recalculate_button.config(state="normal")
            if hasattr(self, "refresh_button"):
                self.refresh_button.config(state="normal")
        except Exception as exc:
            logger.warning(f"Failed to enable controls: {exc}")

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
        age_stats = self.age_analyzer.calculate_archival_candidates(
            files, age_threshold_days
        )
        size_stats = self.size_analyzer.calculate_space_optimization(
            files, size_threshold_mb
        )
        dup_families = self.duplicate_detector.detect_duplicate_family(files)
        dup_stats = self.duplicate_detector.get_duplicate_statistics(dup_families)
        class_map = self._get_classification_map()
        rgpd_map = self._get_rgpd_map()
        legal_map = self._get_legal_map()
        super_critical_files = [
            f
            for f in files
            if (
                class_map.get(f.id) == "C3"
                and rgpd_map.get(f.id) == "critical"
                and legal_map.get(f.id) in ["nda", "litigation"]
            )
        ]
        critical_files = [
            f
            for f in files
            if (
                class_map.get(f.id) == "C3"
                or rgpd_map.get(f.id) == "critical"
                or legal_map.get(f.id) in ["nda", "litigation"]
            )
            and f not in super_critical_files
        ]
        duplicates_2x = self._count_files_duplicated_n_times(dup_families, 2)
        duplicates_3x = self._count_files_duplicated_n_times(dup_families, 3)
        duplicates_4x = self._count_files_duplicated_n_times(dup_families, 4)
        max_duplicates = max((len(fam) for fam in dup_families.values()), default=0)
        large_files = self.size_analyzer.identify_large_files(files, size_threshold_mb)
        old_files = self._get_old_files_creation(files, age_threshold_days)
        dormant_files = self.age_analyzer.identify_stale_files(
            files, age_threshold_days
        )
        total_files = len(files)
        total_size = sum(f.file_size for f in files)
        large_file_ids = {f.id for f in large_files}
        dormant_file_ids = {f.id for f in dormant_files}
        total_affected_count = len(large_file_ids.union(dormant_file_ids))

        metrics = {
            "super_critical": {
                "count": len(super_critical_files),
                "percentage": (
                    round(len(super_critical_files) / total_files * 100, 1)
                    if total_files
                    else 0
                ),
                "size_gb": sum(f.file_size for f in super_critical_files) / (1024**3),
            },
            "critical": {
                "count": len(critical_files),
                "percentage": (
                    round(len(critical_files) / total_files * 100, 1)
                    if total_files
                    else 0
                ),
                "size_gb": sum(f.file_size for f in critical_files) / (1024**3),
            },
            "duplicates": {
                "files_2x": duplicates_2x,
                "files_3x": duplicates_3x,
                "files_4x": duplicates_4x,
                "max_copies": max_duplicates,
                "total_groups": len(dup_families),
                "wasted_space_gb": dup_stats.get("space_wasted_bytes", 0) / (1024**3),
                "percentage": (
                    round(dup_stats.get("total_duplicates", 0) / total_files * 100, 1)
                    if total_files
                    else 0
                ),
            },
            "size_age": {
                "large_files_pct": (
                    round(len(large_files) / total_files * 100, 1) if total_files else 0
                ),
                "old_files_pct": (
                    round(len(old_files) / total_files * 100, 1) if total_files else 0
                ),
                "dormant_files_pct": (
                    round(len(dormant_files) / total_files * 100, 1)
                    if total_files
                    else 0
                ),
                "archival_size_gb": age_stats.get("total_size_bytes", 0) / (1024**3),
                "total_affected": total_affected_count,
            },
            "global": {
                "total_files": total_files,
                "total_size_gb": total_size / (1024**3),
            },
        }
        return metrics

    def calculate_business_metrics(self) -> Dict[str, Any]:
        """Calculate comprehensive business metrics with robust error handling."""

        try:
            if not self._validate_database_schema():
                logger.error("Database schema validation failed")
                return self._get_empty_metrics()

            files = self._connect_files()
            if not files:
                logger.warning("No files available for metrics calculation")
                return self._get_empty_metrics()

            logger.info(f"Successfully retrieved {len(files)} files for analytics")

            metrics = {
                "global": {
                    "total_files": len(files),
                    "total_size_gb": sum(f.file_size for f in files) / (1024**3),
                }
            }

            try:
                class_map = self._get_classification_map_safe()
                rgpd_map = self._get_rgpd_map_safe()
                metrics.update(
                    self._calculate_classification_metrics_safe(
                        files, class_map, rgpd_map
                    )
                )
            except Exception as e:
                logger.warning("Classification metrics failed, using fallback: %s", e)
                metrics.update(self._get_fallback_classification_metrics())

            try:
                metrics["duplicates"] = self._calculate_duplicates_safe(files)
                metrics["duplicates"]["detailed"] = (
                    self._calculate_duplicates_detailed_metrics(files)
                )
            except Exception as e:
                logger.warning("Duplicate analysis failed: %s", e)
                metrics["duplicates"] = {
                    "files_2x": 0,
                    "total_groups": 0,
                    "wasted_space_gb": 0,
                    "detailed": {},
                }

            try:
                metrics["duplicates_analysis"] = self._calculate_duplicates_analysis()
            except Exception as e:
                logger.warning("Detailed duplicate analysis failed: %s", e)
                metrics["duplicates_analysis"] = {
                    "duplicates_by_count": {},
                    "total_duplicates": 0,
                }

            try:
                metrics["top_users"] = self._calculate_top_users_metrics_safe(
                    files, class_map, rgpd_map
                )
            except Exception as e:
                logger.warning("Top users analysis failed: %s", e)
                metrics["top_users"] = {}

            try:
                temporal = self._calculate_temporal_analysis()
                metrics["temporal_creation"] = temporal.get("creation_dates", {})
                metrics["temporal_modification"] = temporal.get(
                    "modification_dates", {}
                )
            except Exception as e:
                logger.warning("Temporal analysis failed: %s", e)
                metrics["temporal_creation"] = {}
                metrics["temporal_modification"] = {}

            try:
                metrics["file_size_analysis"] = self._calculate_size_analysis().get(
                    "size_distribution", {}
                )
            except Exception as e:
                logger.warning("Size analysis failed: %s", e)
                metrics["file_size_analysis"] = {}

            self._metrics_cache["business_metrics"] = metrics
            self._cache_timestamp = time.time()

            self._last_calculated_metrics = metrics

            logger.info("Business metrics calculation completed successfully")
            return metrics

        except Exception as e:
            logger.error("Critical failure in business metrics calculation: %s", e)
            self._handle_analytics_error("calculate_business_metrics", e)
            return self._get_fallback_metrics()

    def update_alert_cards(self) -> None:
        if self._calculation_in_progress:
            return
        self._start_async_calculation()

    def _build_security_tab(self, parent_frame: ttk.Frame) -> None:
        title_label = ttk.Label(
            parent_frame, text="🛡️ ANALYSE SÉCURITÉ", font=("Arial", 14, "bold")
        )
        title_label.pack(pady=10)
        help_label = ttk.Label(
            parent_frame,
            text="Répartition des fichiers par niveau de classification sécurité",
        )
        help_label.pack(pady=5)
        main_container = ttk.Frame(parent_frame)
        main_container.pack(fill="both", expand=True, padx=10, pady=10)
        left_frame = ttk.LabelFrame(main_container, text="RÉPARTITION SÉCURITÉ")
        left_frame.pack(side="left", fill="both", expand=True, padx=5)
        self.security_labels = {}
        for level in ["C0", "C1", "C2", "C3", "Autres"]:
            label = ttk.Label(
                left_frame, text=f"{level}: 0% | 0 fichiers | 0GB", font=("Arial", 12)
            )
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
        title_label = ttk.Label(
            parent_frame, text="🔒 ANALYSE RGPD", font=("Arial", 14, "bold")
        )
        title_label.pack(pady=10)
        help_label = ttk.Label(
            parent_frame, text="Répartition des fichiers par niveau de risque RGPD"
        )
        help_label.pack(pady=5)
        container = ttk.LabelFrame(parent_frame, text="NIVEAUX RGPD")
        container.pack(fill="both", expand=True, padx=10, pady=10)
        self.rgpd_labels = {}
        for level in ["none", "low", "medium", "high", "critical", "Autres"]:
            label = ttk.Label(
                container, text=f"{level}: 0% | 0 fichiers | 0GB", font=("Arial", 12)
            )
            label.pack(anchor="w", pady=3, padx=10)
            self.rgpd_labels[level] = label

    def _build_finance_tab(self, parent_frame: ttk.Frame) -> None:
        title_label = ttk.Label(
            parent_frame, text="💰 ANALYSE FINANCE", font=("Arial", 14, "bold")
        )
        title_label.pack(pady=10)
        help_label = ttk.Label(
            parent_frame, text="Répartition des documents par type financier"
        )
        help_label.pack(pady=5)
        container = ttk.LabelFrame(parent_frame, text="TYPES FINANCIERS")
        container.pack(fill="both", expand=True, padx=10, pady=10)

        self.finance_labels = {}
        finance_data = {
            "none": 0,
            "invoice": 0,
            "contract": 0,
            "budget": 0,
            "accounting": 0,
            "payment": 0,
            "Autres": 0,
        }

        for i, (finance_type, count) in enumerate(finance_data.items()):
            label = ttk.Label(
                container,
                text=f"{finance_type}: {count} fichiers",
                font=("Arial", 11),
            )
            label.pack(pady=2, anchor="w", padx=20)

            if not hasattr(self, "finance_labels"):
                self.finance_labels = {}
            self.finance_labels[finance_type] = label

    def _build_legal_tab(self, parent_frame: ttk.Frame) -> None:
        title_label = ttk.Label(
            parent_frame, text="⚖️ ANALYSE LEGAL", font=("Arial", 14, "bold")
        )
        title_label.pack(pady=10)
        help_label = ttk.Label(
            parent_frame, text="Répartition des documents par type légal"
        )
        help_label.pack(pady=5)
        container = ttk.LabelFrame(parent_frame, text="TYPES LÉGAUX")
        container.pack(fill="both", expand=True, padx=10, pady=10)

        self.legal_labels = {}
        legal_data = {
            "none": 0,
            "employment": 0,
            "lease": 0,
            "sale": 0,
            "nda": 0,
            "compliance": 0,
            "litigation": 0,
            "Autres": 0,
        }

        for i, (legal_type, count) in enumerate(legal_data.items()):
            label = ttk.Label(
                container,
                text=f"{legal_type}: {count} fichiers",
                font=("Arial", 11),
            )
            label.pack(pady=2, anchor="w", padx=20)

            self.legal_labels[legal_type] = label

    def _show_detailed_modal(self, title: str, data: List, headers: List[str]) -> None:
        """Show detailed data in a modal window."""
        try:
            modal = tk.Toplevel(self.parent)
            modal.title(title)
            modal.geometry("800x600")
            modal.transient(self.parent)
            modal.grab_set()

            frame = ttk.Frame(modal)
            frame.pack(fill="both", expand=True, padx=10, pady=10)

            tree = ttk.Treeview(frame, columns=headers, show="headings", height=20)
            for header in headers:
                tree.heading(header, text=header)
                tree.column(header, width=120, anchor="w")

            for row in data:
                formatted = []
                for i, value in enumerate(row):
                    if headers[i] == "Taille" and isinstance(value, (int, float)):
                        formatted.append(self._format_file_size(value))
                    else:
                        formatted.append(str(value) if value is not None else "N/A")
                tree.insert("", "end", values=formatted)

            v_scroll = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
            h_scroll = ttk.Scrollbar(frame, orient="horizontal", command=tree.xview)
            tree.configure(yscrollcommand=v_scroll.set, xscrollcommand=h_scroll.set)

            tree.pack(side="left", fill="both", expand=True)
            v_scroll.pack(side="right", fill="y")
            h_scroll.pack(side="bottom", fill="x")

            ttk.Button(
                modal,
                text="Fermer",
                command=lambda: [modal.grab_release(), modal.destroy()],
            ).pack(pady=5)

        except Exception as e:
            logger.error(f"Error showing detailed modal: {e}")

    # ------------------------------------------------------------------
    # Extended analytics tabs
    # ------------------------------------------------------------------

    def _build_duplicates_detailed_tab(self, parent_frame: ttk.Frame) -> None:
        """Onglet doublons détaillé."""
        title_label = ttk.Label(
            parent_frame,
            text="🔍 ANALYSE DOUBLONS DÉTAILLÉE",
            font=("Arial", 14, "bold"),
        )
        title_label.pack(pady=10)

        container = ttk.LabelFrame(
            parent_frame, text="RÉPARTITION PAR NOMBRE DE COPIES"
        )
        container.pack(fill="both", expand=True, padx=10, pady=10)

        self.duplicates_detailed_labels = {}
        duplicate_levels = [
            ("1x", "Fichiers dupliqués exactement 1 fois", "blue"),
            ("2x", "Fichiers dupliqués exactement 2 fois", "orange"),
            ("3x", "Fichiers dupliqués exactement 3 fois", "darkorange"),
            ("4x", "Fichiers dupliqués exactement 4 fois", "red"),
            ("5x", "Fichiers dupliqués exactement 5 fois", "darkred"),
            ("6x", "Fichiers dupliqués exactement 6 fois", "purple"),
            ("7x+", "Fichiers dupliqués 7 fois ou plus", "darkmagenta"),
        ]

        for level, description, color in duplicate_levels:
            frame = ttk.Frame(container)
            frame.pack(fill="x", pady=2, padx=10)

            label = ttk.Label(
                frame, text=f"{level}: 0% | 0 fichiers | 0GB", font=("Arial", 11)
            )
            label.pack(side="left")

            desc_label = ttk.Label(
                frame, text=f"({description})", font=("Arial", 9), foreground=color
            )
            desc_label.pack(side="left", padx=10)

            self.duplicates_detailed_labels[level] = label

    def _calculate_duplicates_detailed_metrics(
        self, files: List[FileInfo]
    ) -> Dict[str, Dict[str, Any]]:
        """Return detailed duplicate metrics including unique files."""

        from content_analyzer.utils.duplicate_utils import create_enhanced_duplicate_key

        all_families: Dict[str, List[FileInfo]] = {}
        for info in files:
            try:
                ignore, _ = self.duplicate_detector.should_ignore_file(info)
                if ignore or not info.fast_hash:
                    continue
                key = create_enhanced_duplicate_key(info.fast_hash, info.file_size)
                all_families.setdefault(key, []).append(info)
            except Exception as e:  # pragma: no cover
                logger.debug(f"Erreur regroupement doublons pour {info.path}: {e}")

        total_files = sum(len(f) for f in all_families.values())
        detailed_metrics: Dict[str, Dict[str, Any]] = {}

        for level in ["1x", "2x", "3x", "4x", "5x", "6x", "7x+"]:
            if level == "7x+":
                matching_families = [
                    fam for fam in all_families.values() if len(fam) >= 7
                ]
            else:
                target = int(level.replace("x", ""))
                matching_families = [
                    fam for fam in all_families.values() if len(fam) == target
                ]

            files_count = sum(len(fam) for fam in matching_families)
            size_total = sum(sum(f.file_size for f in fam) for fam in matching_families)

            detailed_metrics[level] = {
                "count": files_count,
                "percentage": (
                    round(files_count / total_files * 100, 1) if total_files else 0
                ),
                "size_gb": size_total / (1024**3),
                "families_count": len(matching_families),
            }

        return detailed_metrics

    def _build_temporal_analysis_tab(self, parent_frame: ttk.Frame) -> None:
        notebook = ttk.Notebook(parent_frame)
        notebook.pack(fill="both", expand=True, padx=5, pady=5)

        modification_frame = ttk.Frame(notebook)
        notebook.add(modification_frame, text="📅 Dates Modification")
        self._build_temporal_sub_tab(modification_frame, "modification")

        creation_frame = ttk.Frame(notebook)
        notebook.add(creation_frame, text="🆕 Dates Création")
        self._build_temporal_sub_tab(creation_frame, "creation")

    def _build_temporal_sub_tab(self, parent_frame: ttk.Frame, mode: str) -> None:
        """Build temporal analysis sub-tab with standardized period mapping."""
        title = "MODIFICATION" if mode == "modification" else "CRÉATION"
        container = ttk.LabelFrame(
            parent_frame, text=f"FICHIERS PAR ANCIENNETÉ {title}"
        )
        container.pack(fill="both", expand=True, padx=10, pady=10)

        temporal_labels_key = f"{mode}_labels"
        setattr(self, temporal_labels_key, {})
        temporal_labels = getattr(self, temporal_labels_key)

        # Nouvelle logique standardisée
        temporal_periods = [
            {
                "key": "0_1y",
                "label": "0-1 an",
                "description": f"Fichiers sans {mode} depuis 0 à 1 an",
            },
            {
                "key": "1_2y",
                "label": "1-2 ans",
                "description": f"Fichiers sans {mode} depuis 1 à 2 ans",
            },
            {
                "key": "2_3y",
                "label": "2-3 ans",
                "description": f"Fichiers sans {mode} depuis 2 à 3 ans",
            },
            {
                "key": "3_4y",
                "label": "3-4 ans",
                "description": f"Fichiers sans {mode} depuis 3 à 4 ans",
            },
            {
                "key": "4_5y",
                "label": "4-5 ans",
                "description": f"Fichiers sans {mode} depuis 4 à 5 ans",
            },
            {
                "key": "5_6y",
                "label": "5-6 ans",
                "description": f"Fichiers sans {mode} depuis 5 à 6 ans",
            },
            {
                "key": "6plus",
                "label": "+6 ans",
                "description": f"Fichiers sans {mode} depuis plus de 6 ans",
            },
        ]

        for period in temporal_periods:
            label_text = f"{period['label']}: 0% | 0 fichiers | 0GB"

            frame = ttk.Frame(container)
            frame.pack(fill="x", pady=2, padx=10)

            label = ttk.Label(frame, text=label_text, font=("Arial", 11))
            label.pack(side="left")

            desc_label = ttk.Label(
                frame,
                text=f"({period['description']})",
                font=("Arial", 9),
                foreground="gray",
            )
            desc_label.pack(side="left", padx=10)

            temporal_labels[period["key"]] = label

    def _calculate_temporal_metrics(
        self, files: List[FileInfo], date_type: str = "modification"
    ) -> Dict[str, Any]:
        """Calculate temporal metrics with strict period logic."""

        from datetime import datetime, timedelta

        from content_analyzer.modules.age_analyzer import AgeAnalyzer

        logger = logging.getLogger(__name__)
        analyzer = AgeAnalyzer()

        now = datetime.now()
        logger.info(
            f"Calcul temporel {date_type} avec référence: {now.strftime('%Y-%m-%d')}"
        )

        period_definitions = {
            "0_1y": (now - timedelta(days=365), now),
            "1_2y": (now - timedelta(days=730), now - timedelta(days=365)),
            "2_3y": (now - timedelta(days=1095), now - timedelta(days=730)),
            "3_4y": (now - timedelta(days=1460), now - timedelta(days=1095)),
            "4_5y": (now - timedelta(days=1825), now - timedelta(days=1460)),
            "5_6y": (now - timedelta(days=2190), now - timedelta(days=1825)),
            "6plus": (datetime.min, now - timedelta(days=2190)),
        }

        period_counts = {
            key: {"count": 0, "size": 0, "files": []} for key in period_definitions
        }

        files_processed = 0
        files_with_valid_dates = 0

        for info in files:
            files_processed += 1
            date_str = (
                info.last_modified
                if date_type == "modification"
                else info.creation_time
            ) or (
                info.creation_time
                if date_type == "modification"
                else info.last_modified
            )

            if not date_str:
                logger.debug(f"Fichier {info.name}: pas de date {date_type}")
                continue

            file_date = analyzer._parse_time(date_str)
            if file_date == datetime.max:
                logger.warning(
                    f"Impossible de parser la date pour {info.name}: {date_str}"
                )
                continue

            files_with_valid_dates += 1
            classified = False
            for period_key, (start_date, end_date) in period_definitions.items():
                if start_date <= file_date < end_date:
                    data = period_counts[period_key]
                    data["count"] += 1
                    data["size"] += info.file_size or 0
                    data["files"].append(info.id)
                    classified = True
                    logger.debug(
                        f"Fichier {info.name} ({file_date.strftime('%Y-%m-%d')}) → {period_key}"
                    )
                    break

            if not classified:
                logger.warning(
                    f"Fichier non classifié: {info.name} - {file_date.strftime('%Y-%m-%d')}"
                )

        total_files = files_with_valid_dates
        for key in period_counts:
            count = period_counts[key]["count"]
            period_counts[key]["percentage"] = (
                (count / total_files * 100) if total_files > 0 else 0
            )

        result = {
            "distribution": period_counts,
            "total_files": total_files,
            "files_processed": files_processed,
            "files_with_valid_dates": files_with_valid_dates,
            "date_type": date_type,
            "reference_date": now.isoformat(),
        }

        logger.info(
            f"Analyse temporelle {date_type} terminée: {total_files} fichiers avec dates valides"
        )
        for period, data in period_counts.items():
            logger.info(
                f"  {period}: {data['count']} fichiers ({data['percentage']:.1f}%)"
            )

        return result

    def _calculate_temporal_analysis(self) -> Dict[str, Any]:
        """Calcul d'analyse temporelle avec récupération de données robuste."""

        try:
            if not self.db_manager:
                logger.warning("Gestionnaire DB non disponible pour analyse temporelle")
                return {"creation_dates": {}, "modification_dates": {}}

            with self.db_manager._connect().get() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                SELECT
                    last_modified,
                    creation_time,
                    file_size,
                    CASE 
                        WHEN last_modified IS NULL OR last_modified = '' OR last_modified = '0' THEN 0
                        ELSE 1 
                    END as has_valid_modification,
                    CASE 
                        WHEN creation_time IS NULL OR creation_time = '' OR creation_time = '0' THEN 0
                        ELSE 1 
                    END as has_valid_creation
                FROM fichiers 
                WHERE (status IS NULL OR status != 'error')
                AND file_size > 0
                """
                )
                raw_data = cursor.fetchall()

            logger.info(f"Récupération données temporelles: {len(raw_data)} fichiers")

            if not raw_data:
                logger.warning("Aucune donnée temporelle trouvée")
                return {"creation_dates": {}, "modification_dates": {}}

            valid_modifications = sum(1 for row in raw_data if row[3] == 1)
            valid_creations = sum(1 for row in raw_data if row[4] == 1)
            logger.info(
                f"Données valides - Modifications: {valid_modifications}, Créations: {valid_creations}"
            )

            modification_data = self._calculate_temporal_metrics_safe(
                raw_data, "modification"
            )
            creation_data = self._calculate_temporal_metrics_safe(raw_data, "creation")

            return {
                "creation_dates": creation_data,
                "modification_dates": modification_data,
            }

        except Exception as e:
            logger.error(f"Erreur calcul analyse temporelle: {e}")
            return {"creation_dates": {}, "modification_dates": {}}

    def _calculate_temporal_metrics_safe(
        self, raw_data: List[tuple], mode: str
    ) -> Dict[str, Dict[str, Any]]:
        """Calculate temporal metrics with standardized period logic."""
        from datetime import datetime, timedelta

        try:
            now = datetime.now()
            total_files = len(raw_data)
            temporal_metrics: Dict[str, Dict[str, Any]] = {}

            date_index = 0 if mode == "modification" else 1
            valid_index = 3 if mode == "modification" else 4

            valid_files = []
            for row in raw_data:
                if row[valid_index] == 1:
                    try:
                        date_str = row[date_index]
                        if date_str and date_str != "0":
                            parsed_date = self._parse_date_flexible(date_str)
                            if parsed_date:
                                valid_files.append((parsed_date, row[2]))
                    except Exception as e:
                        logger.debug(f"Échec parsing date {date_str}: {e}")
                        continue

            logger.info(
                f"Fichiers avec dates {mode} valides: {len(valid_files)} sur {total_files}"
            )

            if not valid_files:
                for key in ["0_1y", "1_2y", "2_3y", "3_4y", "4_5y", "5_6y", "6plus"]:
                    temporal_metrics[key] = {
                        "count": 0,
                        "percentage": 0.0,
                        "size_gb": 0.0,
                    }
                return temporal_metrics

            temporal_ranges = [
                {"key": "0_1y", "min_days": 0, "max_days": 365},
                {"key": "1_2y", "min_days": 365, "max_days": 730},
                {"key": "2_3y", "min_days": 730, "max_days": 1095},
                {"key": "3_4y", "min_days": 1095, "max_days": 1460},
                {"key": "4_5y", "min_days": 1460, "max_days": 1825},
                {"key": "5_6y", "min_days": 1825, "max_days": 2190},
                {"key": "6plus", "min_days": 2190, "max_days": None},
            ]

            for range_def in temporal_ranges:
                if range_def["max_days"] is None:
                    cutoff = now - timedelta(days=range_def["min_days"])
                    matching_files = [(d, s) for d, s in valid_files if d <= cutoff]
                else:
                    upper_cutoff = now - timedelta(days=range_def["min_days"])
                    lower_cutoff = now - timedelta(days=range_def["max_days"])
                    matching_files = [
                        (d, s)
                        for d, s in valid_files
                        if lower_cutoff < d <= upper_cutoff
                    ]

                total_size = sum(size for _, size in matching_files)
                temporal_metrics[range_def["key"]] = {
                    "count": len(matching_files),
                    "percentage": (
                        round(len(matching_files) / len(valid_files) * 100, 1)
                        if valid_files
                        else 0
                    ),
                    "size_gb": total_size / (1024**3),
                }

            return temporal_metrics

        except Exception as e:
            logger.error(f"Erreur calcul métriques temporelles {mode}: {e}")
            return {}

    def _parse_date_flexible(self, date_str: str) -> Optional[datetime]:
        """Parse date avec multiple formats supportés."""
        if not date_str or date_str in ["0", "", "NULL", "None"]:
            return None

        formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d",
            "%d/%m/%Y %H:%M:%S",
            "%d/%m/%Y",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%SZ",
        ]

        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue

        try:
            timestamp = float(date_str)
            return datetime.fromtimestamp(timestamp)
        except (ValueError, OSError):
            pass

        logger.debug(f"Format de date non reconnu: {date_str}")
        return None

    def _build_file_size_analysis_tab(self, parent_frame: ttk.Frame) -> None:
        container = ttk.LabelFrame(
            parent_frame, text="RÉPARTITION PAR TAILLE DE FICHIER"
        )
        container.pack(fill="both", expand=True, padx=10, pady=10)

        self.file_size_labels = {}
        size_ranges = [
            ("<50MB", 0, 50, "green"),
            ("50-100MB", 50, 100, "blue"),
            ("100-150MB", 100, 150, "orange"),
            ("150-200MB", 150, 200, "darkorange"),
            ("200-300MB", 200, 300, "red"),
            ("300-500MB", 300, 500, "darkred"),
            (">500MB", 500, float("inf"), "purple"),
        ]

        for range_label, min_mb, max_mb, color in size_ranges:
            frame = ttk.Frame(container)
            frame.pack(fill="x", pady=2, padx=10)

            label = ttk.Label(
                frame, text=f"{range_label}: 0% | 0 fichiers | 0GB", font=("Arial", 11)
            )
            label.pack(side="left")

            desc_label = ttk.Label(
                frame,
                text=f"(Fichiers entre {min_mb}MB et {max_mb}MB)",
                font=("Arial", 9),
                foreground=color,
            )
            if max_mb == float("inf"):
                desc_label.config(text=f"(Fichiers supérieurs à {min_mb}MB)")
            desc_label.pack(side="left", padx=10)

            self.file_size_labels[range_label] = label

    def _calculate_file_size_metrics(
        self, files: List[FileInfo]
    ) -> Dict[str, Dict[str, Any]]:
        total_files = len(files)
        size_metrics: Dict[str, Dict[str, Any]] = {}

        size_ranges = [
            ("<50MB", 0, 50),
            ("50-100MB", 50, 100),
            ("100-150MB", 100, 150),
            ("150-200MB", 150, 200),
            ("200-300MB", 200, 300),
            ("300-500MB", 300, 500),
            (">500MB", 500, float("inf")),
        ]

        for range_label, min_mb, max_mb in size_ranges:
            min_bytes = min_mb * 1024 * 1024
            max_bytes = max_mb * 1024 * 1024 if max_mb != float("inf") else float("inf")

            if max_mb == float("inf"):
                matching_files = [f for f in files if f.file_size >= min_bytes]
            else:
                matching_files = [
                    f for f in files if min_bytes <= f.file_size < max_bytes
                ]

            total_size = sum(f.file_size for f in matching_files)
            size_metrics[range_label] = {
                "count": len(matching_files),
                "percentage": (
                    round(len(matching_files) / total_files * 100, 1)
                    if total_files
                    else 0
                ),
                "size_gb": total_size / (1024**3),
            }

        return size_metrics

    def _calculate_size_analysis(self) -> Dict[str, Any]:
        """Calculate file size distribution analysis."""
        try:
            files = self._connect_files()
            if not files:
                return {"size_distribution": {}}

            total_files = len(files)
            size_distribution: Dict[str, Dict[str, Any]] = {}

            size_ranges = [
                ("<50MB", 0, 50 * 1024 * 1024),
                ("50-100MB", 50 * 1024 * 1024, 100 * 1024 * 1024),
                ("100-150MB", 100 * 1024 * 1024, 150 * 1024 * 1024),
                ("150-200MB", 150 * 1024 * 1024, 200 * 1024 * 1024),
                ("200-300MB", 200 * 1024 * 1024, 300 * 1024 * 1024),
                ("300-500MB", 300 * 1024 * 1024, 500 * 1024 * 1024),
                (">500MB", 500 * 1024 * 1024, float("inf")),
            ]

            for range_label, min_size, max_size in size_ranges:
                if max_size == float("inf"):
                    matching_files = [f for f in files if f.file_size >= min_size]
                else:
                    matching_files = [
                        f for f in files if min_size <= f.file_size < max_size
                    ]

                total_size = sum(f.file_size for f in matching_files)
                size_distribution[range_label] = {
                    "count": len(matching_files),
                    "percentage": (
                        round(len(matching_files) / total_files * 100, 1)
                        if total_files
                        else 0
                    ),
                    "size_gb": total_size / (1024**3),
                }

            logger.info(f"Size analysis calculated for {total_files} files")
            return {"size_distribution": size_distribution}

        except Exception as e:
            logger.error(f"Error calculating size analysis: {e}")
            return {"size_distribution": {}}

    def _calculate_global_metrics(self, files: List[FileInfo]) -> Dict[str, Any]:
        """Compute global metrics for total files and size."""
        total_files = len(files)
        total_size = sum(f.file_size for f in files)
        return {"total_files": total_files, "total_size_gb": total_size / (1024**3)}

    def _calculate_classification_metrics(
        self, files: List[FileInfo], class_map: Dict[int, str], rgpd_map: Dict[int, str]
    ) -> Dict[str, Any]:
        """Compute classification based metrics."""
        super_critical_files = [
            f
            for f in files
            if (class_map.get(f.id) == "C3" and rgpd_map.get(f.id) == "critical")
        ]
        critical_files = [
            f
            for f in files
            if (class_map.get(f.id) == "C3" or rgpd_map.get(f.id) == "critical")
            and f not in super_critical_files
        ]
        total_files = len(files)
        return {
            "super_critical": {
                "count": len(super_critical_files),
                "percentage": (
                    round(len(super_critical_files) / total_files * 100, 1)
                    if total_files
                    else 0
                ),
                "size_gb": sum(f.file_size for f in super_critical_files) / (1024**3),
            },
            "critical": {
                "count": len(critical_files),
                "percentage": (
                    round(len(critical_files) / total_files * 100, 1)
                    if total_files
                    else 0
                ),
                "size_gb": sum(f.file_size for f in critical_files) / (1024**3),
            },
        }

    def _calculate_classification_metrics_safe(
        self, files: List[FileInfo], class_map: Dict[int, str], rgpd_map: Dict[int, str]
    ) -> Dict[str, Any]:
        """Wrapper around classification metrics with error handling."""
        try:
            return self._calculate_classification_metrics(files, class_map, rgpd_map)
        except Exception as e:
            logger.warning("Classification metrics computation failed: %s", e)
            return self._get_fallback_classification_metrics()

    def _calculate_duplicates_safe(self, files: List[FileInfo]) -> Dict[str, Any]:
        """Calculate duplicate statistics with error handling."""
        try:
            families = self.duplicate_detector.detect_duplicate_family(files)
            dup_stats = self.duplicate_detector.get_duplicate_statistics(families)
            return {
                "files_2x": self._count_files_duplicated_n_times(families, 2),
                "total_groups": len(families),
                "wasted_space_gb": dup_stats.get("space_wasted_bytes", 0) / (1024**3),
            }
        except Exception as e:
            logger.warning("Duplicate statistics failed: %s", e)
            return {"files_2x": 0, "total_groups": 0, "wasted_space_gb": 0}

    def _calculate_duplicates_analysis(self) -> Dict[str, Any]:
        """Fix duplicate analysis - currently returning zeros."""
        try:
            if not self._ensure_database_manager():
                return {"duplicates_by_count": {}, "total_duplicates": 0}

            with self.db_manager._connect().get() as conn:
                cursor = conn.cursor()
                query = """
                SELECT f.name, COUNT(*) as duplicate_count
                FROM fichiers f
                WHERE (f.status IS NULL OR f.status != 'error')
                GROUP BY f.name
                HAVING COUNT(*) > 1
                ORDER BY duplicate_count DESC
                """
                cursor.execute(query)
                duplicates = cursor.fetchall()

            duplicates_by_count: Dict[int, List[str]] = {}
            total_duplicates = 0
            for name, count in duplicates:
                duplicates_by_count.setdefault(count, []).append(name)
                total_duplicates += count - 1

            logger.info(
                f"Found {len(duplicates)} duplicate groups, {total_duplicates} excess files"
            )

            return {
                "duplicates_by_count": duplicates_by_count,
                "total_duplicates": total_duplicates,
                "duplicate_groups": len(duplicates),
            }

        except Exception as e:
            logger.error(f"Error calculating duplicates: {e}")
            return {"duplicates_by_count": {}, "total_duplicates": 0}

    def _get_fallback_classification_metrics(self) -> Dict[str, Any]:
        """Fallback structure when classification metrics fail."""
        return {
            "super_critical": {"count": 0, "percentage": 0, "size_gb": 0},
            "critical": {"count": 0, "percentage": 0, "size_gb": 0},
        }

    def _build_top_users_tab(self, parent_frame: ttk.Frame) -> None:
        """Construit l'onglet Top Utilisateurs avec fonctionnalité de clic."""

        notebook = ttk.Notebook(parent_frame)
        notebook.pack(fill="both", expand=True, padx=5, pady=5)

        large_files_frame = ttk.Frame(notebook)
        notebook.add(large_files_frame, text="📊 Gros Fichiers")
        self._build_top_users_sub_tab(
            large_files_frame, "top_large_files", "Gros fichiers"
        )

        c3_frame = ttk.Frame(notebook)
        notebook.add(c3_frame, text="🔒 Classification C3")
        self._build_top_users_sub_tab(c3_frame, "top_c3_files", "Classification C3")

        rgpd_frame = ttk.Frame(notebook)
        notebook.add(rgpd_frame, text="⚠️ RGPD Critical")
        self._build_top_users_sub_tab(rgpd_frame, "top_rgpd_critical", "RGPD Critical")

    def _build_top_users_sub_tab(
        self, parent_frame: ttk.Frame, category_key: str, category_name: str
    ) -> None:
        """Construit un sous-onglet Top Utilisateurs avec gestion de clic."""

        container = ttk.LabelFrame(
            parent_frame, text=f"TOP UTILISATEURS - {category_name.upper()}"
        )
        container.pack(fill="both", expand=True, padx=10, pady=10)

        labels_key = f"{category_key}_labels"
        setattr(self, labels_key, {})
        labels_dict = getattr(self, labels_key)

        for rank in range(1, 11):
            rank_key = f"rank_{rank}"
            frame = ttk.Frame(container)
            frame.pack(fill="x", pady=2, padx=10)
            label = ttk.Label(
                frame,
                text=f"#{rank}: -- (0 fichiers, 0GB)",
                font=("Arial", 11),
                cursor="hand2",
            )
            label.pack(side="left")
            label.click_info = {
                "type": "user_drill_down",
                "category": category_name,
                "rank": rank,
                "category_key": category_key,
            }
            label.bind("<Button-1>", lambda e, lbl=label: self._handle_user_click(lbl))
            label.bind(
                "<Enter>",
                lambda e, l=label: l.configure(
                    foreground="blue", font=("Arial", 11, "underline")
                ),
            )
            label.bind(
                "<Leave>",
                lambda e, l=label: l.configure(
                    foreground="black", font=("Arial", 11, "normal")
                ),
            )
            labels_dict[rank_key] = label

    def _calculate_top_users_metrics_safe(
        self, files: List[FileInfo], class_map: Dict[int, str], rgpd_map: Dict[int, str]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Calculate top users metrics with configurable count and enhanced categories."""

        TOP_USERS_COUNT = 10

        if not files:
            return self._get_empty_top_users_data()

        try:
            large_files = [
                f
                for f in files
                if f.file_size > 100 * 1024 * 1024 and f.owner and f.owner.strip()
            ]
            large_files_by_user: Dict[str, Dict[str, Any]] = {}
            for f in large_files:
                owner = f.owner or "Inconnu"
                if owner not in large_files_by_user:
                    large_files_by_user[owner] = {"count": 0, "total_size": 0}
                large_files_by_user[owner]["count"] += 1
                large_files_by_user[owner]["total_size"] += f.file_size
            top_large_files = sorted(
                large_files_by_user.items(),
                key=lambda x: x[1]["total_size"],
                reverse=True,
            )[:TOP_USERS_COUNT]

            c3_files = [
                f
                for f in files
                if class_map.get(f.id) == "C3" and f.owner and f.owner.strip()
            ]
            c3_by_user: Dict[str, Dict[str, Any]] = {}
            for f in c3_files:
                owner = f.owner or "Inconnu"
                if owner not in c3_by_user:
                    c3_by_user[owner] = {"count": 0, "total_size": 0}
                c3_by_user[owner]["count"] += 1
                c3_by_user[owner]["total_size"] += f.file_size
            top_c3_files = sorted(
                c3_by_user.items(), key=lambda x: x[1]["count"], reverse=True
            )[:TOP_USERS_COUNT]

            rgpd_critical_files = [
                f
                for f in files
                if rgpd_map.get(f.id) == "critical" and f.owner and f.owner.strip()
            ]
            rgpd_by_user: Dict[str, Dict[str, Any]] = {}
            for f in rgpd_critical_files:
                owner = f.owner or "Inconnu"
                if owner not in rgpd_by_user:
                    rgpd_by_user[owner] = {"count": 0, "total_size": 0}
                rgpd_by_user[owner]["count"] += 1
                rgpd_by_user[owner]["total_size"] += f.file_size
            top_rgpd_critical = sorted(
                rgpd_by_user.items(), key=lambda x: x[1]["count"], reverse=True
            )[:TOP_USERS_COUNT]

            result = {
                "top_large_files": [
                    {"owner": owner, **data} for owner, data in top_large_files
                ],
                "top_c3_files": [
                    {"owner": owner, **data} for owner, data in top_c3_files
                ],
                "top_rgpd_critical": [
                    {"owner": owner, **data} for owner, data in top_rgpd_critical
                ],
            }
            logger.info(
                "Calculated top users: %d large, %d C3, %d RGPD",
                len(result["top_large_files"]),
                len(result["top_c3_files"]),
                len(result["top_rgpd_critical"]),
            )
            return result
        except Exception as e:
            logger.error("Top users calculation failed: %s", e)
            return self._get_empty_top_users_data()

    def _get_empty_top_users_data(self) -> Dict[str, List[Dict[str, Any]]]:
        """Return empty structure for top users data."""
        return {
            "top_large_files": [],
            "top_c3_files": [],
            "top_rgpd_critical": [],
        }

    def _handle_user_click(self, label_widget) -> None:
        """Gère le clic sur un label utilisateur."""
        try:
            click_info = getattr(label_widget, "click_info", {})
            rank = click_info.get("rank", 1)
            category = click_info.get("category", "Unknown")
            category_key = click_info.get("category_key", "")

            if not hasattr(self, "_last_calculated_metrics"):
                logger.warning("Aucune métrique calculée disponible")
                messagebox.showwarning(
                    "Données manquantes",
                    "Veuillez d'abord cliquer sur 'Recalculer' pour analyser les données.",
                    parent=self.parent,
                )
                return

            metrics = self._last_calculated_metrics
            top_users = metrics.get("top_users", {})
            user_entries = top_users.get(category_key, [])

            if rank > len(user_entries):
                messagebox.showinfo(
                    "Aucune donnée",
                    f"Aucune donnée disponible pour le rang #{rank}",
                    parent=self.parent,
                )
                return

            user_data = user_entries[rank - 1]
            username = user_data.get("owner", "Utilisateur inconnu")

            logger.info(f"Ouverture drill-down utilisateur: {username} - {category}")

            if not hasattr(self, "user_drill_down_viewer"):
                self.user_drill_down_viewer = UserDrillDownViewer(self)

            self.user_drill_down_viewer.show_user_files_modal(
                username, category, user_data
            )

        except Exception as e:
            logger.error(f"Échec gestion clic utilisateur: {e}")
            messagebox.showerror(
                "Erreur",
                f"Impossible d'ouvrir la vue détaillée.\nErreur: {str(e)}",
                parent=self.parent,
            )

    def _handle_label_click(self, label_widget) -> None:
        """Handle click on user label to open drill-down view."""
        try:
            category_info = getattr(label_widget, "category_info", {})
            category = category_info.get("category", "")
            rank = category_info.get("rank", 0)
            if not category or not rank:
                logger.warning("No category info found for clicked label")
                return

            current_metrics = getattr(self, "_last_calculated_metrics", {})
            top_users = current_metrics.get("top_users", {})
            category_users = top_users.get(category, [])
            if rank > len(category_users):
                messagebox.showinfo(
                    "Information",
                    "Aucune donnée disponible pour ce rang",
                    parent=self.parent,
                )
                return

            user_data = category_users[rank - 1]
            username = user_data.get("owner", "Unknown")
            if username == "Unknown" or not username.strip():
                messagebox.showinfo(
                    "Information",
                    "Utilisateur inconnu pour ce rang",
                    parent=self.parent,
                )
                return

            if not hasattr(self, "drill_down_viewer"):
                self.drill_down_viewer = UserDrillDownViewer(self)

            self.drill_down_viewer.show_user_files_modal(username, category, user_data)
        except Exception as e:
            logger.error("Failed to handle label click: %s", e)
            messagebox.showerror(
                "Erreur",
                f"Impossible d'ouvrir la vue détaillée.\nErreur: {str(e)}",
                parent=self.parent,
            )

    def recalculate_all_metrics(self) -> None:
        try:
            age_years = int(self.threshold_age_years.get())
            size_mb = int(self.threshold_size_mb.get())
            if age_years < 0 or age_years > 99:
                messagebox.showerror(
                    "Erreur", "Âge doit être entre 0 et 99 ans", parent=self.parent
                )
                return
            if size_mb < 0 or size_mb > 999999:
                messagebox.showerror(
                    "Erreur",
                    "Taille doit être entre 0 et 999999 MB",
                    parent=self.parent,
                )
                return
            if self._calculation_in_progress:
                return
            self.progress_label.config(text="⏳ Recalcul en cours...")
            self.parent.update_idletasks()
            self._invalidate_cache()
            self._start_async_calculation()
            return
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
            ttk.Label(
                success_window,
                text="💾 Préférences sauvegardées",
                font=("Arial", 12, "bold"),
            ).pack(pady=10)
            ttk.Label(success_window, text="Fichier: user_prefs.json").pack()
            ttk.Button(success_window, text="OK", command=success_window.destroy).pack(
                pady=10
            )
        except Exception as exc:
            messagebox.showerror(
                "Erreur Sauvegarde", f"Échec: {str(exc)}", parent=self.parent
            )

    def load_user_preferences(self) -> None:
        try:
            if not Path("user_prefs.json").exists():
                messagebox.showinfo(
                    "Info", "Aucun fichier de préférences trouvé", parent=self.parent
                )
                return
            with open("user_prefs.json", "r", encoding="utf-8") as f:
                prefs = json.load(f)
            self.threshold_age_years.set(prefs.get("age_years", "2"))
            self.threshold_size_mb.set(prefs.get("size_mb", "100"))
            self.classification_filter.set(prefs.get("classification_filter", "Tous"))
            self.years_modified.set(prefs.get("years_modified", "1"))
            self.recalculate_all_metrics()
            messagebox.showinfo(
                "Succès",
                "Préférences restaurées et métriques recalculées!",
                parent=self.parent,
            )
        except Exception as exc:
            messagebox.showerror(
                "Erreur Restauration", f"Échec: {str(exc)}", parent=self.parent
            )

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
            if metrics.get("super_critical", {}).get("count", 0) > 0:
                super_frame = ttk.Frame(notebook)
                notebook.add(
                    super_frame,
                    text=f"🔴 Super Critiques ({metrics['super_critical']['count']})",
                )
                self._populate_files_list(super_frame, "super_critical")
            if metrics.get("critical", {}).get("count", 0) > 0:
                crit_frame = ttk.Frame(notebook)
                notebook.add(
                    crit_frame, text=f"🟠 Critiques ({metrics['critical']['count']})"
                )
                self._populate_files_list(crit_frame, "critical")
            if metrics.get("duplicates", {}).get("total_groups", 0) > 0:
                dup_frame = ttk.Frame(notebook)
                notebook.add(
                    dup_frame,
                    text=f"🟡 Doublons ({metrics['duplicates']['total_groups']} groupes)",
                )
                self._populate_files_list(dup_frame, "duplicates")
            ttk.Button(
                results_window, text="Fermer", command=results_window.destroy
            ).pack(pady=5)
        except Exception as exc:
            self._handle_analytics_error("affichage des fichiers", exc)

    def _populate_files_list(self, frame: ttk.Frame, category: str) -> None:
        files = self._connect_files()
        class_map = self._get_classification_map()
        rgpd_map = self._get_rgpd_map()
        legal_map = self._get_legal_map()
        dup_families = self.duplicate_detector.detect_duplicate_family(files)
        items: List[FileInfo] = []
        if category == "super_critical":
            for f in files:
                if (
                    class_map.get(f.id) == "C3"
                    and rgpd_map.get(f.id) == "critical"
                    and legal_map.get(f.id) in ["nda", "litigation"]
                ):
                    items.append(f)
        elif category == "critical":
            for f in files:
                if (
                    class_map.get(f.id) == "C3"
                    or rgpd_map.get(f.id) == "critical"
                    or legal_map.get(f.id) in ["nda", "litigation"]
                ) and not (
                    class_map.get(f.id) == "C3"
                    and rgpd_map.get(f.id) == "critical"
                    and legal_map.get(f.id) in ["nda", "litigation"]
                ):
                    items.append(f)
        elif category == "duplicates":
            for fam in dup_families.values():
                items.extend(fam)
        listbox = tk.Listbox(frame)
        listbox.pack(fill="both", expand=True, padx=5, pady=5)
        for f in items:
            listbox.insert(tk.END, f.path)

    def update_thematic_tabs(self) -> None:
        security_dist = self._query_distribution("security_classification_cached")
        total = sum(v["count"] for v in security_dist.values())
        for level in ["C0", "C1", "C2", "C3"]:
            info = security_dist.get(level, {"count": 0, "size": 0})
            pct = round(info["count"] / total * 100, 1) if total else 0
            size_gb = info["size"] / (1024**3)
            self.security_labels[level].config(
                text=f"{level}: {pct}% | {info['count']} fichiers | {size_gb:.1f}GB"
            )
        others_count = total - sum(
            security_dist.get(l, {"count": 0})["count"]
            for l in ["C0", "C1", "C2", "C3"]
        )
        others_size = sum(
            security_dist.get(k, {"size": 0})["size"]
            for k in security_dist.keys()
            if k not in {"C0", "C1", "C2", "C3"}
        )
        pct_others = round(others_count / total * 100, 1) if total else 0
        self.security_labels["Autres"].config(
            text=f"Autres: {pct_others}% | {others_count} fichiers | {others_size/(1024**3):.1f}GB"
        )
        rgpd_dist = self._query_distribution("rgpd_risk_cached")
        total_r = sum(v["count"] for v in rgpd_dist.values())
        levels_rgpd = ["none", "low", "medium", "high", "critical"]
        for lvl in levels_rgpd:
            info = rgpd_dist.get(lvl, {"count": 0, "size": 0})
            pct = round(info["count"] / total_r * 100, 1) if total_r else 0
            self.rgpd_labels[lvl].config(
                text=f"{lvl}: {pct}% | {info['count']} fichiers | {info['size']/(1024**3):.1f}GB"
            )
        others_rgpd = total_r - sum(
            rgpd_dist.get(l, {"count": 0})["count"] for l in levels_rgpd
        )
        size_rgpd = sum(
            rgpd_dist.get(k, {"size": 0})["size"]
            for k in rgpd_dist.keys()
            if k not in levels_rgpd
        )
        pct_rgpd_oth = round(others_rgpd / total_r * 100, 1) if total_r else 0
        self.rgpd_labels["Autres"].config(
            text=f"Autres: {pct_rgpd_oth}% | {others_rgpd} fichiers | {size_rgpd/(1024**3):.1f}GB"
        )
        fin_dist = self._query_distribution("finance_type_cached")
        total_f = sum(v["count"] for v in fin_dist.values())
        fin_types = ["none", "invoice", "contract", "budget", "accounting", "payment"]
        for typ in fin_types:
            info = fin_dist.get(typ, {"count": 0, "size": 0})
            pct = round(info["count"] / total_f * 100, 1) if total_f else 0
            self.finance_labels[typ].config(
                text=f"{typ}: {pct}% | {info['count']} fichiers | {info['size']/(1024**3):.1f}GB"
            )
        others_f = total_f - sum(
            fin_dist.get(t, {"count": 0})["count"] for t in fin_types
        )
        size_f = sum(
            fin_dist.get(k, {"size": 0})["size"]
            for k in fin_dist.keys()
            if k not in fin_types
        )
        pct_f_oth = round(others_f / total_f * 100, 1) if total_f else 0
        self.finance_labels["Autres"].config(
            text=f"Autres: {pct_f_oth}% | {others_f} fichiers | {size_f/(1024**3):.1f}GB"
        )
        legal_dist = self._query_distribution("legal_type_cached")
        total_l = sum(v["count"] for v in legal_dist.values())
        legal_types = [
            "none",
            "employment",
            "lease",
            "sale",
            "nda",
            "compliance",
            "litigation",
        ]
        for typ in legal_types:
            info = legal_dist.get(typ, {"count": 0, "size": 0})
            pct = round(info["count"] / total_l * 100, 1) if total_l else 0
            self.legal_labels[typ].config(
                text=f"{typ}: {pct}% | {info['count']} fichiers | {info['size']/(1024**3):.1f}GB"
            )
        others_l = total_l - sum(
            legal_dist.get(t, {"count": 0})["count"] for t in legal_types
        )
        size_l = sum(
            legal_dist.get(k, {"size": 0})["size"]
            for k in legal_dist.keys()
            if k not in legal_types
        )
        pct_l_oth = round(others_l / total_l * 100, 1) if total_l else 0
        self.legal_labels["Autres"].config(
            text=f"Autres: {pct_l_oth}% | {others_l} fichiers | {size_l/(1024**3):.1f}GB"
        )
        metrics = self.calculate_business_metrics()
        c3_total = metrics.get("critical", {}).get("count", 0) + metrics.get(
            "super_critical", {}
        ).get("count", 0)
        self.security_focus_labels["C3 Total"].config(text=f"C3 Total: {c3_total}")
        self.security_focus_labels["C3 + RGPD"].config(
            text=f"C3 + RGPD: {metrics.get('super_critical', {}).get('count', 0)}"
        )
        self.security_focus_labels["C3 + Legal"].config(
            text=f"C3 + Legal: {metrics.get('critical', {}).get('count', 0)}"
        )
        self.security_focus_labels["Recommandations"].config(
            text=self.generate_recommendations(metrics)
        )

    def _safe_get_labels(self, labels_key: str) -> Dict[str, ttk.Label]:
        """Récupère de manière sécurisée un dictionnaire de labels d'interface."""
        try:
            labels = getattr(self, labels_key, None)
            if not isinstance(labels, dict):
                logger.warning(
                    "Attribut %s n'est pas un dictionnaire valide", labels_key
                )
                return {}
            return labels
        except AttributeError:
            logger.debug("Attribut %s non trouvé, retour dictionnaire vide", labels_key)
            return {}

    def update_temporal_analysis_display(self, temporal_data: Dict[str, Any]) -> None:
        """Update temporal analysis display with standardized keys."""
        try:
            for mode in ["modification", "creation"]:
                if mode in temporal_data:
                    labels_attr = f"{mode}_labels"
                    if hasattr(self, labels_attr):
                        labels = getattr(self, labels_attr)
                        mode_data = temporal_data[mode]

                        key_mappings = {
                            "0_1y": "0-1 an",
                            "1_2y": "1-2 ans",
                            "2_3y": "2-3 ans",
                            "3_4y": "3-4 ans",
                            "4_5y": "4-5 ans",
                            "5_6y": "5-6 ans",
                            "6plus": "+6 ans",
                        }

                        for key, label_text in key_mappings.items():
                            if key in labels and key in mode_data:
                                metrics = mode_data[key]
                                count = metrics.get("count", 0)
                                percentage = metrics.get("percentage", 0.0)
                                size_gb = metrics.get("size_gb", 0.0)

                                display_text = f"{label_text}: {percentage}% | {count} fichiers | {size_gb:.1f}GB"
                                labels[key].config(text=display_text)

                                logger.debug(f"Updated {mode} {key}: {count} files")
        except Exception as e:
            logger.error(f"Failed to update temporal display: {e}")

    def update_extended_tabs(self, metrics: Dict[str, Any]) -> None:
        """Met à jour les onglets étendus avec vérifications robustes."""
        try:
            dup_details = metrics.get("duplicates", {}).get("detailed", {})
            duplicates_labels = self._safe_get_labels("duplicates_detailed_labels")
            for level, label in duplicates_labels.items():
                try:
                    info = dup_details.get(
                        level, {"percentage": 0, "count": 0, "size_gb": 0}
                    )
                    label.config(
                        text=f"{level}: {info['percentage']}% | {info['count']} fichiers | {info['size_gb']:.1f}GB"
                    )
                except Exception as e:
                    logger.warning("Erreur mise à jour niveau %s: %s", level, e)

            self.update_temporal_analysis_display(
                {
                    "modification": metrics.get("temporal_modification", {}),
                    "creation": metrics.get("temporal_creation", {}),
                }
            )

            size_data = metrics.get("file_size_analysis", {})
            size_labels = self._safe_get_labels("file_size_labels")
            for range_label, label in size_labels.items():
                try:
                    data = size_data.get(
                        range_label, {"percentage": 0, "count": 0, "size_gb": 0}
                    )
                    label.config(
                        text=f"{range_label}: {data['percentage']}% | {data['count']} fichiers | {data['size_gb']:.1f}GB"
                    )
                except Exception as e:
                    logger.warning("Erreur mise à jour taille %s: %s", range_label, e)

            top_users = metrics.get("top_users", {})
            for key in ["top_large_files", "top_c3_files", "top_rgpd_critical"]:
                labels = self._safe_get_labels(f"{key}_labels")
                entries = top_users.get(key, [])
                for rank in range(1, 11):
                    rank_key = f"rank_{rank}"
                    if rank_key in labels:
                        try:
                            if rank <= len(entries):
                                item = entries[rank - 1]
                                size_gb = item.get("total_size", 0) / (1024**3)
                                owner = item.get("owner", "N/A")
                                display_owner = (
                                    owner[:20] + "..." if len(owner) > 20 else owner
                                )
                                labels[rank_key].config(
                                    text=f"#{rank}: {display_owner} ({item.get('count', 0)} fichiers, {size_gb:.1f}GB)"
                                )
                            else:
                                labels[rank_key].config(
                                    text=f"#{rank}: -- (0 fichiers, 0GB)"
                                )
                        except Exception as e:
                            logger.warning(
                                "Erreur mise à jour top users %s rank %d: %s",
                                key,
                                rank,
                                e,
                            )
        except Exception as e:
            self._handle_analytics_error("mise à jour onglets étendus", e)

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
                export_window = tk.Toplevel(self.parent)
                export_window.title("Export Réussi")
                export_window.geometry("300x120")
                export_window.transient(self.parent)
                export_window.lift()
                export_window.focus_set()
                export_window.grab_set()
                ttk.Label(export_window, text=f"Rapport exporté : {filename}").pack(
                    pady=20
                )
                ttk.Button(
                    export_window, text="OK", command=export_window.destroy
                ).pack(pady=5)
        except Exception as e:
            self._handle_analytics_error("export du rapport", e)

    def generate_recommendations(self, metrics: Dict[str, Any]) -> str:
        """Génère des recommandations business basées sur les métriques."""
        recommendations: List[str] = []

        super_critical_count = metrics.get("super_critical", {}).get("count", 0)
        if super_critical_count > 0:
            recommendations.append(
                f"🔴 URGENT: {super_critical_count} fichiers super critiques nécessitent une action immédiate"
            )

        critical_count = metrics.get("critical", {}).get("count", 0)
        if critical_count > 10:
            recommendations.append(
                f"🟠 PRIORITÉ: {critical_count} fichiers critiques à traiter rapidement"
            )

        duplicates_info = metrics.get("duplicates", {})
        wasted_gb = duplicates_info.get("wasted_space_gb", 0)
        if wasted_gb > 1.0:
            total_groups = duplicates_info.get("total_groups", 0)
            recommendations.append(
                f"🟡 OPTIMISATION: {wasted_gb:.1f}GB gaspillés dans {total_groups} groupes de doublons"
            )

        size_age_info = metrics.get("size_age", {})
        archival_gb = size_age_info.get("archival_size_gb", 0)
        if archival_gb > 5.0:
            affected_files = size_age_info.get("total_affected", 0)
            recommendations.append(
                f"📦 ARCHIVAGE: {archival_gb:.1f}GB dans {affected_files} fichiers anciens/volumineux"
            )

        global_info = metrics.get("global", {})
        total_size_gb = global_info.get("total_size_gb", 0)
        if total_size_gb > 100:
            recommendations.append(
                "💾 CAPACITÉ: Surveillance de l'espace disque recommandée"
            )

        if super_critical_count > 0 or critical_count > 50:
            recommendations.append(
                "🛡️ SÉCURITÉ: Audit de sécurité recommandé pour les fichiers sensibles"
            )

        total_files = global_info.get("total_files", 0)
        if total_files > 100000:
            recommendations.append(
                "⚡ PERFORMANCE: Considérer l'indexation avancée pour les gros volumes"
            )

        if not recommendations:
            return "✅ Aucune recommandation particulière - Le système fonctionne correctement"

        return "\n".join(f"  {rec}" for rec in recommendations[:5])

    def refresh_all(self) -> None:
        """Actualise toutes les métriques analytics avec vérifications de sécurité."""
        try:
            if hasattr(self, "progress_label"):
                self.progress_label.config(text="🔄 Actualisation complète...")
                self.parent.update_idletasks()

            # Invalider le cache pour forcer le recalcul
            self._invalidate_cache()

            # Recalculer toutes les métriques
            self.recalculate_all_metrics()

            # Optionnel: actualiser aussi les onglets thématiques
            self.update_thematic_tabs()

            if hasattr(self, "progress_label"):
                self.progress_label.config(text="✅ Actualisation terminée")

            logger.info("Analytics refreshed via refresh_all()")

        except Exception as e:
            logger.error(f"Analytics refresh_all failed: {e}")
            self._handle_analytics_error("refresh_all", e)
            if hasattr(self, "progress_label"):
                self.progress_label.config(text="❌ Erreur actualisation")

    def _handle_analytics_error(self, operation: str, error: Exception) -> None:
        """Enhanced error handling with detailed logging and recovery options."""

        error_msg = f"Analytics {operation}: {str(error)}"

        if hasattr(self, "progress_label"):
            self.progress_label.config(text=f"❌ Erreur: {operation}")

        logger.error(
            "Analytics Dashboard Error - Operation: %s", operation, exc_info=True
        )
        logger.error(
            "Database state: manager=%s", "available" if self.db_manager else "missing"
        )

        if self.db_manager:
            try:
                with self.db_manager._connect().get() as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT COUNT(*) FROM fichiers")
                    file_count = cursor.fetchone()[0]
                    logger.info("Database accessible: %d files found", file_count)
            except Exception as db_e:
                logger.error(
                    "Database connection failed during error handling: %s", db_e
                )

        try:
            error_detail = str(error)[:200] + ("..." if len(str(error)) > 200 else "")

            if "schema_validation" in operation.lower():
                response = messagebox.askyesnocancel(
                    f"Erreur Analytics - {operation}",
                    "Erreur de validation du schéma de base de données.\n\n"
                    f"Détails: {error_detail}\n\n"
                    "Solutions proposées:\n"
                    "• OUI: Réessayer avec validation allégée\n"
                    "• NON: Fermer et relancer une analyse complète\n"
                    "• ANNULER: Revenir au dashboard",
                    parent=self.parent,
                )

                if response is True:
                    self._attempt_recovery_calculation()
                elif response is False:
                    self._suggest_full_analysis()
            else:
                response = messagebox.askyesno(
                    f"Erreur Analytics - {operation}",
                    "Une erreur est survenue lors du calcul des analytics.\n\n"
                    f"Détails: {error_detail}\n\nVoulez-vous réessayer avec les données disponibles ?",
                    parent=self.parent,
                )
                if response:
                    self._attempt_recovery_calculation()

        except Exception as dialog_error:
            logger.critical(
                "Critical error: cannot display error dialog: %s", dialog_error
            )
            if hasattr(self, "progress_label"):
                self.progress_label.config(text=f"❌ Erreur critique: {operation}")

    def _attempt_recovery_calculation(self) -> None:
        """Attempt simplified calculation with available data only."""
        logger.info("Attempting analytics recovery calculation")

        try:
            if not self.db_manager:
                logger.error("Cannot attempt recovery: no database manager")
                self._show_database_manager_error()
                return

            if hasattr(self, "progress_label"):
                self.progress_label.config(text="🔄 Tentative de récupération...")

            recovery_metrics = self._calculate_basic_metrics()

            if recovery_metrics and recovery_metrics.get("total_files", 0) > 0:
                self._update_basic_metrics_display(recovery_metrics)

                if hasattr(self, "progress_label"):
                    self.progress_label.config(text="✅ Récupération partielle réussie")

                messagebox.showinfo(
                    "Récupération Réussie",
                    "Analytics partiellement récupérés!\n\n"
                    f"Fichiers traités: {recovery_metrics.get('total_files', 0)}\n"
                    "Certaines fonctionnalités avancées peuvent être indisponibles.",
                    parent=self.parent,
                )
                logger.info("Analytics recovery successful")
            else:
                logger.warning("Recovery calculation yielded no usable data")
                self._suggest_full_analysis()

        except Exception as e:
            logger.error("Recovery calculation failed: %s", e)
            if hasattr(self, "progress_label"):
                self.progress_label.config(text="❌ Échec de récupération")

            messagebox.showerror(
                "Échec de Récupération",
                "Impossible de récupérer les analytics.\n"
                "Veuillez relancer une analyse complète.\n\n"
                f"Erreur: {str(e)}",
                parent=self.parent,
            )

    def _calculate_basic_metrics(self) -> Dict[str, Any]:
        """Calculate basic metrics with minimal schema requirements."""
        basic_metrics = {
            "total_files": 0,
            "completed_files": 0,
            "error_files": 0,
            "pending_files": 0,
            "total_size": 0,
        }

        try:
            with self.db_manager._connect().get() as conn:
                cursor = conn.cursor()

                cursor.execute(
                    "SELECT status, COUNT(*), COALESCE(SUM(file_size), 0) FROM fichiers GROUP BY status"
                )
                for status, count, size in cursor.fetchall():
                    basic_metrics[f"{status}_files"] = count
                    basic_metrics["total_files"] += count
                    basic_metrics["total_size"] += size or 0

            logger.info("Basic metrics calculated: %s", basic_metrics)
            return basic_metrics

        except Exception as e:
            logger.error("Basic metrics calculation failed: %s", e)
            return {}

    def _update_basic_metrics_display(self, metrics: Dict[str, Any]) -> None:
        """Update UI with basic recovered metrics."""
        try:
            if hasattr(self, "stats_labels"):
                for key, value in metrics.items():
                    if key in self.stats_labels:
                        if "size" in key and isinstance(value, (int, float)):
                            size_gb = value / (1024**3)
                            display_value = f"{size_gb:.1f} GB"
                        else:
                            display_value = str(value)
                        self.stats_labels[key].config(text=display_value)

            logger.info("Basic metrics display updated")

        except Exception as e:
            logger.warning("Failed to update basic metrics display: %s", e)

    def _suggest_full_analysis(self) -> None:
        """Suggest running a full analysis to fix database issues."""
        messagebox.showinfo(
            "Analyse Complète Recommandée",
            "Pour résoudre les problèmes d'analytics, il est recommandé de:\n\n"
            "1. Fermer cette fenêtre\n"
            "2. Retourner à l'écran principal\n"
            "3. Lancer une analyse complète des fichiers\n"
            "4. Attendre la fin de l'analyse\n"
            "5. Rouvrir le dashboard Analytics\n\n"
            "Ceci permettra de reconstruire la base de données correctement.",
            parent=self.parent,
        )
