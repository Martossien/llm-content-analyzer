from __future__ import annotations

import csv
import json
import logging
import sqlite3
import sys
import threading
import time
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk
from typing import Any, Dict, Optional
from content_analyzer.utils.sqlite_utils import SQLiteConnectionManager

import pandas as pd
import yaml

from content_analyzer.modules.duplicate_detector import DuplicateDetector, FileInfo

logger = logging.getLogger(__name__)


class ResultsCache:
    """Simple LRU cache for GUI result pages."""

    def __init__(self, max_size: int = 50) -> None:
        self.cache: Dict[str, list] = {}
        self.access_order: list[str] = []
        self.max_size = max_size

    def get(self, key: str) -> Optional[list]:
        if key in self.cache:
            self.access_order.remove(key)
            self.access_order.append(key)
            return self.cache[key]
        return None

    def put(self, key: str, data: list) -> None:
        if len(self.cache) >= self.max_size:
            oldest = self.access_order.pop(0)
            self.cache.pop(oldest, None)
        self.cache[key] = data
        self.access_order.append(key)

    def invalidate(self) -> None:
        self.cache.clear()
        self.access_order.clear()


from content_analyzer.content_analyzer import ContentAnalyzer
from content_analyzer.modules.api_client import APIClient
from content_analyzer.modules.cache_manager import CacheManager
from content_analyzer.modules.csv_parser import CSVParser
from content_analyzer.modules.db_manager import SafeDBManager as DBManager
from content_analyzer.modules.prompt_manager import PromptManager
from content_analyzer.utils.prompt_validator import (
    PromptSizeValidator,
    TkinterDebouncer,
    get_prompt_size_color,
    validate_prompt_size,
)

from .utils.analysis_thread import AnalysisThread
from .utils.multi_worker_analysis_thread import (
    MultiWorkerAnalysisThread,
    ResumableAnalysisThread,
)
from .utils.progress_tracker import ProgressTracker
from .utils.api_test_thread import APITestThread
from .utils.log_viewer import LogViewer
from .utils.service_monitor import ServiceMonitor


class Tooltip:
    """Simple tooltip for Tkinter widgets."""

    def __init__(self, widget: tk.Widget, text: str) -> None:
        self.widget = widget
        self.text = text
        self.tipwindow: tk.Toplevel | None = None
        widget.bind("<Enter>", self.show)
        widget.bind("<Leave>", self.hide)

    def show(self, _event=None) -> None:
        if self.tipwindow or not self.text:
            return
        x = self.widget.winfo_rootx() + 20
        y = self.widget.winfo_rooty() + self.widget.winfo_height() + 10
        self.tipwindow = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        tw.geometry(f"+{x}+{y}")
        label = tk.Label(
            tw,
            text=self.text,
            background="#ffffe0",
            relief="solid",
            borderwidth=1,
            font=("Consolas", 9),
        )
        label.pack(ipadx=1)

    def hide(self, _event=None) -> None:
        if self.tipwindow:
            self.tipwindow.destroy()
            self.tipwindow = None


class MainWindow:
    """Main GUI window for the Content Analyzer application."""

    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("Content Analyzer GUI v1.0")
        self.root.minsize(1200, 800)
        self._center_window(1200, 800)

        self.is_windows = sys.platform == "win32"
        self.platform_multiplier = 2.5 if self.is_windows else 1.0

        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        self.gui_logger = logging.getLogger("gui")
        self.gui_logger.setLevel(logging.INFO)
        handler = logging.FileHandler(log_dir / "gui.log")
        handler.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        )
        self.gui_logger.addHandler(handler)

        self.config_path = Path("content_analyzer/config/analyzer_config.yaml")
        self.csv_file_path: str | None = None
        self.file_tooltip: Tooltip | None = None

        self.service_monitor = ServiceMonitor(self.config_path)
        self.log_viewer = LogViewer(Path("logs/content_analyzer.log"))

        self.db_manager: DBManager | None = None
        self.analysis_thread: MultiWorkerAnalysisThread | None = None
        self.analysis_running = False
        self.api_test_thread: APITestThread | None = None
        self.api_test_running = False

        # Pagination state for results viewer
        self.results_offset = 0
        self.results_limit = 1000
        self.results_total = 0

        # Cache for paginated results
        self.results_cache = ResultsCache(max_size=50)
        self.duplicate_detector = DuplicateDetector()
        self.dup_stats_labels: dict[str, ttk.Label] = {}

        # Thread-safe progress tracker
        self.progress_tracker = ProgressTracker()

        # IDs for periodic callbacks
        self._logs_update_id: str | None = None
        self._service_update_id: str | None = None

        # Debouncer for heavy refresh operations - more responsive
        refresh_delay = 200 if self.is_windows else 100
        self.results_refresh_debouncer = TkinterDebouncer(
            self.root, delay_ms=refresh_delay
        )

        self.prompt_validator = PromptSizeValidator(self.config_path)
        prompt_delay = 1000 if self.is_windows else 500
        self.prompt_debouncer = TkinterDebouncer(self.root, delay_ms=prompt_delay)

        self.build_ui()
        # Auto-detect and load existing database
        self._detect_and_load_existing_database()
        self.load_api_configuration()
        self.load_exclusions()
        self.load_templates()
        self.template_combobox.bind("<<ComboboxSelected>>", self._on_template_selected)
        self.update_prompt_info()
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)
        self.setup_log_viewer()

        self.update_service_status()
        self.update_logs_display()

        welcome_text = f"""
BIENVENUE DANS CONTENT ANALYZER GUI V1.0
{'='*60}

WORKFLOW SIMPLIFIÉ:
1. 📁 Click 'Browse CSV...' → Automatic import after validation
2. 🔍 Click 'View Results' → See imported files immediately
3. ▶️ Click 'START ANALYSIS' → AI analysis on imported files
4. 📊 Click 'View Results' → See analysis results

NEW: CSV import is now automatic!
No need to run analysis to see your files.

{'='*60}
"""
        # messagebox.showinfo("Welcome", welcome_text)
        self.log_action("Application started - CSV auto-import enabled", "INFO")

    def _detect_and_load_existing_database(self) -> None:
        """Automatically detect and load existing database at application startup."""
        try:
            default_db = Path("analysis_results.db")
            if default_db.exists() and default_db.stat().st_size > 0:
                logger.info(f"Existing database detected: {default_db}")
                if self._validate_existing_database(default_db):
                    from content_analyzer.modules.db_manager import DBManager

                    self.db_manager = DBManager(default_db)
                    self.db_file_path = default_db
                    self._update_ui_for_loaded_database(default_db)
                    self.log_action(
                        f"Auto-loaded existing database: {default_db}", "INFO"
                    )
                    logger.info(
                        f"Database manager initialized automatically with {default_db}"
                    )
                else:
                    logger.info(
                        f"Database {default_db} exists but appears empty - skipping auto-load"
                    )
            else:
                logger.info("No existing database found - will wait for CSV import")
        except Exception as e:
            logger.warning(f"Failed to auto-detect existing database: {e}")

    def _validate_existing_database(self, db_path: Path) -> bool:
        """Validate that database contains actual data, not just schema."""
        try:
            import sqlite3

            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='fichiers'"
                )
                if not cursor.fetchone():
                    logger.debug(f"Database {db_path} missing 'fichiers' table")
                    return False

                cursor.execute("SELECT COUNT(*) FROM fichiers")
                file_count = cursor.fetchone()[0]

                if file_count > 0:
                    logger.info(f"Database {db_path} contains {file_count} files")
                    return True
                logger.debug(f"Database {db_path} has empty 'fichiers' table")
                return False
        except Exception as e:
            logger.warning(f"Database validation failed for {db_path}: {e}")
            return False

    def _update_ui_for_loaded_database(self, db_path: Path) -> None:
        """Update UI elements to reflect auto-loaded database."""
        try:
            if hasattr(self, "db_info_label"):
                self.db_info_label.config(
                    text=f"📁 Database: {db_path.name} (auto-loaded)",
                    foreground="green",
                )

            if hasattr(self, "file_path_label"):
                self.file_path_label.config(
                    text=f"Database ready: {db_path.name}",
                    background="lightgreen",
                )

            self._enable_database_dependent_buttons()
            logger.debug("UI updated for auto-loaded database")
        except Exception as e:
            logger.warning(f"Failed to update UI for auto-loaded database: {e}")

    def _enable_database_dependent_buttons(self) -> None:
        """Enable UI buttons that depend on a loaded database."""
        try:
            for btn_attr in (
                "view_results_button",
                "export_button",
                "analytics_button",
            ):
                if hasattr(self, btn_attr):
                    getattr(self, btn_attr).config(state="normal")
        except Exception as e:
            logger.warning(f"Failed to enable DB dependent buttons: {e}")

    # ------------------------------------------------------------------
    # UI BUILDING
    # ------------------------------------------------------------------
    def _center_window(self, width: int, height: int) -> None:
        self.root.update_idletasks()
        screen_w = self.root.winfo_screenwidth()
        screen_h = self.root.winfo_screenheight()
        x = int((screen_w - width) / 2)
        y = int((screen_h - height) / 2)
        self.root.geometry(f"{width}x{height}+{x}+{y}")

    def create_dialog_window(
        self, parent: tk.Toplevel | tk.Tk, title: str, geometry: str = "400x300"
    ) -> tk.Toplevel:
        """Standardized dialog window factory."""
        dialog = tk.Toplevel(parent)
        dialog.title(title)
        dialog.geometry(geometry)
        dialog.transient(parent)

        # Linux z-order fix: temporarily force window on top
        import platform

        if platform.system() == "Linux":
            dialog.attributes("-topmost", True)
            dialog.after(100, lambda: dialog.attributes("-topmost", False))

        dialog.lift()
        dialog.focus_set()
        dialog.update_idletasks()
        x = parent.winfo_x() + (parent.winfo_width() // 2) - (dialog.winfo_width() // 2)
        y = (
            parent.winfo_y()
            + (parent.winfo_height() // 2)
            - (dialog.winfo_height() // 2)
        )
        dialog.geometry(f"+{x}+{y}")
        dialog.after_idle(lambda: dialog.grab_set())
        dialog.after_idle(lambda: dialog.lift())
        dialog.after_idle(lambda: dialog.focus_set())
        return dialog

    def create_tooltip(self, widget: tk.Widget, text: str) -> Tooltip:
        """Helper to attach a tooltip to a widget."""
        return Tooltip(widget, text)

    def build_ui(self) -> None:
        """Construct all UI sections."""

        # SECTION 1 ------------------------------------------------------
        file_frame = ttk.LabelFrame(self.root, text="File Loading")
        file_frame.pack(fill="x", padx=5, pady=5)
        self.browse_button = ttk.Button(
            file_frame, text="Browse CSV...", width=15, command=self.browse_csv_file
        )
        self.browse_button.pack(side="left", padx=5)
        ttk.Label(file_frame, text="📁").pack(side="left", padx=2)
        self.file_path_label = ttk.Label(
            file_frame,
            text="No file selected",
            font=("Consolas", 9),
            background="lightgray",
        )
        self.file_path_label.pack(side="left", padx=5, fill="x", expand=True)

        status_frame = ttk.Frame(file_frame)
        status_frame.pack(side="right", padx=10)
        self.api_status_label = ttk.Label(status_frame, text="● API", foreground="red")
        self.api_status_label.pack(side="left", padx=2)
        self.cache_status_label = ttk.Label(
            status_frame, text="● Cache", foreground="red"
        )
        self.cache_status_label.pack(side="left", padx=2)
        self.db_status_label = ttk.Label(status_frame, text="● DB", foreground="red")
        self.db_status_label.pack(side="left", padx=2)

        # SECTION 2 ------------------------------------------------------
        config_frame = ttk.Frame(self.root)
        config_frame.pack(fill="x", padx=5, pady=5)

        # Panel 2A: API Configuration
        api_frame = ttk.LabelFrame(config_frame, text="API Configuration")
        api_frame.pack(side="left", fill="both", expand=True, padx=2)
        api_frame.columnconfigure(1, weight=1)

        ttk.Label(api_frame, text="API URL:").grid(
            row=0, column=0, sticky="w", padx=2, pady=2
        )
        self.api_url_entry = ttk.Entry(api_frame, width=30)
        self.api_url_entry.grid(row=0, column=1, sticky="ew", padx=2, pady=2)

        ttk.Label(api_frame, text="API Token:").grid(
            row=1, column=0, sticky="w", padx=2, pady=2
        )
        self.api_token_entry = ttk.Entry(api_frame, width=30, show="*")
        self.api_token_entry.grid(row=1, column=1, sticky="ew", padx=2, pady=2)

        ttk.Label(api_frame, text="Workers:").grid(
            row=2, column=0, sticky="w", padx=2, pady=2
        )
        self.workers_entry = ttk.Entry(api_frame, width=5)
        self.workers_entry.grid(row=2, column=1, sticky="w", padx=2, pady=2)
        workers_tooltip = Tooltip(
            self.workers_entry,
            "Nombre de workers parall\xe8les (optimal: 2-8 pour I/O-bound)\nAuto: laissez vide pour d\xe9tection automatique",
        )

        ttk.Label(api_frame, text="Initial Delay (s):").grid(
            row=3, column=0, sticky="w", padx=2, pady=2
        )
        self.upload_spacing_entry = ttk.Entry(api_frame, width=5)
        self.upload_spacing_entry.grid(row=3, column=1, sticky="w", padx=2, pady=2)

        upload_spacing_tooltip = Tooltip(
            self.upload_spacing_entry,
            "Délai initial entre workers (1-99s)\nAjusté automatiquement selon la performance\nDéfaut: 5s",
        )

        self.adaptive_spacing_var = tk.BooleanVar(value=True)
        self.adaptive_spacing_check = ttk.Checkbutton(
            api_frame,
            text="Auto-ajustement espacement",
            variable=self.adaptive_spacing_var,
        )
        self.adaptive_spacing_check.grid(
            row=4, column=0, columnspan=2, sticky="w", padx=2, pady=2
        )

        self.test_api_button = ttk.Button(
            api_frame, text="Test Connection", command=self.test_api_connection
        )
        self.test_api_button.grid(row=5, column=0, sticky="ew", padx=2, pady=5)

        test_api_button = ttk.Button(
            api_frame,
            text="🧪 TEST API",
            command=self.open_api_test_dialog,
            style="Accent.TButton",
        )
        test_api_button.grid(row=5, column=1, sticky="ew", padx=2, pady=5)
        self.create_tooltip(
            test_api_button,
            "Lance des tests de charge pour diagnostiquer bugs de concurrence et mesurer fiabilité LLM",
        )

        ttk.Button(
            api_frame, text="Save Configuration", command=self.save_api_configuration
        ).grid(row=6, column=0, columnspan=2, sticky="ew", padx=2, pady=5)

        # Panel 2B: Exclusions
        excl_frame = ttk.LabelFrame(config_frame, text="File Exclusions")
        excl_frame.pack(side="left", fill="both", expand=True, padx=2)

        ttk.Label(excl_frame, text="Blocked Extensions:").pack(
            anchor="w", padx=2, pady=2
        )
        list_frame = ttk.Frame(excl_frame)
        list_frame.pack(fill="both", expand=True, padx=2, pady=2)
        self.exclusions_listbox = tk.Listbox(list_frame, height=8, selectmode="single")
        self.exclusions_listbox.pack(side="left", fill="both", expand=True)
        ext_scroll = ttk.Scrollbar(
            list_frame, orient="vertical", command=self.exclusions_listbox.yview
        )
        ext_scroll.pack(side="right", fill="y")
        self.exclusions_listbox.config(yscrollcommand=ext_scroll.set)

        add_frame = ttk.Frame(excl_frame)
        add_frame.pack(side="bottom", fill="x", padx=2, pady=2)
        self.add_ext_entry = ttk.Entry(add_frame, width=8)
        self.add_ext_entry.pack(side="left", padx=2)
        ttk.Button(add_frame, text="+", width=3, command=self.add_extension).pack(
            side="left"
        )
        ttk.Button(
            excl_frame, text="Remove Selected", command=self.remove_extension
        ).pack(side="bottom", fill="x", padx=2, pady=2)
        self.skip_system_var = tk.BooleanVar()
        self.skip_hidden_var = tk.BooleanVar()
        ttk.Checkbutton(
            excl_frame,
            text="Skip System Files",
            variable=self.skip_system_var,
            command=self.toggle_system_files,
        ).pack(side="bottom", anchor="w", padx=2, pady=2)
        ttk.Checkbutton(
            excl_frame,
            text="Skip Hidden Files",
            variable=self.skip_hidden_var,
            command=self.toggle_hidden_files,
        ).pack(side="bottom", anchor="w", padx=2, pady=2)

        # Panel 2C: Prompt Management
        prompt_frame = ttk.LabelFrame(config_frame, text="Prompt Templates")
        prompt_frame.pack(side="left", fill="both", expand=True, padx=2)

        ttk.Label(prompt_frame, text="Active Template:").pack(
            anchor="w", padx=2, pady=5
        )
        self.template_combobox = ttk.Combobox(
            prompt_frame,
            values=["comprehensive", "security_focused"],
            state="readonly",
        )
        self.template_combobox.pack(anchor="w", padx=2, pady=2)

        ttk.Separator(prompt_frame, orient="horizontal").pack(fill="x", padx=2, pady=5)

        self.prompt_info_frame = ttk.Frame(prompt_frame)
        self.prompt_info_frame.pack(fill="x", padx=2, pady=2)

        self.prompt_tpl_label = ttk.Label(self.prompt_info_frame, text="Template: N/A")
        self.prompt_sys_label = ttk.Label(
            self.prompt_info_frame, text="System: 0 chars"
        )
        self.prompt_user_label = ttk.Label(self.prompt_info_frame, text="User: 0 chars")
        self.prompt_total_label = ttk.Label(
            self.prompt_info_frame,
            text="Total: 0 chars",
            font=("Arial", 9, "bold"),
        )

        for lbl in (
            self.prompt_tpl_label,
            self.prompt_sys_label,
            self.prompt_user_label,
            self.prompt_total_label,
        ):
            lbl.pack(anchor="w", pady=1)

        ttk.Button(prompt_frame, text="Edit Template", command=self.edit_template).pack(
            fill="x", padx=2, pady=2
        )
        ttk.Button(prompt_frame, text="Test Prompt", command=self.test_prompt).pack(
            fill="x", padx=2, pady=2
        )
        ttk.Button(prompt_frame, text="Save Template", command=self.save_template).pack(
            fill="x", padx=2, pady=2
        )

        # SECTION 3 ------------------------------------------------------
        progress_frame = ttk.LabelFrame(self.root, text="Analysis Progress")
        progress_frame.pack(fill="x", padx=5, pady=5)

        self.progress_metrics_label = ttk.Label(
            progress_frame,
            text="Files: 0/0 (0%) | Speed: 0/min | Cache Hit: 0% | Errors: 0",
            font=("Arial", 10, "bold"),
        )
        self.progress_metrics_label.pack(anchor="w", padx=5, pady=2)

        self.progress_bar = ttk.Progressbar(
            progress_frame, length=600, mode="determinate"
        )
        self.progress_bar.pack(fill="x", padx=5, pady=5)

        self.current_file_label = ttk.Label(
            progress_frame, text="Current File: None", font=("Consolas", 9)
        )
        self.current_file_label.pack(anchor="w", padx=5, pady=2)

        self.time_estimate_label = ttk.Label(
            progress_frame, text="Estimated Time Remaining: --"
        )
        self.time_estimate_label.pack(anchor="w", padx=5, pady=2)

        # SECTION 4 ------------------------------------------------------
        logs_frame = ttk.LabelFrame(self.root, text="System Logs")
        logs_frame.pack(fill="both", expand=True, padx=5, pady=5)

        control_frame = ttk.Frame(logs_frame)
        control_frame.pack(side="top", fill="x", padx=2, pady=2)
        ttk.Label(control_frame, text="Filter:").pack(side="left", padx=2)
        self.log_filter_combobox = ttk.Combobox(
            control_frame, values=["All", "INFO", "WARN", "ERROR"], state="readonly"
        )
        self.log_filter_combobox.current(0)
        self.log_filter_combobox.pack(side="left", padx=2)
        ttk.Button(control_frame, text="Clear Logs", command=self.clear_logs).pack(
            side="left", padx=5
        )
        self.auto_scroll_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            control_frame, text="Auto-scroll", variable=self.auto_scroll_var
        ).pack(side="left", padx=5)

        text_frame = ttk.Frame(logs_frame)
        text_frame.pack(fill="both", expand=True, padx=2, pady=2)
        self.logs_text = tk.Text(
            text_frame, height=12, wrap="word", state="disabled", font=("Consolas", 9)
        )
        self.logs_text.pack(side="left", fill="both", expand=True)
        log_v_scroll = ttk.Scrollbar(
            text_frame, orient="vertical", command=self.logs_text.yview
        )
        log_v_scroll.pack(side="right", fill="y")
        log_h_scroll = ttk.Scrollbar(
            logs_frame, orient="horizontal", command=self.logs_text.xview
        )
        log_h_scroll.pack(fill="x")
        self.logs_text.config(
            yscrollcommand=log_v_scroll.set, xscrollcommand=log_h_scroll.set
        )

        # SECTION 5 ------------------------------------------------------
        action_frame = ttk.Frame(self.root)
        action_frame.pack(fill="x", padx=5, pady=5)

        self.start_button = ttk.Button(
            action_frame, text="START ANALYSIS", width=15, command=self.start_analysis
        )
        self.start_button.pack(side="left", padx=5, pady=5)
        self.pause_button = ttk.Button(
            action_frame,
            text="PAUSE",
            width=15,
            state="disabled",
            command=self.pause_analysis,
        )
        self.pause_button.pack(side="left", padx=5, pady=5)
        self.stop_button = ttk.Button(
            action_frame,
            text="STOP",
            width=15,
            state="disabled",
            command=self.stop_analysis,
        )
        self.stop_button.pack(side="left", padx=5, pady=5)
        ttk.Button(
            action_frame,
            text="\U0001f465 WORKER STATUS",
            width=15,
            command=self.show_worker_status,
        ).pack(side="left", padx=5, pady=5)
        self.single_button = ttk.Button(
            action_frame,
            text="ANALYZE SELECTED FILE",
            width=20,
            command=self.analyze_selected_file,
        )
        self.single_button.pack(side="left", padx=5, pady=5)
        self.view_results_button = ttk.Button(
            action_frame, text="VIEW RESULTS", width=15, command=self.view_results
        )
        self.view_results_button.pack(side="left", padx=5, pady=5)
        ttk.Button(
            action_frame,
            text="MAINTENANCE",
            width=15,
            command=self.show_maintenance_dialog,
        ).pack(side="left", padx=5, pady=5)
        self.export_button = ttk.Button(
            action_frame, text="EXPORT", width=15, command=self.export_results
        )
        self.export_button.pack(side="left", padx=5, pady=5)
        self.analytics_button = ttk.Button(
            action_frame,
            text="\U0001f4ca ANALYTICS",
            width=15,
            command=self.open_analytics_dashboard,
        )
        self.analytics_button.pack(side="left", padx=5, pady=5)

        # SECTION 5B ----------------------------------------------------
        batch_frame = ttk.LabelFrame(self.root, text="Batch Operations")
        batch_frame.pack(fill="x", padx=5, pady=5)

        self.max_files_var = tk.StringVar(value="0")
        ttk.Button(
            batch_frame, text="START BATCH ANALYSIS", command=self.start_analysis
        ).pack(side="left", padx=5)
        self.filtered_button = ttk.Button(
            batch_frame,
            text="ANALYZE FILTERED FILES",
            command=self.analyze_filtered_files,
        )
        self.filtered_button.pack(side="left", padx=5)
        self.reprocess_button = ttk.Button(
            batch_frame, text="REPROCESS ERRORS", command=self.reprocess_errors
        )
        self.reprocess_button.pack(side="left", padx=5)
        ttk.Label(batch_frame, text="Max Files:").pack(side="left", padx=5)
        ttk.Entry(batch_frame, textvariable=self.max_files_var, width=6).pack(
            side="left"
        )
        ttk.Button(
            batch_frame,
            text="ALL FILES",
            command=lambda: self.max_files_var.set("0"),
        ).pack(side="left", padx=5)

        self.cancel_batch = False
        self.cancel_batch_button = ttk.Button(
            batch_frame,
            text="Cancel",
            command=self.cancel_batch_operation,
            state="disabled",
        )
        self.cancel_batch_button.pack(side="left", padx=5)

        # SECTION 6 ------------------------------------------------------
        status_bar = ttk.Frame(self.root)
        status_bar.pack(side="bottom", fill="x")
        status_bar.configure(style="Status.TFrame")
        self.status_app_label = ttk.Label(status_bar, text="Ready")
        self.status_app_label.pack(side="left", padx=5)
        self.status_config_label = ttk.Label(
            status_bar, text=f"Config: {self.config_path.name}"
        )
        self.status_config_label.pack(side="left", padx=10)
        self.status_db_label = ttk.Label(status_bar, text="DB: No database loaded")
        self.status_db_label.pack(side="right", padx=5)

    # ------------------------------------------------------------------
    # FILE LOADING
    # ------------------------------------------------------------------
    def show_csv_preview(self, info: str) -> None:
        messagebox.showinfo("CSV Preview", info, parent=self.root)

    def browse_csv_file(self) -> None:
        """Sélectionne, valide et importe automatiquement un fichier CSV."""
        file_path = filedialog.askopenfilename(
            title="Select SMBeagle CSV File",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            parent=self.root,
        )
        if file_path:
            try:
                parser = CSVParser(self.config_path)

                # Validation de format sur un échantillon
                df = pd.read_csv(file_path, nrows=10)
                errors = parser.validate_csv_format(Path(file_path))
                if errors:
                    messagebox.showerror(
                        "Invalid CSV Format",
                        "CSV validation failed:\n" + "\n".join(errors),
                        parent=self.root,
                    )
                    self.file_path_label.config(
                        text="Invalid CSV format", background="red"
                    )
                    self.csv_file_path = None
                    return

                # Feedback visuel pendant l'import
                self.file_path_label.config(
                    text="Importing CSV...", background="orange"
                )
                self.root.update_idletasks()

                output_db = Path("analysis_results.db")
                import_result = parser.parse_csv_optimized(
                    csv_file=Path(file_path),
                    db_file=output_db,
                    chunk_size=None,
                )

                if import_result["errors"]:
                    error_msg = "Import failed:\n" + "\n".join(
                        import_result["errors"][:3]
                    )
                    messagebox.showerror("Import Error", error_msg, parent=self.root)
                    self.file_path_label.config(text="Import failed", background="red")
                    self.csv_file_path = None
                    return

                if not self._ensure_database_schema():
                    messagebox.showerror(
                        "Schema Error",
                        "CSV imported but database schema verification failed",
                        parent=self.root,
                    )
                    return

                self.db_manager = DBManager(output_db)
                db_size_mb = output_db.stat().st_size / (1024 * 1024)
                db_size_kb = db_size_mb * 1024
                self._update_db_status_labels(db_size_mb)
                if hasattr(self, "analytics_panel"):
                    self.analytics_panel.set_db_manager(self.db_manager)

                self.csv_file_path = file_path
                self.file_path_label.config(text=file_path, background="lightgreen")

                success_msg = (
                    f"✅ CSV imported successfully!\n\n"
                    f"📁 File: {Path(file_path).name}\n"
                    f"📊 Files imported: {import_result['imported_files']:,}\n"
                    f"⏱️ Processing time: {import_result['processing_time']:.1f}s\n"
                    f"💾 Database: {output_db.name} ({db_size_mb:.1f}MB)\n"
                    f"🔍 Ready for 'VIEW RESULTS'"
                )
                messagebox.showinfo("Import Complete", success_msg, parent=self.root)

                self.log_action(
                    f"CSV imported: {import_result['imported_files']:,} files from {Path(file_path).name}",
                    "INFO",
                )

                if self.file_tooltip:
                    self.file_tooltip.hide()
                tooltip_text = (
                    f"Imported: {import_result['imported_files']:,} files\n"
                    f"Database: {output_db.name} ({db_size_kb:.1f}KB)"
                )
                self.file_tooltip = Tooltip(self.file_path_label, tooltip_text)

            except Exception as e:  # pragma: no cover - I/O errors
                messagebox.showerror(
                    "Import Error", f"Failed to import CSV:\n{str(e)}", parent=self.root
                )
                self.file_path_label.config(text="Import failed", background="red")
                self.csv_file_path = None
                self.log_action(f"CSV import failed: {str(e)}", "ERROR")

    # ------------------------------------------------------------------
    # API CONFIGURATION
    # ------------------------------------------------------------------
    def get_api_token(self) -> str:
        return self.api_token_entry.get().strip()

    def load_api_configuration(self) -> None:
        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
            api_config = config.get("api_config", {})
            self.api_url_entry.delete(0, tk.END)
            self.api_url_entry.insert(0, api_config.get("url", "http://localhost:8080"))
            self.api_token_entry.delete(0, tk.END)
            self.api_token_entry.insert(0, api_config.get("token", ""))
            self.workers_entry.delete(0, tk.END)
            self.workers_entry.insert(0, str(api_config.get("batch_size", 3)))

            pipeline_config = config.get("pipeline_config", {})
            upload_cfg = pipeline_config.get("adaptive_spacing", {})
            self.upload_spacing_entry.delete(0, tk.END)
            self.upload_spacing_entry.insert(
                0, str(upload_cfg.get("initial_delay_seconds", 5))
            )
            self.adaptive_spacing_var.set(
                upload_cfg.get("enable_adaptive_spacing", True)
            )
            self.status_config_label.config(foreground="black")
        except Exception as e:  # pragma: no cover - file errors
            messagebox.showerror(
                "Config Error",
                f"Cannot load configuration:\n{str(e)}",
                parent=self.root,
            )

    def test_api_connection(self) -> None:
        url = self.api_url_entry.get().strip()
        if not url:
            messagebox.showerror("Error", "Please enter API URL", parent=self.root)
            return
        self.test_api_button.config(state="disabled", text="Testing...")
        self.root.update_idletasks()
        try:
            start_time = time.time()
            temp_config = {
                "api_config": {
                    "url": url,
                    "token": self.get_api_token(),
                    "timeout_seconds": 10,
                }
            }
            client = APIClient(temp_config)
            success = client.health_check()
            response_time = int((time.time() - start_time) * 1000)
            if success:
                messagebox.showinfo(
                    "Connection Successful",
                    f"API is accessible!\nResponse time: {response_time}ms",
                    parent=self.root,
                )
                self.api_status_label.config(text="● API Connected", foreground="green")
            else:
                messagebox.showerror(
                    "Connection Failed", "API is not accessible", parent=self.root
                )
                self.api_status_label.config(text="● API Failed", foreground="red")
        except Exception as e:  # pragma: no cover - network errors
            messagebox.showerror(
                "Connection Error", f"Connection failed:\n{str(e)}", parent=self.root
            )
            self.api_status_label.config(text="● API Error", foreground="red")
        finally:
            self.test_api_button.config(state="normal", text="Test Connection")

    # ------------------------------------------------------------------
    # API TESTING
    # ------------------------------------------------------------------
    def open_api_test_dialog(self):
        """Ouvre le dialog de configuration des tests API."""
        if not self.config_path or not self.config_path.exists():
            messagebox.showerror(
                "Erreur", "Configuration requise avant de tester l'API"
            )
            return

        test_window = self.create_dialog_window(
            self.root, "Configuration Test API", "600x500"
        )

        file_frame = ttk.LabelFrame(test_window, text="📄 Fichier de Test")
        file_frame.pack(fill="x", padx=10, pady=5)

        self.test_file_var = tk.StringVar()
        file_entry = ttk.Entry(
            file_frame, textvariable=self.test_file_var, state="readonly"
        )
        file_entry.pack(side="left", fill="x", expand=True, padx=5, pady=5)

        browse_test_button = ttk.Button(
            file_frame,
            text="Parcourir",
            command=self.browse_test_file,
        )
        browse_test_button.pack(side="right", padx=5, pady=5)

        params_frame = ttk.LabelFrame(test_window, text="⚙️ Paramètres de Test")
        params_frame.pack(fill="x", padx=10, pady=5)

        ttk.Label(params_frame, text="Nombre de workers:").grid(
            row=0, column=0, sticky="w", padx=5, pady=2
        )
        self.test_workers_var = tk.IntVar(value=4)
        workers_spin = ttk.Spinbox(
            params_frame, from_=1, to=8, textvariable=self.test_workers_var, width=10
        )
        workers_spin.grid(row=0, column=1, sticky="w", padx=5, pady=2)

        ttk.Label(params_frame, text="Nombre d'itérations:").grid(
            row=1, column=0, sticky="w", padx=5, pady=2
        )
        self.test_iterations_var = tk.IntVar(value=20)
        iterations_spin = ttk.Spinbox(
            params_frame,
            from_=5,
            to=100,
            textvariable=self.test_iterations_var,
            width=10,
        )
        iterations_spin.grid(row=1, column=1, sticky="w", padx=5, pady=2)

        ttk.Label(params_frame, text="Délai entre requêtes (s):").grid(
            row=2, column=0, sticky="w", padx=5, pady=2
        )
        self.test_delay_var = tk.DoubleVar(value=0.5)
        delay_scale = ttk.Scale(
            params_frame,
            from_=0,
            to=5,
            variable=self.test_delay_var,
            orient="horizontal",
        )
        delay_scale.grid(row=2, column=1, sticky="ew", padx=5, pady=2)
        delay_label = ttk.Label(params_frame, text="0.5s")
        delay_label.grid(row=2, column=2, padx=5, pady=2)

        def update_delay_label(*_):
            delay_label.config(text=f"{self.test_delay_var.get():.1f}s")

        self.test_delay_var.trace("w", update_delay_label)

        ttk.Label(params_frame, text="Type de template:").grid(
            row=3, column=0, sticky="w", padx=5, pady=2
        )
        self.test_template_var = tk.StringVar(value="comprehensive")
        template_combo = ttk.Combobox(
            params_frame,
            textvariable=self.test_template_var,
            values=["comprehensive", "security_focused"],
            state="readonly",
            width=15,
        )
        template_combo.grid(row=3, column=1, sticky="w", padx=5, pady=2)

        desc_frame = ttk.LabelFrame(test_window, text="ℹ️ Description")
        desc_frame.pack(fill="both", expand=True, padx=10, pady=5)

        desc_text = tk.Text(desc_frame, height=6, wrap="word", state="disabled")
        desc_text.pack(fill="both", expand=True, padx=5, pady=5)

        desc_content = (
            "Ce test va :\n• Analyser le même fichier plusieurs fois avec différents workers\n"
            "• Détecter les corruptions de contenu et troncatures JSON\n"
            "• Mesurer la variance des classifications LLM\n"
            "• Calculer les métriques de performance et scalabilité\n"
            "• Générer des recommandations d'optimisation\n\n"
            "Durée estimée : 2-5 minutes selon les paramètres."
        )

        desc_text.config(state="normal")
        desc_text.insert("1.0", desc_content)
        desc_text.config(state="disabled")

        buttons_frame = ttk.Frame(test_window)
        buttons_frame.pack(fill="x", padx=10, pady=10)

        start_test_button = ttk.Button(
            buttons_frame,
            text="🚀 Démarrer Test",
            command=lambda: self.start_api_test(test_window),
            style="Accent.TButton",
        )
        start_test_button.pack(side="right", padx=5)

        cancel_button = ttk.Button(
            buttons_frame, text="Annuler", command=test_window.destroy
        )
        cancel_button.pack(side="right", padx=5)

    def browse_test_file(self):
        """Sélection du fichier de test."""
        # Identify the Configuration Test API window to use as parent
        current_parent = None
        for widget in self.root.winfo_children():
            if (
                isinstance(widget, tk.Toplevel)
                and "Configuration Test API" in widget.title()
            ):
                current_parent = widget
                break

        if not current_parent:
            current_parent = self.root

        file_path = filedialog.askopenfilename(
            title="Sélectionner un fichier pour les tests",
            filetypes=[
                ("Tous fichiers supportés", "*.pdf *.docx *.txt *.md"),
                ("PDF", "*.pdf"),
                ("Word", "*.docx"),
                ("Texte", "*.txt *.md"),
                ("Tous", "*.*"),
            ],
            parent=current_parent,
        )

        if current_parent and current_parent.winfo_exists():
            current_parent.lift()
            current_parent.focus_set()
            current_parent.grab_set()

        if file_path:
            self.test_file_var.set(file_path)

    def start_api_test(self, test_window):
        """Démarre les tests API."""
        if not self.test_file_var.get():
            messagebox.showerror(
                "Erreur",
                "Veuillez sélectionner un fichier de test",
                parent=test_window,
            )

            if test_window and test_window.winfo_exists():
                test_window.lift()
                test_window.focus_set()
                test_window.grab_set()

            return

        test_file_path = Path(self.test_file_var.get())
        if not test_file_path.exists():
            messagebox.showerror("Erreur", "Le fichier sélectionné n'existe pas")
            return

        test_window.destroy()

        self.create_test_results_window()

        self.api_test_thread = APITestThread(
            config_path=self.config_path,
            test_file_path=test_file_path,
            iterations=self.test_iterations_var.get(),
            max_workers=self.test_workers_var.get(),
            delay_between_requests=self.test_delay_var.get(),
            template_type=self.test_template_var.get(),
            progress_callback=self.on_api_test_progress,
            completion_callback=self.on_api_test_complete,
        )

        self.api_test_running = True
        self.api_test_thread.start()

        self.log_action(
            f"Test API démarré: {test_file_path.name} ({self.test_iterations_var.get()} itérations, {self.test_workers_var.get()} workers)",
            "INFO",
        )

    def create_test_results_window(self):
        """Crée la fenêtre d'affichage des résultats temps réel."""
        self.test_results_window = tk.Toplevel(self.root)
        self.test_results_window.title("🧪 Résultats Test API - Temps Réel")
        self.test_results_window.geometry("800x600")
        self.test_results_window.transient(self.root)

        self.test_progress_var = tk.StringVar(value="Initialisation...")
        self.test_status_var = tk.StringVar(value="En cours")

        header_frame = ttk.Frame(self.test_results_window)
        header_frame.pack(fill="x", padx=10, pady=5)

        ttk.Label(
            header_frame, text="🧪 Test API en cours", font=("Arial", 14, "bold")
        ).pack(side="left")
        status_label = ttk.Label(
            header_frame, textvariable=self.test_status_var, foreground="blue"
        )
        status_label.pack(side="right")

        progress_frame = ttk.Frame(self.test_results_window)
        progress_frame.pack(fill="x", padx=10, pady=5)

        ttk.Label(progress_frame, textvariable=self.test_progress_var).pack(side="left")
        self.test_progress_bar = ttk.Progressbar(progress_frame, mode="determinate")
        self.test_progress_bar.pack(side="right", fill="x", expand=True, padx=(10, 0))

        self.test_time_var = tk.StringVar(value="0s")
        ttk.Label(progress_frame, textvariable=self.test_time_var).pack(
            side="right", padx=5
        )

        notebook = ttk.Notebook(self.test_results_window)
        notebook.pack(fill="both", expand=True, padx=10, pady=5)

        tech_frame = ttk.Frame(notebook)
        notebook.add(tech_frame, text="📊 Métriques Techniques")

        self.tech_metrics_text = tk.Text(tech_frame, height=10, state="disabled")
        tech_scroll = ttk.Scrollbar(
            tech_frame, orient="vertical", command=self.tech_metrics_text.yview
        )
        self.tech_metrics_text.configure(yscrollcommand=tech_scroll.set)
        self.tech_metrics_text.pack(side="left", fill="both", expand=True)
        tech_scroll.pack(side="right", fill="y")

        llm_frame = ttk.Frame(notebook)
        notebook.add(llm_frame, text="🧠 Fiabilité LLM")

        self.llm_metrics_text = tk.Text(llm_frame, height=10, state="disabled")
        llm_scroll = ttk.Scrollbar(
            llm_frame, orient="vertical", command=self.llm_metrics_text.yview
        )
        self.llm_metrics_text.configure(yscrollcommand=llm_scroll.set)
        self.llm_metrics_text.pack(side="left", fill="both", expand=True)
        llm_scroll.pack(side="right", fill="y")

        perf_frame = ttk.Frame(notebook)
        notebook.add(perf_frame, text="⚡ Performance")

        self.perf_metrics_text = tk.Text(perf_frame, height=10, state="disabled")
        perf_scroll = ttk.Scrollbar(
            perf_frame, orient="vertical", command=self.perf_metrics_text.yview
        )
        self.perf_metrics_text.configure(yscrollcommand=perf_scroll.set)
        self.perf_metrics_text.pack(side="left", fill="both", expand=True)
        perf_scroll.pack(side="right", fill="y")

        buttons_frame = ttk.Frame(self.test_results_window)
        buttons_frame.pack(fill="x", padx=10, pady=10)

        self.stop_test_button = ttk.Button(
            buttons_frame,
            text="⏹️ Arrêter Test",
            command=self.stop_api_test,
            state="normal",
        )
        self.stop_test_button.pack(side="left", padx=5)

        self.export_csv_button = ttk.Button(
            buttons_frame,
            text="📋 Export CSV",
            command=lambda: self.export_test_results("csv"),
            state="disabled",
        )
        self.export_csv_button.pack(side="right", padx=5)

        self.export_json_button = ttk.Button(
            buttons_frame,
            text="📄 Export JSON",
            command=lambda: self.export_test_results("json"),
            state="disabled",
        )
        self.export_json_button.pack(side="right", padx=5)

        # Bouton "Nouveau test" à gauche du bouton "Fermer"
        new_test_button = ttk.Button(
            buttons_frame, 
            text="🆕 Nouveau test", 
            command=self.start_new_test,
            state="disabled"
        )
        new_test_button.pack(side="right", padx=5)
        
        close_button = ttk.Button(
            buttons_frame, text="Fermer", command=self.test_results_window.destroy
        )
        close_button.pack(side="right", padx=5)
        
        # Store reference for enabling after test completion
        self.new_test_button = new_test_button

    def on_api_test_progress(self, progress_data):
        """Callback amélioré pour mise à jour progress des tests API."""
        if (
            not hasattr(self, "test_results_window")
            or not self.test_results_window.winfo_exists()
        ):
            return

        completed = progress_data.get("completed", 0)
        total = progress_data.get("total", 1)
        percentage = progress_data.get("percentage", 0.0)

        self.test_progress_bar["maximum"] = total
        self.test_progress_bar["value"] = completed
        self.test_progress_var.set(f"Test {completed}/{total} ({percentage:.1f}%)")

        elapsed = progress_data.get("elapsed_time", 0)
        eta = progress_data.get("eta", 0)
        self.test_time_var.set(f"Écoulé: {elapsed:.1f}s | ETA: {eta:.1f}s")

        metrics = progress_data.get("current_metrics", {})
        self.update_test_metrics_display(metrics)

    def on_api_test_complete(self, results):
        """Callback pour fin des tests API."""
        self.api_test_running = False
        self.test_status_var.set("Terminé")
        self.stop_test_button.config(state="disabled")
        self.export_csv_button.config(state="normal")
        self.export_json_button.config(state="normal")
        # Activer le bouton "Nouveau test" à la fin du test
        if hasattr(self, "new_test_button"):
            self.new_test_button.config(state="normal")

        if hasattr(self.api_test_thread, "get_summary_report"):
            summary = self.api_test_thread.get_summary_report()
            self.display_final_test_summary(summary)

        self.log_action(f"Test API terminé: {results.get('status', 'unknown')}", "INFO")

    def start_new_test(self):
        """Ferme la fenêtre des résultats et relance la configuration des tests API."""
        try:
            # Fermer la fenêtre des résultats
            if hasattr(self, "test_results_window") and self.test_results_window.winfo_exists():
                self.test_results_window.destroy()
            
            # Nettoyer les références
            if hasattr(self, "api_test_thread"):
                self.api_test_thread = None
            self.api_test_running = False
            
            # Relancer la fenêtre de configuration des tests API
            self.open_api_test_dialog()
            
        except Exception as e:
            self.log_action(f"Erreur lors du redémarrage du test: {str(e)}", "ERROR")
            messagebox.showerror("Erreur", f"Impossible de redémarrer le test:\n{str(e)}")

    def update_test_metrics_display(self, metrics):
        """Met à jour l'affichage des métriques avec toutes les données."""
        if not hasattr(self, "tech_metrics_text"):
            return

        successful = metrics.get("successful_responses", 0)
        corrupted = metrics.get("corrupted_responses", 0)
        truncated = metrics.get("truncated_responses", 0)
        malformed = metrics.get("malformed_json", 0)
        throughput = metrics.get("throughput_per_minute", 0.0)

        classification_variance = metrics.get("classification_variance", {})
        security_classifications = classification_variance.get("security", {})
        rgpd_classifications = classification_variance.get("rgpd", {})

        confidence_stats = metrics.get("confidence_stats", {})
        avg_confidence = confidence_stats.get("mean", 0.0)
        confidence_std = confidence_stats.get("std", 0.0)

        display_text = f"""📊 MÉTRIQUES TEMPS RÉEL

🔥 Performance:
   • Réponses réussies: {successful}
   • Corruptions détectées: {corrupted}
   • Troncatures JSON: {truncated}
   • JSON malformés: {malformed}
   • Débit: {throughput:.1f} req/min

🎯 Fiabilité LLM:
   • Confiance moyenne: {avg_confidence:.1f}%
   • Écart-type confiance: {confidence_std:.1f}%

🔄 Variance Classifications:
   • Sécurité: {list(security_classifications.keys())}
   • RGPD: {list(rgpd_classifications.keys())}

⚡ Workers:
   • Actifs: {metrics.get('worker_efficiency', {})}
"""

        # NOUVEAU: Ajouter section RAW RESPONSES
        raw_responses_text = self._format_raw_responses()
        if raw_responses_text:
            display_text += f"\n{raw_responses_text}"

        self.tech_metrics_text.config(state="normal")
        self.tech_metrics_text.delete("1.0", "end")
        self.tech_metrics_text.insert("1.0", display_text)
        self.tech_metrics_text.config(state="disabled")

        # NOUVEAU: Mettre à jour l'onglet Performance
        self._update_performance_metrics_display(metrics)

    def _format_raw_responses(self):
        """Formate les réponses brutes pour affichage dans l'onglet Métriques Techniques.
        
        Returns:
            str: Texte formaté contenant toutes les réponses brutes des tests
        """
        if not hasattr(self, "api_test_thread") or not self.api_test_thread:
            return ""
        
        if not hasattr(self.api_test_thread, "test_results"):
            return ""
            
        test_results = self.api_test_thread.test_results
        if not test_results:
            return ""
        
        # Logger pour debugging
        logger.info(f"Formatage RAW RESPONSES: {len(test_results)} résultats disponibles")
        
        raw_text = "\n🔍 RAW RESPONSES:\n"
        raw_text += "─" * 50 + "\n"
        
        for i, result in enumerate(test_results, 1):
            raw_response = result.get("raw_response", "")
            worker_id = result.get("worker_id", 0)
            iteration = result.get("iteration", i-1)
            status = result.get("status", "unknown")
            
            # Limiter la taille de chaque réponse pour éviter l'overflow
            if len(raw_response) > 500:
                truncated_response = raw_response[:500] + "...[TRONQUÉ]"
            else:
                truncated_response = raw_response
            
            raw_text += f"Test #{i} (Worker {worker_id}, Iter {iteration}, Status: {status}):\n"
            raw_text += f"{truncated_response}\n"
            raw_text += "─" * 30 + "\n"
            
            # Limiter le nombre total d'affichages pour éviter de surcharger l'interface
            if i >= 10:  # Afficher maximum 10 réponses
                remaining = len(test_results) - i
                if remaining > 0:
                    raw_text += f"... et {remaining} autres réponses\n"
                break
        
        return raw_text

    def _update_performance_metrics_display(self, metrics):
        """Met à jour l'affichage des métriques de performance en temps réel.
        
        Args:
            metrics (dict): Dictionnaire contenant les métriques de performance
        """
        if not hasattr(self, "perf_metrics_text"):
            return

        # Récupération des métriques temporelles
        elapsed_time = metrics.get("elapsed_time", 0.0)
        throughput = metrics.get("throughput_per_minute", 0.0)
        successful_responses = metrics.get("successful_responses", 0)
        total_responses = successful_responses + metrics.get("corrupted_responses", 0) + metrics.get("malformed_json", 0)
        
        # Calcul des temps moyens si disponibles
        response_times = metrics.get("response_times", [])
        avg_api_time = sum(response_times) / len(response_times) if response_times else 0.0
        
        # Récupération des données de worker efficiency
        worker_efficiency = metrics.get("worker_efficiency", {})
        active_workers = len([w for w in worker_efficiency.values() if w > 0])
        
        # Calcul du temps moyen total (approximation)
        avg_total_time = avg_api_time * 1.1 if avg_api_time > 0 else 0.0  # API + overhead
        
        # Formatage du temps écoulé
        if elapsed_time >= 60:
            time_display = f"{int(elapsed_time // 60)}m {int(elapsed_time % 60)}s"
        else:
            time_display = f"{elapsed_time:.1f}s"
        
        # Construction du texte d'affichage
        performance_text = f"""⚡ MÉTRIQUES DE PERFORMANCE TEMPS RÉEL

⏱️ Temporel:
   • Temps écoulé total: {time_display}
   • Débit: {throughput:.1f} requêtes/min
   • Réponses traitées: {total_responses}
   • Taux de succès: {(successful_responses/total_responses*100) if total_responses > 0 else 0:.1f}%

🚀 Temps de Réponse:
   • Temps moyen API: {avg_api_time:.2f}s
   • Temps moyen total: {avg_total_time:.2f}s
   • Échantillons: {len(response_times)}

👥 Workers:
   • Workers actifs: {active_workers}
   • Efficacité par worker: {dict(worker_efficiency) if worker_efficiency else 'N/A'}

📈 Performance Globale:
   • Débit instantané: {throughput:.1f} req/min
   • Efficacité système: {(throughput/60*avg_total_time*100) if throughput > 0 and avg_total_time > 0 else 0:.1f}%
"""

        # Mise à jour de l'affichage
        try:
            self.perf_metrics_text.config(state="normal")
            self.perf_metrics_text.delete("1.0", "end")
            self.perf_metrics_text.insert("1.0", performance_text)
            self.perf_metrics_text.config(state="disabled")
            
            # Logger pour debugging
            logger.debug(f"Onglet Performance mis à jour: {throughput:.1f} req/min, {elapsed_time:.1f}s écoulées")
            
        except Exception as e:
            logger.error(f"Erreur mise à jour onglet Performance: {e}")

    def display_final_test_summary(self, summary):
        """Affiche le résumé final des tests."""
        overview = summary.get("test_overview", {})
        reliability = summary.get("reliability_analysis", {})
        recommendations = summary.get("recommendations", [])

        llm_content = (
            f"Taux de succès: {overview.get('success_rate_percent', 0):.1f}%\n"
            f"Consistance sécurité: {reliability.get('security_consistency_percent', 0):.1f}%\n"
            f"Consistance RGPD: {reliability.get('rgpd_consistency_percent', 0):.1f}%\n"
            f"Score fiabilité global: {reliability.get('overall_reliability_score', 0):.1f}%\n"
            f"Confiance moyenne: {reliability.get('confidence_mean', 0):.1f}%\n\n"
            "🎯 RECOMMANDATIONS:\n" + "\n".join(recommendations)
        )

        self.llm_metrics_text.config(state="normal")
        self.llm_metrics_text.delete("1.0", "end")
        self.llm_metrics_text.insert("1.0", llm_content)
        self.llm_metrics_text.config(state="disabled")

    def stop_api_test(self):
        """Arrête les tests API en cours."""
        if hasattr(self, "api_test_thread") and self.api_test_thread.is_alive():
            self.api_test_thread.stop()
            self.test_status_var.set("Arrêt en cours...")
            self.stop_test_button.config(state="disabled")

    def export_test_results(self, format_type):
        """Exporte les résultats des tests."""
        if hasattr(self, "api_test_thread"):
            try:
                export_path = self.api_test_thread.export_test_results(format_type)
                messagebox.showinfo(
                    "Export réussi", f"Résultats exportés vers:\n{export_path}"
                )
                self.log_action(f"Export test API: {export_path}", "INFO")
            except Exception as e:
                messagebox.showerror(
                    "Erreur export", f"Impossible d'exporter:\n{str(e)}"
                )

    def save_api_configuration(self) -> None:
        try:
            url = self.api_url_entry.get().strip()
            if not url.startswith(("http://", "https://")):
                messagebox.showerror(
                    "Invalid URL",
                    "URL must start with http:// or https://",
                    parent=self.root,
                )
                return
            try:
                workers_input = self.workers_entry.get().strip()
                workers = int(workers_input) if workers_input else 0
                if workers_input and (workers < 1 or workers > 32):
                    raise ValueError("Workers must be between 1 and 32")
            except ValueError as e:
                messagebox.showerror(
                    "Invalid Workers",
                    str(e),
                    parent=self.root,
                )
                return
            try:
                upload_spacing = int(self.upload_spacing_entry.get().strip())
                if upload_spacing < 1 or upload_spacing > 99:
                    raise ValueError("Upload spacing must be between 1 and 99 seconds")
            except ValueError as e:
                messagebox.showerror("Invalid Delay", str(e), parent=self.root)
                return
            with open(self.config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
            config["api_config"]["url"] = url
            config["api_config"]["token"] = self.api_token_entry.get().strip()
            if workers > 0:
                config["api_config"]["batch_size"] = workers
            if "pipeline_config" not in config:
                config["pipeline_config"] = {}
            if "adaptive_spacing" not in config["pipeline_config"]:
                config["pipeline_config"]["adaptive_spacing"] = {}
            config["pipeline_config"]["adaptive_spacing"][
                "initial_delay_seconds"
            ] = upload_spacing
            config["pipeline_config"]["adaptive_spacing"][
                "enable_adaptive_spacing"
            ] = self.adaptive_spacing_var.get()
            with open(self.config_path, "w", encoding="utf-8") as f:
                yaml.safe_dump(config, f, default_flow_style=False, indent=2)
            messagebox.showinfo(
                "Configuration Saved",
                "API configuration has been saved successfully!",
                parent=self.root,
            )
            self.status_config_label.config(
                text=f"Config: {self.config_path.name} (saved)", foreground="green"
            )
            self.log_action("API configuration saved")
        except Exception as e:  # pragma: no cover - file errors
            messagebox.showerror(
                "Save Error", f"Cannot save configuration:\n{str(e)}", parent=self.root
            )

    # ------------------------------------------------------------------
    # EXCLUSIONS MANAGEMENT
    # ------------------------------------------------------------------
    def load_exclusions(self) -> None:
        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
            exclusions = config.get("exclusions", {})
            blocked_exts = exclusions.get("extensions", {}).get("blocked", [])
            self.exclusions_listbox.delete(0, tk.END)
            for ext in blocked_exts:
                self.exclusions_listbox.insert(tk.END, ext)
            file_attrs = exclusions.get("file_attributes", {})
            self.skip_system_var.set(file_attrs.get("skip_system", True))
            self.skip_hidden_var.set(file_attrs.get("skip_hidden", False))
        except Exception as e:  # pragma: no cover
            messagebox.showerror(
                "Load Error", f"Cannot load exclusions:\n{str(e)}", parent=self.root
            )

    def add_extension(self) -> None:
        ext = self.add_ext_entry.get().strip()
        if not ext:
            messagebox.showwarning(
                "Empty Extension", "Please enter an extension", parent=self.root
            )
            return
        if not ext.startswith("."):
            ext = "." + ext
        ext = ext.lower()
        current_exts = [
            self.exclusions_listbox.get(i)
            for i in range(self.exclusions_listbox.size())
        ]
        if ext in current_exts:
            messagebox.showwarning(
                "Duplicate Extension",
                f"Extension {ext} is already blocked",
                parent=self.root,
            )
            return
        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
            blocked_exts = config["exclusions"]["extensions"].get("blocked", [])
            blocked_exts.append(ext)
            config["exclusions"]["extensions"]["blocked"] = blocked_exts
            with open(self.config_path, "w", encoding="utf-8") as f:
                yaml.safe_dump(config, f, default_flow_style=False, indent=2)
            self.exclusions_listbox.insert(tk.END, ext)
            self.add_ext_entry.delete(0, tk.END)
            self.log_action(f"Added extension: {ext}")
        except Exception as e:  # pragma: no cover
            messagebox.showerror(
                "Add Error", f"Cannot add extension:\n{str(e)}", parent=self.root
            )

    def remove_extension(self) -> None:
        selection = self.exclusions_listbox.curselection()
        if not selection:
            messagebox.showwarning(
                "No Selection", "Please select an extension to remove", parent=self.root
            )
            return
        ext = self.exclusions_listbox.get(selection[0])
        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
            blocked_exts = config["exclusions"]["extensions"].get("blocked", [])
            if ext in blocked_exts:
                blocked_exts.remove(ext)
            config["exclusions"]["extensions"]["blocked"] = blocked_exts
            with open(self.config_path, "w", encoding="utf-8") as f:
                yaml.safe_dump(config, f, default_flow_style=False, indent=2)
            self.exclusions_listbox.delete(selection[0])
            self.log_action(f"Removed extension: {ext}")
        except Exception as e:  # pragma: no cover
            messagebox.showerror(
                "Remove Error", f"Cannot remove extension:\n{str(e)}", parent=self.root
            )

    def toggle_system_files(self) -> None:
        skip_system = self.skip_system_var.get()
        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
            config["exclusions"]["file_attributes"]["skip_system"] = skip_system
            with open(self.config_path, "w", encoding="utf-8") as f:
                yaml.safe_dump(config, f, default_flow_style=False, indent=2)
            self.log_action(
                f"Skip system files: {'enabled' if skip_system else 'disabled'}"
            )
        except Exception as e:  # pragma: no cover
            messagebox.showerror(
                "Toggle Error", f"Cannot update setting:\n{str(e)}", parent=self.root
            )

    def toggle_hidden_files(self) -> None:
        skip_hidden = self.skip_hidden_var.get()
        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
            config["exclusions"]["file_attributes"]["skip_hidden"] = skip_hidden
            with open(self.config_path, "w", encoding="utf-8") as f:
                yaml.safe_dump(config, f, default_flow_style=False, indent=2)
            self.log_action(
                f"Skip hidden files: {'enabled' if skip_hidden else 'disabled'}"
            )
        except Exception as e:
            messagebox.showerror(
                "Toggle Error", f"Cannot update setting:\n{str(e)}", parent=self.root
            )

    # ------------------------------------------------------------------
    # PROMPT MANAGEMENT
    # ------------------------------------------------------------------
    def load_templates(self) -> None:
        try:
            prompt_manager = PromptManager(self.config_path)
            templates = prompt_manager.get_available_templates()
            self.template_combobox["values"] = templates
            if templates:
                self.template_combobox.set(templates[0])
        except Exception as e:
            messagebox.showerror(
                "Load Error", f"Cannot load templates:\n{str(e)}", parent=self.root
            )

    def edit_template(self) -> None:
        selected_template = self.template_combobox.get()
        if not selected_template:
            messagebox.showwarning(
                "No Template", "Please select a template to edit", parent=self.root
            )
            return
        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
            template_config = config["templates"].get(selected_template, {})
            system_prompt = template_config.get("system_prompt", "")
            user_template = template_config.get("user_template", "")

            editor_window = self.create_dialog_window(
                self.root, f"Edit Template: {selected_template}", "800x600"
            )

            ttk.Label(editor_window, text="System Prompt:").pack(
                anchor="w", padx=5, pady=5
            )
            system_text = tk.Text(editor_window, height=8, wrap="word")
            system_text.pack(fill="x", padx=5, pady=5)
            system_text.insert(1.0, system_prompt)

            ttk.Label(editor_window, text="User Template:").pack(
                anchor="w", padx=5, pady=5
            )
            user_text = tk.Text(editor_window, height=15, wrap="word")
            user_text.pack(fill="both", expand=True, padx=5, pady=5)
            user_text.insert(1.0, user_template)

            sys_count = ttk.Label(
                editor_window, text="System: 0 chars", foreground="gray"
            )
            sys_count.pack(anchor="w", padx=5)
            user_count = ttk.Label(
                editor_window, text="User: 0 chars", foreground="gray"
            )
            user_count.pack(anchor="w", padx=5)
            total_count = ttk.Label(
                editor_window, text="Total: 0 chars", font=("Arial", 9, "bold")
            )
            total_count.pack(anchor="w", padx=5)

            editor_debouncer = TkinterDebouncer(editor_window, delay_ms=300)

            def update_counts_debounced(_event=None) -> None:
                def _calculate() -> None:
                    try:
                        system_text_content = system_text.get(1.0, tk.END)
                        user_text_content = user_text.get(1.0, tk.END)

                        info = validate_prompt_size(
                            system_text_content, user_text_content
                        )

                        _update_editor_labels(info)

                    except Exception as e:
                        logger.error(f"Error in editor calculation: {e}")

                editor_debouncer.schedule_calculation(_calculate)

            def _update_editor_labels(info: Dict[str, Any]) -> None:
                try:

                    def format_count(size: int) -> tuple[str, str]:
                        color = get_prompt_size_color(size, self.prompt_validator)
                        icon = (
                            "✅"
                            if color == "green"
                            else ("⚠️" if color == "orange" else "❌")
                        )
                        return f"{size:,} chars {icon}", color

                    sys_text_val, sys_color = format_count(info["system_size"])
                    sys_count.config(text=sys_text_val, foreground=sys_color)

                    user_text_val, user_color = format_count(info["user_size"])
                    user_count.config(text=user_text_val, foreground=user_color)

                    total_text_val, total_color = format_count(info["total_size"])
                    total_count.config(
                        text=f"Total: {total_text_val}", foreground=total_color
                    )

                except Exception as e:
                    logger.error(f"Error updating editor labels: {e}")

            system_text.bind("<KeyRelease>", update_counts_debounced)
            user_text.bind("<KeyRelease>", update_counts_debounced)

            def cleanup_editor():
                editor_debouncer.cancel()
                editor_window.destroy()

            editor_window.protocol("WM_DELETE_WINDOW", cleanup_editor)

            buttons_frame = ttk.Frame(editor_window)
            buttons_frame.pack(fill="x", padx=5, pady=5)

            def save_template() -> None:
                try:
                    new_system = system_text.get(1.0, tk.END).strip()
                    new_user = user_text.get(1.0, tk.END).strip()
                    config["templates"][selected_template]["system_prompt"] = new_system
                    config["templates"][selected_template]["user_template"] = new_user
                    with open(self.config_path, "w", encoding="utf-8") as f:
                        yaml.safe_dump(config, f, default_flow_style=False, indent=2)
                    messagebox.showinfo(
                        "Template Saved",
                        f"Template '{selected_template}' saved successfully!",
                        parent=editor_window,
                    )
                    editor_window.destroy()
                    self.log_action(f"Template edited: {selected_template}")
                except Exception as e:
                    messagebox.showerror(
                        "Save Error",
                        f"Cannot save template:\n{str(e)}",
                        parent=editor_window,
                    )

            def cancel_edit() -> None:
                editor_window.destroy()

            ttk.Button(buttons_frame, text="Save Template", command=save_template).pack(
                side="right", padx=5
            )
            ttk.Button(buttons_frame, text="Cancel", command=cancel_edit).pack(
                side="right", padx=5
            )
        except Exception as e:  # pragma: no cover
            messagebox.showerror(
                "Edit Error", f"Cannot edit template:\n{str(e)}", parent=self.root
            )

    def test_prompt(self) -> None:
        selected_template = self.template_combobox.get()
        if not selected_template:
            messagebox.showwarning(
                "No Template", "Please select a template to test", parent=self.root
            )
            return
        try:
            prompt_manager = PromptManager(self.config_path)
            sample_metadata = {
                "file_name": "sample_document.pdf",
                "file_size_readable": "2.5MB",
                "owner": "john.doe@company.com",
                "last_modified": "2024-01-15 14:30:00",
                "metadata_summary": "PDF document, 15 pages, created with Microsoft Word",
            }
            prompt = prompt_manager.build_analysis_prompt(
                sample_metadata, selected_template
            )
            preview_window = self.create_dialog_window(
                self.root, f"Prompt Preview: {selected_template}", "700x500"
            )

            ttk.Label(preview_window, text="Generated Prompt:").pack(
                anchor="w", padx=5, pady=5
            )
            preview_text = tk.Text(preview_window, wrap="word", state="normal")
            preview_text.pack(fill="both", expand=True, padx=5, pady=5)
            preview_text.insert(1.0, prompt)
            preview_text.config(state="disabled")
            ttk.Button(
                preview_window, text="Close", command=preview_window.destroy
            ).pack(pady=5)
            self.log_action(f"Prompt tested: {selected_template}")
        except Exception as e:
            messagebox.showerror(
                "Test Error", f"Cannot test prompt:\n{str(e)}", parent=self.root
            )

    def save_template(self) -> None:
        messagebox.showinfo("Save Template", "Template saved.")

    def _on_template_selected(self, _event: tk.Event) -> None:
        self.update_prompt_info()

    def update_prompt_info(self) -> None:
        """Update prompt size info with debouncing in the main thread."""
        template_name = self.template_combobox.get()
        if not template_name:
            return

        def _calculate_safely() -> None:
            """Perform calculations directly in the Tk event loop."""
            try:
                with open(self.config_path, "r", encoding="utf-8") as f:
                    config = yaml.safe_load(f)

                template_config = config.get("templates", {}).get(template_name, {})
                system_prompt = template_config.get("system_prompt", "")
                user_template = template_config.get("user_template", "")

                info = validate_prompt_size(system_prompt, user_template)

                self._update_prompt_labels(template_name, info)

            except Exception as e:
                logger.error(f"Error calculating prompt info: {e}")
                self._update_prompt_labels_error(str(e))

        self.prompt_debouncer.schedule_calculation(_calculate_safely)

    def _update_prompt_labels(self, template_name: str, info: Dict[str, Any]) -> None:
        """Update GUI labels with color coding (main thread only)."""
        try:

            def format_label(size: int) -> tuple[str, str]:
                color = get_prompt_size_color(size, self.prompt_validator)
                icon = (
                    "✅" if color == "green" else ("⚠️" if color == "orange" else "❌")
                )
                return f"{size:,} chars {icon}", color

            self.prompt_tpl_label.config(text=f"Template: {template_name}")

            sys_text, sys_color = format_label(info["system_size"])
            self.prompt_sys_label.config(
                text=f"System: {sys_text}", foreground=sys_color
            )

            user_text, user_color = format_label(info["user_size"])
            self.prompt_user_label.config(
                text=f"User: {user_text}", foreground=user_color
            )

            total_text, total_color = format_label(info["total_size"])
            self.prompt_total_label.config(
                text=f"Total: {total_text}", foreground=total_color
            )

        except Exception as e:
            logger.error(f"Error updating prompt labels: {e}")

    def _update_prompt_labels_error(self, error_msg: str) -> None:
        """Display error state in labels."""
        error_text = f"Error: {error_msg[:20]}..."
        for label in (
            self.prompt_sys_label,
            self.prompt_user_label,
            self.prompt_total_label,
        ):
            label.config(text=error_text, foreground="red")

    # ------------------------------------------------------------------
    # LOG VIEWER
    # ------------------------------------------------------------------
    def setup_log_viewer(self) -> None:
        self.logs_text.tag_config("INFO", foreground="black")
        self.logs_text.tag_config("WARN", foreground="#FF8C00")
        self.logs_text.tag_config("ERROR", foreground="#DC143C")
        self.logs_text.tag_config("DEBUG", foreground="#808080")
        self.log_file_path = Path("logs/content_analyzer.log")
        self.last_log_size = 0
        self.auto_scroll_logs = True

    def update_logs_display(self) -> None:
        try:
            if not (self.log_file_path.exists() and self.logs_text.winfo_exists()):
                return
            current_size = self.log_file_path.stat().st_size
            if current_size <= self.last_log_size:
                pass
            else:
                with open(
                    self.log_file_path, "r", encoding="utf-8", errors="ignore"
                ) as f:
                    f.seek(self.last_log_size)
                    new_content = f.read()
                selected_filter = self.log_filter_combobox.get()
                for line in new_content.strip().split("\n"):
                    if not line.strip():
                        continue
                    log_level = self.parse_log_level(line)
                    if selected_filter != "All" and log_level != selected_filter:
                        continue
                    self.logs_text.config(state="normal")
                    start_pos = self.logs_text.index(tk.END)
                    self.logs_text.insert(tk.END, line + "\n")
                    end_pos = self.logs_text.index(tk.END)
                    self.logs_text.tag_add(log_level, start_pos, end_pos)
                    self.logs_text.config(state="disabled")
                if self.auto_scroll_var.get():
                    self.logs_text.see(tk.END)
                total_lines = int(self.logs_text.index(tk.END).split(".")[0])
                if total_lines > 1000:
                    self.logs_text.config(state="normal")
                    self.logs_text.delete(1.0, f"{total_lines - 1000}.0")
                    self.logs_text.config(state="disabled")
                self.last_log_size = current_size
        except Exception as e:  # pragma: no cover
            print(f"Log update error: {e}")
        finally:
            if self._logs_update_id:
                self.root.after_cancel(self._logs_update_id)
            logs_delay = 8000 if self.is_windows else 3000
            self._logs_update_id = self.root.after(logs_delay, self.update_logs_display)

    def parse_log_level(self, line: str) -> str:
        if " [INFO] " in line:
            return "INFO"
        if " [WARN] " in line:
            return "WARN"
        if " [ERROR] " in line:
            return "ERROR"
        if " [DEBUG] " in line:
            return "DEBUG"
        return "INFO"

    def clear_logs(self) -> None:
        self.logs_text.config(state="normal")
        self.logs_text.delete(1.0, tk.END)
        self.logs_text.config(state="disabled")
        self.log_action("Logs display cleared", "INFO")

    def log_action(self, message: str, level: str = "INFO") -> None:
        timestamp = time.strftime("%H:%M:%S")
        log_line = f"[{level}] {timestamp} - {message}"
        self.logs_text.config(state="normal")
        start_pos = self.logs_text.index(tk.END)
        self.logs_text.insert(tk.END, log_line + "\n")
        end_pos = self.logs_text.index(tk.END)
        self.logs_text.tag_add(level, start_pos, end_pos)
        self.logs_text.config(state="disabled")
        if self.auto_scroll_var.get():
            self.logs_text.see(tk.END)

        level_map = {
            "INFO": logging.INFO,
            "WARN": logging.WARNING,
            "ERROR": logging.ERROR,
            "DEBUG": logging.DEBUG,
        }
        self.gui_logger.log(level_map.get(level, logging.INFO), message)

    def invalidate_all_caches(self) -> None:
        """Invalidate GUI caches and analytics cache."""
        self.results_cache.invalidate()
        self.results_offset = 0
        self.results_total = 0
        if hasattr(self, "analytics_panel"):
            try:
                self.analytics_panel._invalidate_cache()
            except Exception as exc:  # pragma: no cover
                logger.error("Analytics cache invalidation failed: %s", exc)
        self.log_action("All caches invalidated", "INFO")

    # ------------------------------------------------------------------
    # SERVICE STATUS AND PROGRESS
    # ------------------------------------------------------------------
    def _update_db_status_labels(self, size_mb: float | None) -> None:
        if size_mb is None:
            self.db_status_label.config(text="● DB", foreground="red")
            self.status_db_label.config(text="DB: No database loaded")
        else:
            self.db_status_label.config(
                text=f"● DB {size_mb:.1f}MB", foreground="green"
            )
            self.status_db_label.config(text=f"DB: {size_mb:.1f}MB loaded")

    def update_service_status(self) -> None:
        if not (
            self.api_status_label.winfo_exists()
            and self.cache_status_label.winfo_exists()
            and self.db_status_label.winfo_exists()
        ):
            return

        api_status = self.service_monitor.check_api_status()
        self.api_status_label.config(
            foreground="green" if api_status else "red",
            text="● API" if not api_status else "● API Connected",
        )
        cache_stats = self.service_monitor.check_cache_status()
        cache_hit = cache_stats.get("hit_rate", 0.0)
        self.cache_status_label.config(
            text=f"● Cache {cache_hit:.0f}%",
            foreground="green" if cache_stats else "red",
        )
        db_status = self.service_monitor.check_database_status()
        if db_status["accessible"]:
            self._update_db_status_labels(db_status["size_mb"])
        else:
            self._update_db_status_labels(None)

        if self._service_update_id:
            self.root.after_cancel(self._service_update_id)
        service_delay = 15000 if self.is_windows else 5000
        self._service_update_id = self.root.after(
            service_delay, self.update_service_status
        )

    def get_cache_hit_rate(self) -> float:
        stats = self.service_monitor.check_cache_status()
        return float(stats.get("hit_rate", 0.0))

    def update_progress_display(self) -> None:
        if self.analysis_running and self.db_manager:
            try:
                stats = self.db_manager.get_processing_stats()
                total = stats.get("total_files", 0)
                completed = stats.get("completed", 0)
                errors = stats.get("errors", 0)
                processing = stats.get("processing", 0)
                progress_pct = ((completed + errors) / total * 100) if total > 0 else 0
                current_time = time.time()

                # Calculate processing speed (files per minute)
                if hasattr(self, "last_progress_time") and hasattr(
                    self, "last_completed"
                ):
                    time_diff = current_time - self.last_progress_time
                    files_diff = completed - self.last_completed
                    if time_diff >= 1.0:
                        speed = (files_diff / time_diff * 60) if time_diff > 0 else 0
                        self.last_progress_time = current_time
                        self.last_completed = completed
                    else:
                        speed = getattr(self, "last_speed", 0)
                else:
                    speed = 0
                    self.last_progress_time = current_time
                    self.last_completed = completed

                self.last_speed = speed
                cache_hit_rate = self.get_cache_hit_rate()
                validated = self.progress_tracker.update_progress(progress_pct)
                metrics_text = (
                    f"Files: {completed}/{total} ({validated:.1f}%) | "
                    f"Speed: {speed:.0f}/min | "
                    f"Cache Hit: {cache_hit_rate:.1f}% | "
                    f"Errors: {errors}"
                )
                self.progress_metrics_label.config(text=metrics_text)
                self.progress_bar["value"] = validated
                if hasattr(self, "current_file_path"):
                    self.current_file_label.config(
                        text=f"Current File: {self.current_file_path}"
                    )
                if speed > 0:
                    remaining_files = total - completed - errors
                    remaining_minutes = remaining_files / speed
                    if remaining_minutes > 60:
                        time_str = f"{remaining_minutes/60:.1f} hours"
                    else:
                        time_str = f"{remaining_minutes:.0f} minutes"
                    self.time_estimate_label.config(
                        text=f"Estimated Time Remaining: {time_str}"
                    )
            except Exception as e:
                self.log_action(f"Progress update error: {str(e)}", "ERROR")
        if self.analysis_running:
            self.root.after(2000, self.update_progress_display)

    # ------------------------------------------------------------------
    # ANALYSIS CONTROL
    # ------------------------------------------------------------------
    def validate_configuration(self) -> bool:
        try:
            url = self.api_url_entry.get().strip()
            if not url.startswith(("http://", "https://")):
                messagebox.showerror(
                    "Invalid Configuration",
                    "API URL must start with http:// or https://",
                )
                return False
            try:
                workers_input = self.workers_entry.get().strip()
                if workers_input:
                    workers = int(workers_input)
                    if workers < 1 or workers > 32:
                        raise ValueError("Workers must be between 1 and 32")
            except ValueError:
                messagebox.showerror(
                    "Invalid Configuration",
                    "Workers must be a number between 1 and 32, or empty for auto",
                )
                return False
            return True
        except Exception as e:
            messagebox.showerror(
                "Validation Error", f"Configuration validation failed:\n{str(e)}"
            )
            return False

    def start_analysis(self) -> None:

        # debug si bd pas vide
        if not self.csv_file_path:
            # Check if database exists with pending files
            db_path = Path("analysis_results.db")
            if db_path.exists():
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                pending_count = cursor.execute(
                    "SELECT COUNT(*) FROM fichiers WHERE status='pending'"
                ).fetchone()[0]
                conn.close()
                if pending_count > 0:
                    response = messagebox.askyesno(
                        "Resume Analysis",
                        f"Found {pending_count} pending files.\nResume analysis without CSV?",
                    )
                    if response:
                        self.csv_file_path = "resumed"  # Dummy value
                    else:
                        messagebox.showerror(
                            "No CSV File", "Please select a CSV file first"
                        )
                        return
                else:
                    messagebox.showerror(
                        "No CSV File", "Please select a CSV file first"
                    )
                    return
            else:
                messagebox.showerror("No CSV File", "Please select a CSV file first")
                return

        if not self.validate_configuration():
            return
        if not self.service_monitor.check_api_status():
            response = messagebox.askyesno(
                "API Unavailable",
                "API is not accessible. Continue anyway?",
                parent=self.root,
            )
            if not response:
                return
        try:
            output_db = Path("analysis_results.db")
            workers_input = self.workers_entry.get().strip()
            max_workers = int(workers_input) if workers_input else None
            self.analysis_thread = ResumableAnalysisThread(
                config_path=self.config_path,
                csv_file=Path(self.csv_file_path),
                output_db=output_db,
                max_workers=max_workers,
                progress_callback=self.on_analysis_progress_enhanced,
                completion_callback=self.on_analysis_complete_enhanced,
                error_callback=self.on_analysis_error,
            )
            self.db_manager = DBManager(output_db)
            db_size_mb = output_db.stat().st_size / (1024 * 1024)
            self._update_db_status_labels(db_size_mb)
            if hasattr(self, "analytics_panel"):
                self.analytics_panel.set_db_manager(self.db_manager)
            self.analysis_running = True

            # Initialize progress tracking variables
            self.last_progress_time = time.time()
            self.last_completed = 0
            self.start_button.config(state="disabled")
            self.pause_button.config(state="normal")
            self.stop_button.config(state="normal")
            self.browse_button.config(state="disabled")
            self.analysis_thread.start()
            self.update_progress_display()
            optimal_workers = self.analysis_thread.max_workers
            self.log_action(
                f"Analysis started with {optimal_workers} workers: {self.csv_file_path}",
                "INFO",
            )
            self.status_app_label.config(text="Running Analysis...")
        except Exception as e:
            messagebox.showerror("Start Error", f"Cannot start analysis:\n{str(e)}")
            self.log_action(f"Analysis start failed: {str(e)}", "ERROR")

    def pause_analysis(self) -> None:
        if self.analysis_thread and self.analysis_thread.is_alive():
            self.analysis_thread.pause()
            self.pause_button.config(text="RESUME", command=self.resume_analysis)
            self.log_action("Analysis paused", "INFO")
            self.status_app_label.config(text="Analysis Paused")

    def resume_analysis(self) -> None:
        if self.analysis_thread and self.analysis_thread.is_alive():
            self.analysis_thread.resume()
            self.pause_button.config(text="PAUSE", command=self.pause_analysis)
            self.log_action("Analysis resumed", "INFO")
            self.status_app_label.config(text="Running Analysis...")

    def stop_analysis(self) -> None:
        response = messagebox.askyesno(
            "Stop Analysis",
            "Are you sure you want to stop the analysis?\nProgress will be lost.",
            parent=self.root,
        )
        if not response:
            return
        if self.analysis_thread and self.analysis_thread.is_alive():
            try:
                self.analysis_thread.stop()
            except AttributeError:
                pass
            self.analysis_thread.join(timeout=5)
        self.analysis_running = False
        self.start_button.config(state="normal")
        self.pause_button.config(state="disabled", text="PAUSE")
        self.stop_button.config(state="disabled")
        self.browse_button.config(state="normal")
        self.progress_bar["value"] = 0
        self.progress_metrics_label.config(
            text="Files: 0/0 (0%) | Speed: 0/min | Cache: 0% | Errors: 0"
        )
        self.log_action("Analysis stopped by user", "WARN")
        self.status_app_label.config(text="Stopped")

    def _validate_batch_operation(self, operation_type: str) -> bool:
        """Vérifie qu'il y a des fichiers à traiter avant de lancer l'opération."""
        db_path = Path("analysis_results.db")
        if not db_path.exists():
            messagebox.showerror(
                "No database found", "analysis_results.db missing", parent=self.root
            )
            return False

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        if operation_type == "pending":
            count = cursor.execute(
                "SELECT COUNT(*) FROM fichiers WHERE status='pending'"
            ).fetchone()[0]
        else:
            count = cursor.execute(
                "SELECT COUNT(*) FROM fichiers WHERE status='error'"
            ).fetchone()[0]
        conn.close()
        if count == 0:
            messagebox.showinfo(
                "No Files", f"No {operation_type} files found", parent=self.root
            )
            return False
        return True

    def analyze_selected_file(self) -> None:
        """Allow user to analyze a single file using the configured pipeline."""
        file_path = filedialog.askopenfilename(
            title="Select File to Analyze", parent=self.root
        )
        if not file_path:
            return
        try:
            analyzer = ContentAnalyzer(self.config_path)
            meta = {
                "path": file_path,
                "extension": Path(file_path).suffix,
                "file_size": Path(file_path).stat().st_size,
                "file_attributes": "",
                "last_modified": time.strftime(
                    "%Y-%m-%d %H:%M:%S", time.localtime(Path(file_path).stat().st_mtime)
                ),
            }
            res = analyzer.analyze_single_file(meta)
            self.display_analysis_result(res)
        except Exception as exc:  # pragma: no cover - I/O errors
            messagebox.showerror("Error", str(exc))

    def analyze_filtered_files(self) -> None:
        """Reanalyse les fichiers filtrés/pendants en base."""

        if not self._validate_batch_operation("pending"):
            return

        max_input = self.max_files_var.get().strip()
        limit = None if not max_input or max_input == "0" else int(max_input)

        def _background_task() -> None:
            try:
                db_path = Path("analysis_results.db")
                analyzer = ContentAnalyzer(self.config_path)
                db_mgr = DBManager(db_path)
                files = db_mgr.get_pending_files(limit=limit)
                total = len(files)
                processed = 0
                for row in files:
                    if self.cancel_batch:
                        break
                    res = analyzer.analyze_single_file(row)
                    if res.get("status") in {"completed", "cached"}:
                        llm_data = res.get("result", {})
                        llm_data["processing_time_ms"] = res.get(
                            "processing_time_ms", 0
                        )
                        db_mgr.store_analysis_result(
                            row["id"],
                            res.get("task_id", ""),
                            llm_data,
                            res.get("resume", ""),
                            res.get("raw_response", ""),
                        )
                        db_mgr.update_file_status(row["id"], "completed")
                    else:
                        db_mgr.update_file_status(row["id"], "error", res.get("error"))
                    processed += 1
                    self.root.after(0, self._update_batch_progress, processed, total)
                result = {
                    "processed": processed,
                    "total": total,
                    "status": "completed" if not self.cancel_batch else "cancelled",
                }
                self.root.after(0, self._on_batch_complete, result)
            except Exception as exc:  # pragma: no cover - runtime
                self.root.after(0, self._on_batch_error, str(exc))

        self.cancel_batch = False
        self._set_batch_buttons_state("disabled")
        thread = threading.Thread(target=_background_task, daemon=True)
        thread.start()

    def reprocess_errors(self) -> None:
        """Relance l'analyse des fichiers en erreur."""
        if not self._validate_batch_operation("error"):
            return

        max_input = self.max_files_var.get().strip()
        limit = None if not max_input or max_input == "0" else int(max_input)

        def _background_task() -> None:
            try:
                db_path = Path("analysis_results.db")
                analyzer = ContentAnalyzer(self.config_path)
                db_mgr = DBManager(db_path)
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM fichiers WHERE status='error'")
                rows = cursor.fetchall()
                column_names = [d[0] for d in cursor.description]
                conn.close()
                if limit is not None and limit > 0:
                    rows = rows[:limit]
                total = len(rows)
                processed = 0
                for r in rows:
                    if self.cancel_batch:
                        break
                    row = dict(zip(column_names, r))
                    res = analyzer.analyze_single_file(row)
                    if res.get("status") in {"completed", "cached"}:
                        llm_data = res.get("result", {})
                        llm_data["processing_time_ms"] = res.get(
                            "processing_time_ms", 0
                        )
                        db_mgr.store_analysis_result(
                            row["id"],
                            res.get("task_id", ""),
                            llm_data,
                            res.get("resume", ""),
                            res.get("raw_response", ""),
                        )
                        db_mgr.update_file_status(row["id"], "completed")
                    else:
                        db_mgr.update_file_status(row["id"], "error", res.get("error"))
                    processed += 1
                    self.root.after(0, self._update_batch_progress, processed, total)
                result = {
                    "processed": processed,
                    "total": total,
                    "status": "completed" if not self.cancel_batch else "cancelled",
                }
                self.root.after(0, self._on_batch_complete, result)
            except Exception as exc:  # pragma: no cover - runtime
                self.root.after(0, self._on_batch_error, str(exc))

        self.cancel_batch = False
        self._set_batch_buttons_state("disabled")
        thread = threading.Thread(target=_background_task, daemon=True)
        thread.start()

    def on_analysis_progress(self, info: dict) -> None:
        self.current_file_path = info.get("current_file")

    def on_analysis_progress_enhanced(self, info: dict) -> None:
        """Progress callback with worker metrics."""
        self.current_file_path = info.get("current_workers", {}).get(
            "current_files", {}
        )

        processed = info.get("processed", 0)
        total = info.get("total", 0)
        performance = info.get("performance", {})

        progress_pct = (processed / total * 100) if total > 0 else 0
        validated = self.progress_tracker.update_progress(progress_pct)
        self.progress_bar["value"] = validated

        throughput = performance.get("throughput_per_minute", 0)
        cache_hits = performance.get("cache_hits", 0)
        cache_hit_rate = (cache_hits / max(processed, 1)) * 100
        avg_time = performance.get("avg_processing_time", 0.0)

        active_workers = info.get("current_workers", {}).get("active_workers", 0)
        max_workers = info.get("current_workers", {}).get("max_workers", 0)

        metrics_text = (
            f"Files: {processed}/{total} ({validated:.1f}%) | "
            f"Avg API: {avg_time:.1f}s | "
            f"Speed: {throughput:.0f}/min | "
            f"Cache Hit: {cache_hit_rate:.1f}% | "
            f"Workers: {active_workers}/{max_workers}"
        )
        self.progress_metrics_label.config(text=metrics_text)

    def on_analysis_complete(self, result: dict) -> None:
        self.analysis_running = False
        self.start_button.config(state="normal")
        self.pause_button.config(state="disabled", text="PAUSE")
        self.stop_button.config(state="disabled")
        self.browse_button.config(state="normal")
        status = result.get("status", "unknown")
        files_processed = result.get("files_processed", 0)
        files_total = result.get("files_total", 0)
        processing_time = result.get("processing_time", 0)
        errors = result.get("errors", 0)
        completion_msg = (
            f"Analysis completed!\n\n"
            f"Status: {status}\n"
            f"Files processed: {files_processed}/{files_total}\n"
            f"Processing time: {processing_time:.1f}s\n"
            f"Errors: {errors}"
        )
        if status == "completed":
            messagebox.showinfo("Analysis Complete", completion_msg, parent=self.root)
            self.log_action(
                f"Analysis completed successfully: {files_processed}/{files_total} files",
                "INFO",
            )
            self.status_app_label.config(text="Completed")
        else:
            messagebox.showerror("Analysis Failed", completion_msg, parent=self.root)
            self.log_action(f"Analysis failed: {status}", "ERROR")
            self.status_app_label.config(text="Failed")

        self.invalidate_all_caches()

    def on_analysis_complete_enhanced(self, result: dict) -> None:
        """Completion callback with performance statistics."""
        self.analysis_running = False
        self.start_button.config(state="normal")
        self.pause_button.config(state="disabled", text="PAUSE")
        self.stop_button.config(state="disabled")
        self.browse_button.config(state="normal")

        status = result.get("status", "unknown")
        files_processed = result.get("files_processed", 0)
        files_total = result.get("files_total", 0)
        processing_time = result.get("processing_time", 0)
        speedup = result.get("speedup_estimate", 1.0)
        workers_used = result.get("workers_used", 1)
        perf = result.get("performance_stats", {})
        cache_hit_rate = perf.get("cache_hits", 0) / max(files_processed, 1) * 100

        completion_msg = (
            f"Analysis completed!\n\n"
            f"Status: {status}\n"
            f"Files processed: {files_processed}/{files_total}\n"
            f"Workers used: {workers_used}\n"
            f"Estimated speedup: {speedup:.1f}x\n"
            f"Throughput: {perf.get('throughput_per_minute', 0):.0f} files/min\n"
            f"Cache hit rate: {cache_hit_rate:.1f}%\n"
            f"Processing time: {processing_time:.1f}s"
        )

        if status == "completed":
            messagebox.showinfo("Analysis Complete", completion_msg, parent=self.root)
            self.log_action(
                f"Analysis completed successfully: {files_processed}/{files_total} files",
                "INFO",
            )
            self.status_app_label.config(text="Completed")
        else:
            messagebox.showerror("Analysis Failed", completion_msg, parent=self.root)
            self.log_action(f"Analysis failed: {status}", "ERROR")
            self.status_app_label.config(text="Failed")

        self.invalidate_all_caches()

    def on_analysis_error(self, error: str) -> None:
        self.analysis_running = False
        self.start_button.config(state="normal")
        self.pause_button.config(state="disabled", text="PAUSE")
        self.stop_button.config(state="disabled")
        self.browse_button.config(state="normal")
        messagebox.showerror("Analysis Error", error, parent=self.root)
        self.log_action(f"Analysis error: {error}", "ERROR")
        self.status_app_label.config(text="Error")

    # ------------------------------------------------------------------
    # BATCH OPERATION HELPERS
    # ------------------------------------------------------------------

    def _set_batch_buttons_state(self, state: str) -> None:
        self.start_button.config(state=state)
        self.filtered_button.config(state=state)
        self.reprocess_button.config(state=state)
        self.cancel_batch_button.config(
            state="normal" if state == "disabled" else "disabled"
        )

    def cancel_batch_operation(self) -> None:
        self.cancel_batch = True

    def _update_batch_progress(self, processed: int, total: int) -> None:
        pct = (processed / total * 100) if total else 0
        self.progress_bar["value"] = pct
        self.progress_metrics_label.config(
            text=f"Files: {processed}/{total} ({pct:.1f}%)"
        )

    def _on_batch_complete(self, result: dict) -> None:
        self._set_batch_buttons_state("normal")
        self.progress_bar["value"] = 0
        status = result.get("status")
        msg = f"Batch {status}!\nProcessed: {result.get('processed')}/{result.get('total')}"
        if status == "completed":
            messagebox.showinfo("Batch Complete", msg, parent=self.root)
        else:
            messagebox.showwarning("Batch Cancelled", msg, parent=self.root)

    def _on_batch_error(self, error: str) -> None:
        self._set_batch_buttons_state("normal")
        messagebox.showerror("Batch Error", error, parent=self.root)

    def _update_page_controls(
        self,
        page_label: ttk.Label | None = None,
        prev_btn: ttk.Button | None = None,
        next_btn: ttk.Button | None = None,
    ) -> None:
        """Update pagination controls state in the results viewer."""
        if page_label:
            start = self.results_offset + 1
            end = min(self.results_offset + self.results_limit, self.results_total)
            page_label.config(text=f"Showing {start}-{end} of {self.results_total}")
        if prev_btn:
            state = "normal" if self.results_offset > 0 else "disabled"
            prev_btn.config(state=state)
        if next_btn:
            state = (
                "normal"
                if self.results_offset + self.results_limit < self.results_total
                else "disabled"
            )
            next_btn.config(state=state)

    # ------------------------------------------------------------------
    # RESULTS VIEWER AND EXPORTS
    # ------------------------------------------------------------------
    def view_results(self) -> None:
        """Ouvre une fenêtre pour visualiser les résultats d'analyse."""
        try:
            if not self._ensure_database_schema():
                messagebox.showerror(
                    "Database Error",
                    "Cannot access database. Please import a CSV file first or check database integrity.",
                    parent=self.root,
                )
                return

            db_path = Path("analysis_results.db")
            if not db_path.exists():
                messagebox.showwarning(
                    "No Results",
                    "No analysis results database found.\nPlease import a CSV file first using 'Browse CSV...'",
                    parent=self.root,
                )
                self.log_action("Results viewer: no database found", "WARN")
                return

            total_count = self._safe_get_results_count("All", "All")
            if total_count == 0:
                messagebox.showinfo(
                    "No Data",
                    "Database exists but contains no files.\nPlease import a CSV file first.",
                    parent=self.root,
                )
                return
            # debug refresh View Result
            self.results_cache.invalidate()
            self.results_offset = 0
            self.results_total = 0

            results_window = self.create_dialog_window(
                self.root, "Analysis Results Viewer", "1400x700"
            )
            results_window.protocol(
                "WM_DELETE_WINDOW",
                lambda win=results_window: self._cleanup_results_window(win),
            )

            controls_frame = ttk.Frame(results_window)
            controls_frame.pack(fill="x", padx=10, pady=5)

            ttk.Label(controls_frame, text="Filter by Status:").pack(
                side="left", padx=5
            )
            status_filter = ttk.Combobox(
                controls_frame,
                values=["All", "completed", "error", "pending"],
                state="readonly",
            )
            status_filter.set("All")
            status_filter.pack(side="left", padx=5)

            ttk.Label(controls_frame, text="Filter by Classification:").pack(
                side="left", padx=10
            )
            classification_filter = ttk.Combobox(
                controls_frame,
                values=["All", "C0", "C1", "C2", "C3"],
                state="readonly",
            )
            classification_filter.set("All")
            classification_filter.pack(side="left", padx=5)

            self.show_duplicates_var = tk.BooleanVar(value=False)
            duplicates_check = ttk.Checkbutton(
                controls_frame,
                text="\U0001f501 Afficher uniquement les doublons FastHash+Taille",
                variable=self.show_duplicates_var,
                command=lambda: self._on_duplicates_filter_changed(
                    tree,
                    status_filter,
                    classification_filter,
                    page_label,
                    prev_page_btn,
                    next_page_btn,
                ),
            )
            duplicates_check.pack(side="left", padx=10)

            tree_frame = ttk.Frame(results_window)
            tree_frame.pack(fill="both", expand=True, padx=10, pady=5)

            columns = (
                "ID",
                "Name",
                "Host",
                "Extension",
                "Username",
                "Path",
                "Size",
                "Owner",
                "Creation Time",
                "Last Modified",
                "Status",
                "Type",
                "Security",
                "Sec_Conf",
                "RGPD",
                "RGPD_Conf",
                "Finance",
                "Fin_Conf",
                "Legal",
                "Legal_Conf",
                "Résumé",
                "Confidence",
                "Processing Time",
            )
            tree = ttk.Treeview(tree_frame, columns=columns, show="headings", height=20)

            tree.heading("ID", text="ID")
            tree.heading("Name", text="Name")
            tree.heading("Host", text="Host")
            tree.heading("Extension", text="Extension")
            tree.heading("Username", text="Username")
            tree.heading("Path", text="Path")
            tree.heading("Size", text="Size (bytes)")
            tree.heading("Owner", text="Owner")
            tree.heading("Creation Time", text="Created")
            tree.heading("Last Modified", text="Last Modified")
            tree.heading("Status", text="Status")
            tree.heading("Type", text="Type")
            tree.heading("Security", text="Security")
            tree.heading("Sec_Conf", text="Sec%")
            tree.heading("RGPD", text="RGPD")
            tree.heading("RGPD_Conf", text="RGPD%")
            tree.heading("Finance", text="Finance")
            tree.heading("Fin_Conf", text="Fin%")
            tree.heading("Legal", text="Legal")
            tree.heading("Legal_Conf", text="Legal%")
            tree.heading("Résumé", text="Résumé")
            tree.heading("Confidence", text="Confidence")
            tree.heading("Processing Time", text="Proc. Time (ms)")

            tree.column("ID", width=50)
            tree.column("Name", width=200)
            tree.column("Host", width=120)
            tree.column("Extension", width=80)
            tree.column("Username", width=150)
            tree.column("Path", width=400)
            tree.column("Size", width=100)
            tree.column("Owner", width=200)
            tree.column("Creation Time", width=120)
            tree.column("Last Modified", width=120)
            tree.column("Status", width=80)
            tree.column("Type", width=150)
            tree.column("Security", width=80)
            tree.column("Sec_Conf", width=70)
            tree.column("RGPD", width=80)
            tree.column("RGPD_Conf", width=70)
            tree.column("Finance", width=80)
            tree.column("Fin_Conf", width=70)
            tree.column("Legal", width=80)
            tree.column("Legal_Conf", width=70)
            tree.column("Résumé", width=200)
            tree.column("Confidence", width=80)
            tree.column("Processing Time", width=100)

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

            # Navigation controls must be created before callbacks referencing
            # them to avoid late-binding issues.
            nav_frame = ttk.Frame(results_window)
            nav_frame.pack(fill="x", padx=10, pady=5)

            page_label = ttk.Label(nav_frame, text="")
            page_label.pack(side="right", padx=5)

            prev_page_btn = ttk.Button(nav_frame, text="◀ PREV 1000")
            prev_page_btn.pack(side="right", padx=2)

            next_page_btn = ttk.Button(nav_frame, text="NEXT 1000 ▶")
            next_page_btn.pack(side="right", padx=2)

            first_file_btn = ttk.Button(nav_frame, text="FIRST FILE")
            first_file_btn.pack(side="left", padx=2)

            prev_file_btn = ttk.Button(nav_frame, text="◀ PREV FILE")
            prev_file_btn.pack(side="left", padx=2)

            next_file_btn = ttk.Button(nav_frame, text="NEXT FILE ▶")
            next_file_btn.pack(side="left", padx=2)

            last_file_btn = ttk.Button(nav_frame, text="LAST FILE")
            last_file_btn.pack(side="left", padx=2)

            ttk.Label(nav_frame, text="GO TO FILE:").pack(side="left", padx=5)
            goto_var = tk.StringVar()
            goto_entry = ttk.Entry(nav_frame, textvariable=goto_var, width=6)
            goto_entry.pack(side="left")

            jump_btn = ttk.Button(nav_frame, text="JUMP")
            jump_btn.pack(side="left", padx=2)

            # ---- Callback definitions ----
            def goto_first() -> None:
                items = tree.get_children()
                if items:
                    tree.selection_set(items[0])
                    tree.see(items[0])

            def goto_prev() -> None:
                sel = tree.selection()
                items = tree.get_children()
                if sel:
                    idx = items.index(sel[0])
                    if idx > 0:
                        tree.selection_set(items[idx - 1])
                        tree.see(items[idx - 1])

            def goto_next() -> None:
                sel = tree.selection()
                items = tree.get_children()
                if sel:
                    idx = items.index(sel[0])
                    if idx < len(items) - 1:
                        tree.selection_set(items[idx + 1])
                        tree.see(items[idx + 1])

            def goto_last() -> None:
                items = tree.get_children()
                if items:
                    tree.selection_set(items[-1])
                    tree.see(items[-1])

            def change_page(delta: int) -> None:
                self.results_offset += delta * self.results_limit
                if self.results_offset < 0:
                    self.results_offset = 0
                if self.results_offset >= self.results_total:
                    self.results_offset = max(
                        0, self.results_total - self.results_limit
                    )
                self.refresh_results_table(
                    tree,
                    status_filter.get(),
                    classification_filter.get(),
                    self.results_offset,
                    page_label,
                    prev_page_btn,
                    next_page_btn,
                )

            def jump() -> None:
                """Jump to a specific file ID across all pages."""
                val = goto_var.get().strip()
                if not val.isdigit():
                    messagebox.showwarning(
                        "Invalid ID",
                        "Please enter a numeric file ID",
                        parent=tree.winfo_toplevel(),
                    )
                    return

                target_id = int(val)

                db_path = Path("analysis_results.db")
                if not db_path.exists():
                    messagebox.showerror(
                        "Database Missing",
                        "No analysis database found.",
                        parent=tree.winfo_toplevel(),
                    )
                    self.log_action("Jump failed: no database", "ERROR")
                    return

                try:
                    self.root.config(cursor="wait")

                    conn = sqlite3.connect(db_path)
                    cursor = conn.cursor()

                    base_query = """
        FROM fichiers f
        LEFT JOIN reponses_llm r ON f.id = r.fichier_id
        WHERE 1=1
                    """
                    params: list[Any] = []

                    if status_filter.get() != "All":
                        base_query += " AND f.status = ?"
                        params.append(status_filter.get())

                    if classification_filter.get() != "All":
                        base_query += " AND r.security_analysis LIKE ?"
                        params.append(
                            f'%"classification": "{classification_filter.get()}"%'
                        )

                    # Duplicate filtering handled via DuplicateDetector

                    # Verify file existence with current filters
                    cursor.execute(
                        "SELECT COUNT(*) " + base_query + " AND f.id = ?",
                        params + [target_id],
                    )
                    if cursor.fetchone()[0] == 0:
                        conn.close()
                        self.root.config(cursor="")
                        messagebox.showwarning(
                            "File Not Found",
                            f"File ID {target_id} not found with current filters.",
                            parent=tree.winfo_toplevel(),
                        )
                        self.log_action(
                            f"Jump failed: ID {target_id} not found", "WARN"
                        )
                        return

                    # Count rows with higher ID (ORDER BY id DESC)
                    cursor.execute(
                        "SELECT COUNT(*) " + base_query + " AND f.id > ?",
                        params + [target_id],
                    )
                    position = cursor.fetchone()[0]
                    conn.close()

                    # Determine page offset
                    page_index = position // self.results_limit
                    self.results_offset = page_index * self.results_limit

                    # Refresh table at new page
                    self.refresh_results_table(
                        tree,
                        status_filter.get(),
                        classification_filter.get(),
                        self.results_offset,
                        page_label,
                        prev_page_btn,
                        next_page_btn,
                    )

                    # Highlight the target item
                    for itm in tree.get_children():
                        if tree.item(itm)["values"][0] == target_id:
                            tree.selection_set(itm)
                            tree.see(itm)
                            tree.focus(itm)
                            break

                    self.log_action(f"Jumped to file ID {target_id}")

                except Exception as e:
                    messagebox.showerror(
                        "Jump Error",
                        f"Failed to jump to file ID {target_id}:\n{str(e)}",
                        parent=tree.winfo_toplevel(),
                    )
                    self.log_action(f"Jump error: {str(e)}", "ERROR")
                finally:
                    self.root.config(cursor="")

            # ---- Command configuration ----
            first_file_btn.configure(command=goto_first)
            prev_file_btn.configure(command=goto_prev)
            next_file_btn.configure(command=goto_next)
            last_file_btn.configure(command=goto_last)
            jump_btn.configure(command=jump)
            prev_page_btn.configure(command=lambda: change_page(-1))
            next_page_btn.configure(command=lambda: change_page(1))

            refresh_btn = ttk.Button(
                controls_frame,
                text="Refresh",
                command=lambda: (
                    self.results_cache.invalidate(),
                    self.force_refresh_results_table(
                        tree,
                        status_filter.get(),
                        classification_filter.get(),
                        self.results_offset,
                        page_label,
                        prev_page_btn,
                        next_page_btn,
                    ),
                ),
            )

            refresh_btn.pack(side="right", padx=5)

            export_btn = ttk.Button(
                controls_frame,
                text="Export to CSV",
                command=lambda tr=tree: self.export_results_to_csv(tr),
            )
            export_btn.pack(side="right", padx=5)

            self.results_offset = 0
            self.refresh_results_table(
                tree,
                "All",
                "All",
                self.results_offset,
                page_label,
                prev_page_btn,
                next_page_btn,
            )

            status_filter.bind(
                "<<ComboboxSelected>>",
                lambda e, tr=tree, sf=status_filter, cf=classification_filter: (
                    setattr(self, "results_offset", 0),
                    self.refresh_results_table(
                        tr,
                        sf.get(),
                        cf.get(),
                        self.results_offset,
                        page_label,
                        prev_page_btn,
                        next_page_btn,
                    ),
                ),
            )
            classification_filter.bind(
                "<<ComboboxSelected>>",
                lambda e, tr=tree, sf=status_filter, cf=classification_filter: (
                    setattr(self, "results_offset", 0),
                    self.refresh_results_table(
                        tr,
                        sf.get(),
                        cf.get(),
                        self.results_offset,
                        page_label,
                        prev_page_btn,
                        next_page_btn,
                    ),
                ),
            )

            tree.bind(
                "<Double-1>", lambda e: self.show_file_details(tree, results_window)
            )

        except Exception as e:
            messagebox.showerror(
                "Results Error", f"Failed to open results viewer:\n{str(e)}"
            )
            self.log_action(f"Results viewer failed: {str(e)}", "ERROR")

    def _cleanup_results_window(self, window: tk.Toplevel) -> None:
        """Cleanup resources when closing the results viewer."""
        self.results_refresh_debouncer.cancel()
        self.results_offset = 0
        self.results_total = 0
        window.destroy()

    def _create_duplicate_stats_panel(self, parent_frame: tk.Widget) -> None:
        """Crée le panneau de statistiques des doublons."""
        self.dup_stats_labels: dict[str, ttk.Label] = {}

        stats_frame = ttk.LabelFrame(
            parent_frame, text="\U0001f4ca Statistiques des Doublons"
        )
        stats_frame.pack(fill="x", padx=10, pady=5)

        metrics_frame = ttk.Frame(stats_frame)
        metrics_frame.pack(fill="x", padx=5, pady=5)

        for i in range(4):
            metrics_frame.columnconfigure(i, weight=1)

        def create_card(column: int, title: str, icon: str) -> ttk.Label:
            card = ttk.Frame(metrics_frame, padding=5, relief="groove", borderwidth=1)
            card.grid(row=0, column=column, padx=5, pady=2, sticky="nsew")
            ttk.Label(card, text=icon, font=("Arial", 14)).pack()
            value_label = ttk.Label(card, text="0", font=("Arial", 12, "bold"))
            value_label.pack()
            ttk.Label(card, text=title).pack()
            return value_label

        self.dup_stats_labels["families"] = create_card(0, "Familles", "\U0001f465")
        self.dup_stats_labels["files"] = create_card(1, "Fichiers", "\U0001f4c1")
        self.dup_stats_labels["space"] = create_card(2, "Espace", "\U0001f4be")
        self.dup_stats_labels["distribution"] = create_card(
            3, "Distribution", "\U0001f4c8"
        )

    def _update_duplicate_stats(
        self, status_filter: str, classification_filter: str
    ) -> None:
        """Met à jour les statistiques selon les filtres actifs."""
        try:
            db_path = Path("analysis_results.db")
            if not db_path.exists():
                stats = {
                    "total_families": 0,
                    "total_duplicates": 0,
                    "total_sources": 0,
                    "total_copies": 0,
                    "space_wasted_bytes": 0,
                    "largest_family_size": 0,
                    "average_family_size": 0,
                }
            else:
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                query = (
                    "SELECT f.id, f.path, f.fast_hash, f.file_size, f.creation_time, "
                    "f.last_modified FROM fichiers f LEFT JOIN reponses_llm r ON f.id = r.fichier_id WHERE 1=1"
                )
                params: list[Any] = []
                if status_filter != "All":
                    query += " AND f.status = ?"
                    params.append(status_filter)
                if classification_filter and classification_filter != "All":
                    query += " AND r.security_classification_cached = ?"
                    params.append(classification_filter)
                cursor.execute(query, params)
                rows = cursor.fetchall()
                conn.close()

                files = [
                    FileInfo(
                        id=r[0],
                        path=r[1],
                        fast_hash=r[2],
                        file_size=r[3] or 0,
                        creation_time=r[4],
                        last_modified=r[5],
                    )
                    for r in rows
                ]
                families = self.duplicate_detector.detect_duplicate_family(files)
                stats = self.duplicate_detector.get_duplicate_statistics(families)

            fam_label = self.dup_stats_labels.get("families")
            if fam_label is not None:
                color = "orange" if stats["total_families"] > 0 else "green"
                fam_label.config(text=str(stats["total_families"]), foreground=color)

            files_label = self.dup_stats_labels.get("files")
            if files_label is not None:
                color = (
                    "red"
                    if stats["total_copies"] > stats["total_sources"]
                    else ("orange" if stats["total_copies"] > 0 else "green")
                )
                files_label.config(
                    text=f"{stats['total_duplicates']} ({stats['total_copies']} copies)",
                    foreground=color,
                )

            space_label = self.dup_stats_labels.get("space")
            if space_label is not None:
                wasted = stats["space_wasted_bytes"]
                color = (
                    "red"
                    if wasted > 1024 * 1024 * 1024
                    else ("orange" if wasted > 100 * 1024 * 1024 else "green")
                )
                space_label.config(
                    text=self._format_file_size(wasted),
                    foreground=color,
                )

            dist_label = self.dup_stats_labels.get("distribution")
            if dist_label is not None:
                dist_label.config(
                    text=f"Max: {stats['largest_family_size']} | Moy: {stats['average_family_size']}",
                )
        except Exception as e:
            logger.error("Error updating duplicate stats: %s", e)

    def _format_file_size(self, size_bytes: int) -> str:
        """Format size with intelligent units."""
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size_bytes < 1024:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.1f} PB"

    def _format_percentage(self, part: int, total: int) -> str:
        """Format percentage safely."""
        if total == 0:
            return "0%"
        return f"{(part / total * 100):.1f}%"

    # ------------------------------------------------------------------
    # Database helpers
    # ------------------------------------------------------------------

    def _ensure_database_schema(self) -> bool:
        """Ensure that the analysis database and tables exist."""
        try:
            db_path = Path("analysis_results.db")

            # Create new DB if missing
            if not db_path.exists():
                self.log_action("Database missing, creating new schema", "INFO")
                if not getattr(self, "db_manager", None):
                    self.db_manager = DBManager(db_path)
                return True

            with SQLiteConnectionManager(db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT name FROM sqlite_master
                    WHERE type='table' AND name='fichiers'
                    """
                )
                table_exists = cursor.fetchone() is not None
                if not table_exists:
                    self.log_action("Table 'fichiers' missing, creating schema", "WARN")
                    from content_analyzer.modules.csv_parser import CSVParser

                    parser = CSVParser(self.config_path)
                    with SQLiteConnectionManager(db_path) as new_conn:
                        parser._ensure_schema(new_conn)
                    return True

            return True

        except Exception as e:  # pragma: no cover - runtime safeguard
            self.log_action(f"Schema verification failed: {str(e)}", "ERROR")
            return False

    def _safe_get_results_count(
        self, status_filter: str, classification_filter: str
    ) -> int:
        """Count results safely ensuring the schema exists."""
        if not self._ensure_database_schema():
            return 0

        try:
            if hasattr(self, "show_duplicates_var") and self.show_duplicates_var.get():
                return len(
                    self._get_duplicate_file_ids(status_filter, classification_filter)
                )
            db_path = Path("analysis_results.db")
            with SQLiteConnectionManager(db_path) as conn:
                cursor = conn.cursor()
                query = "SELECT COUNT(*) FROM fichiers f LEFT JOIN reponses_llm r ON f.id = r.fichier_id WHERE 1=1"
                params: list[Any] = []
                if status_filter != "All":
                    query += " AND f.status = ?"
                    params.append(status_filter)
                if classification_filter and classification_filter != "All":
                    query += " AND r.security_classification_cached = ?"
                    params.append(classification_filter)
                cursor.execute(query, params)
                count = cursor.fetchone()[0]
                return count
        except Exception as e:  # pragma: no cover - runtime safeguard
            self.log_action(f"Safe count failed: {str(e)}", "ERROR")
            return 0

    def _safe_get_optimized_results(
        self,
        status_filter: str,
        classification_filter: str,
        offset: int = 0,
        limit: int = 1000,
    ) -> list[tuple]:
        """Retrieve optimized results ensuring the schema exists."""
        if not self._ensure_database_schema():
            self.log_action("Cannot execute query: schema verification failed", "ERROR")
            return []

        try:
            if self.is_windows and limit > 500:
                limit = 500
            db_path = Path("analysis_results.db")
            with SQLiteConnectionManager(db_path) as conn:
                cursor = conn.cursor()

                base_query = """
                SELECT f.id, f.name, f.host, f.extension, f.username, f.path,
                       f.file_size, f.owner, f.creation_time, f.last_modified, f.status,
                       r.security_classification_cached,
                       r.security_confidence,
                       r.rgpd_risk_cached,
                       r.rgpd_confidence,
                       r.finance_type_cached,
                       r.finance_confidence,
                       r.legal_type_cached,
                       r.legal_confidence,
                       r.document_resume,
                       r.confidence_global,
                       r.processing_time_ms
                FROM fichiers f
                LEFT JOIN reponses_llm r ON f.id = r.fichier_id
                WHERE 1=1
                """

                params: list[Any] = []

                if status_filter != "All":
                    base_query += " AND f.status = ?"
                    params.append(status_filter)

                if classification_filter and classification_filter != "All":
                    base_query += " AND r.security_classification_cached = ?"
                    params.append(classification_filter)

                base_query += " ORDER BY f.id DESC LIMIT ? OFFSET ?"
                params.extend([limit, offset])

                cursor.execute(base_query, params)
                rows = cursor.fetchall()

            if hasattr(self, "show_duplicates_var") and self.show_duplicates_var.get():
                dup_ids = self._get_duplicate_file_ids(
                    status_filter, classification_filter
                )
                rows = [r for r in rows if r[0] in dup_ids]
            return rows

        except Exception as e:  # pragma: no cover - runtime safeguard
            self.log_action(f"Safe query failed: {str(e)}", "ERROR")
            return []

    def _safe_get_optimized_results_with_duplicates_info(
        self,
        status_filter: str,
        classification_filter: str,
        offset: int = 0,
        limit: int = 1000,
    ) -> list[tuple]:
        """Wrapper around _get_optimized_results_with_duplicates_info with schema checks."""
        if not self._ensure_database_schema():
            return []

        base_results = self._safe_get_optimized_results(
            status_filter, classification_filter, offset=offset, limit=limit
        )

        if not (
            hasattr(self, "show_duplicates_var") and self.show_duplicates_var.get()
        ):
            return [row + ("",) for row in base_results]

        ids = [r[0] for r in base_results]
        if not ids:
            return [row + ("",) for row in base_results]

        try:
            db_path = Path("analysis_results.db")
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            placeholders = ",".join("?" * len(ids))
            cursor.execute(
                f"SELECT id, fast_hash FROM fichiers WHERE id IN ({placeholders})",
                ids,
            )
            hash_map = {row[0]: row[1] for row in cursor.fetchall()}
            conn.close()

            files = [
                FileInfo(
                    id=r[0],
                    path=r[5],
                    fast_hash=hash_map.get(r[0]),
                    file_size=r[6] or 0,
                    creation_time=r[8],
                    last_modified=r[9],
                )
                for r in base_results
            ]

            self.log_action(
                f"Processing {len(files)} files for duplicate analysis",
                "DEBUG",
            )

            families = self.duplicate_detector.detect_duplicate_family(files)

            file_type_map: dict[int, str] = {}
            for fam_files in families.values():
                if len(fam_files) > 1:
                    source = self.duplicate_detector.identify_source(fam_files)
                    file_type_map[source.id] = "SOURCE"
                    for f in fam_files:
                        if f.id != source.id:
                            file_type_map[f.id] = f"COPY_{source.id}"

            enriched: list[tuple] = []
            for r in base_results:
                enriched.append(r + (file_type_map.get(r[0], ""),))

            if self.show_duplicates_var.get():
                enriched = self._sort_by_duplicate_families(enriched)

            return enriched

        except Exception as e:  # pragma: no cover - runtime safeguard
            self.log_action(f"Duplicate analysis failed: {str(e)}", "ERROR")
            base_results = self._safe_get_optimized_results(
                status_filter, classification_filter, offset=offset, limit=limit
            )
            return [row + ("",) for row in base_results]

    # ------------------------------------------------------------------
    # Optimized DB queries
    # ------------------------------------------------------------------

    def _get_optimized_results(
        self,
        status_filter: str,
        classification_filter: str,
        offset: int = 0,
        limit: int = 1000,
    ) -> list[tuple]:
        if self.is_windows and limit > 500:
            limit = 500
        db_path = Path("analysis_results.db")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        base_query = """
        SELECT f.id, f.name, f.host, f.extension, f.username, f.path,
               f.file_size, f.owner, f.creation_time, f.last_modified, f.status,
               r.security_classification_cached,
               r.security_confidence,
               r.rgpd_risk_cached,
               r.rgpd_confidence,
               r.finance_type_cached,
               r.finance_confidence,
               r.legal_type_cached,
               r.legal_confidence,
               r.document_resume,
               r.confidence_global,
               r.processing_time_ms
        FROM fichiers f
        LEFT JOIN reponses_llm r ON f.id = r.fichier_id
        WHERE 1=1
        """

        params: list[Any] = []

        if status_filter != "All":
            base_query += " AND f.status = ?"
            params.append(status_filter)

        if classification_filter and classification_filter != "All":
            base_query += " AND r.security_classification_cached = ?"
            params.append(classification_filter)

        base_query += " ORDER BY f.id DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        cursor.execute(base_query, params)
        rows = cursor.fetchall()
        conn.close()
        if hasattr(self, "show_duplicates_var") and self.show_duplicates_var.get():
            dup_ids = self._get_duplicate_file_ids(status_filter, classification_filter)
            rows = [r for r in rows if r[0] in dup_ids]
        return rows

    def _get_results_count(self, status_filter: str, classification_filter: str) -> int:
        if hasattr(self, "show_duplicates_var") and self.show_duplicates_var.get():
            return len(
                self._get_duplicate_file_ids(status_filter, classification_filter)
            )
        db_path = Path("analysis_results.db")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        query = "SELECT COUNT(*) FROM fichiers f LEFT JOIN reponses_llm r ON f.id = r.fichier_id WHERE 1=1"
        params: list[Any] = []
        if status_filter != "All":
            query += " AND f.status = ?"
            params.append(status_filter)
        if classification_filter and classification_filter != "All":
            query += " AND r.security_classification_cached = ?"
            params.append(classification_filter)
        cursor.execute(query, params)
        count = cursor.fetchone()[0]
        conn.close()
        return count

    def _get_duplicate_file_ids(
        self, status_filter: str, classification_filter: str
    ) -> set[int]:
        db_path = Path("analysis_results.db")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        query = "SELECT f.id, f.path, f.fast_hash, f.file_size, f.creation_time, f.last_modified FROM fichiers f LEFT JOIN reponses_llm r ON f.id = r.fichier_id WHERE 1=1"
        params: list[Any] = []
        if status_filter != "All":
            query += " AND f.status = ?"
            params.append(status_filter)
        if classification_filter and classification_filter != "All":
            query += " AND r.security_classification_cached = ?"
            params.append(classification_filter)
        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()

        files = [
            FileInfo(
                id=r[0],
                path=r[1],
                fast_hash=r[2],
                file_size=r[3] or 0,
                creation_time=r[4],
                last_modified=r[5],
            )
            for r in rows
        ]
        families = self.duplicate_detector.detect_duplicate_family(files)
        dup_ids: set[int] = set()
        for fam in families.values():
            dup_ids.update(f.id for f in fam)
        return dup_ids

    def _get_optimized_results_with_duplicates_info(
        self,
        status_filter: str,
        classification_filter: str,
        offset: int = 0,
        limit: int = 1000,
    ) -> list[tuple]:
        """Return paginated results enriched with duplicate type info."""
        base_results = self._get_optimized_results(
            status_filter, classification_filter, offset=offset, limit=limit
        )

        # When duplicate filter is inactive simply return rows with empty Type
        if not (
            hasattr(self, "show_duplicates_var") and self.show_duplicates_var.get()
        ):
            return [row + ("",) for row in base_results]

        ids = [r[0] for r in base_results]
        if not ids:
            return [row + ("",) for row in base_results]

        db_path = Path("analysis_results.db")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        placeholders = ",".join("?" * len(ids))
        cursor.execute(
            f"SELECT id, fast_hash FROM fichiers WHERE id IN ({placeholders})",
            ids,
        )
        hash_map = {row[0]: row[1] for row in cursor.fetchall()}
        conn.close()

        files = [
            FileInfo(
                id=r[0],
                path=r[5],
                fast_hash=hash_map.get(r[0]),
                file_size=r[6] or 0,
                creation_time=r[8],
                last_modified=r[9],
            )
            for r in base_results
        ]

        families = self.duplicate_detector.detect_duplicate_family(files)

        file_type_map: dict[int, str] = {}
        for fam_files in families.values():
            if len(fam_files) > 1:
                source = self.duplicate_detector.identify_source(fam_files)
                file_type_map[source.id] = "SOURCE"
                for f in fam_files:
                    if f.id != source.id:
                        file_type_map[f.id] = f"COPY_{source.id}"

        enriched: list[tuple] = []
        for r in base_results:
            enriched.append(r + (file_type_map.get(r[0], ""),))

        if self.show_duplicates_var.get():
            enriched = self._sort_by_duplicate_families(enriched)

        return enriched

    def _sort_by_duplicate_families(self, results: list[tuple]) -> list[tuple]:
        """Group results so that each duplicate family is contiguous."""
        families: dict[str, list[tuple]] = {}
        non_duplicates: list[tuple] = []

        for row in results:
            file_type = row[-1] or ""
            if file_type == "SOURCE":
                key = f"family_{row[0]}"
                families.setdefault(key, []).append(row)
            elif file_type and file_type.startswith("COPY_"):
                source_id = file_type.split("COPY_")[1]
                key = f"family_{source_id}"
                families.setdefault(key, []).append(row)
            else:
                non_duplicates.append(row)

        sorted_results: list[tuple] = []
        for fam_rows in families.values():
            fam_rows.sort(key=lambda x: (0 if x[-1] == "SOURCE" else 1, x[8]))
            sorted_results.extend(fam_rows)
        sorted_results.extend(non_duplicates)
        return sorted_results

    def refresh_results_table(
        self,
        tree,
        status_filter,
        classification_filter,
        offset: int = 0,
        page_label: ttk.Label | None = None,
        prev_btn: ttk.Button | None = None,
        next_btn: ttk.Button | None = None,
    ):
        """Schedule a debounced refresh of the results table."""
        self.results_refresh_debouncer.schedule_calculation(
            self._perform_refresh_results_table,
            tree,
            status_filter,
            classification_filter,
            offset,
            page_label,
            prev_btn,
            next_btn,
        )

    def force_refresh_results_table(
        self,
        tree,
        status_filter,
        classification_filter,
        offset: int = 0,
        page_label: ttk.Label | None = None,
        prev_btn: ttk.Button | None = None,
        next_btn: ttk.Button | None = None,
    ) -> None:
        """Immediate refresh without debouncing."""
        self.results_refresh_debouncer.cancel()
        self._perform_refresh_results_table(
            tree,
            status_filter,
            classification_filter,
            offset,
            page_label,
            prev_btn,
            next_btn,
        )

    def _perform_refresh_results_table(
        self,
        tree,
        status_filter,
        classification_filter,
        offset: int = 0,
        page_label: ttk.Label | None = None,
        prev_btn: ttk.Button | None = None,
        next_btn: ttk.Button | None = None,
    ) -> None:
        """Refresh the results table with filters and pagination."""
        try:
            tree.delete(*tree.get_children())
            tree.update_idletasks()

            cache_key = f"{status_filter}_{classification_filter}_{self.results_offset}"
            cached = self.results_cache.get(cache_key)
            if cached is not None:
                rows = cached
            else:
                rows = self._safe_get_optimized_results_with_duplicates_info(
                    status_filter,
                    classification_filter,
                    offset=offset,
                    limit=self.results_limit,
                )
                self.results_cache.put(cache_key, rows)

            if hasattr(self, "show_duplicates_var") and self.show_duplicates_var.get():
                self.results_total = len([r for r in rows if r[-1]])
            else:
                self.results_total = self._safe_get_results_count(
                    status_filter, classification_filter
                )

            self._insert_rows_batch(tree, rows)

            self._update_page_controls(page_label, prev_btn, next_btn)
            self.log_action(
                f"Results table refreshed with duplicates info: {len(rows)} entries",
                "INFO",
            )

        except Exception as e:
            messagebox.showerror(
                "Refresh Error", f"Failed to refresh results:\n{str(e)}"
            )
            self.log_action(f"Results refresh failed: {str(e)}", "ERROR")

    def _insert_rows_batch(self, tree, rows) -> None:
        """Insert rows including the duplicate Type column."""
        if not self.is_windows:
            for row in rows:
                if len(row) == 22:
                    row = row + ("",)
                (
                    file_id,
                    name,
                    host,
                    extension,
                    username,
                    path,
                    size,
                    owner,
                    creation_time,
                    last_modified,
                    status,
                    security_class,
                    security_conf,
                    rgpd_risk,
                    rgpd_conf,
                    finance_type,
                    finance_conf,
                    legal_type,
                    legal_conf,
                    resume,
                    confidence,
                    proc_time,
                    file_type,
                ) = row

                display_type = file_type or ""

                tree.insert(
                    "",
                    "end",
                    values=(
                        file_id,
                        name,
                        host,
                        extension,
                        username,
                        (
                            (path or "")[-50:] + "..."
                            if path and len(path) > 50
                            else (path or "")
                        ),
                        size,
                        owner,
                        creation_time,
                        last_modified,
                        status,
                        display_type,
                        security_class,
                        security_conf or 0,
                        rgpd_risk,
                        rgpd_conf or 0,
                        finance_type,
                        finance_conf or 0,
                        legal_type,
                        legal_conf or 0,
                        resume or "",
                        confidence or 0,
                        proc_time or 0,
                    ),
                )
            return

        batch_size = 25

        def insert_batch(start: int = 0) -> None:
            end = min(start + batch_size, len(rows))
            for i in range(start, end):
                row = rows[i]
                if len(row) == 22:
                    row = row + ("",)
                (
                    file_id,
                    name,
                    host,
                    extension,
                    username,
                    path,
                    size,
                    owner,
                    creation_time,
                    last_modified,
                    status,
                    security_class,
                    security_conf,
                    rgpd_risk,
                    rgpd_conf,
                    finance_type,
                    finance_conf,
                    legal_type,
                    legal_conf,
                    resume,
                    confidence,
                    proc_time,
                    file_type,
                ) = row

                display_type = file_type or ""

                tree.insert(
                    "",
                    "end",
                    values=(
                        file_id,
                        name,
                        host,
                        extension,
                        username,
                        (
                            (path or "")[-50:] + "..."
                            if path and len(path) > 50
                            else (path or "")
                        ),
                        size,
                        owner,
                        creation_time,
                        last_modified,
                        status,
                        display_type,
                        security_class,
                        security_conf or 0,
                        rgpd_risk,
                        rgpd_conf or 0,
                        finance_type,
                        finance_conf or 0,
                        legal_type,
                        legal_conf or 0,
                        resume or "",
                        confidence or 0,
                        proc_time or 0,
                    ),
                )
            if end < len(rows):
                self.root.after(5, lambda: insert_batch(end))

        insert_batch()

    def _on_duplicates_filter_changed(
        self,
        tree,
        status_filter,
        classification_filter,
        page_label,
        prev_btn,
        next_btn,
    ) -> None:
        """Callback triggered when duplicate filter checkbox changes."""
        self.results_cache.invalidate()
        self.results_offset = 0
        filter_state = self.show_duplicates_var.get()
        self.log_action(f"Duplicates filter changed: {filter_state}", "INFO")
        self.force_refresh_results_table(
            tree,
            status_filter.get(),
            classification_filter.get(),
            0,
            page_label,
            prev_btn,
            next_btn,
        )

    def show_file_details(self, tree, parent_window: tk.Toplevel | tk.Tk | None = None):
        """Affiche les détails complets d'un fichier sélectionné."""
        selection = tree.selection()
        if not selection:
            return

        item = tree.item(selection[0])
        file_id = item["values"][0]

        if parent_window is None:
            parent_window = tree.winfo_toplevel()

        try:
            db_path = Path("analysis_results.db")
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            cursor.execute(
                """
        SELECT f.path, f.file_size, f.owner, f.last_modified, f.status,
               r.security_analysis, r.security_confidence,
               r.rgpd_analysis, r.rgpd_confidence,
               r.finance_analysis, r.finance_confidence,
               r.legal_analysis, r.legal_confidence,
               r.document_resume, r.llm_response_complete,
               r.confidence_global, r.processing_time_ms, r.created_at
        FROM fichiers f
        LEFT JOIN reponses_llm r ON f.id = r.fichier_id
        WHERE f.id = ?
            """,
                (file_id,),
            )

            row = cursor.fetchone()
            conn.close()

            if not row:
                messagebox.showwarning(
                    "No Data", "No details found for this file", parent=parent_window
                )
                return

            details_window = self.create_dialog_window(
                parent_window, f"File Details - ID {file_id}", "800x600"
            )

            text_widget = tk.Text(details_window, wrap="word", font=("Consolas", 10))
            text_widget.pack(fill="both", expand=True, padx=10, pady=10)

            (
                path,
                size,
                owner,
                modified,
                status,
                security,
                sec_conf,
                rgpd,
                rgpd_conf,
                finance,
                fin_conf,
                legal,
                legal_conf,
                resume,
                raw_response,
                confidence,
                proc_time,
                created,
            ) = row

            llm_json = {
                "security": json.loads(security) if security else {},
                "rgpd": json.loads(rgpd) if rgpd else {},
                "finance": json.loads(finance) if finance else {},
                "legal": json.loads(legal) if legal else {},
            }

            analysis_text = self._format_analysis_display(llm_json)

            details_content = f"""FILE ANALYSIS DETAILS
====================

File Information:
• Path: {path}
• Size: {size:,} bytes ({size/1024:.1f} KB)
• Owner: {owner or 'N/A'}
• Last Modified: {modified or 'N/A'}
• Analysis Status: {status}
• Analysis Date: {created or 'N/A'}
• Processing Time: {proc_time or 0} ms
• Confidence Score: {confidence or 0}%
• Sec Conf: {sec_conf or 0}% | RGPD Conf: {rgpd_conf or 0}%
• Fin Conf: {fin_conf or 0}% | Legal Conf: {legal_conf or 0}%

Résumé:
{resume or 'N/A'}

{analysis_text}

RAW RESPONSE:
{raw_response or ''}
"""

            text_widget.insert(1.0, details_content)
            text_widget.config(state="disabled")

            ttk.Button(
                details_window, text="Close", command=details_window.destroy
            ).pack(pady=5)

            self.log_action(f"File details viewed: ID {file_id}", "INFO")

        except Exception as e:
            messagebox.showerror(
                "Details Error",
                f"Failed to show file details:\n{str(e)}",
                parent=parent_window,
            )
            self.log_action(f"File details failed: {str(e)}", "ERROR")

    def display_analysis_result(self, result_data: dict) -> None:
        """Affiche un résultat d'analyse formaté dans une fenêtre."""
        try:
            if result_data.get("status") == "completed":
                llm_response = result_data.get("result", {})
                formatted_text = self._format_analysis_display(llm_response)
            else:
                error_msg = result_data.get("error", "Erreur inconnue")
                formatted_text = f"❌ ERREUR : {error_msg}"
        except json.JSONDecodeError as e:
            logging.error(f"Invalid JSON response: {e}")
            formatted_text = "❌ Réponse invalide de l'API"

        window = self.create_dialog_window(self.root, "Analysis Result", "600x500")

        text_widget = tk.Text(window, wrap="word", font=("Consolas", 10))
        text_widget.pack(fill="both", expand=True, padx=10, pady=10)
        text_widget.insert(1.0, formatted_text)
        text_widget.config(state="disabled")

        ttk.Button(window, text="Close", command=window.destroy).pack(pady=5)

    def _format_analysis_display(self, llm_response: dict) -> str:
        if not isinstance(llm_response, dict):
            return "❌ ERREUR : Données corrompues détectées"

        formatted = "🔍 RÉSULTATS D'ANALYSE\n" + "=" * 50 + "\n\n"

        resume = llm_response.get("resume")
        if isinstance(resume, str) and resume:
            formatted += f"📄 RÉSUMÉ\n   {resume}\n\n"

        for domain in ["security", "rgpd", "finance", "legal"]:
            section_data = llm_response.get(domain)
            if not isinstance(section_data, dict):
                formatted += f"⚠️ {domain.upper()}: Données non disponibles\n\n"
                continue

            if domain == "security":
                classification = section_data.get("classification", "Non classifié")
                confidence = section_data.get("confidence", 0)
                justification = section_data.get(
                    "justification", "Aucune justification"
                )
                formatted += (
                    f"🛡️ SÉCURITÉ\n   Classification: {classification}\n"
                    f"   Confiance: {confidence}%\n   Justification: {justification}\n\n"
                )
            elif domain == "rgpd":
                risk_level = section_data.get("risk_level", "unknown")
                data_types = section_data.get("data_types", [])
                formatted += (
                    "🔒 RGPD\n   Niveau de risque: "
                    f"{risk_level.upper()}\n   Types de données: {', '.join(data_types) if data_types else 'Aucune'}\n\n"
                )
            elif domain == "finance":
                doc_type = section_data.get("document_type", "none")
                amounts = section_data.get("amounts", [])
                formatted += f"💰 FINANCE\n   Type de document: {doc_type}\n"
                if amounts:
                    formatted += "   Montants détectés:\n"
                    for amt in amounts:
                        value = amt.get("value", "")
                        context = amt.get("context", "")
                        formatted += f"     • {value} ({context})\n"
                formatted += "\n"
            elif domain == "legal":
                contract_type = section_data.get("contract_type", "none")
                parties = section_data.get("parties", [])
                formatted += f"⚖️ LÉGAL\n   Type de contrat: {contract_type}\n   Parties: {', '.join(parties) if parties else 'Aucune'}\n"

        return formatted

    def export_results(self) -> None:
        """Lance la fenêtre d'export des résultats en différents formats."""
        try:
            db_path = Path("analysis_results.db")
            if not db_path.exists():
                messagebox.showwarning(
                    "No Results",
                    "No analysis results found to export",
                    parent=self.root,
                )
                return

            export_window = self.create_dialog_window(
                self.root, "Export Analysis Results", "500x400"
            )

            format_frame = ttk.LabelFrame(export_window, text="Export Format")
            format_frame.pack(fill="x", padx=10, pady=10)

            self.export_format = tk.StringVar(value="csv")
            ttk.Radiobutton(
                format_frame,
                text="CSV (Excel compatible)",
                variable=self.export_format,
                value="csv",
            ).pack(anchor="w", padx=5, pady=2)
            ttk.Radiobutton(
                format_frame,
                text="JSON (structured data)",
                variable=self.export_format,
                value="json",
            ).pack(anchor="w", padx=5, pady=2)
            ttk.Radiobutton(
                format_frame,
                text="Excel (.xlsx)",
                variable=self.export_format,
                value="excel",
            ).pack(anchor="w", padx=5, pady=2)

            filter_frame = ttk.LabelFrame(export_window, text="Filters")
            filter_frame.pack(fill="x", padx=10, pady=10)

            self.export_status_filter = tk.StringVar(value="All")
            ttk.Label(filter_frame, text="Status:").grid(
                row=0, column=0, sticky="w", padx=5, pady=2
            )
            status_combo = ttk.Combobox(
                filter_frame,
                textvariable=self.export_status_filter,
                values=["All", "completed", "error", "pending"],
                state="readonly",
            )
            status_combo.grid(row=0, column=1, sticky="ew", padx=5, pady=2)

            self.export_classification_filter = tk.StringVar(value="All")
            ttk.Label(filter_frame, text="Security Classification:").grid(
                row=1, column=0, sticky="w", padx=5, pady=2
            )
            classif_combo = ttk.Combobox(
                filter_frame,
                textvariable=self.export_classification_filter,
                values=["All", "C0", "C1", "C2", "C3"],
                state="readonly",
            )
            classif_combo.grid(row=1, column=1, sticky="ew", padx=5, pady=2)

            filter_frame.columnconfigure(1, weight=1)

            options_frame = ttk.LabelFrame(export_window, text="Options")
            options_frame.pack(fill="x", padx=10, pady=10)

            self.include_raw_json = tk.BooleanVar(value=False)
            ttk.Checkbutton(
                options_frame,
                text="Include raw JSON analyses",
                variable=self.include_raw_json,
            ).pack(anchor="w", padx=5, pady=2)

            self.include_statistics = tk.BooleanVar(value=True)
            ttk.Checkbutton(
                options_frame,
                text="Include summary statistics",
                variable=self.include_statistics,
            ).pack(anchor="w", padx=5, pady=2)

            self.export_duplicates_var = tk.BooleanVar(value=False)
            ttk.Checkbutton(
                options_frame,
                text="Doublons FastHash+Taille",
                variable=self.export_duplicates_var,
            ).pack(anchor="w", padx=5, pady=2)

            buttons_frame = ttk.Frame(export_window)
            buttons_frame.pack(fill="x", padx=10, pady=10)

            ttk.Button(
                buttons_frame,
                text="Export",
                command=lambda: self.perform_export(export_window),
            ).pack(side="right", padx=5)
            ttk.Button(
                buttons_frame, text="Cancel", command=export_window.destroy
            ).pack(side="right", padx=5)

            self.log_action("Export dialog opened", "INFO")

        except Exception as e:
            messagebox.showerror(
                "Export Error",
                f"Failed to open export dialog:\n{str(e)}",
                parent=self.root,
            )
            self.log_action(f"Export dialog failed: {str(e)}", "ERROR")

    def perform_export(self, export_window):
        """Exécute l'export selon les paramètres sélectionnés."""
        try:
            format_ext = {"csv": ".csv", "json": ".json", "excel": ".xlsx"}
            file_ext = format_ext[self.export_format.get()]

            export_path = filedialog.asksaveasfilename(
                title="Export Results",
                defaultextension=file_ext,
                filetypes=[
                    (f"{self.export_format.get().upper()} files", f"*{file_ext}"),
                    ("All files", "*.*"),
                ],
                parent=export_window,
            )

            if not export_path:
                return

            db_path = Path("analysis_results.db")
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            query = """
        SELECT f.id, f.name, f.host, f.extension, f.username, f.hostname,
               f.unc_directory, f.creation_time, f.last_write_time, f.readable,
               f.writeable, f.deletable, f.directory_type, f.base, f.path,
               f.file_size, f.owner, f.fast_hash, f.access_time,
               f.file_attributes, f.file_signature, f.last_modified, f.status,
               r.security_analysis, r.rgpd_analysis, r.finance_analysis, r.legal_analysis,
               r.confidence_global, r.processing_time_ms, r.created_at
        FROM fichiers f
        LEFT JOIN reponses_llm r ON f.id = r.fichier_id
        WHERE 1=1
        """
            params = []

            if self.export_status_filter.get() != "All":
                query += " AND f.status = ?"
                params.append(self.export_status_filter.get())

            if self.export_classification_filter.get() != "All":
                query += " AND r.security_analysis LIKE ?"
                params.append(
                    f'%"classification": "{self.export_classification_filter.get()}"%'
                )

            cursor.execute(query, params)
            rows = cursor.fetchall()
            conn.close()
            if self.export_duplicates_var.get():
                dup_ids = self._get_duplicate_file_ids(
                    self.export_status_filter.get(),
                    self.export_classification_filter.get(),
                )
                rows = [r for r in rows if r[0] in dup_ids]

            if self.export_format.get() == "csv":
                self.export_to_csv(rows, export_path)
            elif self.export_format.get() == "json":
                self.export_to_json(rows, export_path)
            elif self.export_format.get() == "excel":
                self.export_to_excel(rows, export_path)

            export_window.destroy()
            messagebox.showinfo(
                "Export Complete",
                f"Results exported successfully to:\n{Path(export_path).name}",
                parent=self.root,
            )
            self.log_action(
                f"Results exported to {self.export_format.get().upper()}: {Path(export_path).name} ({len(rows)} records)",
                "INFO",
            )

        except Exception as e:
            messagebox.showerror(
                "Export Error",
                f"Failed to export results:\n{str(e)}",
                parent=export_window,
            )
            self.log_action(f"Export failed: {str(e)}", "ERROR")

    def export_to_csv(self, rows, export_path):
        """Exporte les résultats au format CSV."""
        with open(export_path, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)

            headers = [
                "ID",
                "Name",
                "Host",
                "Extension",
                "Username",
                "Hostname",
                "UNCDirectory",
                "CreationTime",
                "LastWriteTime",
                "Readable",
                "Writeable",
                "Deletable",
                "DirectoryType",
                "Base",
                "Path",
                "FileSize",
                "Owner",
                "FastHash",
                "AccessTime",
                "FileAttributes",
                "FileSignature",
                "LastModified",
                "Status",
                "Security Classification",
                "RGPD Risk",
                "Finance Type",
                "Legal Type",
                "Confidence",
                "Processing Time (ms)",
                "Analysis Date",
            ]

            if self.include_raw_json.get():
                headers.extend(
                    [
                        "Security JSON",
                        "RGPD JSON",
                        "Finance JSON",
                        "Legal JSON",
                    ]
                )

            writer.writerow(headers)

            for row in rows:
                (
                    file_id,
                    name,
                    host,
                    extension,
                    username,
                    hostname,
                    unc_dir,
                    creation_time,
                    last_write_time,
                    readable,
                    writeable,
                    deletable,
                    directory_type,
                    base,
                    path,
                    size,
                    owner,
                    fast_hash,
                    access_time,
                    file_attributes,
                    file_signature,
                    last_modified,
                    status,
                    security,
                    rgpd,
                    finance,
                    legal,
                    confidence,
                    proc_time,
                    created,
                ) = row

                try:
                    security_data = json.loads(security) if security else {}
                    security_class = security_data.get("classification", "N/A")
                except Exception:
                    security_class = "N/A"

                try:
                    rgpd_data = json.loads(rgpd) if rgpd else {}
                    rgpd_risk = rgpd_data.get("risk_level", "N/A")
                except Exception:
                    rgpd_risk = "N/A"

                try:
                    finance_data = json.loads(finance) if finance else {}
                    finance_type = finance_data.get("document_type", "N/A")
                except Exception:
                    finance_type = "N/A"

                try:
                    legal_data = json.loads(legal) if legal else {}
                    legal_type = legal_data.get("contract_type", "N/A")
                except Exception:
                    legal_type = "N/A"

                data_row = [
                    file_id,
                    name,
                    host,
                    extension,
                    username,
                    hostname,
                    unc_dir,
                    creation_time,
                    last_write_time,
                    readable,
                    writeable,
                    deletable,
                    directory_type,
                    base,
                    path,
                    size,
                    owner,
                    fast_hash,
                    access_time,
                    file_attributes,
                    file_signature,
                    last_modified,
                    status,
                    security_class,
                    rgpd_risk,
                    finance_type,
                    legal_type,
                    confidence,
                    proc_time,
                    created,
                ]

                if self.include_raw_json.get():
                    data_row.extend([security, rgpd, finance, legal])

                writer.writerow(data_row)

    def export_to_json(self, rows, export_path):
        """Exporte les résultats au format JSON."""
        export_data = {
            "export_info": {
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "total_records": len(rows),
                "filters_applied": {
                    "status": self.export_status_filter.get(),
                    "classification": self.export_classification_filter.get(),
                },
            },
            "results": [],
        }

        for row in rows:
            (
                file_id,
                name,
                host,
                extension,
                username,
                hostname,
                unc_dir,
                creation_time,
                last_write_time,
                readable,
                writeable,
                deletable,
                directory_type,
                base,
                path,
                size,
                owner,
                fast_hash,
                access_time,
                file_attributes,
                file_signature,
                last_modified,
                status,
                security,
                rgpd,
                finance,
                legal,
                confidence,
                proc_time,
                created,
            ) = row

            file_data = {
                "id": file_id,
                "file_info": {
                    "name": name,
                    "host": host,
                    "extension": extension,
                    "username": username,
                    "hostname": hostname,
                    "unc_directory": unc_dir,
                    "creation_time": creation_time,
                    "last_write_time": last_write_time,
                    "readable": readable,
                    "writeable": writeable,
                    "deletable": deletable,
                    "directory_type": directory_type,
                    "base": base,
                    "path": path,
                    "size_bytes": size,
                    "owner": owner,
                    "fast_hash": fast_hash,
                    "access_time": access_time,
                    "file_attributes": file_attributes,
                    "file_signature": file_signature,
                    "last_modified": last_modified,
                },
                "analysis": {
                    "status": status,
                    "confidence": confidence,
                    "processing_time_ms": proc_time,
                    "created_at": created,
                    "security": json.loads(security) if security else None,
                    "rgpd": json.loads(rgpd) if rgpd else None,
                    "finance": json.loads(finance) if finance else None,
                    "legal": json.loads(legal) if legal else None,
                },
            }

            export_data["results"].append(file_data)

        with open(export_path, "w", encoding="utf-8") as jsonfile:
            json.dump(export_data, jsonfile, indent=2, ensure_ascii=False)

    def export_to_excel(self, rows, export_path):
        """Exporte les résultats au format Excel avec plusieurs feuilles."""
        try:
            import pandas as pd

            main_data = []
            for row in rows:
                (
                    file_id,
                    name,
                    host,
                    extension,
                    username,
                    hostname,
                    unc_dir,
                    creation_time,
                    last_write_time,
                    readable,
                    writeable,
                    deletable,
                    directory_type,
                    base,
                    path,
                    size,
                    owner,
                    fast_hash,
                    access_time,
                    file_attributes,
                    file_signature,
                    last_modified,
                    status,
                    security,
                    rgpd,
                    finance,
                    legal,
                    confidence,
                    proc_time,
                    created,
                ) = row

                try:
                    security_data = json.loads(security) if security else {}
                    rgpd_data = json.loads(rgpd) if rgpd else {}
                    finance_data = json.loads(finance) if finance else {}
                    legal_data = json.loads(legal) if legal else {}
                except Exception:
                    security_data = rgpd_data = finance_data = legal_data = {}

                main_data.append(
                    {
                        "ID": file_id,
                        "Name": name,
                        "Host": host,
                        "Extension": extension,
                        "Username": username,
                        "Hostname": hostname,
                        "UNCDirectory": unc_dir,
                        "CreationTime": creation_time,
                        "LastWriteTime": last_write_time,
                        "Readable": readable,
                        "Writeable": writeable,
                        "Deletable": deletable,
                        "DirectoryType": directory_type,
                        "Base": base,
                        "Path": path,
                        "FileSize": size,
                        "Owner": owner,
                        "FastHash": fast_hash,
                        "AccessTime": access_time,
                        "FileAttributes": file_attributes,
                        "FileSignature": file_signature,
                        "LastModified": last_modified,
                        "Status": status,
                        "Security Classification": security_data.get(
                            "classification", "N/A"
                        ),
                        "RGPD Risk": rgpd_data.get("risk_level", "N/A"),
                        "Finance Type": finance_data.get("document_type", "N/A"),
                        "Legal Type": legal_data.get("contract_type", "N/A"),
                        "Confidence": confidence,
                        "Processing Time (ms)": proc_time,
                        "Analysis Date": created,
                    }
                )

            with pd.ExcelWriter(export_path, engine="openpyxl") as writer:
                df_main = pd.DataFrame(main_data)
                df_main.to_excel(writer, sheet_name="Analysis Results", index=False)

                if self.include_statistics.get():
                    stats_data = {
                        "Metric": [
                            "Total Files",
                            "Completed Analyses",
                            "Failed Analyses",
                            "Average Confidence",
                            "C0 Classifications",
                            "C1 Classifications",
                            "C2 Classifications",
                            "C3 Classifications",
                        ],
                        "Value": [
                            len(rows),
                            sum(1 for r in rows if r[22] == "completed"),
                            sum(1 for r in rows if r[22] == "error"),
                            (
                                sum(r[27] for r in rows if r[27])
                                / len([r for r in rows if r[27]])
                                if any(r[27] for r in rows)
                                else 0
                            ),
                            sum(1 for r in rows if r[23] and "C0" in str(r[23])),
                            sum(1 for r in rows if r[23] and "C1" in str(r[23])),
                            sum(1 for r in rows if r[23] and "C2" in str(r[23])),
                            sum(1 for r in rows if r[23] and "C3" in str(r[23])),
                        ],
                    }
                    df_stats = pd.DataFrame(stats_data)
                    df_stats.to_excel(writer, sheet_name="Statistics", index=False)

        except ImportError:
            messagebox.showerror(
                "Excel Export Error",
                "Excel export requires pandas library.\nPlease use CSV format instead.",
            )
            return

    def export_results_to_csv(self, tree) -> None:
        """Export the currently displayed results table to a CSV file."""
        try:
            export_path = filedialog.asksaveasfilename(
                title="Export Table to CSV",
                defaultextension=".csv",
                filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
                parent=tree.winfo_toplevel(),
            )
            if not export_path:
                return

            with open(export_path, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(
                    [
                        "ID",
                        "File Path",
                        "Status",
                        "Security",
                        "RGPD",
                        "Finance",
                        "Legal",
                        "Confidence",
                        "Processing Time",
                    ]
                )

                for item in tree.get_children():
                    writer.writerow(tree.item(item)["values"])

            messagebox.showinfo(
                "Export Complete",
                f"Table exported successfully to: {Path(export_path).name}",
                parent=tree.winfo_toplevel(),
            )
            self.log_action(
                f"Table exported to CSV: {Path(export_path).name}",
                "INFO",
            )

        except Exception as e:
            messagebox.showerror(
                "Export Error",
                f"Failed to export table:\n{str(e)}",
                parent=tree.winfo_toplevel(),
            )
            self.log_action(f"Table export failed: {str(e)}", "ERROR")

    # ------------------------------------------------------------------
    # MAINTENANCE
    # ------------------------------------------------------------------
    def show_maintenance_dialog(self) -> None:
        self.log_action("Maintenance dialog opened", "INFO")
        maintenance_window = self.create_dialog_window(
            self.root, "System Maintenance", "500x400"
        )

        db_frame = ttk.LabelFrame(maintenance_window, text="Database Maintenance")
        db_frame.pack(fill="x", padx=10, pady=10)
        ttk.Button(
            db_frame,
            text="Reset Database",
            command=lambda: self.reset_database(maintenance_window),
        ).pack(fill="x", padx=5, pady=2)
        ttk.Button(
            db_frame,
            text="Compact Database",
            command=lambda: self.compact_database(maintenance_window),
        ).pack(fill="x", padx=5, pady=2)
        ttk.Button(
            db_frame,
            text="Backup Database",
            command=lambda: self.backup_database(maintenance_window),
        ).pack(fill="x", padx=5, pady=2)
        ttk.Button(
            db_frame,
            text="Restore Database",
            command=lambda: self.restore_database(maintenance_window),
        ).pack(fill="x", padx=5, pady=2)
        ttk.Button(
            db_frame,
            text="Verify Integrity",
            command=lambda: self.verify_database_integrity(maintenance_window),
        ).pack(fill="x", padx=5, pady=2)
        ttk.Button(
            db_frame,
            text="Repair Corruption",
            command=lambda: self.repair_database_corruption(maintenance_window),
        ).pack(fill="x", padx=5, pady=2)

        cache_frame = ttk.LabelFrame(maintenance_window, text="Cache Maintenance")
        cache_frame.pack(fill="x", padx=10, pady=10)
        ttk.Button(
            cache_frame,
            text="Clear Cache",
            command=lambda: self.clear_cache(maintenance_window),
        ).pack(fill="x", padx=5, pady=2)
        ttk.Button(
            cache_frame,
            text="Cache Statistics",
            command=lambda: self.show_cache_stats(maintenance_window),
        ).pack(fill="x", padx=5, pady=2)

        config_frame = ttk.LabelFrame(maintenance_window, text="Configuration")
        config_frame.pack(fill="x", padx=10, pady=10)
        ttk.Button(
            config_frame,
            text="Export Configuration",
            command=lambda: self.export_configuration(maintenance_window),
        ).pack(fill="x", padx=5, pady=2)
        ttk.Button(
            config_frame,
            text="Import Configuration",
            command=lambda: self.import_configuration(maintenance_window),
        ).pack(fill="x", padx=5, pady=2)
        ttk.Button(
            config_frame,
            text="Reset to Defaults",
            command=lambda: self.reset_configuration(maintenance_window),
        ).pack(fill="x", padx=5, pady=2)

        ttk.Button(
            maintenance_window, text="Close", command=maintenance_window.destroy
        ).pack(pady=10)

    def show_worker_status(self) -> None:
        """Display detailed worker status in a modal window."""
        if not (
            hasattr(self, "analysis_thread")
            and self.analysis_thread
            and self.analysis_thread.is_alive()
        ):
            messagebox.showinfo(
                "Worker Status", "No active analysis running", parent=self.root
            )
            return

        try:
            status = self.analysis_thread.get_worker_status()
            status_window = self.create_dialog_window(
                self.root, "Worker Status Monitor", "600x400"
            )

            perf_frame = ttk.LabelFrame(status_window, text="Performance Metrics")
            perf_frame.pack(fill="x", padx=10, pady=5)

            perf = status.get("performance", {})
            metrics_text = f"""
Workers Active: {status.get('active_workers', 0)}/{status.get('max_workers', 0)}
Files Processed: {perf.get('processed', 0)}
Throughput: {perf.get('throughput_per_minute', 0):.1f} files/min
Cache Hit Rate: {perf.get('cache_hits', 0) / max(perf.get('processed', 1), 1) * 100:.1f}%
Average Processing: {perf.get('avg_processing_time', 0):.2f}s/file
Errors: {perf.get('errors', 0)}
"""

            ttk.Label(perf_frame, text=metrics_text, font=("Consolas", 10)).pack(
                anchor="w", padx=10, pady=5
            )

            files_frame = ttk.LabelFrame(status_window, text="Current Files")
            files_frame.pack(fill="both", expand=True, padx=10, pady=5)

            files_listbox = tk.Listbox(files_frame, font=("Consolas", 9))
            files_listbox.pack(fill="both", expand=True, padx=5, pady=5)

            current_files = status.get("current_files", {})
            for worker_id, file_path in current_files.items():
                files_listbox.insert(
                    tk.END, f"Worker {worker_id}: {Path(file_path).name}"
                )
            if not current_files:
                files_listbox.insert(tk.END, "No files currently being processed")

            controls_frame = ttk.Frame(status_window)
            controls_frame.pack(fill="x", padx=10, pady=5)

            def refresh_status() -> None:
                status_window.destroy()
                self.show_worker_status()

            ttk.Button(controls_frame, text="Refresh", command=refresh_status).pack(
                side="left", padx=5
            )
            ttk.Button(
                controls_frame, text="Close", command=status_window.destroy
            ).pack(side="right", padx=5)
        except Exception as e:
            messagebox.showerror(
                "Status Error",
                f"Failed to get worker status:\n{str(e)}",
                parent=self.root,
            )

    def reset_database(self, parent_window: tk.Toplevel) -> None:
        response = messagebox.askyesno(
            "Confirm Reset",
            "This will delete ALL analysis data!\nAre you sure?",
            parent=parent_window,
        )
        if not response:
            return
        try:
            import time
            import platform
            
            db_path = Path("analysis_results.db")
            if not db_path.exists():
                # Database doesn't exist, just create new one
                self.db_manager = DBManager(db_path)
                self.invalidate_all_caches()
                self._update_db_status_labels(0.0)
                if hasattr(self, "analytics_panel"):
                    self.analytics_panel.set_db_manager(self.db_manager)
                messagebox.showinfo(
                    "Database Reset",
                    "Database has been reset successfully!",
                    parent=parent_window,
                )
                self.log_action("Database reset completed (no existing DB)", "INFO")
                return
            
            # WINDOWS-SAFE: Close all DB connections before deleting file
            self.log_action("Closing DB connections for Windows-safe reset", "INFO")
            
            # 1. Force close all connections using our new method
            if hasattr(self, "db_manager") and self.db_manager:
                self.db_manager.force_close_all_connections_windows_safe()
            
            # 2. Also close any cache manager connections that might exist
            if hasattr(self, "cache_manager") and self.cache_manager:
                self.cache_manager.force_close_all_connections_windows_safe()
            
            # 3. Windows retry pattern for file deletion
            retry_count = 5
            is_windows = platform.system() == "Windows"
            
            for attempt in range(retry_count):
                try:
                    db_path.unlink()
                    self.log_action(f"Database file deleted (attempt {attempt + 1})", "INFO")
                    break
                except (PermissionError, OSError) as e:
                    if "WinError 32" in str(e) or "being used by another process" in str(e):
                        if attempt < retry_count - 1:
                            wait_time = (attempt + 1) * 0.5  # Increasing wait time
                            self.log_action(
                                f"File locked (attempt {attempt + 1}), waiting {wait_time}s...", 
                                "WARNING"
                            )
                            time.sleep(wait_time)
                            continue
                        else:
                            raise Exception(
                                f"Windows file lock persists after {retry_count} attempts. "
                                f"Please ensure no other applications are using the database file."
                            )
                    else:
                        raise  # Different error, re-raise immediately
            
            # 4. Recreate DB manager and update UI
            self.db_manager = DBManager(db_path)
            self.invalidate_all_caches()
            self._update_db_status_labels(0.0)
            if hasattr(self, "analytics_panel"):
                self.analytics_panel.set_db_manager(self.db_manager)
                
            messagebox.showinfo(
                "Database Reset",
                "Database has been reset successfully!",
                parent=parent_window,
            )
            self.log_action("Database reset completed", "INFO")
            
        except Exception as e:
            error_msg = f"Cannot reset database:\n{str(e)}"
            messagebox.showerror("Reset Error", error_msg, parent=parent_window)
            self.log_action(f"Database reset failed: {str(e)}", "ERROR")

    def compact_database(self, parent_window: tk.Toplevel) -> None:
        """Compacte la base SQLite avec VACUUM pour optimiser l'espace."""
        try:
            db_path = Path("analysis_results.db")
            if not db_path.exists():
                messagebox.showwarning(
                    "No Database",
                    "No database file found to compact",
                    parent=parent_window,
                )
                return

            size_before = db_path.stat().st_size / (1024 * 1024)
            with SQLiteConnectionManager(db_path) as conn:
                conn.execute("VACUUM")
            size_after = db_path.stat().st_size / (1024 * 1024)
            saved_mb = size_before - size_after

            messagebox.showinfo(
                "Database Compacted",
                f"Database compacted successfully!\n"
                f"Size before: {size_before:.2f}MB\n"
                f"Size after: {size_after:.2f}MB\n"
                f"Space saved: {saved_mb:.2f}MB",
                parent=parent_window,
            )
            self.log_action(f"Database compacted: saved {saved_mb:.2f}MB", "INFO")

        except Exception as e:
            messagebox.showerror(
                "Compaction Error",
                f"Failed to compact database:\n{str(e)}",
                parent=parent_window,
            )
            self.log_action(f"Database compaction failed: {str(e)}", "ERROR")

    def backup_database(self, parent_window: tk.Toplevel) -> None:
        """Crée une sauvegarde timestampée de la base de données."""
        try:
            db_path = Path("analysis_results.db")
            if not db_path.exists():
                messagebox.showwarning(
                    "No Database",
                    "No database file found to backup",
                    parent=parent_window,
                )
                return

            timestamp = time.strftime("%Y%m%d_%H%M%S")
            backup_path = db_path.with_name(f"analysis_results_backup_{timestamp}.db")

            import shutil

            shutil.copy2(db_path, backup_path)
            size_mb = backup_path.stat().st_size / (1024 * 1024)
            messagebox.showinfo(
                "Backup Created",
                f"Database backed up successfully!\n"
                f"Backup file: {backup_path.name}\n"
                f"Size: {size_mb:.2f}MB",
                parent=parent_window,
            )
            self.log_action(
                f"Database backup created: {backup_path.name} ({size_mb:.2f}MB)",
                "INFO",
            )

        except Exception as e:
            messagebox.showerror(
                "Backup Error",
                f"Failed to backup database:\n{str(e)}",
                parent=parent_window,
            )
            self.log_action(f"Database backup failed: {str(e)}", "ERROR")

    def restore_database(self, parent_window: tk.Toplevel) -> None:
        """Restore database from backup with validation."""
        backup_file = filedialog.askopenfilename(
            title="Select Backup File",
            parent=parent_window,
            filetypes=[("SQLite DB", "*.db"), ("All", "*.*")],
        )
        if not backup_file:
            return
        try:
            db_path = Path("analysis_results.db")
            with SQLiteConnectionManager(Path(backup_file)) as conn:
                result = conn.execute("PRAGMA integrity_check").fetchone()[0]
                if result != "ok":
                    messagebox.showerror(
                        "Invalid Backup",
                        "Selected backup is corrupted",
                        parent=parent_window,
                    )
                    return
            import shutil

            shutil.copy2(backup_file, db_path)
            self.log_action("Database restored from backup", "INFO")
            messagebox.showinfo(
                "Restore Complete",
                "Database restored successfully",
                parent=parent_window,
            )
            self.invalidate_all_caches()
        except Exception as exc:
            messagebox.showerror(
                "Restore Error",
                f"Failed to restore database:\n{exc}",
                parent=parent_window,
            )
            self.log_action(f"Database restore failed: {exc}", "ERROR")

    def verify_database_integrity(self, parent_window: tk.Toplevel) -> None:
        """Run comprehensive database integrity checks."""
        try:
            db_path = Path("analysis_results.db")
            with SQLiteConnectionManager(db_path) as conn:
                result = conn.execute("PRAGMA integrity_check").fetchone()[0]
            messagebox.showinfo(
                "Integrity Check",
                f"Database integrity: {result}",
                parent=parent_window,
            )
            self.log_action(f"Integrity check result: {result}", "INFO")
        except Exception as exc:
            messagebox.showerror(
                "Integrity Error",
                f"Integrity check failed:\n{exc}",
                parent=parent_window,
            )
            self.log_action(f"Integrity check failed: {exc}", "ERROR")

    def repair_database_corruption(self, parent_window: tk.Toplevel) -> None:
        """Attempt automatic corruption repair."""
        try:
            db_path = Path("analysis_results.db")
            with SQLiteConnectionManager(db_path) as conn:
                conn.execute("PRAGMA wal_checkpoint(RESTART)")
                conn.execute("VACUUM")
            messagebox.showinfo(
                "Repair Complete", "Database repair completed", parent=parent_window
            )
            self.log_action("Database repair completed", "INFO")
        except Exception as exc:
            messagebox.showerror(
                "Repair Error", f"Repair failed:\n{exc}", parent=parent_window
            )
            self.log_action(f"Database repair failed: {exc}", "ERROR")

    def clear_cache(self, parent_window: tk.Toplevel) -> None:
        """Vide complètement le cache SQLite."""
        try:
            import time
            import platform
            
            cache_db = Path("analysis_results_cache.db")
            if not cache_db.exists():
                messagebox.showinfo(
                    "No Cache", "No cache database found", parent=parent_window
                )
                return

            # Get stats before clearing
            cache_manager = CacheManager(cache_db)
            stats_before = cache_manager.get_stats()
            entries_before = stats_before.get("total_entries", 0)
            size_before = stats_before.get("cache_size_mb", 0)

            # WINDOWS-SAFE: Close cache connections before deleting file
            self.log_action("Closing cache connections for Windows-safe clear", "INFO")
            
            # 1. Force close cache connections using our new method
            cache_manager.force_close_all_connections_windows_safe()
            
            # 2. Also close any main cache manager instance that might exist
            if hasattr(self, "cache_manager") and self.cache_manager:
                self.cache_manager.force_close_all_connections_windows_safe()
            
            # 3. Windows retry pattern for cache file deletion
            retry_count = 5
            is_windows = platform.system() == "Windows"
            
            for attempt in range(retry_count):
                try:
                    cache_db.unlink()
                    self.log_action(f"Cache file deleted (attempt {attempt + 1})", "INFO")
                    break
                except (PermissionError, OSError) as e:
                    if "WinError 32" in str(e) or "being used by another process" in str(e):
                        if attempt < retry_count - 1:
                            wait_time = (attempt + 1) * 0.5  # Increasing wait time
                            self.log_action(
                                f"Cache file locked (attempt {attempt + 1}), waiting {wait_time}s...", 
                                "WARNING"
                            )
                            time.sleep(wait_time)
                            continue
                        else:
                            raise Exception(
                                f"Windows cache file lock persists after {retry_count} attempts. "
                                f"Please ensure no other applications are using the cache file."
                            )
                    else:
                        raise  # Different error, re-raise immediately

            # 4. Invalidate all caches and update UI
            self.invalidate_all_caches()

            messagebox.showinfo(
                "Cache Cleared",
                f"Cache cleared successfully!\n"
                f"Entries removed: {entries_before}\n"
                f"Space freed: {size_before:.2f}MB",
                parent=parent_window,
            )
            self.log_action(
                f"Cache cleared: {entries_before} entries, {size_before:.2f}MB freed",
                "INFO",
            )

        except Exception as e:
            error_msg = f"Failed to clear cache:\n{str(e)}"
            messagebox.showerror("Clear Error", error_msg, parent=parent_window)
            self.log_action(f"Cache clear failed: {str(e)}", "ERROR")

    def show_cache_stats(self, parent_window: tk.Toplevel) -> None:
        """Affiche les statistiques détaillées du cache."""
        try:
            cache_db = Path("analysis_results_cache.db")
            if not cache_db.exists():
                messagebox.showinfo(
                    "No Cache", "No cache database found", parent=parent_window
                )
                return

            cache_manager = CacheManager(cache_db)
            stats = cache_manager.get_stats()

            stats_window = tk.Toplevel(parent_window)
            stats_window.title("Cache Statistics")
            stats_window.geometry("400x300")
            stats_window.transient(parent_window)
            stats_window.grab_set()

            stats_text = tk.Text(stats_window, wrap="word", state="normal")
            stats_text.pack(fill="both", expand=True, padx=10, pady=10)

            stats_content = f"""Cache Statistics Report
========================

Total Entries: {stats.get('total_entries', 0):,}
Hit Rate: {stats.get('hit_rate', 0):.2f}%
Cache Size: {stats.get('cache_size_mb', 0):.2f} MB
Oldest Entry: {stats.get('oldest_entry', 'N/A')}
Cleanup Needed: {'Yes' if stats.get('cleanup_needed', False) else 'No'}

Database Path: {cache_db}
Last Updated: {time.strftime('%Y-%m-%d %H:%M:%S')}
"""

            stats_text.insert(1.0, stats_content)
            stats_text.config(state="disabled")

            ttk.Button(stats_window, text="Close", command=stats_window.destroy).pack(
                pady=5
            )

            self.log_action("Cache statistics viewed", "INFO")

        except Exception as e:
            messagebox.showerror(
                "Stats Error",
                f"Failed to get cache stats:\n{str(e)}",
                parent=parent_window,
            )
            self.log_action(f"Cache stats failed: {str(e)}", "ERROR")

    def export_configuration(self, parent_window: tk.Toplevel) -> None:
        """Exporte la configuration actuelle vers un fichier."""
        try:
            export_path = filedialog.asksaveasfilename(
                title="Export Configuration",
                defaultextension=".yaml",
                filetypes=[("YAML files", "*.yaml"), ("All files", "*.*")],
                parent=parent_window,
            )

            if not export_path:
                return

            timestamp = time.strftime("%Y%m%d_%H%M%S")
            export_path = Path(export_path)
            if not export_path.stem.endswith(timestamp):
                export_path = export_path.with_name(
                    f"{export_path.stem}_{timestamp}{export_path.suffix}"
                )

            import shutil

            shutil.copy2(self.config_path, export_path)

            messagebox.showinfo(
                "Configuration Exported",
                f"Configuration exported successfully!\nFile: {export_path.name}",
                parent=parent_window,
            )
            self.log_action(f"Configuration exported to: {export_path.name}", "INFO")

        except Exception as e:
            messagebox.showerror(
                "Export Error",
                f"Failed to export configuration:\n{str(e)}",
                parent=parent_window,
            )
            self.log_action(f"Configuration export failed: {str(e)}", "ERROR")

    def import_configuration(self, parent_window: tk.Toplevel) -> None:
        """Importe une configuration depuis un fichier."""
        try:
            import_path = filedialog.askopenfilename(
                title="Import Configuration",
                filetypes=[("YAML files", "*.yaml"), ("All files", "*.*")],
                parent=parent_window,
            )

            if not import_path:
                return

            with open(import_path, "r", encoding="utf-8") as f:
                imported_config = yaml.safe_load(f)

            required_sections = ["api_config", "exclusions", "templates"]
            missing_sections = [
                sec for sec in required_sections if sec not in imported_config
            ]

            if missing_sections:
                messagebox.showerror(
                    "Invalid Configuration",
                    f"Configuration file is missing required sections:\n{', '.join(missing_sections)}",
                    parent=parent_window,
                )
                return

            response = messagebox.askyesno(
                "Confirm Import",
                "This will replace your current configuration.\nAre you sure?",
                parent=parent_window,
            )
            if not response:
                return

            backup_path = self.config_path.with_name(
                f"analyzer_config_backup_{time.strftime('%Y%m%d_%H%M%S')}.yaml"
            )
            import shutil

            shutil.copy2(self.config_path, backup_path)
            shutil.copy2(import_path, self.config_path)

            self.load_api_configuration()
            self.load_exclusions()
            self.load_templates()

            messagebox.showinfo(
                "Configuration Imported",
                f"Configuration imported successfully!\nPrevious config backed up as: {backup_path.name}",
                parent=parent_window,
            )
            self.log_action(
                f"Configuration imported from: {Path(import_path).name}", "INFO"
            )

        except Exception as e:
            messagebox.showerror(
                "Import Error",
                f"Failed to import configuration:\n{str(e)}",
                parent=parent_window,
            )
            self.log_action(f"Configuration import failed: {str(e)}", "ERROR")

    def reset_configuration(self, parent_window: tk.Toplevel) -> None:
        """Remet la configuration aux valeurs par défaut."""
        try:
            response = messagebox.askyesno(
                "Confirm Reset",
                "This will reset ALL settings to default values!\nAre you sure?",
                parent=parent_window,
            )
            if not response:
                return

            backup_path = self.config_path.with_name(
                f"analyzer_config_backup_{time.strftime('%Y%m%d_%H%M%S')}.yaml"
            )
            import shutil

            shutil.copy2(self.config_path, backup_path)

            default_config = {
                "project": {
                    "name": "llm-content-analyzer",
                    "version": "2.3.0",
                    "stack_philosophy": "minimal_dependencies_maximum_efficiency",
                },
                "api_config": {
                    "url": "http://localhost:8080",
                    "token": "sk-default-token",
                    "max_tokens": 32000,
                    "timeout_seconds": 300,
                    "batch_size": 100,
                },
                "exclusions": {
                    "extensions": {
                        "blocked": [".tmp", ".log", ".bak", ".cache"],
                        "low_priority": [".txt", ".ini", ".cfg"],
                        "high_priority": [".pdf", ".docx", ".doc", ".xlsx"],
                    },
                    "file_size": {"min_bytes": 100, "max_bytes": 104857600},
                    "file_attributes": {"skip_system": True, "skip_hidden": False},
                },
                "templates": {
                    "comprehensive": {
                        "system_prompt": "Tu es un expert en analyse de documents pour entreprise.",
                        "user_template": "Fichier: {{ file_name }}\nAnalyse ce document.",
                    }
                },
            }

            with open(self.config_path, "w", encoding="utf-8") as f:
                yaml.safe_dump(default_config, f, default_flow_style=False, indent=2)

            self.load_api_configuration()
            self.load_exclusions()
            self.load_templates()

            messagebox.showinfo(
                "Configuration Reset",
                f"Configuration reset to defaults!\nPrevious config backed up as: {backup_path.name}",
                parent=parent_window,
            )
            self.log_action("Configuration reset to defaults", "INFO")

        except Exception as e:
            messagebox.showerror(
                "Reset Error",
                f"Failed to reset configuration:\n{str(e)}",
                parent=parent_window,
            )
            self.log_action(f"Configuration reset failed: {str(e)}", "ERROR")

    # ------------------------------------------------------------------
    # ANALYTICS PANEL
    # ------------------------------------------------------------------
    def build_analytics_section(self) -> None:
        """Section analytics avec onglets complets"""
        analytics_frame = ttk.LabelFrame(self.root, text="📊 Analytics Dashboard")
        analytics_frame.pack(fill="both", expand=True, padx=5, pady=5)
        from .analytics_panel import AnalyticsPanel

        self.analytics_panel = AnalyticsPanel(analytics_frame, self.db_manager)

        controls_frame = ttk.Frame(analytics_frame)
        controls_frame.pack(fill="x", padx=5, pady=5)

        ttk.Button(
            controls_frame,
            text="🔄 Actualiser Tout",
            command=self.refresh_all_analytics,
        ).pack(side="left", padx=5)
        ttk.Button(
            controls_frame,
            text="📊 Export Analytics",
            command=self.export_analytics_report,
        ).pack(side="left", padx=5)

    def open_analytics_dashboard(self) -> None:
        """Open analytics dashboard in a modal window with robust DB validation."""

        # Prevent multiple dashboard windows
        if hasattr(self, "analytics_window") and self.analytics_window.winfo_exists():
            self.analytics_window.focus_set()
            return

        if not hasattr(self, "db_manager") or self.db_manager is None:
            logger.error("Database manager not available for analytics dashboard")
            self._detect_and_load_existing_database()
            if not hasattr(self, "db_manager") or self.db_manager is None:
                messagebox.showerror(
                    "Erreur Database",
                    "Aucune base de données active trouvée.\n\n"
                    "Actions possibles:\n"
                    "• Importer un fichier CSV via 'Browse CSV...'\n"
                    "• Charger une base existante via 'Load Database'\n"
                    "• Vérifier que analysis_results.db existe et contient des données",
                    parent=self.root,
                )
                return
            else:
                logger.info("Database manager successfully auto-detected on retry")

        try:
            with self.db_manager._connect().get() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM fichiers")
                file_count = cursor.fetchone()[0]
                if file_count == 0:
                    messagebox.showwarning(
                        "Aucune Donnée",
                        "Aucun fichier trouvé dans la base de données.\n"
                        "Veuillez analyser des fichiers d'abord.",
                        parent=self.root,
                    )
                    return
        except Exception as e:
            logger.error(f"Database connectivity test failed: {e}")
            messagebox.showerror(
                "Erreur de Connexion",
                f"Impossible de se connecter à la base de données:\n{e}\n\n"
                "Veuillez recharger votre fichier CSV.",
                parent=self.root,
            )
            return

        self.analytics_window = tk.Toplevel(self.root)
        self.analytics_window.title("📊 Analytics Dashboard - Business Intelligence")
        self.analytics_window.geometry("1400x900")
        self.analytics_window.transient(self.root)
        self.analytics_window.grab_set()

        from .analytics_panel import AnalyticsPanel

        analytics_panel = AnalyticsPanel(self.analytics_window, self.db_manager)
        self.analytics_panel = analytics_panel

        def on_analytics_close():
            try:
                self.analytics_window.grab_release()
                self.analytics_window.destroy()
                if hasattr(self, "analytics_window"):
                    delattr(self, "analytics_window")
                logger.info("Analytics window closed properly")
            except Exception as e:
                logger.warning(f"Error during analytics window cleanup: {e}")

        self.analytics_window.protocol("WM_DELETE_WINDOW", on_analytics_close)

        controls = ttk.Frame(self.analytics_window)
        controls.pack(fill="x", padx=5, pady=5)

        ttk.Button(
            controls,
            text="🔄 Actualiser Tout",
            command=self.refresh_all_analytics,
        ).pack(side="left", padx=5)

        ttk.Button(
            controls,
            text="📊 Export Analytics",
            command=self.export_analytics_report,
        ).pack(side="left", padx=5)

        ttk.Button(
            controls,
            text="🔧 Diagnostic DB",
            command=self._run_database_diagnostic,
        ).pack(side="left", padx=5)

        ttk.Button(
            controls,
            text="Fermer",
            command=self.analytics_window.destroy,
        ).pack(side="right", padx=5)

        logger.info("Analytics dashboard opened successfully with validated database")

    def _run_database_diagnostic(self) -> None:
        """Run comprehensive database diagnostic for analytics troubleshooting."""
        try:
            if not self.db_manager:
                messagebox.showerror("Diagnostic", "Database manager not available")
                return

            diagnostic_results: list[str] = []

            try:
                with self.db_manager._connect().get() as conn:
                    cursor = conn.cursor()

                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                    tables = [row[0] for row in cursor.fetchall()]
                    diagnostic_results.append(f"✅ Tables found: {', '.join(tables)}")

                    if "fichiers" in tables:
                        cursor.execute("SELECT COUNT(*) FROM fichiers")
                        file_count = cursor.fetchone()[0]
                        diagnostic_results.append(f"✅ Files in database: {file_count}")

                    if "reponses_llm" in tables:
                        cursor.execute("SELECT COUNT(*) FROM reponses_llm")
                        response_count = cursor.fetchone()[0]
                        diagnostic_results.append(
                            f"✅ Analysis responses: {response_count}"
                        )

                    cursor.execute("PRAGMA integrity_check")
                    integrity = cursor.fetchone()[0]
                    diagnostic_results.append(f"✅ Database integrity: {integrity}")

            except Exception as e:
                diagnostic_results.append(f"❌ Database error: {str(e)}")

            result_text = "\n".join(diagnostic_results)
            messagebox.showinfo(
                "Diagnostic Database",
                f"Résultats du diagnostic:\n\n{result_text}",
                parent=(
                    self.analytics_window
                    if hasattr(self, "analytics_window")
                    else self.root
                ),
            )

        except Exception as exc:  # pragma: no cover - runtime safety
            messagebox.showerror(
                "Erreur Diagnostic",
                f"Impossible d'exécuter le diagnostic: {str(exc)}",
            )

    def refresh_all_analytics(self) -> None:
        if hasattr(self, "analytics_panel"):
            self.analytics_panel.refresh_all()

    def export_analytics_report(self) -> None:
        try:
            export_path = filedialog.asksaveasfilename(
                title="Export Analytics Report",
                defaultextension=".json",
                filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
            )
            if not export_path:
                return
            data = {"analytics": "none"}
            with open(export_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            messagebox.showinfo("Export", f"Analytics exported to {export_path}")
        except Exception as exc:  # pragma: no cover
            messagebox.showerror("Export Error", str(exc))

    def __del__(self) -> None:
        """Cleanup scheduled callbacks on destruction."""
        try:
            self.on_close()
        except Exception:
            pass

    def on_close(self) -> None:
        """Handle window close and cancel callbacks."""
        if self._logs_update_id:
            try:
                self.root.after_cancel(self._logs_update_id)
            except Exception:
                pass
            self._logs_update_id = None
        if self._service_update_id:
            try:
                self.root.after_cancel(self._service_update_id)
            except Exception:
                pass
            self._service_update_id = None
        if hasattr(self, "prompt_debouncer"):
            self.prompt_debouncer.cancel()
        if hasattr(self, "results_refresh_debouncer"):
            self.results_refresh_debouncer.cancel()
        self.root.destroy()
