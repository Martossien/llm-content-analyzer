from __future__ import annotations

import logging
import time
from pathlib import Path
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

import pandas as pd
import yaml
import sqlite3
import json
import csv

from content_analyzer.content_analyzer import ContentAnalyzer
from content_analyzer.modules.api_client import APIClient
from content_analyzer.modules.csv_parser import CSVParser
from content_analyzer.modules.db_manager import DBManager
from content_analyzer.modules.prompt_manager import PromptManager
from content_analyzer.modules.cache_manager import CacheManager

from .utils.analysis_thread import AnalysisThread
from .utils.service_monitor import ServiceMonitor
from .utils.log_viewer import LogViewer


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
        self.analysis_thread: AnalysisThread | None = None
        self.analysis_running = False

        self.build_ui()
        self.load_api_configuration()
        self.load_exclusions()
        self.load_templates()
        self.setup_log_viewer()

        self.update_service_status()
        self.update_logs_display()

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

        ttk.Label(api_frame, text="Max Context:").grid(
            row=1, column=0, sticky="w", padx=2, pady=2
        )
        self.max_context_entry = ttk.Entry(api_frame, width=10)
        self.max_context_entry.grid(row=1, column=1, sticky="ew", padx=2, pady=2)

        ttk.Label(api_frame, text="Workers:").grid(
            row=2, column=0, sticky="w", padx=2, pady=2
        )
        self.workers_entry = ttk.Entry(api_frame, width=5)
        self.workers_entry.grid(row=2, column=1, sticky="w", padx=2, pady=2)

        self.test_api_button = ttk.Button(
            api_frame, text="Test Connection", command=self.test_api_connection
        )
        self.test_api_button.grid(row=3, column=0, sticky="ew", padx=2, pady=5)

        ttk.Button(
            api_frame, text="Save Configuration", command=self.save_api_configuration
        ).grid(row=3, column=1, sticky="ew", padx=2, pady=5)

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
        self.single_button = ttk.Button(
            action_frame,
            text="ANALYZE SELECTED FILE",
            width=20,
            command=self.analyze_selected_file,
        )
        self.single_button.pack(side="left", padx=5, pady=5)
        ttk.Button(
            action_frame, text="VIEW RESULTS", width=15, command=self.view_results
        ).pack(side="left", padx=5, pady=5)
        ttk.Button(
            action_frame,
            text="MAINTENANCE",
            width=15,
            command=self.show_maintenance_dialog,
        ).pack(side="left", padx=5, pady=5)
        ttk.Button(
            action_frame, text="EXPORT", width=15, command=self.export_results
        ).pack(side="left", padx=5, pady=5)

        # SECTION 5B ----------------------------------------------------
        batch_frame = ttk.LabelFrame(self.root, text="Batch Operations")
        batch_frame.pack(fill="x", padx=5, pady=5)

        self.max_files_var = tk.StringVar(value="0")
        ttk.Button(batch_frame, text="START BATCH ANALYSIS", command=self.start_analysis).pack(side="left", padx=5)
        ttk.Button(batch_frame, text="ANALYZE FILTERED FILES", command=self.analyze_filtered_files).pack(side="left", padx=5)
        ttk.Button(batch_frame, text="REPROCESS ERRORS", command=self.reprocess_errors).pack(side="left", padx=5)
        ttk.Label(batch_frame, text="Max Files:").pack(side="left", padx=5)
        ttk.Entry(batch_frame, textvariable=self.max_files_var, width=6).pack(side="left")
        ttk.Button(batch_frame, text="ALL FILES", command=lambda: self.max_files_var.set("0")).pack(side="left", padx=5)

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
        messagebox.showinfo("CSV Preview", info)

    def browse_csv_file(self) -> None:
        file_path = filedialog.askopenfilename(
            title="Select SMBeagle CSV File",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
        )
        if file_path:
            try:
                parser = CSVParser(self.config_path)
                df = pd.read_csv(file_path, nrows=10)
                errors = parser.validate_csv_format(df)
                if errors:
                    messagebox.showerror(
                        "Invalid CSV Format",
                        "CSV validation failed:\n" + "\n".join(errors),
                    )
                    self.file_path_label.config(background="red")
                    self.csv_file_path = None
                    return
                self.csv_file_path = file_path
                self.file_path_label.config(text=file_path, background="lightgreen")
                file_size = Path(file_path).stat().st_size
                row_count = len(df)
                info = f"Loaded: {row_count} rows, {file_size/1024:.1f}KB"
                self.show_csv_preview(info)
                if self.file_tooltip:
                    self.file_tooltip.hide()
                self.file_tooltip = Tooltip(self.file_path_label, info)
                self.log_action(f"CSV file selected: {file_path}")
            except Exception as e:  # pragma: no cover - I/O errors
                messagebox.showerror("File Error", f"Cannot read CSV file:\n{str(e)}")

    # ------------------------------------------------------------------
    # API CONFIGURATION
    # ------------------------------------------------------------------
    def get_api_token(self) -> str:
        with open(self.config_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        return cfg.get("api_config", {}).get("token", "")

    def load_api_configuration(self) -> None:
        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
            api_config = config.get("api_config", {})
            self.api_url_entry.delete(0, tk.END)
            self.api_url_entry.insert(0, api_config.get("url", "http://localhost:8080"))
            self.max_context_entry.delete(0, tk.END)
            self.max_context_entry.insert(0, str(api_config.get("max_tokens", 100000)))
            self.workers_entry.delete(0, tk.END)
            self.workers_entry.insert(0, str(api_config.get("batch_size", 3)))
            self.status_config_label.config(foreground="black")
        except Exception as e:  # pragma: no cover - file errors
            messagebox.showerror(
                "Config Error", f"Cannot load configuration:\n{str(e)}"
            )

    def test_api_connection(self) -> None:
        url = self.api_url_entry.get().strip()
        if not url:
            messagebox.showerror("Error", "Please enter API URL")
            return
        self.test_api_button.config(state="disabled", text="Testing...")
        self.root.update()
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
                )
                self.api_status_label.config(text="● API Connected", foreground="green")
            else:
                messagebox.showerror("Connection Failed", "API is not accessible")
                self.api_status_label.config(text="● API Failed", foreground="red")
        except Exception as e:  # pragma: no cover - network errors
            messagebox.showerror("Connection Error", f"Connection failed:\n{str(e)}")
            self.api_status_label.config(text="● API Error", foreground="red")
        finally:
            self.test_api_button.config(state="normal", text="Test Connection")

    def save_api_configuration(self) -> None:
        try:
            url = self.api_url_entry.get().strip()
            if not url.startswith(("http://", "https://")):
                messagebox.showerror(
                    "Invalid URL", "URL must start with http:// or https://"
                )
                return
            try:
                max_context = int(self.max_context_entry.get())
                if max_context < 1000 or max_context > 500000:
                    raise ValueError("Max context must be between 1000 and 500000")
            except ValueError as e:
                messagebox.showerror("Invalid Max Context", str(e))
                return
            try:
                workers = int(self.workers_entry.get())
                if workers < 1 or workers > 10:
                    raise ValueError("Workers must be between 1 and 10")
            except ValueError as e:
                messagebox.showerror("Invalid Workers", str(e))
                return
            with open(self.config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
            config["api_config"]["url"] = url
            config["api_config"]["max_tokens"] = max_context
            config["api_config"]["batch_size"] = workers
            with open(self.config_path, "w", encoding="utf-8") as f:
                yaml.safe_dump(config, f, default_flow_style=False, indent=2)
            messagebox.showinfo(
                "Configuration Saved", "API configuration has been saved successfully!"
            )
            self.status_config_label.config(
                text=f"Config: {self.config_path.name} (saved)",
                foreground="green"
            )
            self.log_action("API configuration saved")
        except Exception as e:  # pragma: no cover - file errors
            messagebox.showerror("Save Error", f"Cannot save configuration:\n{str(e)}")

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
            messagebox.showerror("Load Error", f"Cannot load exclusions:\n{str(e)}")

    def add_extension(self) -> None:
        ext = self.add_ext_entry.get().strip()
        if not ext:
            messagebox.showwarning("Empty Extension", "Please enter an extension")
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
                "Duplicate Extension", f"Extension {ext} is already blocked"
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
            messagebox.showerror("Add Error", f"Cannot add extension:\n{str(e)}")

    def remove_extension(self) -> None:
        selection = self.exclusions_listbox.curselection()
        if not selection:
            messagebox.showwarning(
                "No Selection", "Please select an extension to remove"
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
            messagebox.showerror("Remove Error", f"Cannot remove extension:\n{str(e)}")

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
            messagebox.showerror("Toggle Error", f"Cannot update setting:\n{str(e)}")

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
            messagebox.showerror("Toggle Error", f"Cannot update setting:\n{str(e)}")

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
            messagebox.showerror("Load Error", f"Cannot load templates:\n{str(e)}")

    def edit_template(self) -> None:
        selected_template = self.template_combobox.get()
        if not selected_template:
            messagebox.showwarning("No Template", "Please select a template to edit")
            return
        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
            template_config = config["templates"].get(selected_template, {})
            system_prompt = template_config.get("system_prompt", "")
            user_template = template_config.get("user_template", "")

            editor_window = tk.Toplevel(self.root)
            editor_window.title(f"Edit Template: {selected_template}")
            editor_window.geometry("800x600")
            editor_window.transient(self.root)
            editor_window.grab_set()

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
                    )
                    editor_window.destroy()
                    self.log_action(f"Template edited: {selected_template}")
                except Exception as e:
                    messagebox.showerror(
                        "Save Error", f"Cannot save template:\n{str(e)}"
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
            messagebox.showerror("Edit Error", f"Cannot edit template:\n{str(e)}")

    def test_prompt(self) -> None:
        selected_template = self.template_combobox.get()
        if not selected_template:
            messagebox.showwarning("No Template", "Please select a template to test")
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
            preview_window = tk.Toplevel(self.root)
            preview_window.title(f"Prompt Preview: {selected_template}")
            preview_window.geometry("700x500")
            preview_window.transient(self.root)
            preview_window.grab_set()

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
            messagebox.showerror("Test Error", f"Cannot test prompt:\n{str(e)}")

    def save_template(self) -> None:
        messagebox.showinfo("Save Template", "Template saved.")

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
            if not self.log_file_path.exists():
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
            self.root.after(3000, self.update_logs_display)

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

    # ------------------------------------------------------------------
    # SERVICE STATUS AND PROGRESS
    # ------------------------------------------------------------------
    def update_service_status(self) -> None:
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
            db_text = f"● DB {db_status['size_mb']:.1f}MB"
        else:
            db_text = "● DB"
        self.db_status_label.config(
            text=db_text,
            foreground="green" if db_status["accessible"] else "red",
        )
        self.root.after(5000, self.update_service_status)

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
                if hasattr(self, "last_progress_time"):
                    time_diff = current_time - self.last_progress_time
                    files_diff = completed - getattr(self, "last_completed", 0)
                    speed = (files_diff / time_diff * 60) if time_diff > 0 else 0
                else:
                    speed = 0
                cache_hit_rate = self.get_cache_hit_rate()
                metrics_text = (
                    f"Files: {completed}/{total} ({progress_pct:.1f}%) | "
                    f"Speed: {speed:.0f}/min | "
                    f"Cache Hit: {cache_hit_rate:.1f}% | "
                    f"Errors: {errors}"
                )
                self.progress_metrics_label.config(text=metrics_text)
                self.progress_bar["value"] = progress_pct
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
                self.last_progress_time = current_time
                self.last_completed = completed
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
                max_context = int(self.max_context_entry.get())
                if max_context < 1000:
                    raise ValueError("Too small")
            except ValueError:
                messagebox.showerror(
                    "Invalid Configuration", "Max context must be a number >= 1000"
                )
                return False
            try:
                workers = int(self.workers_entry.get())
                if workers < 1 or workers > 10:
                    raise ValueError("Out of range")
            except ValueError:
                messagebox.showerror(
                    "Invalid Configuration", "Workers must be between 1 and 10"
                )
                return False
            return True
        except Exception as e:
            messagebox.showerror(
                "Validation Error", f"Configuration validation failed:\n{str(e)}"
            )
            return False

    def start_analysis(self) -> None:
        if not self.csv_file_path:
            messagebox.showerror("No CSV File", "Please select a CSV file first")
            return
        if not self.validate_configuration():
            return
        if not self.service_monitor.check_api_status():
            response = messagebox.askyesno(
                "API Unavailable", "API is not accessible. Continue anyway?"
            )
            if not response:
                return
        try:
            output_db = Path("analysis_results.db")
            self.analysis_thread = AnalysisThread(
                config_path=self.config_path,
                csv_file=Path(self.csv_file_path),
                output_db=output_db,
                progress_callback=self.on_analysis_progress,
                completion_callback=self.on_analysis_complete,
                error_callback=self.on_analysis_error,
            )
            self.db_manager = DBManager(output_db)
            self.status_db_label.config(
                text=f"DB: {output_db.name} ({output_db.stat().st_size/1024:.1f}KB)"
            )
            self.analysis_running = True
            self.start_button.config(state="disabled")
            self.pause_button.config(state="normal")
            self.stop_button.config(state="normal")
            self.browse_button.config(state="disabled")
            self.analysis_thread.start()
            self.update_progress_display()
            self.log_action(f"Analysis started: {self.csv_file_path}", "INFO")
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
        )
        if not response:
            return
        if self.analysis_thread and self.analysis_thread.is_alive():
            self.analysis_thread.stop()
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

    def analyze_selected_file(self) -> None:
        """Allow user to analyze a single file using the configured pipeline."""
        file_path = filedialog.askopenfilename(title="Select File to Analyze")
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
            messagebox.showinfo(
                "Single File Analysis", json.dumps(res, indent=2)
            )
        except Exception as exc:  # pragma: no cover - I/O errors
            messagebox.showerror("Error", str(exc))

    def analyze_filtered_files(self) -> None:
        """Reanalyse les fichiers filtrés/pendants en base."""
        try:
            db_path = Path("analysis_results.db")
            if not db_path.exists():
                return
            analyzer = ContentAnalyzer(self.config_path)
            db_mgr = DBManager(db_path)
            files = db_mgr.get_pending_files(limit=int(self.max_files_var.get() or 0))
            for row in files:
                res = analyzer.analyze_single_file(row)
                if res.get("status") in {"completed", "cached"}:
                    db_mgr.store_analysis_result(row["id"], res.get("task_id", ""), res.get("result", {}))
                    db_mgr.update_file_status(row["id"], "completed")
        except Exception as exc:
            messagebox.showerror("Batch Error", str(exc))

    def reprocess_errors(self) -> None:
        """Relance l'analyse des fichiers en erreur."""
        try:
            db_path = Path("analysis_results.db")
            if not db_path.exists():
                return
            analyzer = ContentAnalyzer(self.config_path)
            db_mgr = DBManager(db_path)
            conn = sqlite3.connect(db_path)
            rows = conn.execute("SELECT * FROM fichiers WHERE status='error'").fetchall()
            conn.close()
            columns = ["id", "path", "file_size", "owner", "fast_hash", "access_time", "file_attributes", "file_signature", "last_modified", "status", "exclusion_reason", "priority_score", "special_flags", "processed_at"]
            for r in rows[: int(self.max_files_var.get() or len(rows))]:
                row = dict(zip(columns, r))
                res = analyzer.analyze_single_file(row)
                if res.get("status") in {"completed", "cached"}:
                    db_mgr.store_analysis_result(row["id"], res.get("task_id", ""), res.get("result", {}))
                    db_mgr.update_file_status(row["id"], "completed")
        except Exception as exc:
            messagebox.showerror("Reprocess Error", str(exc))

    def on_analysis_progress(self, info: dict) -> None:
        self.current_file_path = info.get("current_file")

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
        errors = result.get("errors", [])
        completion_msg = (
            f"Analysis completed!\n\n"
            f"Status: {status}\n"
            f"Files processed: {files_processed}/{files_total}\n"
            f"Processing time: {processing_time:.1f}s\n"
            f"Errors: {len(errors)}"
        )
        if status == "completed":
            messagebox.showinfo("Analysis Complete", completion_msg)
            self.log_action(
                f"Analysis completed successfully: {files_processed}/{files_total} files",
                "INFO",
            )
            self.status_app_label.config(text="Completed")
        else:
            messagebox.showerror("Analysis Failed", completion_msg)
            self.log_action(f"Analysis failed: {status}", "ERROR")
            self.status_app_label.config(text="Failed")

    def on_analysis_error(self, error: str) -> None:
        self.analysis_running = False
        self.start_button.config(state="normal")
        self.pause_button.config(state="disabled", text="PAUSE")
        self.stop_button.config(state="disabled")
        self.browse_button.config(state="normal")
        messagebox.showerror("Analysis Error", error)
        self.log_action(f"Analysis error: {error}", "ERROR")
        self.status_app_label.config(text="Error")

    # ------------------------------------------------------------------
    # RESULTS VIEWER AND EXPORTS
    # ------------------------------------------------------------------
    def view_results(self) -> None:
        """Ouvre une fenêtre pour visualiser les résultats d'analyse."""
        try:
            db_path = Path("analysis_results.db")
            if not db_path.exists():
                messagebox.showwarning(
                    "No Results",
                    "No analysis results database found.\nPlease run an analysis first.",
                )
                self.log_action("Results viewer: no database found", "WARN")
                return

            results_window = tk.Toplevel(self.root)
            results_window.title("Analysis Results Viewer")
            results_window.geometry("1000x700")
            results_window.transient(self.root)

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

            refresh_btn = ttk.Button(
                controls_frame,
                text="Refresh",
                command=lambda: self.refresh_results_table(
                    tree, status_filter.get(), classification_filter.get()
                ),
            )
            refresh_btn.pack(side="right", padx=5)

            export_btn = ttk.Button(
                controls_frame,
                text="Export to CSV",
                command=lambda: self.export_results_to_csv(tree),
            )
            export_btn.pack(side="right", padx=5)

            tree_frame = ttk.Frame(results_window)
            tree_frame.pack(fill="both", expand=True, padx=10, pady=5)

            columns = (
                "ID",
                "File Path",
                "Status",
                "Security",
                "RGPD",
                "Finance",
                "Legal",
                "Confidence",
                "Processing Time",
            )
            tree = ttk.Treeview(tree_frame, columns=columns, show="headings", height=20)

            tree.heading("ID", text="ID")
            tree.heading("File Path", text="File Path")
            tree.heading("Status", text="Status")
            tree.heading("Security", text="Security")
            tree.heading("RGPD", text="RGPD")
            tree.heading("Finance", text="Finance")
            tree.heading("Legal", text="Legal")
            tree.heading("Confidence", text="Confidence")
            tree.heading("Processing Time", text="Proc. Time (ms)")

            tree.column("ID", width=50)
            tree.column("File Path", width=300)
            tree.column("Status", width=80)
            tree.column("Security", width=80)
            tree.column("RGPD", width=80)
            tree.column("Finance", width=80)
            tree.column("Legal", width=80)
            tree.column("Confidence", width=80)
            tree.column("Processing Time", width=100)

            v_scrollbar = ttk.Scrollbar(tree_frame, orient="vertical", command=tree.yview)
            h_scrollbar = ttk.Scrollbar(tree_frame, orient="horizontal", command=tree.xview)
            tree.configure(yscrollcommand=v_scrollbar.set, xscrollcommand=h_scrollbar.set)

            tree.pack(side="left", fill="both", expand=True)
            v_scrollbar.pack(side="right", fill="y")
            h_scrollbar.pack(side="bottom", fill="x")

            self.refresh_results_table(tree, "All", "All")

            status_filter.bind(
                "<<ComboboxSelected>>",
                lambda e: self.refresh_results_table(
                    tree, status_filter.get(), classification_filter.get()
                ),
            )
            classification_filter.bind(
                "<<ComboboxSelected>>",
                lambda e: self.refresh_results_table(
                    tree, status_filter.get(), classification_filter.get()
                ),
            )

            tree.bind("<Double-1>", lambda e: self.show_file_details(tree))

            # Navigation controls
            nav_frame = ttk.Frame(results_window)
            nav_frame.pack(fill="x", padx=10, pady=5)

            def goto_first():
                items = tree.get_children()
                if items:
                    tree.selection_set(items[0])
                    tree.see(items[0])

            def goto_prev():
                sel = tree.selection()
                items = tree.get_children()
                if sel:
                    idx = items.index(sel[0])
                    if idx > 0:
                        tree.selection_set(items[idx - 1])
                        tree.see(items[idx - 1])

            def goto_next():
                sel = tree.selection()
                items = tree.get_children()
                if sel:
                    idx = items.index(sel[0])
                    if idx < len(items) - 1:
                        tree.selection_set(items[idx + 1])
                        tree.see(items[idx + 1])

            def goto_last():
                items = tree.get_children()
                if items:
                    tree.selection_set(items[-1])
                    tree.see(items[-1])

            ttk.Button(nav_frame, text="FIRST FILE", command=goto_first).pack(side="left", padx=2)
            ttk.Button(nav_frame, text="◀ PREV FILE", command=goto_prev).pack(side="left", padx=2)
            ttk.Button(nav_frame, text="NEXT FILE ▶", command=goto_next).pack(side="left", padx=2)
            ttk.Button(nav_frame, text="LAST FILE", command=goto_last).pack(side="left", padx=2)

            ttk.Label(nav_frame, text="GO TO FILE:").pack(side="left", padx=5)
            goto_var = tk.StringVar()
            ttk.Entry(nav_frame, textvariable=goto_var, width=6).pack(side="left")

            def jump():
                val = goto_var.get().strip()
                if not val.isdigit():
                    return
                items = tree.get_children()
                for itm in items:
                    if tree.item(itm)["values"][0] == int(val):
                        tree.selection_set(itm)
                        tree.see(itm)
                        break

            ttk.Button(nav_frame, text="JUMP", command=jump).pack(side="left", padx=2)

            self.log_action("Results viewer opened", "INFO")

        except Exception as e:
            messagebox.showerror("Results Error", f"Failed to open results viewer:\n{str(e)}")
            self.log_action(f"Results viewer failed: {str(e)}", "ERROR")

    def refresh_results_table(self, tree, status_filter, classification_filter):
        """Rafraîchit le tableau des résultats avec filtres."""
        try:
            for item in tree.get_children():
                tree.delete(item)

            db_path = Path("analysis_results.db")
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            query = """
        SELECT f.id, f.path, f.status,
               r.security_analysis, r.rgpd_analysis, r.finance_analysis, r.legal_analysis,
               r.confidence_global, r.processing_time_ms
        FROM fichiers f
        LEFT JOIN reponses_llm r ON f.id = r.fichier_id
        WHERE 1=1
        """
            params = []

            if status_filter != "All":
                query += " AND f.status = ?"
                params.append(status_filter)

            if classification_filter != "All":
                query += " AND r.security_analysis LIKE ?"
                params.append(f'%"classification": "{classification_filter}"%')

            query += " ORDER BY f.id DESC LIMIT 1000"

            cursor.execute(query, params)
            rows = cursor.fetchall()
            conn.close()

            for row in rows:
                file_id, file_path, status, security, rgpd, finance, legal, confidence, proc_time = row

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

                tree.insert(
                    "",
                    "end",
                    values=(
                        file_id,
                        file_path[-50:] if len(file_path) > 50 else file_path,
                        status,
                        security_class,
                        rgpd_risk,
                        finance_type,
                        legal_type,
                        confidence or 0,
                        proc_time or 0,
                    ),
                )

            self.log_action(f"Results table refreshed: {len(rows)} entries", "INFO")

        except Exception as e:
            messagebox.showerror("Refresh Error", f"Failed to refresh results:\n{str(e)}")
            self.log_action(f"Results refresh failed: {str(e)}", "ERROR")

    def show_file_details(self, tree):
        """Affiche les détails complets d'un fichier sélectionné."""
        selection = tree.selection()
        if not selection:
            return

        item = tree.item(selection[0])
        file_id = item["values"][0]

        try:
            db_path = Path("analysis_results.db")
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            cursor.execute(
                """
        SELECT f.path, f.file_size, f.owner, f.last_modified, f.status,
               r.security_analysis, r.rgpd_analysis, r.finance_analysis, r.legal_analysis,
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
                messagebox.showwarning("No Data", "No details found for this file")
                return

            details_window = tk.Toplevel(self.root)
            details_window.title(f"File Details - ID {file_id}")
            details_window.geometry("800x600")
            details_window.transient(self.root)

            text_widget = tk.Text(details_window, wrap="word", font=("Consolas", 10))
            text_widget.pack(fill="both", expand=True, padx=10, pady=10)

            path, size, owner, modified, status, security, rgpd, finance, legal, confidence, proc_time, created = row

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

SECURITY ANALYSIS:
{json.dumps(json.loads(security) if security else {}, indent=2)}

RGPD ANALYSIS:
{json.dumps(json.loads(rgpd) if rgpd else {}, indent=2)}

FINANCE ANALYSIS:
{json.dumps(json.loads(finance) if finance else {}, indent=2)}

LEGAL ANALYSIS:
{json.dumps(json.loads(legal) if legal else {}, indent=2)}
"""

            text_widget.insert(1.0, details_content)
            text_widget.config(state="disabled")

            ttk.Button(details_window, text="Close", command=details_window.destroy).pack(
                pady=5
            )

            self.log_action(f"File details viewed: ID {file_id}", "INFO")

        except Exception as e:
            messagebox.showerror("Details Error", f"Failed to show file details:\n{str(e)}")
            self.log_action(f"File details failed: {str(e)}", "ERROR")

    def export_results(self) -> None:
        """Lance la fenêtre d'export des résultats en différents formats."""
        try:
            db_path = Path("analysis_results.db")
            if not db_path.exists():
                messagebox.showwarning("No Results", "No analysis results found to export")
                return

            export_window = tk.Toplevel(self.root)
            export_window.title("Export Analysis Results")
            export_window.geometry("500x400")
            export_window.transient(self.root)
            export_window.grab_set()

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
            ttk.Label(filter_frame, text="Status:").grid(row=0, column=0, sticky="w", padx=5, pady=2)
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

            buttons_frame = ttk.Frame(export_window)
            buttons_frame.pack(fill="x", padx=10, pady=10)

            ttk.Button(
                buttons_frame,
                text="Export",
                command=lambda: self.perform_export(export_window),
            ).pack(side="right", padx=5)
            ttk.Button(buttons_frame, text="Cancel", command=export_window.destroy).pack(
                side="right", padx=5
            )

            self.log_action("Export dialog opened", "INFO")

        except Exception as e:
            messagebox.showerror("Export Error", f"Failed to open export dialog:\n{str(e)}")
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
        SELECT f.id, f.path, f.file_size, f.owner, f.last_modified, f.status,
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
            )
            self.log_action(
                f"Results exported to {self.export_format.get().upper()}: {Path(export_path).name} ({len(rows)} records)",
                "INFO",
            )

        except Exception as e:
            messagebox.showerror("Export Error", f"Failed to export results:\n{str(e)}")
            self.log_action(f"Export failed: {str(e)}", "ERROR")

    def export_to_csv(self, rows, export_path):
        """Exporte les résultats au format CSV."""
        with open(export_path, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)

            headers = [
                "ID",
                "File Path",
                "File Size",
                "Owner",
                "Last Modified",
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
                headers.extend([
                    "Security JSON",
                    "RGPD JSON",
                    "Finance JSON",
                    "Legal JSON",
                ])

            writer.writerow(headers)

            for row in rows:
                (
                    file_id,
                    path,
                    size,
                    owner,
                    modified,
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
                    path,
                    size,
                    owner,
                    modified,
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
                path,
                size,
                owner,
                modified,
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
                    "path": path,
                    "size_bytes": size,
                    "owner": owner,
                    "last_modified": modified,
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
                    path,
                    size,
                    owner,
                    modified,
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
                        "File Path": path,
                        "File Size": size,
                        "Owner": owner,
                        "Last Modified": modified,
                        "Status": status,
                        "Security Classification": security_data.get("classification", "N/A"),
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
                            sum(1 for r in rows if r[5] == "completed"),
                            sum(1 for r in rows if r[5] == "error"),
                            sum(r[10] for r in rows if r[10])
                            / len([r for r in rows if r[10]])
                            if any(r[10] for r in rows)
                            else 0,
                            sum(1 for r in rows if r[6] and "C0" in str(r[6])),
                            sum(1 for r in rows if r[6] and "C1" in str(r[6])),
                            sum(1 for r in rows if r[6] and "C2" in str(r[6])),
                            sum(1 for r in rows if r[6] and "C3" in str(r[6])),
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
                parent=self.root,
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
            )
            self.log_action(
                f"Table exported to CSV: {Path(export_path).name}",
                "INFO",
            )

        except Exception as e:
            messagebox.showerror("Export Error", f"Failed to export table:\n{str(e)}")
            self.log_action(f"Table export failed: {str(e)}", "ERROR")

    # ------------------------------------------------------------------
    # MAINTENANCE
    # ------------------------------------------------------------------
    def show_maintenance_dialog(self) -> None:
        self.log_action("Maintenance dialog opened", "INFO")
        maintenance_window = tk.Toplevel(self.root)
        maintenance_window.title("System Maintenance")
        maintenance_window.geometry("500x400")
        maintenance_window.transient(self.root)
        maintenance_window.grab_set()

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

    def reset_database(self, parent_window: tk.Toplevel) -> None:
        response = messagebox.askyesno(
            "Confirm Reset",
            "This will delete ALL analysis data!\nAre you sure?",
            parent=parent_window,
        )
        if not response:
            return
        try:
            db_path = Path("analysis_results.db")
            if db_path.exists():
                db_path.unlink()
            self.db_manager = DBManager(db_path)
            messagebox.showinfo(
                "Database Reset",
                "Database has been reset successfully!",
                parent=parent_window,
            )
            self.log_action("Database reset completed", "INFO")
        except Exception as e:
            messagebox.showerror(
                "Reset Error", f"Cannot reset database:\n{str(e)}", parent=parent_window
            )
            self.log_action(f"Database reset failed: {str(e)}", "ERROR")

    def compact_database(self, parent_window: tk.Toplevel) -> None:
        """Compacte la base SQLite avec VACUUM pour optimiser l'espace."""
        try:
            db_path = Path("analysis_results.db")
            if not db_path.exists():
                messagebox.showwarning(
                    "No Database", "No database file found to compact", parent=parent_window
                )
                return

            size_before = db_path.stat().st_size / (1024 * 1024)
            conn = sqlite3.connect(db_path)
            conn.execute("VACUUM")
            conn.close()
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
                "Compaction Error", f"Failed to compact database:\n{str(e)}", parent=parent_window
            )
            self.log_action(f"Database compaction failed: {str(e)}", "ERROR")

    def backup_database(self, parent_window: tk.Toplevel) -> None:
        """Crée une sauvegarde timestampée de la base de données."""
        try:
            db_path = Path("analysis_results.db")
            if not db_path.exists():
                messagebox.showwarning(
                    "No Database", "No database file found to backup", parent=parent_window
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
                "Backup Error", f"Failed to backup database:\n{str(e)}", parent=parent_window
            )
            self.log_action(f"Database backup failed: {str(e)}", "ERROR")

    def clear_cache(self, parent_window: tk.Toplevel) -> None:
        """Vide complètement le cache SQLite."""
        try:
            cache_db = Path("analysis_results_cache.db")
            if not cache_db.exists():
                messagebox.showinfo("No Cache", "No cache database found", parent=parent_window)
                return

            cache_manager = CacheManager(cache_db)
            stats_before = cache_manager.get_stats()
            entries_before = stats_before.get("total_entries", 0)
            size_before = stats_before.get("cache_size_mb", 0)

            cache_manager.cleanup_expired()
            cache_db.unlink()

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
            messagebox.showerror("Clear Error", f"Failed to clear cache:\n{str(e)}", parent=parent_window)
            self.log_action(f"Cache clear failed: {str(e)}", "ERROR")

    def show_cache_stats(self, parent_window: tk.Toplevel) -> None:
        """Affiche les statistiques détaillées du cache."""
        try:
            cache_db = Path("analysis_results_cache.db")
            if not cache_db.exists():
                messagebox.showinfo("No Cache", "No cache database found", parent=parent_window)
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

            ttk.Button(stats_window, text="Close", command=stats_window.destroy).pack(pady=5)

            self.log_action("Cache statistics viewed", "INFO")

        except Exception as e:
            messagebox.showerror("Stats Error", f"Failed to get cache stats:\n{str(e)}", parent=parent_window)
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
                export_path = export_path.with_name(f"{export_path.stem}_{timestamp}{export_path.suffix}")

            import shutil

            shutil.copy2(self.config_path, export_path)

            messagebox.showinfo(
                "Configuration Exported",
                f"Configuration exported successfully!\nFile: {export_path.name}",
                parent=parent_window,
            )
            self.log_action(f"Configuration exported to: {export_path.name}", "INFO")

        except Exception as e:
            messagebox.showerror("Export Error", f"Failed to export configuration:\n{str(e)}", parent=parent_window)
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
            missing_sections = [sec for sec in required_sections if sec not in imported_config]

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
            self.log_action(f"Configuration imported from: {Path(import_path).name}", "INFO")

        except Exception as e:
            messagebox.showerror("Import Error", f"Failed to import configuration:\n{str(e)}", parent=parent_window)
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
            messagebox.showerror("Reset Error", f"Failed to reset configuration:\n{str(e)}", parent=parent_window)
            self.log_action(f"Configuration reset failed: {str(e)}", "ERROR")
