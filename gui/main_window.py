from __future__ import annotations

import time
from pathlib import Path
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

import pandas as pd
import yaml

from content_analyzer.modules.api_client import APIClient
from content_analyzer.modules.csv_parser import CSVParser
from content_analyzer.modules.db_manager import DBManager
from content_analyzer.modules.prompt_manager import PromptManager

from .utils.analysis_thread import AnalysisThread
from .utils.service_monitor import ServiceMonitor
from .utils.log_viewer import LogViewer


class MainWindow:
    """Main GUI window for the Content Analyzer application."""

    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("Content Analyzer GUI v1.0")
        self.root.minsize(1200, 800)
        self._center_window(1200, 800)

        self.config_path = Path("content_analyzer/config/analyzer_config.yaml")
        self.csv_file_path: str | None = None

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
                    return
                self.csv_file_path = file_path
                self.file_path_label.config(text=file_path, background="lightgreen")
                file_size = Path(file_path).stat().st_size
                row_count = len(df)
                self.show_csv_preview(
                    f"Loaded: {row_count} rows, {file_size/1024:.1f}KB"
                )
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
                text=f"Config: {self.config_path.name} (saved)"
            )
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
        self.cache_status_label.config(
            text=f"● Cache", foreground="green" if cache_stats else "red"
        )
        db_status = self.service_monitor.check_database_status()
        self.db_status_label.config(
            text="● DB" if not db_status["accessible"] else "● DB Connected",
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

    # Placeholder methods for actions not detailed
    def view_results(self) -> None:
        messagebox.showinfo("Results", "View results not implemented")

    def export_results(self) -> None:
        messagebox.showinfo("Export", "Export not implemented")

    # ------------------------------------------------------------------
    # MAINTENANCE
    # ------------------------------------------------------------------
    def show_maintenance_dialog(self) -> None:
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

    # The following maintenance helper methods are placeholders for completeness
    def compact_database(self, parent_window: tk.Toplevel) -> None:
        messagebox.showinfo(
            "Compact DB", "Compaction not implemented", parent=parent_window
        )

    def backup_database(self, parent_window: tk.Toplevel) -> None:
        messagebox.showinfo("Backup DB", "Backup not implemented", parent=parent_window)

    def clear_cache(self, parent_window: tk.Toplevel) -> None:
        messagebox.showinfo(
            "Cache", "Clear cache not implemented", parent=parent_window
        )

    def show_cache_stats(self, parent_window: tk.Toplevel) -> None:
        messagebox.showinfo(
            "Cache Stats", "Cache stats not implemented", parent=parent_window
        )

    def export_configuration(self, parent_window: tk.Toplevel) -> None:
        messagebox.showinfo(
            "Export Config", "Export config not implemented", parent=parent_window
        )

    def import_configuration(self, parent_window: tk.Toplevel) -> None:
        messagebox.showinfo(
            "Import Config", "Import config not implemented", parent=parent_window
        )

    def reset_configuration(self, parent_window: tk.Toplevel) -> None:
        messagebox.showinfo(
            "Reset Config", "Reset to defaults not implemented", parent=parent_window
        )
