from pathlib import Path
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import pandas as pd

from content_analyzer.modules.csv_parser import CSVParser
from content_analyzer.modules.db_manager import DBManager

from .utils.service_monitor import ServiceMonitor
from .utils.analysis_thread import AnalysisThread
from .utils.log_viewer import LogViewer


class MainWindow:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Content Analyzer GUI")
        self.config_path = Path("content_analyzer/config/analyzer_config.yaml")
        self.service_monitor = ServiceMonitor(self.config_path)
        self.log_viewer = LogViewer(Path("logs/content_analyzer.log"))
        self.db_manager = None
        self.analysis_thread = None
        self.analysis_running = False
        self.csv_file_path = ""

        self.build_ui()
        self.update_service_status()
        self.update_logs()

    # ------------------------------------------------------------- UI BUILD
    def build_ui(self):
        self.file_frame = ttk.LabelFrame(self.root, text="File Loading")
        self.file_frame.pack(fill="x", padx=5, pady=5)

        ttk.Button(
            self.file_frame,
            text="Browse",
            command=self.browse_csv_file,
        ).pack(side="left")
        self.csv_path_label = ttk.Label(
            self.file_frame,
            text="No file selected",
        )
        self.csv_path_label.pack(side="left", padx=5)

        self.api_status_label = ttk.Label(self.file_frame, text="API ?")
        self.api_status_label.pack(side="left", padx=5)
        self.cache_status_label = ttk.Label(self.file_frame, text="Cache ?")
        self.cache_status_label.pack(side="left", padx=5)
        self.db_status_label = ttk.Label(self.file_frame, text="DB ?")
        self.db_status_label.pack(side="left", padx=5)

        # Start button
        self.start_button = ttk.Button(
            self.root, text="START ANALYSIS", command=self.start_analysis
        )
        self.start_button.pack(pady=5)

        # Progress
        self.progress_label = ttk.Label(self.root, text="Progress")
        self.progress_label.pack()
        self.progress_bar = ttk.Progressbar(self.root, length=400)
        self.progress_bar.pack()

        # Logs
        self.log_frame = ttk.LabelFrame(self.root, text="Logs")
        self.log_frame.pack(
            fill="both",
            expand=True,
            padx=5,
            pady=5,
        )
        self.log_text = tk.Text(self.log_frame, height=10)
        self.log_text.pack(fill="both", expand=True)

    # ----------------------- File actions -----------------------
    def browse_csv_file(self):
        file_path = filedialog.askopenfilename(
            title="Select SMBeagle CSV", filetypes=[("CSV files", "*.csv")]
        )
        if file_path:
            parser = CSVParser(self.config_path)
            df = pd.read_csv(file_path, nrows=5)
            errors = parser.validate_csv_format(df)
            if errors:
                messagebox.showerror("Invalid CSV", f"Errors: {errors}")
                return
            self.csv_file_path = file_path
            self.csv_path_label.config(text=file_path)
            # preview not implemented

    # ------------------------------------------------------------- status
    def update_service_status(self):
        api_status = self.service_monitor.check_api_status()
        self.api_status_label.config(
            text="API OK" if api_status else "API FAIL",
            foreground="green" if api_status else "red",
        )
        cache_status = self.service_monitor.check_cache_status()
        self.cache_status_label.config(
            text=f"Cache {cache_status.get('hit_rate', 0)}%",
            foreground="green",
        )
        db_status = self.service_monitor.check_database_status()
        self.db_status_label.config(
            text="DB OK" if db_status["accessible"] else "DB Missing",
            foreground="green" if db_status["accessible"] else "red",
        )
        self.root.after(5000, self.update_service_status)

    # ------------------------------------------------------------- logs
    def update_logs(self):
        lines = self.log_viewer.tail_logs(50)
        self.log_text.delete(1.0, tk.END)
        for line in lines:
            self.log_text.insert(tk.END, line + "\n")
        self.root.after(5000, self.update_logs)

    # ------------------------------------------------------------- progress
    def update_progress(self):
        if self.analysis_running and self.db_manager:
            stats = self.db_manager.get_processing_stats()
            total = stats.get("total_files", 0)
            completed = stats.get("completed", 0)
            errors = stats.get("errors", 0)
            pct = (completed + errors) / total * 100 if total else 0
            self.progress_label.config(
                text=f"Files: {completed}/{total} ({pct:.1f}%) | Err: {errors}"
            )
            self.progress_bar["value"] = pct
            self.root.after(2000, self.update_progress)

    # ------------------------------------------------------------- start
    def start_analysis(self):
        if not self.csv_file_path:
            messagebox.showerror("Error", "Please select CSV file first")
            return
        output_db = Path("analysis_results.db")
        self.analysis_thread = AnalysisThread(
            self.config_path,
            Path(self.csv_file_path),
            output_db,
            self.on_progress,
            self.on_complete,
        )
        self.db_manager = DBManager(output_db)
        self.analysis_running = True
        self.analysis_thread.start()
        self.update_progress()

    def on_progress(self, info):
        pass  # placeholder for callback

    def on_complete(self, result):
        self.analysis_running = False
        messagebox.showinfo("Done", f"Status: {result.get('status')}")
        self.start_button.config(state="normal")
