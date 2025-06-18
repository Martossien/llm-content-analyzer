import threading
from pathlib import Path
from typing import Callable

from content_analyzer.content_analyzer import ContentAnalyzer


class AnalysisThread(threading.Thread):
    def __init__(
        self,
        config_path: Path,
        csv_file: Path,
        output_db: Path,
        progress_callback: Callable,
        completion_callback: Callable,
    ):
        super().__init__(daemon=True)
        self.config_path = config_path
        self.csv_file = csv_file
        self.output_db = output_db
        self.progress_callback = progress_callback
        self.completion_callback = completion_callback

    def run(self) -> None:
        analyzer = ContentAnalyzer(self.config_path)
        result = analyzer.analyze_batch(self.csv_file, self.output_db)
        self.completion_callback(result)
