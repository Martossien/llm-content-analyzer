import threading
import time
from pathlib import Path
from typing import Callable, Optional, Dict, Any

from content_analyzer.content_analyzer import ContentAnalyzer
from content_analyzer.modules.db_manager import SafeDBManager as DBManager


class AnalysisThread(threading.Thread):
    def __init__(
        self,
        config_path: Path,
        csv_file: Path,
        output_db: Path,
        progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
        completion_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
        error_callback: Optional[Callable[[str], None]] = None,
    ) -> None:
        super().__init__(daemon=True)
        self.config_path = config_path
        self.csv_file = csv_file
        self.output_db = output_db
        self.progress_callback = progress_callback
        self.completion_callback = completion_callback
        self.error_callback = error_callback

        self.is_paused = False
        self.should_stop = False
        self.current_file: Optional[str] = None

    def pause(self) -> None:
        self.is_paused = True

    def resume(self) -> None:
        self.is_paused = False

    def stop(self) -> None:
        self.should_stop = True

    def run(self) -> None:
        try:
            with ContentAnalyzer(self.config_path) as analyzer, DBManager(self.output_db) as db_mgr:
                parse_res = analyzer.csv_parser.parse_csv(self.csv_file, self.output_db)
                files = db_mgr.get_pending_files(limit=None)
                total = len(files)
                processed = 0
                for row in files:
                    if self.should_stop:
                        break
                    while self.is_paused and not self.should_stop:
                        time.sleep(0.5)
                    self.current_file = row.get("path")
                    single_res = analyzer.analyze_single_file(row)
                    if single_res.get("status") in {"completed", "cached"}:
                        llm_data = single_res.get("result", {})
                        llm_data["processing_time_ms"] = single_res.get(
                            "processing_time_ms", 0
                        )
                        db_mgr.store_analysis_result(
                            row["id"],
                            single_res.get("task_id", ""),
                            llm_data,
                            single_res.get("resume", ""),
                            single_res.get("raw_response", ""),
                        )
                        db_mgr.update_file_status(row["id"], "completed")
                    else:
                        db_mgr.update_file_status(
                            row["id"], "error", single_res.get("error")
                        )
                    processed += 1
                    if self.progress_callback:
                        self.progress_callback(
                            {
                                "current_file": self.current_file,
                                "processed": processed,
                                "total": total,
                            }
                        )
                    if self.should_stop:
                        break
            try:
                stats = db_mgr.get_processing_stats()
                with db_mgr._connect() as conn:
                    cursor = conn.cursor()
                    total_time_ms = cursor.execute(
                        "SELECT SUM(processing_time_ms) FROM reponses_llm"
                    ).fetchone()[0] or 0
                    error_count = cursor.execute(
                        "SELECT COUNT(*) FROM fichiers WHERE status='error'"
                    ).fetchone()[0] or 0

                result = {
                    "status": "completed" if not self.should_stop else "stopped",
                    "files_processed": processed,
                    "files_total": total,
                    "processing_time": total_time_ms / 1000,
                    "errors": error_count,
                }
            except Exception as e:
                result = {
                    "status": "completed" if not self.should_stop else "stopped",
                    "files_processed": processed,
                    "files_total": total,
                    "processing_time": 0,
                    "errors": [f"Stats calculation error: {str(e)}"],
                }
            if self.completion_callback:
                self.completion_callback(result)
        except Exception as exc:  # pragma: no cover - runtime errors
            if self.error_callback:
                self.error_callback(str(exc))
            else:
                raise
