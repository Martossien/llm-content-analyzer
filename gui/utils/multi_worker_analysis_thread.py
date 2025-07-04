"""Multi-worker analysis thread using ThreadPoolExecutor."""

from __future__ import annotations

import concurrent.futures
import threading
import time
import os
import queue
import yaml
from pathlib import Path
from typing import Callable, Optional, Dict, Any, List

from content_analyzer.content_analyzer import ContentAnalyzer
from content_analyzer.modules.db_manager import DBManager


class PerformanceMonitor:
    """Real time performance monitor for workers."""

    def __init__(self) -> None:
        self.start_time = time.time()
        self.lock = threading.Lock()
        self.metrics: Dict[str, Any] = {
            "processed": 0,
            "errors": 0,
            "cache_hits": 0,
            "avg_processing_time": 0.0,
            "worker_utilization": {},
            "throughput_per_minute": 0.0,
        }

    def record_completion(self, worker_id: int, processing_time: float, was_cached: bool = False) -> None:
        with self.lock:
            self.metrics["processed"] += 1
            if was_cached:
                self.metrics["cache_hits"] += 1

            processed = self.metrics["processed"]
            current_avg = self.metrics["avg_processing_time"]
            self.metrics["avg_processing_time"] = (current_avg * (processed - 1) + processing_time) / processed

            self.metrics.setdefault("worker_utilization", {}).setdefault(worker_id, 0)
            self.metrics["worker_utilization"][worker_id] += 1

            elapsed = time.time() - self.start_time
            self.metrics["throughput_per_minute"] = (processed / elapsed) * 60 if elapsed > 0 else 0.0

    def record_error(self, worker_id: int) -> None:
        with self.lock:
            self.metrics["errors"] += 1

    def get_stats(self) -> Dict[str, Any]:
        with self.lock:
            return self.metrics.copy()


class LegacyMultiWorkerAnalysisThread(threading.Thread):
    """Analysis thread running multiple workers in parallel (legacy implementation)."""

    def __init__(
        self,
        config_path: Path,
        csv_file: Path,
        output_db: Path,
        max_workers: Optional[int] = None,
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

        self.max_workers = self._calculate_optimal_workers(max_workers)

        self.is_paused = threading.Event()
        self.should_stop = threading.Event()

        self.performance_monitor = PerformanceMonitor()
        self.current_files: Dict[int, str] = {}
        self.files_lock = threading.Lock()

        self.db_manager: Optional[DBManager] = None

    # ------------------------------------------------------------------
    # Control helpers
    # ------------------------------------------------------------------
    def _calculate_optimal_workers(self, count: Optional[int]) -> int:
        if count and count > 0:
            return min(count, 32)
        cpu_count = os.cpu_count() or 1
        optimal = min(32, cpu_count + 4)
        return min(optimal, 8)

    def pause(self) -> None:
        self.is_paused.set()

    def resume(self) -> None:
        self.is_paused.clear()

    def stop(self) -> None:
        self.should_stop.set()
        self.is_paused.clear()

    def get_worker_status(self) -> Dict[str, Any]:
        with self.files_lock:
            current = self.current_files.copy()
        stats = self.performance_monitor.get_stats()
        return {
            "active_workers": len(current),
            "max_workers": self.max_workers,
            "current_files": current,
            "performance": stats,
            "is_paused": self.is_paused.is_set(),
            "should_stop": self.should_stop.is_set(),
        }

    # ------------------------------------------------------------------
    # Worker function
    # ------------------------------------------------------------------
    def _analyze_single_file_worker(self, file_row: Dict[str, Any], worker_id: int) -> Dict[str, Any]:
        start = time.time()
        file_path = file_row.get("path", "Unknown")
        with self.files_lock:
            self.current_files[worker_id] = file_path
        try:
            if self.is_paused.is_set():
                self.is_paused.wait()
            if self.should_stop.is_set():
                return {"status": "cancelled", "error": "Analysis stopped"}

            analyzer = ContentAnalyzer(self.config_path)
            result = analyzer.analyze_single_file(file_row)

            duration = time.time() - start
            was_cached = result.get("status") == "cached"
            self.performance_monitor.record_completion(worker_id, duration, was_cached)
            return result
        except Exception as exc:  # pragma: no cover - runtime errors
            self.performance_monitor.record_error(worker_id)
            return {"status": "error", "error": str(exc)}
        finally:
            with self.files_lock:
                self.current_files.pop(worker_id, None)

    # ------------------------------------------------------------------
    # Main thread run
    # ------------------------------------------------------------------
    def run(self) -> None:  # pragma: no cover - integration thread
        start_time = time.time()
        processed = 0
        total_errors = 0
        try:
            self.db_manager = DBManager(self.output_db)

            analyzer = ContentAnalyzer(self.config_path)
            analyzer.csv_parser.parse_csv(self.csv_file, self.output_db)

            files = self.db_manager.get_pending_files(limit=None)
            total_files = len(files)

            if total_files == 0:
                if self.completion_callback:
                    self.completion_callback({
                        "status": "completed",
                        "files_processed": 0,
                        "files_total": 0,
                        "processing_time": 0,
                        "errors": 0,
                    })
                return

            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_file = {
                    executor.submit(self._analyze_single_file_worker, row, idx % self.max_workers): row
                    for idx, row in enumerate(files)
                }

                for future in concurrent.futures.as_completed(future_to_file):
                    if self.should_stop.is_set():
                        break
                    file_row = future_to_file[future]
                    try:
                        result = future.result()
                        status = result.get("status")
                        if status in {"completed", "cached"}:
                            llm_data = result.get("result", {})
                            llm_data["processing_time_ms"] = result.get("processing_time_ms", 0)
                            self.db_manager.store_analysis_result(
                                file_row["id"],
                                result.get("task_id", ""),
                                llm_data,
                                result.get("resume", ""),
                                result.get("raw_response", ""),
                            )
                            self.db_manager.update_file_status(file_row["id"], "completed")
                        else:
                            self.db_manager.update_file_status(file_row["id"], "error", result.get("error", ""))
                            total_errors += 1

                        processed += 1
                        if self.progress_callback:
                            self.progress_callback({
                                "processed": processed,
                                "total": total_files,
                                "current_workers": self.get_worker_status(),
                                "performance": self.performance_monitor.get_stats(),
                            })
                    except Exception as exc:  # pragma: no cover - result errors
                        total_errors += 1
                        self.db_manager.update_file_status(file_row["id"], "error", str(exc))

            total_time = time.time() - start_time
            final_stats = self.performance_monitor.get_stats()
            result = {
                "status": "completed" if not self.should_stop.is_set() else "stopped",
                "files_processed": processed,
                "files_total": total_files,
                "processing_time": total_time,
                "errors": total_errors,
                "performance_stats": final_stats,
                "workers_used": self.max_workers,
                "speedup_estimate": self._calculate_speedup(total_time, processed, final_stats.get("avg_processing_time", 0.0)),
            }
            if self.completion_callback:
                self.completion_callback(result)
        except Exception as exc:  # pragma: no cover - setup errors
            if self.error_callback:
                self.error_callback(f"Multi-worker analysis failed: {str(exc)}")
        finally:
            if self.db_manager:
                self.db_manager.close()

    # ------------------------------------------------------------------
    def _calculate_speedup(self, total_time: float, processed: int, avg_processing_time: float) -> float:
        if avg_processing_time <= 0 or processed == 0:
            return 1.0
        sequential_time = processed * avg_processing_time
        return max(1.0, sequential_time / total_time) if total_time > 0 else 1.0


class EnhancedMultiWorkerAnalysisThread(threading.Thread):
    """Thread multi-workers avec pipeline adaptatif et upload en avance."""

    def __init__(
        self,
        config_path: Path,
        csv_file: Path,
        output_db: Path,
        max_workers: Optional[int] = None,
        progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
        completion_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
        error_callback: Optional[Callable[[str], None]] = None,
    ) -> None:
        super().__init__(daemon=True)

        with open(config_path, "r", encoding="utf-8") as f:
            self.full_config = yaml.safe_load(f)

        self.config_path = config_path
        self.csv_file = csv_file
        self.output_db = output_db
        self.progress_callback = progress_callback
        self.completion_callback = completion_callback
        self.error_callback = error_callback

        self.max_workers = max_workers or self._calculate_optimal_workers(None)

        from content_analyzer.modules.adaptive_pipeline_manager import AdaptivePipelineManager

        self.pipeline_manager = AdaptivePipelineManager(self.full_config)

        self.should_stop = threading.Event()
        self.is_paused = threading.Event()

        self.performance_monitor = PerformanceMonitor()

    def _calculate_optimal_workers(self, count: Optional[int]) -> int:
        if count and count > 0:
            return min(count, 32)
        cpu_count = os.cpu_count() or 1
        optimal = min(32, cpu_count + 4)
        return min(optimal, 8)

    # Placeholder upload worker for demonstration
    def _upload_worker(self, file_batch: List[Dict[str, Any]], worker_id: int) -> None:
        for file_row in file_batch:
            if self.should_stop.is_set():
                break
            delay = self.pipeline_manager.should_delay_upload()
            if delay > 0:
                time.sleep(delay)
            self.pipeline_manager.register_upload_start()
            start = time.time()
            try:
                analyzer = ContentAnalyzer(self.config_path)
                result = analyzer.analyze_single_file(file_row)
                response_time = time.time() - start
                self.pipeline_manager.record_api_response_time(response_time)
                self.pipeline_manager.processing_queue.put((file_row, result))
            except Exception as exc:  # pragma: no cover - runtime
                self.performance_monitor.record_error(worker_id)

    def _processing_worker(self, worker_id: int) -> None:
        while not self.should_stop.is_set():
            try:
                file_row, upload_result = self.pipeline_manager.processing_queue.get(timeout=1.0)
            except queue.Empty:
                continue
            if file_row is None:
                break
            self.pipeline_manager.register_llm_processing_start()
            start = time.time()
            try:
                analyzer = ContentAnalyzer(self.config_path)
                final_result = upload_result
                if final_result.get("status") in {"completed", "cached"}:
                    llm_data = final_result.get("result", {})
                    llm_data["processing_time_ms"] = final_result.get("processing_time_ms", 0)
                    with DBManager(self.output_db) as db:
                        db.store_analysis_result(
                            file_row.get("id"),
                            final_result.get("task_id", ""),
                            llm_data,
                            final_result.get("resume", ""),
                            final_result.get("raw_response", ""),
                        )
                        db.update_file_status(file_row.get("id"), "completed")
                else:
                    with DBManager(self.output_db) as db:
                        db.update_file_status(file_row.get("id"), "error", final_result.get("error", ""))
                    self.performance_monitor.record_error(worker_id)
                proc_time = time.time() - start
                self.pipeline_manager.register_llm_processing_complete()
                self.performance_monitor.record_completion(worker_id, proc_time, False)
            except Exception as exc:  # pragma: no cover - runtime
                self.performance_monitor.record_error(worker_id)

    def run(self) -> None:  # pragma: no cover - integration thread
        try:
            db_manager = DBManager(self.output_db)
            analyzer = ContentAnalyzer(self.config_path)
            analyzer.csv_parser.parse_csv(self.csv_file, self.output_db)
            files = db_manager.get_pending_files(limit=None)
            if not files:
                if self.completion_callback:
                    self.completion_callback({"status": "completed", "files_processed": 0, "files_total": 0, "processing_time": 0, "errors": 0})
                return

            upload_workers = max(1, self.max_workers // 3)
            processing_workers = self.max_workers - upload_workers
            chunk_size = max(1, len(files) // upload_workers)
            file_chunks = [files[i:i + chunk_size] for i in range(0, len(files), chunk_size)]

            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                upload_futures = [executor.submit(self._upload_worker, chunk, i) for i, chunk in enumerate(file_chunks)]
                processing_futures = [executor.submit(self._processing_worker, i) for i in range(processing_workers)]

                while not all(f.done() for f in upload_futures) or not self.pipeline_manager.processing_queue.empty():
                    if self.should_stop.is_set():
                        break
                    if self.progress_callback:
                        status = self.pipeline_manager.get_pipeline_status()
                        status.update(self.performance_monitor.get_stats())
                        self.progress_callback(status)
                    time.sleep(0.5)

                for _ in range(processing_workers):
                    self.pipeline_manager.processing_queue.put((None, None))

                concurrent.futures.wait(processing_futures, timeout=30)

            if self.completion_callback:
                stats = self.performance_monitor.get_stats()
                self.completion_callback({
                    "status": "completed",
                    "files_processed": stats.get("processed", 0),
                    "files_total": len(files),
                    "processing_time": time.time() - self.pipeline_manager.last_upload_time,
                    "errors": stats.get("errors", 0),
                    "performance_stats": stats,
                    "workers_used": self.max_workers,
                })
        except Exception as exc:  # pragma: no cover - setup errors
            if self.error_callback:
                self.error_callback(str(exc))


# Backwards compatibility
MultiWorkerAnalysisThread = EnhancedMultiWorkerAnalysisThread

