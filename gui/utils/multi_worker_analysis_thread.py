"""Multi-worker analysis thread using ThreadPoolExecutor."""

from __future__ import annotations

import concurrent.futures
import threading
import time
import os
import yaml
import logging
from pathlib import Path
from typing import Callable, Optional, Dict, Any, List

from content_analyzer.content_analyzer import ContentAnalyzer
from content_analyzer.modules.db_manager import DBManager

logger = logging.getLogger(__name__)


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
            "timeouts": 0,
            "timeout_details": {},
            "adaptive_timeout_avg": 0.0,
        }

    def record_completion(
        self, worker_id: int, processing_time: float, was_cached: bool = False
    ) -> None:
        with self.lock:
            self.metrics["processed"] += 1
            if was_cached:
                self.metrics["cache_hits"] += 1

            processed = self.metrics["processed"]
            current_avg = self.metrics["avg_processing_time"]
            self.metrics["avg_processing_time"] = (
                current_avg * (processed - 1) + processing_time
            ) / processed

            self.metrics.setdefault("worker_utilization", {}).setdefault(worker_id, 0)
            self.metrics["worker_utilization"][worker_id] += 1

            elapsed = time.time() - self.start_time
            self.metrics["throughput_per_minute"] = (
                (processed / elapsed) * 60 if elapsed > 0 else 0.0
            )

    def record_error(self, worker_id: int) -> None:
        with self.lock:
            self.metrics["errors"] += 1

    def record_timeout(self, timeout_value: int, spacing: float) -> None:
        """Enregistre un timeout avec contexte."""
        with self.lock:
            self.metrics["timeouts"] += 1
            key = f"{timeout_value}s"
            self.metrics.setdefault("timeout_details", {}).setdefault(key, 0)
            self.metrics["timeout_details"][key] += 1

            current_avg = self.metrics.get("adaptive_timeout_avg", 0.0)
            total = self.metrics["timeouts"]
            self.metrics["adaptive_timeout_avg"] = (
                current_avg * (total - 1) + timeout_value
            ) / total

    def get_stats(self) -> Dict[str, Any]:
        with self.lock:
            return self.metrics.copy()

    def get_gui_safe_snapshot(self) -> Dict[str, Any]:
        with self.lock:
            snapshot = {
                "processed": self.metrics["processed"],
                "errors": self.metrics["errors"],
                "cache_hits": self.metrics["cache_hits"],
                "avg_processing_time": self.metrics["avg_processing_time"],
                "throughput_per_minute": self.metrics["throughput_per_minute"],
                "worker_utilization": dict(self.metrics.get("worker_utilization", {})),
                "timestamp": time.time(),
            }
            return snapshot


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

    def _calculate_worker_distribution(self) -> tuple[int, int]:
        """Determine upload vs processing worker counts."""
        if self.max_workers == 1:
            return 1, 0
        if self.max_workers == 2:
            return 1, 1
        upload_workers = max(1, self.max_workers // 3)
        processing_workers = max(1, self.max_workers - upload_workers)
        return upload_workers, processing_workers

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
    def _analyze_single_file_worker(
        self, file_row: Dict[str, Any], worker_id: int
    ) -> Dict[str, Any]:
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
                    self.completion_callback(
                        {
                            "status": "completed",
                            "files_processed": 0,
                            "files_total": 0,
                            "processing_time": 0,
                            "errors": 0,
                        }
                    )
                return

            with concurrent.futures.ThreadPoolExecutor(
                max_workers=self.max_workers
            ) as executor:
                future_to_file = {
                    executor.submit(
                        self._analyze_single_file_worker, row, idx % self.max_workers
                    ): row
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
                            llm_data["processing_time_ms"] = result.get(
                                "processing_time_ms", 0
                            )
                            self.db_manager.store_analysis_result(
                                file_row["id"],
                                result.get("task_id", ""),
                                llm_data,
                                result.get("resume", ""),
                                result.get("raw_response", ""),
                            )
                            self.db_manager.update_file_status(
                                file_row["id"], "completed"
                            )
                        else:
                            self.db_manager.update_file_status(
                                file_row["id"], "error", result.get("error", "")
                            )
                            total_errors += 1

                        processed += 1
                        if self.progress_callback:
                            self.progress_callback(
                                {
                                    "processed": processed,
                                    "total": total_files,
                                    "current_workers": self.get_worker_status(),
                                    "performance": self.performance_monitor.get_stats(),
                                }
                            )
                    except Exception as exc:  # pragma: no cover - result errors
                        total_errors += 1
                        self.db_manager.update_file_status(
                            file_row["id"], "error", str(exc)
                        )

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
                "speedup_estimate": self._calculate_speedup(
                    total_time, processed, final_stats.get("avg_processing_time", 0.0)
                ),
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
    def _calculate_speedup(
        self, total_time: float, processed: int, avg_processing_time: float
    ) -> float:
        if avg_processing_time <= 0 or processed == 0:
            return 1.0
        sequential_time = processed * avg_processing_time
        return max(1.0, sequential_time / total_time) if total_time > 0 else 1.0


class SmartMultiWorkerAnalysisThread(threading.Thread):
    """Multi-worker analysis thread with adaptive throttling."""

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

        self.adaptive_manager = self._init_adaptive_manager()

        self.db_manager: Optional[DBManager] = None

    # ------------------------------------------------------------------
    # Config helpers
    # ------------------------------------------------------------------
    def _init_adaptive_manager(self) -> "AdaptivePipelineManager":
        from content_analyzer.modules.adaptive_pipeline_manager import (
            AdaptivePipelineManager,
        )

        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f)
        except Exception:
            cfg = {}

        return AdaptivePipelineManager(cfg or {}, max_workers=self.max_workers)

    def _calculate_optimal_workers(self, count: Optional[int]) -> int:
        if count and count > 0:
            return min(count, 32)
        cpu_count = os.cpu_count() or 1
        optimal = min(32, cpu_count + 4)
        return min(optimal, 8)

    # ------------------------------------------------------------------
    # Control helpers
    # ------------------------------------------------------------------
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
        pipeline_status = self.adaptive_manager.get_pipeline_status()
        return {
            "active_workers": len(current),
            "max_workers": self.max_workers,
            "current_spacing": pipeline_status.get("current_spacing"),
            "current_files": current,
            "performance": stats,
            "pipeline": pipeline_status,
            "is_paused": self.is_paused.is_set(),
            "should_stop": self.should_stop.is_set(),
        }

    # ------------------------------------------------------------------
    # Worker function
    # ------------------------------------------------------------------
    def _smart_worker_task(
        self, file_row: Dict[str, Any], worker_id: int
    ) -> Dict[str, Any]:
        if self.should_stop.is_set():
            return {"status": "cancelled", "error": "stopped_before_start"}

        if self.is_paused.is_set():
            self.is_paused.wait()
        if self.should_stop.is_set():
            return {"status": "cancelled", "error": "stopped_after_pause"}

        delay = self.adaptive_manager.should_delay_upload()
        if delay > 0:
            sleep_inc = 0.5
            remaining = delay
            while remaining > 0 and not self.should_stop.is_set():
                time.sleep(min(sleep_inc, remaining))
                remaining -= sleep_inc
            if self.should_stop.is_set():
                return {"status": "cancelled", "error": "stopped_during_delay"}
        self.adaptive_manager.register_upload_start()
        self.adaptive_manager.register_llm_processing_start()

        start = time.time()
        file_path = file_row.get("path", "Unknown")
        with self.files_lock:
            self.current_files[worker_id] = file_path
        try:
            analyzer = ContentAnalyzer(self.config_path, stop_event=self.should_stop)
            analyzer._worker_count = self.max_workers
            result = analyzer.analyze_single_file(file_row)
            if self.should_stop.is_set():
                return {"status": "cancelled", "error": "stopped_after_analysis"}
            duration = time.time() - start
            was_cached = result.get("status") == "cached"
            self.performance_monitor.record_completion(worker_id, duration, was_cached)
            self.adaptive_manager.record_api_response_time(duration)
            self.adaptive_manager.register_llm_processing_complete()
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
                    self.completion_callback(
                        {
                            "status": "completed",
                            "files_processed": 0,
                            "files_total": 0,
                            "processing_time": 0,
                            "errors": 0,
                        }
                    )
                return

            with concurrent.futures.ThreadPoolExecutor(
                max_workers=self.max_workers
            ) as executor:
                future_to_file = {}
                for idx, row in enumerate(files):
                    if self.should_stop.is_set():
                        logger.info("Stop requested - halting task submission")
                        break
                    future = executor.submit(
                        self._smart_worker_task, row, idx % self.max_workers
                    )
                    future_to_file[future] = row

                for future, file_row in future_to_file.items():
                    if self.should_stop.is_set():
                        logger.info("Stop requested - cancelling remaining tasks")
                        if not future.done():
                            future.cancel()
                        continue
                    try:
                        current_spacing = getattr(
                            self.adaptive_manager, "current_spacing", 30.0
                        )
                        adaptive_timeout = int(max(60, min(current_spacing + 30, 120)))
                        logger.debug(
                            "🕐 Timeout adaptatif: %ds (espacement: %.1fs)",
                            adaptive_timeout,
                            current_spacing,
                        )

                        result = None
                        start_wait = time.time()
                        while not future.done():
                            if self.should_stop.is_set():
                                if not future.done():
                                    future.cancel()
                                logger.info(
                                    "Stop requested during wait for: %s",
                                    file_row.get("path", "unknown"),
                                )
                                break
                            try:
                                result = future.result(timeout=0.5)
                                break
                            except concurrent.futures.TimeoutError:
                                if time.time() - start_wait > adaptive_timeout:
                                    raise
                                continue

                        if self.should_stop.is_set() and result is None:
                            logger.info(
                                "Processing skipped for %s due to stop request",
                                file_row.get("path", "unknown"),
                            )
                            continue

                        if result is None and future.done() and not future.cancelled():
                            result = future.result()

                        if not result:
                            continue
                        status = result.get("status")
                        if status in {"completed", "cached"}:
                            llm_data = result.get("result", {})
                            llm_data["processing_time_ms"] = result.get(
                                "processing_time_ms", 0
                            )
                            self.db_manager.store_analysis_result(
                                file_row["id"],
                                result.get("task_id", ""),
                                llm_data,
                                result.get("resume", ""),
                                result.get("raw_response", ""),
                            )
                            self.db_manager.update_file_status(
                                file_row["id"], "completed"
                            )
                        else:
                            self.db_manager.update_file_status(
                                file_row["id"], "error", result.get("error", "")
                            )
                            total_errors += 1

                        processed += 1
                        if self.progress_callback:
                            self.progress_callback(
                                {
                                    "processed": processed,
                                    "total": total_files,
                                    "current_workers": self.get_worker_status(),
                                    "performance": self.performance_monitor.get_stats(),
                                }
                            )
                    except concurrent.futures.TimeoutError:
                        logger.error(
                            "🕐 TIMEOUT WORKER: %s | Espacement: %.1fs | Timeout: %ds | Workers: %d",
                            file_row.get("path", "unknown"),
                            current_spacing,
                            adaptive_timeout,
                            self.max_workers,
                        )
                        self.db_manager.update_file_status(
                            file_row["id"], "error", f"timeout_{adaptive_timeout}s"
                        )
                        self.performance_monitor.record_timeout(
                            adaptive_timeout, current_spacing
                        )
                        total_errors += 1
                    except concurrent.futures.CancelledError:
                        logger.info(
                            "Future cancelled for %s",
                            file_row.get("path", "unknown"),
                        )
                        if self.db_manager:
                            self.db_manager.update_file_status(
                                file_row["id"], "cancelled", "User requested stop"
                            )
                    except Exception as exc:  # pragma: no cover - result errors
                        total_errors += 1
                        self.db_manager.update_file_status(
                            file_row["id"], "error", str(exc)
                        )

            total_time = time.time() - start_time
            final_stats = self.performance_monitor.get_stats()
            final_status = "stopped" if self.should_stop.is_set() else "completed"
            result = {
                "status": final_status,
                "files_processed": processed,
                "files_total": total_files,
                "processing_time": total_time,
                "errors": total_errors,
                "performance_stats": final_stats,
                "workers_used": self.max_workers,
                "speedup_estimate": self._calculate_speedup(
                    total_time,
                    processed,
                    final_stats.get("avg_processing_time", 0.0),
                ),
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
    def _calculate_speedup(
        self, total_time: float, processed: int, avg_processing_time: float
    ) -> float:
        if avg_processing_time <= 0 or processed == 0:
            return 1.0
        sequential_time = processed * avg_processing_time
        return max(1.0, sequential_time / total_time) if total_time > 0 else 1.0


# Backwards compatibility
MultiWorkerAnalysisThread = SmartMultiWorkerAnalysisThread
