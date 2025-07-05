import concurrent.futures
import csv
import hashlib
import json
import logging
import re
import threading
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from statistics import mean, stdev
from typing import Any, Callable, Dict, List, Optional

from content_analyzer.content_analyzer import ContentAnalyzer

logger = logging.getLogger(__name__)


@dataclass
class TestMetrics:
    successful_responses: int = 0
    corrupted_responses: int = 0
    truncated_responses: int = 0
    malformed_json: int = 0
    response_times: List[float] = field(default_factory=list)
    throughput_per_minute: float = 0.0
    worker_efficiency: Dict[int, float] = field(default_factory=dict)
    classification_variance: Dict[str, Dict[str, int]] = field(default_factory=dict)
    confidence_stats: Dict[str, float] = field(default_factory=dict)
    expected_hash: str = ""
    response_hashes: List[str] = field(default_factory=list)


class APITestThread(threading.Thread):
    """Thread dedicated to stress testing API calls."""

    def __init__(
        self,
        config_path: Path,
        test_file_path: Path,
        iterations: int,
        max_workers: int,
        delay_between_requests: float,
        template_type: str,
        progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
        completion_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> None:
        super().__init__(daemon=True)
        self.config_path = Path(config_path)
        self.test_file_path = Path(test_file_path)
        self.iterations = int(iterations)
        self.max_workers = max(1, int(max_workers))
        self.delay_between_requests = float(delay_between_requests)
        self.template_type = template_type
        self.progress_callback = progress_callback
        self.completion_callback = completion_callback
        self.should_stop = threading.Event()
        self.test_results: List[Dict[str, Any]] = []
        self.metrics = TestMetrics()

    # ------------------------------------------------------------------
    def stop(self) -> None:
        self.should_stop.set()

    # ------------------------------------------------------------------
    def run(self) -> None:  # pragma: no cover - integration thread
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as ex:
            futures = []
            for i in range(self.iterations):
                if self.should_stop.is_set():
                    break
                worker_id = i % self.max_workers
                futures.append(ex.submit(self._test_api_worker, i, worker_id))
            for fut in concurrent.futures.as_completed(futures):
                if self.should_stop.is_set():
                    break
                try:
                    res = fut.result()
                    self.test_results.append(res)
                    self._update_metrics(res)
                    if self.progress_callback:
                        self.progress_callback(
                            {
                                "completed": len(self.test_results),
                                "total": self.iterations,
                                "metrics": asdict(self.metrics),
                            }
                        )
                except Exception as exc:  # pragma: no cover - runtime errors
                    logger.error("Test worker failed: %s", exc)
        if self.completion_callback:
            self.completion_callback(
                {
                    "status": "completed",
                    "metrics": asdict(self.metrics),
                    "results": self.test_results,
                }
            )

    # ------------------------------------------------------------------
    def _test_api_worker(self, iteration: int, worker_id: int) -> Dict[str, Any]:
        start = time.time()
        analyzer = ContentAnalyzer(self.config_path)
        file_row = {
            "id": f"test_{iteration}_{worker_id}",
            "path": str(self.test_file_path),
            "file_size": self.test_file_path.stat().st_size,
            "owner": "test_user",
            "last_modified": "2024-01-01 00:00:00",
            "file_signature": "unknown",
        }
        meta = {
            "file_name": Path(file_row["path"]).name,
            "file_size_readable": analyzer._format_file_size(file_row["file_size"]),
            "owner": file_row.get("owner", "test"),
            "last_modified": file_row.get("last_modified", ""),
            "file_extension": Path(file_row["path"]).suffix,
            "file_signature": file_row.get("file_signature", "unknown"),
            "metadata_summary": f"Fichier {Path(file_row['path']).suffix}, {file_row.get('file_size',0)} bytes",
        }
        prompt = analyzer.prompt_manager.build_analysis_prompt(
            meta, analysis_type=self.template_type
        )
        if self.delay_between_requests > 0:
            time.sleep(self.delay_between_requests * worker_id)
        api_start = time.time()
        result = analyzer.analyze_single_file(file_row)
        api_duration = time.time() - api_start
        raw_content = result.get("raw_response", "")
        quality = self._analyze_response_quality(result, raw_content)
        total_duration = time.time() - start
        return {
            **result,
            "iteration": iteration,
            "worker_id": worker_id,
            "api_duration": api_duration,
            "total_duration": total_duration,
            "quality": quality,
            "prompt_hash": hashlib.md5(prompt.encode()).hexdigest(),
            "response_hash": hashlib.md5(raw_content.encode()).hexdigest(),
            "raw_response": raw_content,
        }

    # ------------------------------------------------------------------
    def _analyze_response_quality(
        self, result: Dict[str, Any], raw_content: str
    ) -> Dict[str, Any]:
        quality = {"status": "success", "issues": []}
        corruption_patterns = [
            r"[a-z]\s+[a-z]\s+[a-z]",
            r"(?:mais|pour|avec)\s+[a-z]{1,2}\s+",
        ]
        for pat in corruption_patterns:
            if re.search(pat, raw_content, re.IGNORECASE):
                quality["status"] = "corrupted"
                quality["issues"].append("french_text_corruption")
                break
        if raw_content.strip() and not raw_content.strip().endswith("}"):
            quality["status"] = "truncated"
            quality["issues"].append("json_truncation")
        if result.get("status") == "error" and "Failed to extract JSON" in str(
            result.get("error", "")
        ):
            quality["status"] = "malformed_json"
            quality["issues"].append("json_syntax_error")
        if len(raw_content.strip()) < 10:
            quality["status"] = "empty_response"
            quality["issues"].append("empty_content")
        return quality

    # ------------------------------------------------------------------
    def _update_metrics(self, res: Dict[str, Any]) -> None:
        status = res.get("quality", {}).get("status")
        if status == "success":
            self.metrics.successful_responses += 1
        elif status == "corrupted":
            self.metrics.corrupted_responses += 1
        elif status == "truncated":
            self.metrics.truncated_responses += 1
        elif status == "malformed_json":
            self.metrics.malformed_json += 1
        self.metrics.response_times.append(res.get("api_duration", 0.0))
        worker_id = res.get("worker_id", 0)
        self.metrics.worker_efficiency.setdefault(worker_id, 0)
        self.metrics.worker_efficiency[worker_id] += 1
        elapsed = sum(self.metrics.response_times)
        processed = len(self.metrics.response_times)
        self.metrics.throughput_per_minute = (
            (processed / elapsed) * 60 if elapsed else 0.0
        )
        self.metrics.response_hashes.append(res.get("response_hash", ""))

    # ------------------------------------------------------------------
    def get_final_metrics(self) -> Dict[str, Any]:
        m = asdict(self.metrics)
        if self.metrics.response_times:
            m["avg_response_time"] = mean(self.metrics.response_times)
            if len(self.metrics.response_times) > 1:
                m["std_response_time"] = stdev(self.metrics.response_times)
            else:
                m["std_response_time"] = 0.0
        else:
            m["avg_response_time"] = 0.0
            m["std_response_time"] = 0.0
        return m

    # ------------------------------------------------------------------
    def export_test_results(self, format_type: str = "csv") -> Path:
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        if format_type == "csv":
            export_path = Path(f"api_test_results_{timestamp}.csv")
            with open(export_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(
                    [
                        "iteration",
                        "worker_id",
                        "status",
                        "quality_status",
                        "api_duration",
                        "total_duration",
                        "response_size",
                        "prompt_hash",
                        "response_hash",
                        "issues",
                    ]
                )
                for r in self.test_results:
                    writer.writerow(
                        [
                            r.get("iteration", ""),
                            r.get("worker_id", ""),
                            r.get("status", ""),
                            r.get("quality", {}).get("status", ""),
                            r.get("api_duration", 0.0),
                            r.get("total_duration", 0.0),
                            len(r.get("raw_response", "")),
                            r.get("prompt_hash", ""),
                            r.get("response_hash", ""),
                            "|".join(r.get("quality", {}).get("issues", [])),
                        ]
                    )
        else:
            export_path = Path(f"api_test_detailed_{timestamp}.json")
            with open(export_path, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "test_config": {
                            "file_path": str(self.test_file_path),
                            "iterations": self.iterations,
                            "max_workers": self.max_workers,
                            "template_type": self.template_type,
                            "delay_between_requests": self.delay_between_requests,
                        },
                        "metrics": self.get_final_metrics(),
                        "detailed_results": self.test_results,
                    },
                    f,
                    indent=2,
                    ensure_ascii=False,
                )
        return export_path
