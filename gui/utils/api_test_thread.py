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
from collections import Counter
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

    # ------------------------------------------------------------------
    def analyze_llm_reliability(self, responses: List[Dict]) -> Dict[str, Any]:
        """Analyse détaillée de la variance et cohérence LLM."""
        security_classifications = []
        rgpd_levels = []
        confidences = []

        for response in responses:
            result = response.get("result", {})
            if "security" in result:
                security_classifications.append(result["security"].get("classification"))
            if "rgpd" in result:
                rgpd_levels.append(result["rgpd"].get("risk_level"))

            sec_conf = result.get("security", {}).get("confidence", 0)
            rgpd_conf = result.get("rgpd", {}).get("confidence", 0)
            finance_conf = result.get("finance", {}).get("confidence", 0)
            legal_conf = result.get("legal", {}).get("confidence", 0)
            global_conf = (sec_conf + rgpd_conf + finance_conf + legal_conf) / 4
            confidences.append(global_conf)

        security_counts = Counter(security_classifications)
        rgpd_counts = Counter(rgpd_levels)

        security_consistency = (
            (max(security_counts.values()) / len(security_classifications)) * 100
            if security_classifications
            else 0
        )
        rgpd_consistency = (
            (max(rgpd_counts.values()) / len(rgpd_levels)) * 100 if rgpd_levels else 0
        )

        confidence_mean = mean(confidences) if confidences else 0
        confidence_std = stdev(confidences) if len(confidences) > 1 else 0

        return {
            "security_distribution": dict(security_counts),
            "rgpd_distribution": dict(rgpd_counts),
            "security_consistency_percent": security_consistency,
            "rgpd_consistency_percent": rgpd_consistency,
            "confidence_mean": confidence_mean,
            "confidence_std": confidence_std,
            "overall_reliability_score": (security_consistency + rgpd_consistency) / 2,
            "total_responses": len(responses),
        }

    # ------------------------------------------------------------------
    def calculate_scalability_metrics(self) -> Dict[str, Any]:
        """Calcule métriques de scalabilité et recommandations."""
        if not self.test_results:
            return {"error": "No test results available"}

        worker_performance: Dict[int, List[float]] = {}
        for result in self.test_results:
            worker_id = result.get("worker_id", 0)
            duration = result.get("api_duration", 0)
            worker_performance.setdefault(worker_id, []).append(duration)

        worker_avg_times = {
            wid: mean(times) if times else 0 for wid, times in worker_performance.items()
        }

        fastest_worker_time = min(worker_avg_times.values()) if worker_avg_times else 0
        theoretical_max_throughput = (60 / fastest_worker_time) if fastest_worker_time > 0 else 0

        optimal_workers = min(8, max(2, self.max_workers))
        if self.metrics.throughput_per_minute > 0:
            efficiency = self.metrics.throughput_per_minute / (
                theoretical_max_throughput * self.max_workers
            )
            if efficiency < 0.7:
                optimal_workers = max(1, self.max_workers - 1)
            elif efficiency > 0.9:
                optimal_workers = min(8, self.max_workers + 1)

        return {
            "worker_avg_times": worker_avg_times,
            "theoretical_max_throughput": theoretical_max_throughput,
            "current_efficiency": self.metrics.throughput_per_minute / theoretical_max_throughput
            if theoretical_max_throughput > 0
            else 0,
            "recommended_workers": optimal_workers,
            "scalability_score": min(100, (self.metrics.throughput_per_minute / self.max_workers) * 10),
        }

    # ------------------------------------------------------------------
    def get_summary_report(self) -> Dict[str, Any]:
        """Génère rapport de synthèse complet."""
        total_tests = len(self.test_results)
        successful = self.metrics.successful_responses

        reliability_analysis = self.analyze_llm_reliability(self.test_results)
        scalability_metrics = self.calculate_scalability_metrics()

        return {
            "test_overview": {
                "total_tests": total_tests,
                "successful_responses": successful,
                "success_rate_percent": (successful / total_tests * 100) if total_tests > 0 else 0,
                "corrupted_responses": self.metrics.corrupted_responses,
                "truncated_responses": self.metrics.truncated_responses,
                "malformed_json": self.metrics.malformed_json,
            },
            "performance_summary": {
                "avg_response_time": mean(self.metrics.response_times) if self.metrics.response_times else 0,
                "throughput_per_minute": self.metrics.throughput_per_minute,
                "workers_used": self.max_workers,
            },
            "reliability_analysis": reliability_analysis,
            "scalability_metrics": scalability_metrics,
            "recommendations": self._generate_recommendations(
                reliability_analysis, scalability_metrics
            ),
        }

    # ------------------------------------------------------------------
    def _generate_recommendations(self, reliability: Dict, scalability: Dict) -> List[str]:
        """Génère recommandations basées sur les résultats."""
        recommendations = []

        if reliability.get("overall_reliability_score", 0) < 80:
            recommendations.append(
                "⚠️ Variance LLM élevée détectée - Vérifier prompts et température"
            )

        if self.metrics.corrupted_responses > 0:
            recommendations.append(
                "🚨 Corruption de réponses détectée - Réduire la charge ou vérifier API"
            )

        if self.metrics.truncated_responses > 0:
            recommendations.append(
                "✂️ Troncature JSON détectée - Augmenter timeout ou réduire workers"
            )

        efficiency = scalability.get("current_efficiency", 0)
        if efficiency < 0.5:
            recommendations.append(
                f"📉 Efficacité faible ({efficiency:.1%}) - Réduire à {scalability.get('recommended_workers')} workers"
            )
        elif efficiency > 0.9:
            recommendations.append(
                f"📈 Excellente efficacité - Possibilité d'augmenter à {scalability.get('recommended_workers')} workers"
            )

        if not recommendations:
            recommendations.append("✅ Configuration optimale détectée")

        return recommendations

