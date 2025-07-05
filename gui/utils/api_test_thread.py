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
        start_time = time.time()
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
                                "percentage": (len(self.test_results) / self.iterations)
                                * 100,
                                "current_metrics": asdict(self.metrics),
                                "elapsed_time": time.time() - start_time,
                                "eta": self._calculate_eta(
                                    start_time, len(self.test_results), self.iterations
                                ),
                            }
                        )
                except Exception as exc:  # pragma: no cover - runtime errors
                    logger.error("Test worker failed: %s", exc)
                    self.metrics.corrupted_responses += 1

        if self.completion_callback:
            final_stats = self._generate_final_report()
            self.completion_callback(
                {
                    "status": "completed",
                    "metrics": asdict(self.metrics),
                    "results": self.test_results,
                    "final_report": final_stats,
                    "total_duration": time.time() - start_time,
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
        result = analyzer.analyze_single_file(file_row, force_analysis=True)
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
    def _update_metrics(self, result: Dict[str, Any]) -> None:
        """Met à jour les métriques en comptant toutes les réponses."""
        status = result.get("status", "error")
        processing_time = result.get("processing_time", result.get("api_duration", 0.0))

        if status in ["completed", "cached"]:
            self.metrics.successful_responses += 1
        elif status == "error":
            self.metrics.corrupted_responses += 1

        if processing_time > 0:
            self.metrics.response_times.append(processing_time)

        if self.metrics.response_times:
            avg_time = sum(self.metrics.response_times) / len(
                self.metrics.response_times
            )
            self.metrics.throughput_per_minute = (
                60.0 / avg_time if avg_time > 0 else 0.0
            )

        worker_id = result.get("worker_id", 0)
        self.metrics.worker_efficiency.setdefault(worker_id, 0)
        self.metrics.worker_efficiency[worker_id] += 1

        if status == "completed" and "result" in result:
            self._analyze_response_variance(result["result"])

        self.metrics.response_hashes.append(result.get("response_hash", ""))

    # ------------------------------------------------------------------
    def _analyze_response_variance(self, analysis_result: Dict[str, Any]) -> None:
        """Analyse la variance des classifications LLM avec validation."""
        if not analysis_result or not isinstance(analysis_result, dict):
            return

        if "security" in analysis_result and isinstance(
            analysis_result["security"], dict
        ):
            sec_class = analysis_result["security"].get("classification", "unknown")
            if sec_class:
                self.metrics.classification_variance.setdefault("security", {})
                self.metrics.classification_variance["security"].setdefault(
                    sec_class, 0
                )
                self.metrics.classification_variance["security"][sec_class] += 1

        if "rgpd" in analysis_result and isinstance(analysis_result["rgpd"], dict):
            rgpd_risk = analysis_result["rgpd"].get("risk_level", "unknown")
            if rgpd_risk:
                self.metrics.classification_variance.setdefault("rgpd", {})
                self.metrics.classification_variance["rgpd"].setdefault(rgpd_risk, 0)
                self.metrics.classification_variance["rgpd"][rgpd_risk] += 1

        confidence = analysis_result.get("confidence_global")
        if isinstance(confidence, (int, float)) and confidence > 0:
            vals = self.metrics.confidence_stats.setdefault("values", [])
            vals.append(confidence)
            if len(vals) >= 2:
                try:
                    self.metrics.confidence_stats["mean"] = mean(vals)
                    self.metrics.confidence_stats["std"] = (
                        stdev(vals) if len(vals) > 1 else 0.0
                    )
                except (ValueError, TypeError):
                    self.metrics.confidence_stats["values"] = [
                        v for v in vals if isinstance(v, (int, float))
                    ]

    # ------------------------------------------------------------------
    def _is_valid_response(self, response: Any) -> bool:
        """Vérifie qu'une réponse de test est exploitable."""
        return (
            response is not None
            and isinstance(response, dict)
            and response.get("status") == "completed"
            and "result" in response
            and response.get("result") is not None
            and isinstance(response["result"], dict)
        )

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
        logger.info(f"Analyse fiabilité LLM: {len(responses)} réponses à traiter")

        valid_responses: List[Dict[str, Any]] = []
        for i, response in enumerate(responses):
            if self._is_valid_response(response):
                valid_responses.append(response)
            else:
                logger.debug(
                    "Réponse %d invalide: %s - %s", i, type(response), response
                )

        logger.info(
            "Réponses valides: %d, échouées: %d",
            len(valid_responses),
            len(responses) - len(valid_responses),
        )

        if not valid_responses:
            return {
                "security_variance": 0,
                "rgpd_variance": 0,
                "confidence_mean": 0,
                "confidence_std": 0,
                "security_consistency_percent": 0,
                "rgpd_consistency_percent": 0,
                "overall_reliability_score": 0,
                "total_responses": len(responses),
                "valid_responses": 0,
                "failed_responses": len(responses),
            }

        security_counts = Counter(
            r["result"].get("security", {}).get("classification", "unknown")
            for r in valid_responses
            if "security" in r["result"]
        )

        rgpd_counts = Counter(
            r["result"].get("rgpd", {}).get("risk_level", "unknown")
            for r in valid_responses
            if "rgpd" in r["result"]
        )

        confidences = [
            r["result"].get("confidence_global", 0)
            for r in valid_responses
            if "confidence_global" in r["result"]
            and isinstance(r["result"].get("confidence_global"), (int, float))
        ]

        try:
            confidence_mean = mean(confidences) if confidences else 0
            confidence_std = stdev(confidences) if len(confidences) > 1 else 0
        except (ValueError, TypeError):
            confidence_mean = 0
            confidence_std = 0

        return {
            "security_variance": self._calculate_variance(security_counts),
            "rgpd_variance": self._calculate_variance(rgpd_counts),
            "confidence_mean": confidence_mean,
            "confidence_std": confidence_std,
            "security_consistency_percent": self._calculate_consistency(
                security_counts
            ),
            "rgpd_consistency_percent": self._calculate_consistency(rgpd_counts),
            "overall_reliability_score": self._calculate_reliability_score(
                security_counts, rgpd_counts, confidences
            ),
            "total_responses": len(responses),
            "valid_responses": len(valid_responses),
            "failed_responses": len(responses) - len(valid_responses),
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
            wid: mean(times) if times else 0
            for wid, times in worker_performance.items()
        }

        fastest_worker_time = min(worker_avg_times.values()) if worker_avg_times else 0
        theoretical_max_throughput = (
            (60 / fastest_worker_time) if fastest_worker_time > 0 else 0
        )

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
            "current_efficiency": (
                self.metrics.throughput_per_minute / theoretical_max_throughput
                if theoretical_max_throughput > 0
                else 0
            ),
            "recommended_workers": optimal_workers,
            "scalability_score": min(
                100, (self.metrics.throughput_per_minute / self.max_workers) * 10
            ),
        }

    # ------------------------------------------------------------------
    def get_summary_report(self) -> Dict[str, Any]:
        """Génère rapport de synthèse avec gestion d'erreurs robuste."""
        try:
            total_tests = len(self.test_results)
            successful = self.metrics.successful_responses

            try:
                reliability_analysis = self.analyze_llm_reliability(self.test_results)
            except Exception as e:  # pragma: no cover - safeguard
                logger.error(f"Erreur analyse fiabilité LLM: {e}")
                reliability_analysis = {
                    "error": f"Analyse échouée: {str(e)}",
                    "security_variance": 0,
                    "rgpd_variance": 0,
                    "confidence_mean": 0,
                    "confidence_std": 0,
                }

            try:
                scalability_metrics = self.calculate_scalability_metrics()
            except Exception as e:  # pragma: no cover - safeguard
                logger.error(f"Erreur calcul scalabilité: {e}")
                scalability_metrics = {"error": f"Calcul échoué: {str(e)}"}

            return {
                "test_overview": {
                    "total_tests": total_tests,
                    "successful_responses": successful,
                    "success_rate_percent": (
                        (successful / total_tests * 100) if total_tests > 0 else 0
                    ),
                    "corrupted_responses": self.metrics.corrupted_responses,
                    "truncated_responses": self.metrics.truncated_responses,
                    "malformed_json": self.metrics.malformed_json,
                },
                "performance_summary": {
                    "avg_response_time": (
                        mean(self.metrics.response_times)
                        if self.metrics.response_times
                        else 0
                    ),
                    "throughput_per_minute": self.metrics.throughput_per_minute,
                    "workers_used": self.max_workers,
                },
                "reliability_analysis": reliability_analysis,
                "scalability_metrics": scalability_metrics,
                "recommendations": self._generate_recommendations(),
            }
        except Exception as e:  # pragma: no cover - safeguard
            logger.error(f"Erreur génération rapport: {e}")
            return {
                "error": f"Rapport échoué: {str(e)}",
                "test_overview": {
                    "total_tests": (
                        len(self.test_results) if hasattr(self, "test_results") else 0
                    )
                },
            }

    # ------------------------------------------------------------------
    def _generate_recommendations(self) -> List[str]:
        """Génère recommandations basées sur les résultats."""
        recommendations = []

        reliability_score = (
            (self.metrics.successful_responses / len(self.test_results) * 100)
            if self.test_results
            else 0
        )

        if reliability_score < 95:
            recommendations.append(
                f"⚠️ Fiabilité faible ({reliability_score:.1f}%) - Vérifier configuration API"
            )

        if self.metrics.response_times:
            avg_time = sum(self.metrics.response_times) / len(
                self.metrics.response_times
            )
            if avg_time > 10.0:
                recommendations.append(
                    f"🐌 Temps de réponse élevé ({avg_time:.1f}s) - Optimiser workers ou timeouts"
                )

        security_variance = self._calculate_classification_variance("security")
        if security_variance > 20:
            recommendations.append(
                f"🔄 Variance sécurité élevée ({security_variance:.1f}%) - Améliorer prompt"
            )

        if not recommendations:
            recommendations.append("✅ Configuration optimale détectée")

        return recommendations

    # ------------------------------------------------------------------
    def _calculate_eta(self, start_time: float, completed: int, total: int) -> float:
        """Calcule le temps estimé restant pour la fin."""
        if completed == 0:
            return 0.0
        elapsed = time.time() - start_time
        rate = completed / elapsed
        remaining = total - completed
        return remaining / rate if rate > 0 else 0.0

    # ------------------------------------------------------------------
    def _calculate_classification_variance(self, domain: str) -> float:
        """Calcule un pourcentage de variance pour un domaine donné."""
        if domain not in self.metrics.classification_variance:
            return 0.0
        classifications = self.metrics.classification_variance[domain]
        total = sum(classifications.values())
        if total <= 1:
            return 0.0
        unique_classifications = len(classifications)
        max_possible_unique = min(total, 5)
        variance_percentage = (unique_classifications / max_possible_unique) * 100
        return min(variance_percentage, 100.0)

    # ------------------------------------------------------------------
    def _calculate_variance(self, counts: Counter) -> float:
        """Calcule variance en pourcentage."""
        if not counts or sum(counts.values()) == 0:
            return 0.0
        total = sum(counts.values())
        max_count = max(counts.values())
        return 100 - ((max_count / total) * 100)

    # ------------------------------------------------------------------
    def _calculate_consistency(self, counts: Counter) -> float:
        """Calcule pourcentage de consistance."""
        if not counts or sum(counts.values()) == 0:
            return 0.0
        total = sum(counts.values())
        max_count = max(counts.values())
        return (max_count / total) * 100

    # ------------------------------------------------------------------
    def _calculate_reliability_score(
        self,
        security_counts: Counter,
        rgpd_counts: Counter,
        confidences: List[float],
    ) -> float:
        """Score global de fiabilité pondéré."""
        security_consistency = self._calculate_consistency(security_counts)
        rgpd_consistency = self._calculate_consistency(rgpd_counts)

        confidence_score = 0.0
        if confidences:
            avg_conf = mean(confidences)
            std_conf = stdev(confidences) if len(confidences) > 1 else 0
            confidence_score = avg_conf * (1 - min(std_conf / 100, 0.5))

        return (
            security_consistency * 0.4 + rgpd_consistency * 0.4 + confidence_score * 0.2
        )

    # ------------------------------------------------------------------
    def _generate_final_report(self) -> Dict[str, Any]:
        """Génère un rapport final détaillé à la fin des tests."""
        total_responses = len(self.test_results)
        if total_responses == 0:
            return {"error": "No responses to analyze"}

        security_variance = self._calculate_classification_variance("security")
        rgpd_variance = self._calculate_classification_variance("rgpd")

        return {
            "reliability_score": (self.metrics.successful_responses / total_responses)
            * 100,
            "variance_analysis": {
                "security_consistency": 100 - security_variance,
                "rgpd_consistency": 100 - rgpd_variance,
            },
            "performance_analysis": {
                "avg_response_time": (
                    sum(self.metrics.response_times) / len(self.metrics.response_times)
                    if self.metrics.response_times
                    else 0
                ),
                "throughput_per_minute": self.metrics.throughput_per_minute,
            },
            "recommendations": self._generate_recommendations(),
        }
