#!/usr/bin/env python3
"""Content Analyzer orchestrator - production ready."""

import argparse
import hashlib
import logging
import time
from pathlib import Path
from typing import Any, Dict, Optional, List
import sys

if __package__ is None and __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import yaml


from content_analyzer.modules import (
    CSVParser,
    APIClient,
    CacheManager,
    FileFilter,
    DBManager,
    PromptManager,
)


logger = logging.getLogger(__name__)


class ContentAnalyzer:
    """Main orchestrator for modular content analysis."""

    def __init__(self, config_path: Optional[Path] = None) -> None:
        """Initialise modules and validate configuration."""

        default_cfg = (
            Path(__file__).resolve().parent / "config" / "analyzer_config.yaml"
        )
        self.config_path = config_path or default_cfg
        with open(self.config_path, "r", encoding="utf-8") as f:
            self.config = yaml.safe_load(f)

        # Instantiate modules
        self.csv_parser = CSVParser(self.config_path)
        self.file_filter = FileFilter(self.config_path)
        self.prompt_manager = PromptManager(self.config_path)
        self.api_client = APIClient(self.config)

        cache_ttl = (
            self.config.get("modules", {})
            .get("cache_manager", {})
            .get("ttl_hours", 168)
        )
        self.cache_manager = CacheManager(Path("cache_prompts.db"), ttl_hours=cache_ttl)

        # temporary DB until analyze_batch provides real one
        self.db_manager = DBManager(Path(":memory:"))

        # runtime options
        self.enable_cache = False
        self.analysis_type = "comprehensive"
        self.priority_threshold = 0

        if not self.api_client.health_check():
            logger.warning("API-DOC-IA unreachable")

    # ------------------------------------------------------------------
    def analyze_batch(
        self,
        csv_file: Path,
        output_db: Path,
        max_files: int = 1000,
        enable_cache: bool = True,
    ) -> Dict[str, Any]:
        """Execute full workflow on a batch of files."""

        start = time.perf_counter()
        stats: Dict[str, Any] = {
            "status": "completed",
            "files_total": 0,
            "files_processed": 0,
            "files_excluded": 0,
            "cache_hit_rate": 0.0,
            "processing_time": 0.0,
            "api_calls_made": 0,
            "errors": [],
            "performance_metrics": {},
        }

        parse_result = self.csv_parser.parse_csv(csv_file, output_db)
        stats["files_total"] = parse_result.get("total_files", 0)
        stats["files_excluded"] = stats["files_total"] - parse_result.get(
            "imported_files", 0
        )

        if parse_result.get("errors"):
            stats["status"] = "failed"
            stats["errors"].extend(parse_result["errors"])
            return stats

        self.db_manager = DBManager(output_db)

        if enable_cache:
            cache_db = output_db.with_name(f"{output_db.stem}_cache.db")
            self.cache_manager = CacheManager(
                cache_db, ttl_hours=self.cache_manager.ttl_hours
            )
            self.enable_cache = True
        else:
            self.enable_cache = False

        pending_files = self.db_manager.get_pending_files(
            limit=max_files, priority_threshold=self.priority_threshold
        )

        processed: List[Dict[str, Any]] = []

        for row in pending_files:
            ok, reason = self.file_filter.should_process_file(row)
            if not ok:
                self.db_manager.update_file_status(row["id"], "excluded", reason)
                stats["files_excluded"] += 1
                continue
            score = self.file_filter.calculate_priority_score(row)
            flags = ",".join(self.file_filter.get_special_flags(row))
            self.db_manager.update_file_status(row["id"], "processing")
            processed.append({**row, "priority_score": score, "special_flags": flags})

        for row in sorted(processed, key=lambda x: x["priority_score"], reverse=True):
            try:
                single_res = self.analyze_single_file(
                    row, analysis_type=self.analysis_type
                )
            except Exception as exc:  # pragma: no cover - unexpected
                logger.critical(f"Unexpected error: {exc}")
                self.db_manager.update_file_status(row["id"], "error", str(exc))
                stats["errors"].append(str(exc))
                continue

            if single_res.get("status") in {"completed", "cached"}:
                self.db_manager.store_analysis_result(
                    row["id"],
                    single_res.get("task_id", ""),
                    single_res.get("result", {}),
                )
                self.db_manager.update_file_status(row["id"], "completed")
                stats["files_processed"] += 1
                if single_res.get("status") != "cached":
                    stats["api_calls_made"] += 1
            else:
                self.db_manager.update_file_status(
                    row["id"], "error", single_res.get("error")
                )
                stats["errors"].append(single_res.get("error"))

        if self.enable_cache:
            stats["cache_hit_rate"] = self.cache_manager.get_stats().get(
                "hit_rate", 0.0
            )

        stats["processing_time"] = round(time.perf_counter() - start, 2)
        stats["performance_metrics"] = self.db_manager.get_processing_stats()
        return stats

    # ------------------------------------------------------------------
    def analyze_single_file(
        self,
        file_metadata: Dict[str, Any],
        analysis_type: str = "comprehensive",
    ) -> Dict[str, Any]:
        """Analyze one file using intelligent cache."""

        start = time.perf_counter()
        prompt = self.prompt_manager.build_analysis_prompt(file_metadata, analysis_type)
        prompt_hash = hashlib.sha256(prompt.encode()).hexdigest()

        cached = None
        if self.enable_cache:
            cached = self.cache_manager.get_cached_result(
                file_metadata.get("fast_hash", ""), prompt_hash
            )

        if cached:
            return {
                "status": "cached",
                "result": cached,
                "processing_time": round(time.perf_counter() - start, 2),
                "task_id": "cached",
            }

        try:
            api_result = self.api_client.analyze_file(file_metadata["path"], prompt)
        except Exception as e:  # pragma: no cover - network problems
            logger.error(f"API error for {file_metadata['path']}: {e}")
            return {"status": "failed", "error": str(e)}

        if api_result.get("status") != "completed":
            return {"status": "failed", "error": "api_failed"}

        result = api_result.get("result", {})
        if self.enable_cache:
            self.cache_manager.store_result(
                file_metadata.get("fast_hash", ""), prompt_hash, result
            )

        return {
            "status": "completed",
            "result": result,
            "task_id": api_result.get("task_id"),
            "processing_time": round(time.perf_counter() - start, 2),
        }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Content Analyzer Brique 2 - Production Ready"
    )
    parser.add_argument(
        "--input", required=True, type=Path, help="Fichier CSV SMBeagle enrichi"
    )
    parser.add_argument(
        "--output", required=True, type=Path, help="Base SQLite de sortie"
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/analyzer_config.yaml"),
        help="Configuration YAML",
    )
    parser.add_argument(
        "--enable-cache", action="store_true", help="Active le cache intelligent"
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=1000,
        help="Nombre maximum de fichiers à traiter",
    )
    parser.add_argument(
        "--priority-threshold",
        type=int,
        default=0,
        help="Score priorité minimum",
    )
    parser.add_argument(
        "--analysis-type",
        choices=["comprehensive", "security_focused"],
        default="comprehensive",
        help="Type d'analyse",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Logs détaillés")

    args = parser.parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level)

    analyzer = ContentAnalyzer(args.config)
    analyzer.priority_threshold = args.priority_threshold
    analyzer.analysis_type = args.analysis_type

    result = analyzer.analyze_batch(
        csv_file=args.input,
        output_db=args.output,
        max_files=args.max_files,
        enable_cache=args.enable_cache,
    )

    print(f"✅ Status: {result['status']}")
    print(f"📊 Files processed: {result['files_processed']}/{result['files_total']}")
    print(f"🚀 Cache hit rate: {result['cache_hit_rate']:.1f}%")
    print(f"⏱️  Processing time: {result['processing_time']:.1f}s")

    if result["errors"]:
        print(f"⚠️  Errors: {len(result['errors'])}")
        for error in result["errors"][:5]:
            print(f"   - {error}")


if __name__ == "__main__":  # pragma: no cover - CLI usage
    main()
