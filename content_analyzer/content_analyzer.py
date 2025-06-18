#!/usr/bin/env python3
"""
Content Analyzer - Orchestrateur principal Brique 2
Architecture modulaire avec stack minimal
"""

import argparse
import hashlib
import logging
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional

if __package__ is None and __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    __package__ = "content_analyzer"

import yaml

from content_analyzer.modules import (
    APIClient,
    CacheManager,
    CSVParser,
    DBManager,
    FileFilter,
    PromptManager,
)

# Configuration logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class ContentAnalyzer:
    """Orchestrateur principal pour l'analyse de contenu LLM"""

    def __init__(self, config_path: Optional[Path] = None) -> None:
        """Initialise TOUS les modules avec configuration YAML."""

        default_cfg = Path(__file__).resolve().parent / "config" / "analyzer_config.yaml"
        self.config_path = config_path or default_cfg
        with open(self.config_path, "r", encoding="utf-8") as f:
            self.config = yaml.safe_load(f)

        exclusions = Path(__file__).resolve().parent / "config" / "exclusions_config.yaml"
        prompts = Path(__file__).resolve().parent / "config" / "prompts_config.yaml"

        cache_ttl = self.config.get("modules", {}).get("cache_manager", {}).get("ttl_hours", 168)
        self.cache_manager = CacheManager(Path("cache_prompts.db"), ttl_hours=cache_ttl)
        self.csv_parser = CSVParser(self.config_path)
        self.file_filter = FileFilter(exclusions)
        self.api_client = APIClient(self.config)
        self.db_manager = DBManager(Path(":memory:"))
        self.prompt_manager = PromptManager(prompts)

        self.enable_cache = False
        self.max_files = 1000
        self.priority_threshold = 0

    def analyze_batch(self, csv_file: Path, output_db: Path) -> Dict[str, Any]:
        """WORKFLOW COMPLET - Pipeline bout-en-bout."""

        start = time.perf_counter()
        error_count = 0

        parse_result = self.csv_parser.parse_csv(csv_file, output_db)
        if parse_result["errors"]:
            logger.error(f"Parsing errors: {parse_result['errors']}")
            return {"status": "failed", "errors": parse_result["errors"]}

        logger.info(f"Imported {parse_result['imported_files']} files")

        # Réinitialise gestionnaires dépendant de la base
        self.db_manager = DBManager(output_db)
        if self.enable_cache:
            cache_db = output_db.with_name(f"{output_db.stem}_cache.db")
            cache_ttl = self.config.get("modules", {}).get("cache_manager", {}).get("ttl_hours", 168)
            self.cache_manager = CacheManager(cache_db, ttl_hours=cache_ttl)

        pending_files = self.db_manager.get_pending_files(
            limit=self.max_files, priority_threshold=self.priority_threshold
        )
        processed_files = []

        conn = sqlite3.connect(output_db)
        for file_row in pending_files:
            should_process, reason = self.file_filter.should_process_file(file_row)
            if not should_process:
                self.db_manager.update_file_status(file_row["id"], "excluded", reason)
                continue
            priority = self.file_filter.calculate_priority_score(file_row)
            flags = ",".join(self.file_filter.get_special_flags(file_row))
            conn.execute(
                "UPDATE fichiers SET priority_score = ?, special_flags = ? WHERE id = ?",
                (priority, flags, file_row["id"]),
            )
            processed_files.append({**file_row, "priority_score": priority})
        conn.commit()
        conn.close()

        for file_row in sorted(
            processed_files, key=lambda x: x.get("priority_score", 0), reverse=True
        ):
            prompt = self.prompt_manager.build_analysis_prompt(file_row, "comprehensive")
            prompt_hash = hashlib.sha256(prompt.encode()).hexdigest()

            cached_result = None
            if self.enable_cache:
                cached_result = self.cache_manager.get_cached_result(
                    file_row.get("fast_hash"), prompt_hash
                )

            if cached_result:
                logger.debug(f"Cache HIT for {file_row['path']}")
                analysis_result = cached_result
                task_id = "cached"
            else:
                logger.debug(f"Cache MISS - API call for {file_row['path']}")
                try:
                    api_result = self.api_client.analyze_file(file_row["path"], prompt)
                except Exception as exc:  # pragma: no cover - network issues
                    logger.error(f"API error: {exc}")
                    self.db_manager.update_file_status(file_row["id"], "error", str(exc))
                    error_count += 1
                    continue

                if api_result.get("status") == "completed":
                    analysis_result = api_result.get("result", {})
                    task_id = api_result.get("task_id")
                    if self.enable_cache:
                        self.cache_manager.store_result(
                            file_row["fast_hash"], prompt_hash, analysis_result
                        )
                else:
                    logger.error(f"API failed: {api_result}")
                    self.db_manager.update_file_status(file_row["id"], "error", "api_failed")
                    error_count += 1
                    continue

            self.db_manager.store_analysis_result(file_row["id"], task_id, analysis_result)
            self.db_manager.update_file_status(file_row["id"], "completed")

        final_stats = self.db_manager.get_processing_stats()
        cache_stats = self.cache_manager.get_stats() if self.enable_cache else {"hit_rate": 0.0}
        processing_time = time.perf_counter() - start

        return {
            "status": "completed",
            "files_processed": final_stats.get("completed", 0),
            "cache_hit_rate": cache_stats.get("hit_rate", 0.0),
            "total_time": processing_time,
            "errors": error_count,
        }

    def analyze_single_file(self, file_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Analyse un fichier unique avec cache intelligent."""

        start = time.perf_counter()
        prompt = self.prompt_manager.build_analysis_prompt(file_metadata, "comprehensive")
        prompt_hash = hashlib.sha256(prompt.encode()).hexdigest()

        cached = None
        if self.enable_cache:
            cached = self.cache_manager.get_cached_result(
                file_metadata.get("fast_hash"), prompt_hash
            )
        if cached:
            return {
                "status": "cached",
                "result": cached,
                "processing_time": time.perf_counter() - start,
            }

        try:
            api_result = self.api_client.analyze_file(file_metadata["path"], prompt)
        except Exception as exc:  # pragma: no cover - network issue
            logger.error("API error: %s", exc)
            return {"status": "failed", "error": str(exc)}

        if api_result.get("status") != "completed":
            return {"status": "failed", "error": "api_failed"}

        result = api_result.get("result", {})
        if self.enable_cache:
            self.cache_manager.store_result(
                file_metadata.get("fast_hash"), prompt_hash, result
            )
        return {
            "status": "completed",
            "result": result,
            "task_id": api_result.get("task_id"),
            "processing_time": time.perf_counter() - start,
        }


def main() -> None:
    parser = argparse.ArgumentParser(description="Content Analyzer Brique 2")
    parser.add_argument("--input", required=True, help="Fichier CSV SMBeagle")
    parser.add_argument("--output", required=True, help="Base SQLite sortie")
    parser.add_argument("--config", default="content_analyzer/config/analyzer_config.yaml")
    parser.add_argument("--enable-cache", action="store_true")
    parser.add_argument("--max-files", type=int, default=1000)
    parser.add_argument("--priority-threshold", type=int, default=0)
    args = parser.parse_args()

    analyzer = ContentAnalyzer(Path(args.config))
    analyzer.enable_cache = args.enable_cache
    analyzer.max_files = args.max_files
    analyzer.priority_threshold = args.priority_threshold

    result = analyzer.analyze_batch(Path(args.input), Path(args.output))

    print(f"✅ Processed {result['files_processed']} files")
    print(f"📊 Cache hit rate: {result['cache_hit_rate']}%")


if __name__ == "__main__":
    main()
