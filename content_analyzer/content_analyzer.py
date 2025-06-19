#!/usr/bin/env python3
"""
Content Analyzer - Orchestrateur principal Brique 2
Architecture modulaire avec stack minimal validé
"""

import sys
import logging
import time
import json
import argparse
from pathlib import Path
from typing import Optional, Dict, Any

import yaml

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    __package__ = "content_analyzer"

from content_analyzer.modules.csv_parser import CSVParser
from content_analyzer.modules.file_filter import FileFilter
from content_analyzer.modules.cache_manager import CacheManager
from content_analyzer.modules.api_client import APIClient
from content_analyzer.modules.db_manager import DBManager
from content_analyzer.modules.prompt_manager import PromptManager

# Configuration logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ContentAnalyzer:
    """Orchestrateur principal pour l'analyse de contenu LLM"""
    
    def __init__(self, config_path: Optional[Path] = None) -> None:
        """Initialise l'analyseur de contenu et charge la configuration."""

        self.config_path = config_path or Path("content_analyzer/config/analyzer_config.yaml")
        with open(self.config_path, "r", encoding="utf-8") as f:
            self.config = yaml.safe_load(f)

        logger.info("Initialisation Content Analyzer V2.3")
        logger.info("Stack: tenacity + circuitbreaker + bibliothèques standard")

        # Attributs et modules principaux
        self.enable_cache: bool = True

        self.csv_parser = CSVParser(self.config_path)
        self.file_filter = FileFilter(self.config_path)
        cache_db = Path("analysis_results_cache.db")
        self.cache_manager = CacheManager(cache_db)
        self.api_client = APIClient(self.config)
        self.db_manager = DBManager(Path("analysis_results.db"))
        self.prompt_manager = PromptManager(self.config_path)

    # ------------------------------------------------------------------
    # Single file analysis
    # ------------------------------------------------------------------
    def analyze_single_file(self, file_row: Dict[str, Any]) -> Dict[str, Any]:
        """Execute the full analysis workflow for a single file."""
        start = time.perf_counter()
        try:
            should_process, reason = self.file_filter.should_process_file(file_row)
            if not should_process:
                return {"status": "filtered", "reason": reason}

            file_row["priority_score"] = self.file_filter.calculate_priority_score(file_row)

            cache_used = False
            cached = None
            if self.enable_cache:
                cached = self.cache_manager.get_cached_result(
                    file_row.get("fast_hash", ""),
                    "default_prompt_hash",
                )
            if cached:
                return {
                    "status": "cached",
                    "result": cached,
                    "task_id": "",
                    "processing_time_ms": int((time.perf_counter() - start) * 1000),
                    "cache_used": True,
                }

            prompt = self.prompt_manager.build_analysis_prompt(
                file_row, analysis_type="comprehensive"
            )
            api_result = self.api_client.analyze_file(file_row["path"], prompt)

            if self.enable_cache and api_result.get("status") == "completed":
                self.cache_manager.store_result(
                    file_row.get("fast_hash", ""),
                    "default_prompt_hash",
                    api_result.get("result", {}),
                )

            return {
                "status": api_result.get("status", "error"),
                "result": api_result.get("result", {}),
                "task_id": api_result.get("task_id", ""),
                "processing_time_ms": int((time.perf_counter() - start) * 1000),
                "cache_used": False,
            }
        except Exception as exc:  # pragma: no cover - runtime errors
            return {"status": "error", "error": str(exc)}

    # ------------------------------------------------------------------
    # Batch analysis helper
    # ------------------------------------------------------------------
    def analyze_batch(self, csv_file: Path, output_db: Path) -> Dict[str, Any]:
        """Analyse un ensemble de fichiers listés dans un CSV."""
        parse_res = self.csv_parser.parse_csv(csv_file, output_db)
        self.db_manager = DBManager(output_db)
        files = self.db_manager.get_pending_files(limit=100000)

        processed = 0
        start_time = time.perf_counter()
        for row in files:
            res = self.analyze_single_file(row)
            if res.get("status") in {"completed", "cached"}:
                self.db_manager.store_analysis_result(
                    row["id"], res.get("task_id", ""), res.get("result", {})
                )
                self.db_manager.update_file_status(row["id"], "completed")
            else:
                self.db_manager.update_file_status(
                    row["id"], "error", res.get("error")
                )
            processed += 1

        return {
            "status": "completed",
            "files_processed": processed,
            "files_total": parse_res.get("total_files", 0),
            "processing_time": round(time.perf_counter() - start_time, 2),
            "errors": parse_res.get("errors", []),
        }
        
    def analyze(self, input_file: Path, output_file: Path) -> bool:
        """
        Lance l'analyse complète d'un fichier CSV SMBeagle
        
        Args:
            input_file: Fichier CSV SMBeagle enrichi
            output_file: Base SQLite de sortie
            
        Returns:
            True si succès, False sinon
        """
        logger.info(f"Analyse: {input_file} -> {output_file}")
        
        # TODO: Implémenter orchestration modulaire
        # 1. csv_parser.py - Parsing CSV -> SQLite
        # 2. file_filter.py - Filtrage + scoring priorité  
        # 3. cache_manager.py - Cache SQLite intelligent
        # 4. api_client.py - Client HTTP avec protections
        # 5. prompt_manager.py - Templates prompts
        # 6. db_manager.py - Gestion base SQLite
        
        return True

def main(argv: Optional[list[str]] = None) -> None:
    """Point d'entrée principal"""
    parser = argparse.ArgumentParser(description="Content Analyzer Brique 2")
    parser.add_argument("csv", nargs="?", help="SMBeagle CSV file")
    parser.add_argument("output", nargs="?", help="SQLite output file")
    args = parser.parse_args(argv)

    logger.info("=== Content Analyzer Brique 2 V2.3 ===")
    logger.info("Stack minimal: tenacity + circuitbreaker")

    if args.csv and args.output:
        analyzer = ContentAnalyzer()
        analyzer.analyze_batch(Path(args.csv), Path(args.output))
    else:
        analyzer = ContentAnalyzer()
        logger.info("Content Analyzer initialisé - Prêt pour développement Codex")

if __name__ == "__main__":
    main()
