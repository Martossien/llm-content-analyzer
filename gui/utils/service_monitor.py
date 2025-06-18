from __future__ import annotations
import sqlite3
from pathlib import Path
import yaml
from typing import Dict

from content_analyzer.modules.api_client import APIClient
from content_analyzer.modules.cache_manager import CacheManager


class ServiceMonitor:
    """Check status of API, cache and database."""

    def __init__(self, config_path: Path):
        self.config_path = Path(config_path)

    def _load_config(self) -> Dict:
        with open(self.config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def check_api_status(self) -> bool:
        cfg = self._load_config()
        client = APIClient(cfg)
        return client.health_check()

    def check_cache_status(self) -> Dict[str, float]:
        cache_db = Path("analysis_results_cache.db")
        if not cache_db.exists():
            return {"hit_rate": 0.0}
        cache = CacheManager(cache_db)
        return cache.get_stats()

    def check_database_status(self) -> Dict[str, float]:
        db_path = Path("analysis_results.db")
        try:
            conn = sqlite3.connect(db_path)
            conn.execute("SELECT 1")
            conn.close()
            size = db_path.stat().st_size / (1024 * 1024)
            return {"accessible": True, "size_mb": size}
        except Exception:
            return {"accessible": False, "size_mb": 0.0}
