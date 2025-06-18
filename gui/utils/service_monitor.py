from __future__ import annotations
import sqlite3
import time
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

    # ------------------------------------------------------------------
    # Extended helpers
    # ------------------------------------------------------------------
    def _get_api_url(self) -> str:
        cfg = self._load_config()
        return str(cfg.get("api_config", {}).get("url", ""))

    def _measure_api_response_time(self) -> float:
        try:
            cfg = self._load_config()
            client = APIClient(cfg)
            start = time.time()
            ok = client.health_check()
            if not ok:
                return -1.0
            return (time.time() - start) * 1000
        except Exception:
            return -1.0

    def _get_cache_size(self) -> float:
        cache_db = Path("analysis_results_cache.db")
        if not cache_db.exists():
            return 0.0
        return cache_db.stat().st_size / (1024 * 1024)

    def _count_database_tables(self) -> int:
        db_path = Path("analysis_results.db")
        if not db_path.exists():
            return 0
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            count = cur.execute(
                "SELECT COUNT(name) FROM sqlite_master WHERE type='table'"
            ).fetchone()[0]
            conn.close()
            return int(count)
        except Exception:
            return 0

    def get_detailed_status(self) -> Dict:
        """Return a detailed status report of services."""
        db_status = self.check_database_status()
        return {
            "api": {
                "status": self.check_api_status(),
                "url": self._get_api_url(),
                "response_time": self._measure_api_response_time(),
            },
            "cache": {
                "status": True,
                "stats": self.check_cache_status(),
                "size_mb": self._get_cache_size(),
            },
            "database": {
                "status": db_status.get("accessible", False),
                "path": "analysis_results.db",
                "size_mb": db_status.get("size_mb", 0.0),
                "tables": self._count_database_tables(),
            },
        }
