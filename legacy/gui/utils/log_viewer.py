from pathlib import Path
from typing import List


class LogViewer:
    def __init__(self, log_path: Path) -> None:
        self.log_path = log_path

    def tail_logs(self, lines: int = 50) -> List[str]:
        if not self.log_path.exists():
            return []
        with open(self.log_path, "r", encoding="utf-8", errors="ignore") as f:
            data = f.readlines()
        return data[-lines:]
