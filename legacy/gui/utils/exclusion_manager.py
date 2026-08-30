from __future__ import annotations
from pathlib import Path
import yaml
from typing import List


class ExclusionManager:
    def __init__(self, config_path: Path):
        self.config_path = Path(config_path)

    def _load_config(self) -> dict:
        with open(self.config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def _save_config(self, cfg: dict) -> None:
        with open(self.config_path, "w", encoding="utf-8") as f:
            yaml.safe_dump(cfg, f, default_flow_style=False)

    def add_extension(self, ext: str) -> None:
        if not ext.startswith("."):
            ext = "." + ext
        cfg = self._load_config()
        exts: List[str] = cfg["exclusions"]["extensions"].get("blocked", [])
        if ext not in exts:
            exts.append(ext)
            cfg["exclusions"]["extensions"]["blocked"] = exts
            self._save_config(cfg)

    def remove_extension(self, ext: str) -> None:
        cfg = self._load_config()
        exts: List[str] = cfg["exclusions"]["extensions"].get("blocked", [])
        if ext in exts:
            exts.remove(ext)
            cfg["exclusions"]["extensions"]["blocked"] = exts
            self._save_config(cfg)

    def toggle_system_files(self, skip: bool) -> None:
        cfg = self._load_config()
        cfg["exclusions"]["file_attributes"]["skip_system"] = bool(skip)
        self._save_config(cfg)
