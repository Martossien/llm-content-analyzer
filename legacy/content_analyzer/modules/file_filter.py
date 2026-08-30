import fnmatch
import yaml
from pathlib import Path
from typing import Any, Dict, List, Tuple


class FileFilter:
    def __init__(self, config_path: Path) -> None:
        """Charge la configuration unifiée YAML."""
        with open(config_path, "r", encoding="utf-8") as f:
            full_cfg = yaml.safe_load(f)
        self.cfg = full_cfg

    def should_process_file(self, file_row: Dict[str, Any]) -> Tuple[bool, str]:
        ext = str(file_row.get("extension", "")).lower()
        if ext and not ext.startswith("."):
            ext = "." + ext
        size = int(file_row.get("file_size", 0))
        path = file_row.get("path", "")
        attrs = file_row.get("file_attributes", "")
        attr_tokens = [a.strip().lower() for a in attrs.replace(",", " ").split()]
        rules = self.cfg["exclusions"]

        if ext in rules["extensions"].get("blocked", []):
            return False, "blocked_extension"
        if size < rules["file_size"].get("min_bytes", 0):
            return False, "too_small"
        if size > rules["file_size"].get("max_bytes", 1 << 30):
            return False, "too_large"
        if rules["file_attributes"].get("skip_system") and "system" in attr_tokens:
            return False, "system_file"
        if rules["file_attributes"].get("skip_hidden") and "hidden" in attr_tokens:
            return False, "hidden_file"
        for pattern in rules["paths"].get("excluded_patterns", []):
            if fnmatch.fnmatch(path, pattern):
                return False, "excluded_path"
        return True, "ok"

    def calculate_priority_score(self, file_row: Dict[str, Any]) -> int:
        ext = str(file_row.get("extension", "")).lower()
        if ext and not ext.startswith("."):
            ext = "." + ext
        size = int(file_row.get("file_size", 0))
        age = file_row.get("last_modified", "0")
        attrs = file_row.get("file_attributes", "")
        attr_tokens = [a.strip().lower() for a in attrs.replace(",", " ").split()]
        score_cfg = self.cfg["scoring"]
        weight_size = score_cfg.get("size_weight", 30)
        weight_type = score_cfg.get("type_weight", 40)
        weight_age = score_cfg.get("age_weight", 20)
        weight_special = score_cfg.get("special_weight", 10)

        max_size = self.cfg["exclusions"]["file_size"].get("max_bytes", 1)
        size_ratio = min(size / max_size, 1.0)
        size_score = size_ratio * weight_size

        type_score = 0
        if ext in self.cfg["exclusions"]["extensions"].get("high_priority", []):
            type_score = weight_type
        elif ext in self.cfg["exclusions"]["extensions"].get("low_priority", []):
            type_score = weight_type * 0.3
        else:
            type_score = weight_type * 0.6

        age_score = weight_age  # simplification

        special = 0
        if "hidden" in attr_tokens:
            special += weight_special / 2
        if "system" in attr_tokens:
            special += weight_special / 2

        total = size_score + type_score + age_score + special
        return int(min(total, 100))

    def get_special_flags(self, file_row: Dict[str, Any]) -> List[str]:
        flags: List[str] = []
        attrs = file_row.get("file_attributes", "")
        attr_tokens = [a.strip().lower() for a in attrs.replace(",", " ").split()]
        if "hidden" in attr_tokens:
            flags.append("hidden_file")
        if "system" in attr_tokens:
            flags.append("system_file")
        if file_row.get("file_signature") and file_row.get("extension"):
            if (
                not file_row["file_signature"]
                .lower()
                .endswith(file_row["extension"].lower())
            ):
                flags.append("signature_mismatch")
        return flags
