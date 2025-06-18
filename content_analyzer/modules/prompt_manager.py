import json
from pathlib import Path
from typing import Any, Dict, List, Tuple

import jinja2
import yaml


class PromptManager:
    """Gestionnaire de templates Jinja2 pour la génération de prompts."""

    def __init__(self, config_path: Path) -> None:
        with open(config_path, "r", encoding="utf-8") as f:
            full_cfg = yaml.safe_load(f)
        self.cfg = full_cfg
        self.env = jinja2.Environment(autoescape=False)

    def build_analysis_prompt(
        self, file_metadata: Dict[str, Any], analysis_type: str = "comprehensive"
    ) -> str:
        tpl_cfg = self.cfg["templates"].get(analysis_type)
        if not tpl_cfg:
            raise ValueError(f"Unknown template: {analysis_type}")
        user_tpl = self.env.from_string(tpl_cfg["user_template"])
        rendered = user_tpl.render(**file_metadata)
        system_prompt = tpl_cfg.get("system_prompt", "")
        return f"{system_prompt}\n{rendered}"

    def get_available_templates(self) -> List[str]:
        return list(self.cfg.get("templates", {}).keys())

    def validate_template(self, template_name: str) -> Tuple[bool, str]:
        tpl_cfg = self.cfg["templates"].get(template_name)
        if not tpl_cfg:
            return False, "template_not_found"
        try:
            self.env.parse(tpl_cfg["user_template"])
            return True, "ok"
        except jinja2.TemplateSyntaxError as exc:
            return False, str(exc)
