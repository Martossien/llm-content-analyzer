import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Tuple

import jinja2
import yaml

from content_analyzer.utils.prompt_validator import (
    PromptSizeValidator,
    validate_prompt_size,
)

logger = logging.getLogger(__name__)


class PromptManager:
    """Gestionnaire de templates Jinja2 pour la génération de prompts."""

    def __init__(self, config_path: Path) -> None:
        with open(config_path, "r", encoding="utf-8") as f:
            full_cfg = yaml.safe_load(f)
        self.cfg = full_cfg
        self.env = jinja2.Environment(autoescape=False)
        self.config_path = config_path
        self.validator = PromptSizeValidator(config_path)

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

    def save_template(
        self,
        name: str,
        system_prompt: str,
        user_template: str,
        force: bool = False,
    ) -> Dict[str, Any]:
        """Save template with comprehensive validation."""
        try:
            info = validate_prompt_size(system_prompt, user_template, self.validator.max_size)

            status = "saved"
            if info["total_size"] > self.validator.critical_threshold and not force:
                status = "warning"
                logger.warning(
                    "Template %s exceeds critical threshold: %d bytes > %d bytes",
                    name,
                    info["total_size"],
                    self.validator.critical_threshold,
                )
            else:
                self.cfg.setdefault("templates", {})[name] = {
                    "system_prompt": system_prompt,
                    "user_template": user_template,
                }
                with open(self.config_path, "w", encoding="utf-8") as f:
                    yaml.safe_dump(self.cfg, f, default_flow_style=False, indent=2)

                logger.info(
                    "Template %s saved successfully (%d bytes)",
                    name,
                    info["total_size"],
                )

            info["status"] = status
            return info

        except Exception as e:  # pragma: no cover
            logger.error("Error saving template %s: %s", name, e)
            return {
                "status": "error",
                "error": str(e),
                "total_size": 0,
                "within_limit": False,
            }
