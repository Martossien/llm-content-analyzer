# GUI Corrections Notes

This file summarises the fixes applied to solve the four blocking issues in the GUI.

## Main Changes
- added verification of `fichiers` table existence in the results viewer to avoid
  `no such table` errors.
- the API configuration panel now exposes a field to edit the API token and the
  token is saved and loaded from `analyzer_config.yaml`.
- prompts are now generated through `PromptManager` and the YAML templates have
  been updated with a structured JSON instruction.
- analysis results are parsed and formatted for display using the new
  `display_analysis_result` helper.

## Questions
1. Should the `brique2_analyzer` section fully replace `api_config` or stay as a
   compatibility alias?
2. Is additional formatting desired for other windows such as exports?
