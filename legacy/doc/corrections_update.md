# GUI Corrections Summary

This document lists the main fixes and questions applied by Codex.

## Changes
- Added verification of the `fichiers` table in `refresh_results_table`.
- Implemented token field in API configuration panel with save/load support.
- Updated `analyze_single_file` to build structured prompts using `PromptManager`.
- Added formatted display of analysis results instead of raw JSON.
- Updated comprehensive prompt template in `analyzer_config.yaml`.

## Questions
- Should additional templates be added for other analysis modes?
- Are there plans for more granular caching policies?
