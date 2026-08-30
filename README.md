# Doc-IA analyzer v3 (`docia`)

Classification **RGPD / finance / sécurité / juridique** des fichiers d'un partage réseau par une
LLM locale, à partir d'un scan [SMBeagle](https://github.com/Martossien/smbeagle_enriched) et de
blocs de texte produits par [DocFuse](https://github.com/Martossien/DocFuse).

> Version 3 : réécriture complète du POC (conservé dans `legacy/` pour référence). Conception :
> [`docs/DESIGN_V3.md`](docs/DESIGN_V3.md). Contexte du projet :
> `~/Doc-IA/docs/ANALYSE_2026-08-30.md`.

## Principe

```
CSV SMBeagle ──ingest──▶ SQLite (files) ──plan──▶ fichiers à analyser
                                                    │
                     DocFuse (bibliothèque, sur le poste) : extraction + OCR + blocs ≤ N tokens
                                                    │  block_001.md … (## SOURCE: par fichier)
                          LLM (vLLM direct ou open-webui natif) ── JSON multi-fichiers (schéma imposé)
                                                    │
                                          SQLite (analyses) ──export──▶ CSV / JSON
```

- Le poste Windows fait l'extraction (il a l'accès SMB et le CPU) ; **seul du texte** part vers
  le serveur, jamais les documents.
- Le JSON est garanti par `response_format` (json_schema) — aucune « réparation » côté client.
- **Reprise** : une analyse est liée à la version de contenu du fichier, au prompt et au modèle ;
  relancer `run` ne renvoie que ce qui manque. Un rescan ne réanalyse que les fichiers modifiés.

## Installation

```bash
pip install "docia @ git+https://github.com/Martossien/llm-content-analyzer"   # cœur (CLI)
# développement :
uv venv --python 3.11 .venv && uv pip install --python .venv/bin/python -e /chemin/DocFuse -e ".[dev]"
```

Python ≥ 3.11. Dépendances : `docfuse` ≥ 0.2.0, `httpx`.

## Utilisation

```bash
docia init                          # écrit docia.toml (config commentée)
docia ingest scan.csv               # import du CSV SMBeagle (19 colonnes, guillemets sélectifs)
docia plan                          # exclusions + score de priorité
docia run --limit 500               # blocs DocFuse → LLM → analyses (reprend où il s'était arrêté)
docia status                        # compteurs par statut, blocs, classifications
docia export --format csv -o resultats.csv
docia retry                         # remet les fichiers en erreur à « à analyser »
```

Serveur LLM : voir `~/Doc-IA/bench_vllm/serve_qwen38.sh` (vLLM + Qwen3.8-27B) ou open-webui 0.11
(`transport = "openwebui"`, `base_url = "http://serveur:8080/api"`, clé `sk-`).

## Développement

```bash
.venv/bin/ruff check src tests && .venv/bin/ruff format --check src tests
.venv/bin/mypy --strict src/docia
.venv/bin/python -m pytest
```

Licence Apache 2.0.
