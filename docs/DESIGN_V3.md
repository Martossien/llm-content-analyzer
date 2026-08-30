# Doc-IA analyzer v3 — conception

*30/08/2026 — remplace le POC `legacy/` (18 000 lignes, 59 % de GUI Tkinter). Contexte et décisions amont : `~/Doc-IA/docs/ANALYSE_2026-08-30.md` (D1–D7).*

## 1. Rôle

Sur un **poste Windows** ayant accès aux partages : lire le CSV SMBeagle, tenir une base
SQLite des fichiers, extraire le texte **localement** avec DocFuse (bibliothèque), envoyer des
**blocs texte** de 16–64K tokens à une LLM (vLLM direct ou open-webui natif), recevoir un
**JSON multi-fichiers** garanti par schéma, persister une analyse par fichier, **reprendre** après
interruption sans rien refaire d'inutile. Tout en CLI d'abord ; GUI et exe en 4b.

## 2. Non-négociables

- Un fichier = un `## SOURCE:` entier dans un bloc (DocFuse ne coupe jamais) ; la LLM rend une
  entrée par `file_ref` = valeur exacte de la ligne `## SOURCE:`.
- Aucune « réparation » de JSON : le schéma est imposé au serveur (`response_format`
  `json_schema`), le client **valide** et rejette proprement.
- Jamais de perte silencieuse : chaque fichier a un statut et une raison (exclu, introuvable,
  extraction en erreur, absent de la réponse, hors plafond…).
- Reprise : une analyse est liée à `(file_id, content_version, prompt_hash, model)`. Relancer
  `run` ne renvoie que ce qui manque. Un bloc `sent` sans résultat au redémarrage est renvoyé.
- Rescan : un fichier déjà connu dont `fast_hash`, `size` ou `last_write_time` change prend
  `content_version + 1` et redevient à analyser ; sinon il est inchangé (compteur exact).
- Compatible Windows (chemins UNC, `normcase` pour les clés), zéro dépendance réseau hors LLM,
  dépendances minimales : `docfuse`, `httpx`. Stdlib pour SQLite, TOML, JSON, argparse.

## 3. Architecture

```
src/docia/
├── cli.py            argparse : init | ingest | plan | run | status | export | retry
├── config.py         docia.toml (tomllib) → Config (dataclass), validation
├── models.py         dataclasses partagées (FileRow, BlockSpec, FileAnalysis, …)
├── db.py             SQLite (WAL), schéma versionné, requêtes (upsert, sélection, statuts)
├── ingest/
│   └── smbeagle_csv.py   parseur CSV 19 colonnes à guillemets sélectifs (porté du legacy)
├── filter.py         exclusions (extensions, taille, dossiers système) + score de priorité
├── blocks/
│   └── builder.py    DocFuse : run_analysis(split_context) → split_by_budget → blocs .md
├── llm/
│   ├── schema.py     JSON Schema de sortie (validé xgrammar) + prompt system + prompt_hash
│   ├── client.py     httpx async, N en vol, retries ; transports vllm | openwebui
│   └── parse.py      validation de la réponse, corrélation file_ref → fichiers du bloc
├── pipeline.py       run : sélection → blocs → envoi → persistance → statuts
└── prompts/docia_v3.md
```

## 4. Base SQLite

| Table | Rôle |
|---|---|
| `meta` | `schema_version` |
| `scans` | un import CSV : compteurs nouveaux / modifiés / inchangés |
| `files` | un chemin = une ligne (`path` UNIQUE, clé `normcase`) ; métadonnées SMBeagle ; `content_version` ; `status` ∈ pending, excluded, queued, done, error ; `exclusion_reason`, `priority_score` |
| `runs` | un `run` (config figée en JSON, modèle, prompt_hash) |
| `blocks` | une partie DocFuse : chemin du `.md`, tokens, statut built / sent / done / error, tentatives, usage, latence |
| `block_files` | fichiers d'un bloc : `file_ref` exact, statut d'extraction, `oversized` |
| `analyses` | résultat par fichier : 4 domaines à plat + JSON brut ; UNIQUE (`file_id`, `content_version`, `prompt_hash`, `model`) |

Migrations : liste ordonnée de fonctions `migrate_<n>` appliquées à `init`/ouverture ; jamais
d'`ALTER` implicite ailleurs.

## 5. Blocs (DocFuse)

1. Sélection des fichiers `pending` (ordre : `priority_score` desc, puis chemin) par lots de
   `batch_files` (défaut 200) existants sur disque (sinon → `error: introuvable`).
2. `run_analysis(input_path=[…], split_context=True, context_limit=block_tokens,
   tokenizer_engine=…, recursive=False)` puis `split_by_budget(result)`.
3. Une partie = un bloc : `write_markdown_corpus(result, path, margin, part=…)` dans
   `work_dir/run_<id>/block_<n>.md` ; `block_files` reçoit `file_ref = result.files[i].relative_path`.
4. Fichiers non extraits (`result.ignored`, statut `error`) → `files.status = error` + raison ;
   partie `oversized` → bloc envoyé quand même (le plafond serveur peut être plus grand), sinon
   `error: hors plafond`.

## 6. LLM

- Requête : system = `prompts/docia_v3.md` ; user = le bloc `.md` ; `temperature 0` ;
  `max_tokens = 400 × nb_fichiers + 500` (plafonné) ; `response_format = json_schema`.
- Transport `vllm` : `POST {base_url}/chat/completions` (base `http://host:8000/v1`).
- Transport `openwebui` : `POST {base_url}/chat/completions` (base `http://host:8080/api`), le
  bloc passe en `files:[{"type":"text","context":"full","name":"block_001.md","file":{"data":{"content":…}}}]`,
  clé API `Authorization: Bearer sk-…` ; la clé `sources` de la réponse est ignorée.
- Concurrence : `asyncio.Semaphore(max_in_flight)` ; timeouts (connect 10 s, read
  `timeout_s`, défaut 900) ; retries 3 avec backoff sur 5xx / 429 / timeouts, jamais sur 4xx.
- `prompt_hash = sha256(prompt + schéma + modèle)[:16]`.

## 7. Validation de la réponse

`parse.py` : `json.loads` → structure (`files` liste, clés requises, énumérations, entiers
0–100) → corrélation `file_ref` : exact, sinon nom de base unique, sinon `error: file_ref inconnu`.
Fichiers du bloc absents de la réponse → `error: absent de la réponse` (retentés une fois dans un
bloc plus petit, puis `error`). Entrées en trop → journalisées, ignorées.

## 8. Leçons du banc du 30/08 (intégrées)

- Pas de `disable_any_whitespace` côté serveur ; `amounts[].value` en `number` ; échelle 0–100
  dite dans le prompt ; `maxItems` sur les tableaux ; schéma sans `pattern`+`maxLength`.
- Blocs de 16–64K : au-delà, MTP et prefix cache se dégradent, et un bloc trop gros = trop de
  fichiers perdus en cas d'échec.

## 9. Étapes

- 4a (ce chantier) : cœur + CLI + tests (serveur OpenAI factice) + test d'intégration contre
  vLLM/open-webui s'ils tournent.
- 4b : GUI CustomTkinter (calquée sur DocFuse), PyInstaller `Docia.spec`, banc réel sur un partage.
