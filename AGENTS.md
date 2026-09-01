# AGENTS.md — Doc-IA analyzer (`docia`)

> Guide de reprise pour tout agent (humain ou IA) qui travaille sur ce projet.
> **Le lire en premier.** Conception détaillée : `docs/DESIGN_V3.md` ; réglages :
> `docs/REGLAGES.md` ; côté utilisateur : `docs/GUIDE_UTILISATEUR.md`.

## 1. Le projet en une phrase

Sur un **poste Windows** ayant accès aux partages, `docia` importe un scan SMBeagle
(ou le pilote), tient une base SQLite par campagne, extrait le texte **localement**
avec DocFuse (OCR compris), envoie des blocs de texte à une LLM locale (vLLM ou
open-webui), reçoit un JSON multi-fichiers **garanti par schéma**, persiste une
analyse par fichier (sécurité C0–C3, RGPD, finance, juridique, conservation) et
restitue : GUI, rapport HTML, Excel, CSV Power BI. Rien ne quitte l'organisme.

## 2. Non-négociables

- **Jamais de perte silencieuse** : chaque fichier a un statut et une raison ; un run
  qui laisse des fichiers en plan sort en erreur, jamais « done » avec le code 0.
- **Jamais de troncature** d'un gros fichier : contexte natif du modèle, puis segments
  complets agrégés (`llm/aggregate.py`). Le comptage exact `/tokenize` et la seconde
  passe (`pipeline._Run._second_pass`) corrigent l'estimation.
- **Mémoire bornée quelle que soit la taille de la campagne** (934 000 fichiers
  mesurés) : curseurs SQLite en flux, `executemany` par lots, filtres/tri/limite en
  SQL, jamais `list(...)` sur une table entière. Profiler avec `/usr/bin/time -v`
  sur une campagne de cet ordre avant de dire « rapide ».
- **Reprise** : une analyse est liée à `(file_id, content_version, prompt_hash,
  model)` ; relancer `run` ne renvoie que ce qui manque.
- **Une seule règle « l'analyse qui fait foi »** : `docia.db.sql.latest_analysis_sql`.
  Ne jamais la recopier (elle a divergé une fois, en cinq exemplaires).
- **Le prompt est une variable** (profils en base, `docia prompt`) ; le thinking est
  activé par défaut ; `reasoning_effort=low`.
- **Aucune JSON « réparation »** : le schéma est imposé au serveur, le client valide et
  rejette (`llm/parse.py`).
- Dépendances minimales : `docfuse`, `httpx`, stdlib. GUI = extra `[gui]`.

## 3. Architecture

```
src/docia/
├── cli.py, cli_tools.py   argparse (init | scan | ingest | plan | run | status | export |
│                          report | prompt | review | backup | restore | reanalyze | doctor…)
├── config.py              docia.toml (tomllib) → Config ; `update_toml` réécrit sans
│                          perdre les commentaires
├── models.py              dataclasses figées (FileRow, BlockSpec, FileAnalysis…)
├── db/                    SQLite (WAL) — database.py (classe Database), schema.py
│                          (SCHEMA_VERSION, migrations une transaction par version,
│                          sauvegarde avant migration), sql.py (fragments, règles)
├── ingest/smbeagle_csv.py CSV 19 colonnes à guillemets sélectifs, import par lots
├── scan.py                pilote SMBeagle.exe (progress JSON, manifeste, codes retour)
├── filter.py              exclusions + score de priorité, en flux
├── blocks/builder.py      DocFuse → blocs .md ≤ N tokens, segments des gros fichiers
├── llm/                   client.py (httpx, retries, comptage exact), schema.py,
│                          parse.py (validation stricte), aggregate.py (segments)
├── pipeline.py            le run : classe `_Run`, une méthode par étape
├── service.py             couche service (campagnes, run avec ETA, réanalyse,
│                          sauvegardes) — à exposer 1:1 en REST (v4)
├── views.py               statistiques SQL (doublons, ancienneté, risque, rétention…)
├── report/                html.py, markdown.py, excel.py (write_only), powerbi.py
├── quick.py, bench.py     analyse rapide d'un dossier ; banc vLLM
└── gui/                   CustomTkinter : app.py, tab_*.py, lazy.py (écrans
                           paresseux), helpers.py (fonctions pures testées)
```

## 4. Vérification — avant tout commit

```bash
scripts/check.sh          # ruff + ruff format + mypy --strict + pytest → « VERDICT: OK »
scripts/e2e_local.sh      # test complet local (scanner → OCR → vLLM → base → rapports)
```

Ne jamais pousser sans `VERDICT: OK`. La CI (2 OS × 3 Python) construit `Docia.exe`
et le prouve par quatre tests de fumée (CLI, extracteurs DocFuse, **OCR réel** sur un
PDF scanné généré, GUI `--smoke`). Arrêter vLLM après chaque mesure.

## 5. Conventions

- Docstrings et commentaires en français, orientés **pourquoi** (le bug qu'on évite,
  le chiffre mesuré). Identifiants en anglais.
- Tout `noqa` porte sa raison (`# noqa: BLE001 — l'affichage n'est jamais critique`).
- Messages de commit narratifs (`fix(db): la dernière analyse se choisit parmi…`).
- Un test par bug corrigé, nommé par le fait qu'il protège.

## 6. Pièges connus

- `Database` ouvre en WAL ; une base sur support protégé se rouvre en lecture seule.
- `bulk_load` retire les index le temps d'un import et pose un marqueur `meta` : une
  seconde connexion **ne doit pas** recréer les index pendant ce temps.
- `docia.toml` peut porter un BOM ; `update_toml` relit ce qu'il écrit et refuse
  d'écrire une configuration infidèle.
- Le `.md` d'un bloc `built` ne doit jamais être effacé (reprise impossible sinon).
- DocFuse est **épinglé par commit** dans `pyproject.toml` : après une livraison
  DocFuse, pousser DocFuse **puis** remonter l'épingle ici, sinon la CI et l'exe
  n'en voient rien.

## 7. En attente

- Retirer `legacy/` (POC 2025, 18 000 lignes) du dépôt — l'historique git le garde.
- v4 : `docia serve` (REST 1:1 sur `service.py`) et GUI web ; chapitre README
  « pré-requis Windows » avec captures prises sur un poste Windows.
