# Doc-IA analyzer v3 (`docia`)

Classification **RGPD / finance / sécurité / juridique** des fichiers d'un partage réseau par une
LLM locale, à partir d'un scan [SMBeagle](https://github.com/Martossien/smbeagle_enriched) et de
blocs de texte produits par [DocFuse](https://github.com/Martossien/DocFuse).

> Version 3 : réécriture complète du POC (conservé dans `legacy/` pour référence). Conception :
> [`docs/DESIGN_V3.md`](docs/DESIGN_V3.md). Contexte du projet :
> `~/Doc-IA/docs/ANALYSE_2026-08-30.md`.

**Guide de l'utilisateur (illustré)** : [`docs/GUIDE_UTILISATEUR.md`](docs/GUIDE_UTILISATEUR.md).

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
docia scan --local-path D:\\partage   # étape 0 : lance SMBeagle_enriched (à côté de Docia.exe), importe et prépare
docia ingest scan.csv               # ou : import d'un CSV SMBeagle déjà produit (19 colonnes)
docia plan                          # exclusions + score de priorité
docia run --limit 500               # blocs DocFuse → LLM → analyses (reprend où il s'était arrêté)
docia status                        # compteurs par statut, blocs, classifications
docia export --format csv -o resultats.csv
docia retry                         # remet les fichiers en erreur à « à analyser »
docia quick DOSSIER                 # analyse immédiate d'un fichier/dossier, sans CSV
docia bench                         # vitesse de la LLM : prefill/decode, JSON valides, fichiers/heure
docia prompt list|show|save|use     # le prompt est une variable : profils nommés en base
docia review ID --status validated  # vérification humaine (validé / corrigé + commentaire)
docia report --format html          # rapport autonome : hygiène (doublons, ancienneté…) et risque
docia export --format xlsx|powerbi  # classeur Excel ; dossier CSV au schéma stable pour Power BI
docia backup [--out DIR]            # sauvegarde horodatée de la base (<base>.backups/, rotation 10)
docia restore SAUVEGARDE.sqlite     # restaure par-dessus la base (l'actuelle est d'abord sauvegardée)
docia reanalyze --scope errors|all|filter [--where security=C3]  # relancer : erreurs, tout, ou une sélection
docia campaigns                     # campagnes récentes et leur avancement
docia gui                           # interface : Accueil (scanner ou CSV, 4 étapes, relance) / Résultats & vérification / Statistiques / Rapports ; mode admin : Prompt, Serveur & performances
```

Cinq domaines par fichier : sécurité (C0–C3), RGPD, finance, juridique, **conservation**
(durée, fondement : valeur probante, légal, fiscal, RH, contractuel). Très gros fichiers : découpés
en segments complets puis agrégés (jamais tronqués). Doublons exacts : héritent de l'analyse de
l'original. Raisonnement (thinking) activé par défaut.

Couche service (`docia.service`) : campagnes, run avec événements de progression (durée, débit,
reste à faire), réanalyse ciblée, sauvegarde/restauration. La CLI, l'interface et le futur serveur
web de pilotage à distance (v4) passent tous par cette couche.

Serveur LLM : voir `~/Doc-IA/bench_vllm/serve_qwen38.sh` (vLLM + Qwen3.8-27B) ou open-webui 0.11
(`transport = "openwebui"`, `base_url = "http://serveur:8080/api"`, clé `sk-`).

## OCR embarqué

`Docia.exe` embarque **Tesseract** (fra + eng, recette `DocFuse-OCR.spec`) : les PDF scannés,
courriers et factures numérisés sont lus par OCR automatiquement (pages sans texte natif),
sans rien installer sur le poste. Le build exige `TESSERACT_HOME` (la CI l'installe) ; la CI
prouve l'OCR de l'exe sur un PDF image généré (`scripts/make_scanned_pdf.py`), Tesseract retiré
du PATH. En développement (Linux/Windows), DocFuse utilise le `tesseract` du système s'il existe.

## Scanner SMBeagle_enriched (étape 0)

`SMBeagle.exe` ([smbeagle_enriched](https://github.com/Martossien/smbeagle_enriched)) est un programme
séparé : docia le pilote en sous-processus (`docia scan`, onglet Accueil → « Scanner maintenant »).
Placer l'exécutable **à côté de `Docia.exe`** (ou `scan.smbeagle_path`). Options envoyées :
`--sizefile --access-time --fileattributes --ownerfile --fasthash --file-signature`
`--preserve-access-time --progress-json --manifest`. Le CSV et le manifeste sont rangés dans
`<base>.scans/`, le scan importé garde le manifeste (`scans.kind='scan'`).

**Rescan** : seuls les fichiers nouveaux ou modifiés (empreinte, taille, date) repartent en analyse ;
la date de dernier accès retenue pour « non accédé depuis N ans » est la **première observée**
(`access_time_first`), pour que le hachage et l'extraction de l'audit ne rajeunissent pas les fichiers.

## Développement

```bash
.venv/bin/ruff check src tests && .venv/bin/ruff format --check src tests
.venv/bin/mypy --strict src/docia
.venv/bin/python -m pytest
```

Licence Apache 2.0.
