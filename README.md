# Doc-IA analyzer (`docia`)

Audit du contenu d'un partage de fichiers par une **LLM locale** (rien ne quitte l'organisme).
Pour chaque fichier : **sensibilité (C0–C3)**, **données personnelles (RGPD)**, **finance**,
**juridique**, **durée de conservation** — plus l'hygiène du partage : **doublons (espace
récupérable)**, **fichiers non accédés depuis N ans**, **candidats au nettoyage**. Restitution :
interface graphique, rapport HTML pour la direction, classeur Excel, CSV pour Power BI Report Server.

📖 **[Guide de l'utilisateur illustré](docs/GUIDE_UTILISATEUR.md)** — installation, les 4 étapes,
lecture des résultats, vérification humaine, FAQ.

![Accueil de Doc-IA](docs/images/01_accueil.png)

## Installation sur un poste Windows (utilisateur)

1. Copier dans un même dossier : **`Docia.exe`** (artefact `Docia-windows-x64` de la
   [CI](../../actions)) et **`SMBeagle.exe`** (release
   [smbeagle_enriched v4.2.0](https://github.com/Martossien/smbeagle_enriched/releases/tag/v4.2.0)).
2. Lancer `Docia.exe` (double-clic = interface ; en console, `Docia.exe doctor` vérifie que tout
   est en place : DocFuse, pdfium, **OCR Tesseract embarqué** — rien d'autre à installer, ni .NET
   ni Tesseract).
3. Mode administrateur (interrupteur en haut à droite) → *Serveur & performances* : adresse du
   serveur LLM, **Tester la connexion**, Enregistrer.

Le mode de scan standard est le **scan local Windows** : un lecteur réseau mappé (`P:\`) ou un
dossier (`\\serveur\partage\Finance`), avec le compte de la session (droit de lecture suffisant).

## Principe

```
SMBeagle.exe (scan local Win32, piloté par docia) ──▶ CSV 19 colonnes + manifeste
        │ docia scan : import + préparation (exclusions, priorité)
        ▼
SQLite « campagne » (un fichier .sqlite par périmètre audité)
        │ docia run (reprenable ; rescan = seulement les fichiers modifiés)
        ▼
DocFuse (bibliothèque, sur le poste) : extraction + OCR + blocs ≤ N tokens
        │ seul du TEXTE part vers le serveur, jamais les documents
        ▼
LLM locale (vLLM direct, ou open-webui natif) ── JSON garanti par json_schema
        │ comptage exact /tokenize avant envoi ; gros fichiers en segments complets agrégés
        ▼
SQLite (analyses, revues humaines) ──▶ GUI · rapport HTML · Excel · Power BI · CSV/JSON
```

- **Reprise partout** : une analyse est liée au contenu du fichier, au prompt et au modèle ;
  relancer ne refait que ce qui manque. Les doublons exacts héritent de l'analyse de l'original.
- **Jamais tronqué, jamais « trop long »** : les très gros fichiers sont découpés en segments
  complets puis agrégés ; chaque bloc est compté exactement par le serveur avant envoi et
  re-découpé au besoin dans le même run.
- **Dates d'accès honnêtes** : l'ancienneté (« non accédé depuis N ans ») s'appuie sur la première
  date observée — l'audit lui-même (hachage, OCR, extraction) ne « rajeunit » pas les fichiers.
- **Raisonnement (thinking)** activé par défaut, effort `medium`, budget **imposé** (6 000 tokens)
  — réglages mesurés sur banc, modifiables dans l'onglet Serveur.

## Ligne de commande

```bash
docia init                          # écrit docia.toml (config commentée)
docia doctor                        # état du poste : DocFuse, pdfium, OCR, scanner, serveur
docia scan --local-path "P:\\"      # étape 0 : scanner → import → préparation (SMBeagle piloté)
docia ingest scan.csv               # ou : import d'un CSV SMBeagle déjà produit (19 colonnes)
docia run --limit 500               # blocs DocFuse → LLM → analyses (reprend où il s'était arrêté)
docia status                        # compteurs par statut, blocs, classifications
docia report --format html          # rapport autonome (hygiène + risque + conservation)
docia export --format csv|json|xlsx|powerbi -o SORTIE
docia quick DOSSIER                 # analyse immédiate d'un fichier/dossier, sans scan
docia prompt list|show|save|use     # le prompt est une variable : profils nommés en base
docia review ID --status validated  # vérification humaine (validé / corrigé + commentaire)
docia reanalyze --scope errors|all|filter [--where security=C3]
docia backup [--out DIR]            # sauvegarde horodatée (<base>.backups/, rotation 10)
docia restore SAUVEGARDE.sqlite     # restauration (l'actuelle est d'abord sauvegardée)
docia campaigns                     # campagnes récentes et leur avancement
docia retry                         # remet les fichiers en erreur à « à analyser »
docia bench                         # vitesse de la LLM : prefill/decode, raisonnement, fichiers/heure
docia gui                           # interface (aussi : double-clic sur Docia.exe)
```

Toutes ces opérations passent par la couche **`docia.service`** (campagnes, événements de
progression avec temps restant, réanalyse, sauvegardes) — la même que l'interface et, demain,
le serveur web de pilotage à distance (v4).

## Serveur LLM

Référence : vLLM + Qwen3.8-27B, contexte natif 262 144, structured outputs xgrammar,
`--reasoning-parser qwen3` (budget de raisonnement imposé par requête) — script
`~/Doc-IA/bench_vllm/serve_qwen38.sh`. Alternative : open-webui ≥ 0.11 en API native
(`transport = "openwebui"`, `base_url = "http://serveur:8080/api"`, clé `sk-`) — dans ce mode le
budget de raisonnement n'est pas relayé, seul le renvoi à budget doublé protège.

## Qualité

- `scripts/check.sh` : ruff + mypy strict + pytest (234 tests), **VERDICT** explicite — rien n'est
  poussé sans `VERDICT: OK`.
- CI GitHub (Ubuntu + Windows) : lint/tests 3.11–3.13, build de `Docia.exe` (PyInstaller,
  bibliothèques des extracteurs et **Tesseract embarqués**), puis l'exe est **exécuté** sur le
  runner : `doctor`, extraction réelle de 11 formats bureautiques, **OCR d'un PDF scanné généré**
  (Tesseract retiré du PATH), ouverture/fermeture de l'interface.
- `scripts/e2e_local.sh` : test complet sur la machine de développement (scanner → OCR → vLLM →
  base → rapports, 28 vérifications automatiques — 28/28 au 30/08).

Référence de tous les réglages : [`docs/REGLAGES.md`](docs/REGLAGES.md).
Conception détaillée : [`docs/DESIGN_V3.md`](docs/DESIGN_V3.md) (§13 budget de raisonnement,
§14 étape scanner, §15 comptage exact). POC d'origine conservé dans `legacy/`.

## Installation développeur

```bash
uv venv --python 3.11 .venv && uv pip install --python .venv/bin/python -e /chemin/DocFuse -e ".[dev,gui]"
bash scripts/check.sh        # doit finir par VERDICT: OK
```

Python ≥ 3.11. Dépendances cœur : `docfuse` ≥ 0.2.0, `httpx`. Licence Apache 2.0.
