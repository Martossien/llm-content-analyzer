# Changelog

Toutes les modifications notables de Doc-IA analyzer (`docia`) sont documentées ici.
Le format suit [Keep a Changelog](https://keepachangelog.com/fr/1.1.0/) ; le projet
adhère au [Semantic Versioning](https://semver.org/spec/v2.0.0.html). L'historique
antérieur à la v3 (POC 2025) est dans git.

## [Unreleased]

### Modifié

- **Le run est une machine à étapes** — `pipeline._execute` (501 lignes, complexité
  cyclomatique 94, huit fermetures imbriquées partageant des ensembles mutables)
  devient la classe `_Run` : une méthode par étape (reprise, contexte servi,
  construction des blocs, envoi, seconde passe, doublons, ménage, clôture), l'état
  sur l'objet. Complexité maximale 13. Événements, journal et rapport inchangés.
- **`db.py` devient le paquet `docia.db`** — `database.py` (classe `Database`),
  `schema.py` (versions, migrations, index attendus), `sql.py` (fragments et règles
  SQL partagées). `from docia.db import …` fonctionne comme avant. Les deux copies
  divergentes de `split_sql_statements` sont réduites à une (vérifiée identique sur
  les sept migrations) ; les helpers morts (`_stamp`, `_free_path`,
  `_idempotent_create_index`) sont retirés.
- **`views.py` devient le paquet `docia.views`** — `_common` (socle), `axes`,
  `hygiene`, `risk`, `retention`, `review`, `overview` ; la façade réexporte
  tout, `views.overview(db)` ne change pas.
- **`scan.run_scan`** (153 lignes, complexité 31) découpé : lancement, suivi de
  la sortie (`_follow_scanner`), manifeste, contrôle du résultat.
- **`report/markdown.render_markdown`** (322 lignes) : une fonction par section,
  comme le rendu HTML. **`report/excel.write_workbook`** (298 lignes) : un onglet =
  une fonction `_sheet_*`. **`ingest.import_csv`** : compteurs dans `_ImportTally`,
  écriture d'un lot et avertissements de fin extraits.
- **`Database` assemblée par mixins** — `db/core.py` (connexion, pragmas,
  transaction, migrations, chargement en masse) et une opération par table :
  `files.py` (scans, fichiers, plan), `blocks.py` (runs, blocs), `analyses.py`,
  `prompts.py` (prompts, revues), `stats.py`. `database.py` ne fait plus que les
  assembler ; l'API `db.upsert_files(...)` ne change pas. 1 775 lignes → 6 modules
  de 50 à 580 lignes.
- **`service.py` devient le paquet `docia.service`** — `_common` (erreurs,
  résultats, constantes), `campaigns` (état, récentes), `backups`, `ingest`
  (import, scan, plan), `runs` (run, réanalyse, revue) ; façade qui réexporte tout.
- **Le journal sort de `cli.py`** → `docia/journal.py` (console, `docia.log`,
  rotation, garde-fou des pannes attendues) ; `cli.py` passe de 1 041 à ~800 lignes
  et ne fait plus que dispatcher.
- Derniers identifiants français (`suite_d_un_champ_ouvert`, `tronque`, `noms`…)
  passés en anglais ; les docstrings restent en français.
- README : la mise en garde sur smbeagle v4.2.0 quitte l'étape 1 de l'installation
  pour une section « Compatibilité du scanner ».
- **DocFuse épinglé sur un commit** dans `pyproject.toml` (build reproductible :
  un push sur `DocFuse@main` ne peut plus casser la CI ni l'exe). Remonter
  l'épingle fait partie de toute livraison DocFuse (voir `AGENTS.md`).

### Corrigé

- **Sous-répertoires non lus par le scanner annoncés** — smbeagle_enriched (≥ main du
  02/09) compte les dossiers fermés par ACL et les jonctions ignorées dans le
  manifeste (`counts.dirs_unreadable`, `unreadable_directories`) ; `docia scan` les
  relaie en une ligne (journal et fenêtre) : leurs fichiers manquent à l'inventaire.
- **Réanalyse ciblée sur une classification périmée** — `reanalyze --where
  security=C3` retenait « la dernière analyse » sans exiger qu'elle porte sur le
  contenu actuel (copie locale de la règle, sans `content_version`) : un fichier
  modifié depuis son analyse était ciblé sur une classe qui ne décrit plus rien.
  La règle unique `latest_analysis_sql` s'applique désormais là aussi.
- **Une date non collectée vieillissait le fichier** — un scanner sans
  `--access-time` écrit `01/01/0001`, un FILETIME nul `01/01/1601` : ces valeurs
  rangeaient le fichier dans « non accédé depuis 10 ans » et parmi les candidats au
  nettoyage. Toute clé antérieure à 1601 vaut désormais « inconnue » (Python et SQL,
  `MIN_DATE_KEY`).
- **Les tests écrivaient dans le vrai `~/.config/docia/recent.json`** de la machine
  (campagnes `/tmp/pytest-of-…` listées dans l'accueil) : `DOCIA_HOME` est isolé
  pour chaque test.
- **Le banc e2e ne prouvait rien sur l'ancienneté** : tous ses fichiers étaient
  datés de l'instant. Cinq fichiers sont vieillis (6 ans, 2 ans) et trois contrôles
  vérifient les seuils et la préservation de la date d'accès par le scan.
- **Montant `NaN`/`Infinity` dans une réponse du modèle** — `json.loads` les accepte ;
  ils passaient la validation, étaient sommés, exportés vers Excel/Power BI et
  réécrits en JSON invalide dans `raw`. Rejetés comme mal formés.

### Performance

- **Reprise d'une campagne** : `Database.pending_blocks` lisait les fichiers de
  chaque bloc en attente par une requête séparée (20 000 blocs = 20 001
  allers-retours avant le premier envoi). Deux requêtes en tout désormais.

### Technique

- **Tests sans écran** : `docia doctor` avec sondes doublées (OCR, scanner,
  serveur), l'écran Accueil et la **fenêtre entière** (`DociaApp` : onglets, mode
  administrateur, création de campagne, enregistrement du `docia.toml`, plantage
  de thread, travaux de fond) construits sur des doublures de `customtkinter` et
  `tkinter` — `cli_tools` 34 → 70 %, `gui/tab_home` 14 → 68 %, `gui/app` 46 → 84 %,
  projet 77 → 88 % (614 tests).
- **`legacy/` retiré du dépôt** (POC 2025, 76 fichiers, 18 000 lignes) : l'historique git
  le garde ; le code vivant est `src/docia`.
- `.coverage` ignoré par git.

## [3.0.0] — 2026-08-30

Réécriture complète (v3) du POC : SQLite par campagne, blocs DocFuse, JSON
multi-fichiers garanti par schéma, reprise, scan piloté (`SMBeagle.exe`), GUI
CustomTkinter, rapports HTML / Markdown / Excel / Power BI, `Docia.exe` avec OCR
embarqué. Détail : `docs/DESIGN_V3.md`, `docs/GUIDE_UTILISATEUR.md`.
