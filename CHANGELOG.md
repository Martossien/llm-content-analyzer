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
- **DocFuse épinglé sur un commit** dans `pyproject.toml` (build reproductible :
  un push sur `DocFuse@main` ne peut plus casser la CI ni l'exe). Remonter
  l'épingle fait partie de toute livraison DocFuse (voir `AGENTS.md`).

### Corrigé

- **Montant `NaN`/`Infinity` dans une réponse du modèle** — `json.loads` les accepte ;
  ils passaient la validation, étaient sommés, exportés vers Excel/Power BI et
  réécrits en JSON invalide dans `raw`. Rejetés comme mal formés.

### Performance

- **Reprise d'une campagne** : `Database.pending_blocks` lisait les fichiers de
  chaque bloc en attente par une requête séparée (20 000 blocs = 20 001
  allers-retours avant le premier envoi). Deux requêtes en tout désormais.

### Technique

- **`legacy/` retiré du dépôt** (POC 2025, 76 fichiers, 18 000 lignes) : l'historique git
  le garde ; le code vivant est `src/docia`.
- `.coverage` ignoré par git.

## [3.0.0] — 2026-08-30

Réécriture complète (v3) du POC : SQLite par campagne, blocs DocFuse, JSON
multi-fichiers garanti par schéma, reprise, scan piloté (`SMBeagle.exe`), GUI
CustomTkinter, rapports HTML / Markdown / Excel / Power BI, `Docia.exe` avec OCR
embarqué. Détail : `docs/DESIGN_V3.md`, `docs/GUIDE_UTILISATEUR.md`.
