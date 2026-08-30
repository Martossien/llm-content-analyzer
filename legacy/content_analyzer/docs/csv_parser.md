# Module `csv_parser`

Ce module permet d'importer les fichiers CSV générés par SMBeagle dans une base
SQLite optimisée pour le pipeline d'analyse IA.

## Utilisation

```python
from pathlib import Path
from content_analyzer.modules.csv_parser import CSVParser

parser = CSVParser(Path("content_analyzer/config/analyzer_config.yaml"))
result = parser.parse_csv(Path("scan.csv"), Path("analysis.db"))
```

Le dictionnaire `result` contient les statistiques suivantes :
- `total_files` : nombre total de lignes lues
- `imported_files` : lignes effectivement insérées en base
- `errors` : liste d'erreurs rencontrées
- `processing_time` : temps de traitement en secondes
- `validation_stats` : informations sur les lignes invalides

## Configuration

Les paramètres sont chargés depuis `analyzer_config.yaml` :

```yaml
modules:
  csv_parser:
    chunk_size: 10000        # Taille des blocs de lecture pandas
    validation_strict: true  # Arrêt sur erreur de format
    encoding: "utf-8"        # Encodage du fichier CSV
```

## Schéma SQLite créé

Le module crée la table `fichiers` si elle n'existe pas avec les index requis
pour les modules suivants du pipeline.

