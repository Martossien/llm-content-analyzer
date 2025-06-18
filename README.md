# llm-content-analyzer

Module d'analyse de contenu de fichiers par LLM avec classification multi-domaines et cache intelligent.

## Description

llm-content-analyzer traite les fichiers détectés par SMBeagle pour les analyser via API LLM et les classifier selon des critères de sécurité (C0-C3), conformité RGPD, finance et aspects légaux. Le module utilise un cache SQLite basé sur FastHash pour optimiser les performances et une architecture modulaire avec configuration YAML centralisée.

## Fonctionnalités

- **Analyse multi-domaines** : Classification sécurité C0-C3, RGPD, finance, legal, forensique
- **Cache intelligent** : SQLite custom basé sur FastHash avec stratégies d'éviction
- **Filtrage avancé** : Exclusions configurables par environnement et scoring de priorité
- **API robuste** : Client HTTP avec retry automatique (tenacity) et protection surcharge (circuitbreaker)
- **Configuration centralisée** : YAML pour tous les paramètres et templates de prompts
- **Monitoring intégré** : Métriques de performance et logs détaillés

## Architecture

content_analyzer/
├── content_analyzer.py # Orchestrateur principal
├── modules/
│ ├── csv_parser.py # Parser SMBeagle CSV → SQLite
│ ├── cache_manager.py # Cache SQLite intelligent
│ ├── file_filter.py # Filtrage et scoring priorité
│ ├── api_client.py # Client HTTP avec protections
│ ├── db_manager.py # Gestionnaire base SQLite
│ └── prompt_manager.py # Templates prompts configurables
├── config/
│ ├── analyzer_config.yaml # Configuration principale
│ ├── exclusions_config.yaml # Règles d'exclusion
│ └── prompts_config.yaml # Templates prompts
└── tests/ # Tests unitaires par module

## Installation

### Prérequis

- Python 3.9+
- Windows (environnement cible)
- API-DOC-IA accessible sur localhost:8080

### Dépendances

Les bibliothèques standard Python sont utilisées : `sqlite3`, `requests`, `pandas`, `yaml`, `pathlib`, `logging`, `json`, `jinja2`.

### Configuration

1. Copier `config/analyzer_config.yaml.example` vers `config/analyzer_config.yaml`
2. Configurer l'URL et token API-DOC-IA
3. Ajuster les paramètres selon l'environnement

## Utilisation

### Analyse basique

python content_analyzer/content_analyzer.py
--input data/raw_scan_20250618.csv
--config config/analyzer_config.yaml
--output data/analysis_results.db

python content_analyzer/content_analyzer.py
--input data/raw_scan_20250618.csv
--config config/analyzer_config.yaml
--output data/analysis_results.db
--enable-cache
--enable-monitoring
--workers 3

config/analyzer_config.yaml

brique2_analyzer:
api_url: "http://localhost:8080"
api_token: "sk-XXXXXXXXXXXXXXXXXXXXXXXXX"
max_tokens: 32000
timeout_seconds: 300
batch_size: 100

retry_config:
max_attempts: 3
wait_strategy: "exponential"
wait_min: 4
wait_max: 10


## Base de données

Le module utilise SQLite avec schema optimisé :

- **Table fichiers** : Métadonnées SMBeagle + état traitement + flags
- **Table reponses_llm** : Réponses structurées par domaine d'analyse
- **Table cache_prompts** : Cache intelligent basé sur FastHash composite
- **Table metriques** : Monitoring performance et qualité

## Monitoring

Les métriques sont collectées en temps réel :

- Performance API (temps réponse, taux succès)
- Efficacité cache (hit rate, économies)
- Qualité analyse (confiance, cohérence)
- Utilisation ressources (mémoire, stockage)

## Structure de sortie

Le module génère une base SQLite avec analyses structurées JSON pour chaque domaine ( suivant le prompt ):

{
"security": {"classification": "C2", "confidence": 85},
"rgpd": {"risk_level": "medium", "data_types": ["email", "phone"]},
"finance": {"document_type": "invoice", "amounts": [{"value": "1500€"}]},
"legal": {"contract_type": "service_agreement", "parties": [...]}
}

## Intégration

Ce module s'intègre dans un pipeline à 3 briques :

1. **SMBeagle enrichi** (scan fichiers + métadonnées) → 
2. **llm-content-analyzer** (analyse contenu) → 
3. **reports-generator** (rapports Excel/PDF)

## Licence

Open source - 

## Support


