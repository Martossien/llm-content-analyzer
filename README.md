llm-content-analyzer

Module d'analyse de contenu de fichiers par intelligence artificielle pour la classification multi-domaines et l'audit de conformité. Cette brique traite les données générées par SMBeagle enrichi pour analyser le contenu des fichiers via LLM et les classifier selon des critères de sécurité, conformité RGPD, finance et aspects légaux.
Architecture du Pipeline
llm-content-analyzer constitue la deuxième brique d'un pipeline d'analyse en trois étapes :


smbeagle_enriched → llm-content-analyzer → reports-generator
    (Brique 1)          (Brique 2)         (Brique 3)
Scan + métadonnées   Analyse contenu IA   Rapports Excel/PDF

Fonctionnalités
Classification Multi-Domaines

Sécurité : Classification C0 (Public), C1 (Interne), C2 (Confidentiel), C3 (Secret)
RGPD : Détection de données personnelles et évaluation des risques
Finance : Identification de documents financiers (factures, contrats, budgets)
Legal : Analyse de contrats et documents juridiques

Architecture Modulaire

Cache intelligent : SQLite basé sur une clé FastHash+Taille pour éviter les analyses redondantes
Filtrage avancé : Exclusions configurables et scoring de priorité
Templates de prompts : Configuration flexible via Jinja2 et YAML
Interface graphique : GUI complète pour la gestion et le monitoring

Robustesse Production

Retry automatique : Gestion des erreurs réseau avec tenacity
Protection surcharge : Circuit breaker pour l'API LLM
Monitoring : Métriques de performance et logs détaillés
Configuration centralisée : YAML pour tous les paramètres

Installation
Prérequis

Python 3.9+
API-DOC-IA accessible (généralement localhost:8080)
Fichiers CSV générés par SMBeagle enrichi

Installation des dépendances
pip install -r requirements.txt

Configuration
Copier et adapter le fichier de configuration :
cp content_analyzer/config/analyzer_config.yaml.example content_analyzer/config/analyzer_config.yaml

Configurer l'URL et le token de l'API-DOC-IA dans le fichier YAML.
Utilisation
Interface en ligne de commande

# Analyse basique
python content_analyzer/content_analyzer.py scan_smbeagle.csv analysis_results.db

# Analyse avec cache activé
python content_analyzer/content_analyzer.py --input scan.csv --output results.db --enable-cache

Interface graphique
python gui/main.py

L'interface GUI permet :

Import automatique de fichiers CSV SMBeagle
Configuration de l'API et des exclusions
Lancement d'analyses par lot ou fichier unique
Visualisation des résultats avec filtres
Export en CSV, JSON, Excel

Workflow typique

Import CSV : Sélectionner un fichier CSV SMBeagle enrichi
Configuration : Ajuster les paramètres API et exclusions
Analyse : Lancer l'analyse automatique ou manuelle
Résultats : Consulter les classifications dans l'interface
Export : Exporter les données pour génération de rapports

Architecture Technique
Modules principaux

content_analyzer/
├── content_analyzer.py      # Orchestrateur principal
├── modules/
│   ├── csv_parser.py        # Parse CSV SMBeagle → SQLite
│   ├── api_client.py        # Client HTTP + retry/circuit breaker
│   ├── cache_manager.py     # Cache SQLite intelligent
│   ├── file_filter.py       # Filtrage + scoring priorité
│   ├── db_manager.py        # Gestionnaire base SQLite
│   └── prompt_manager.py    # Templates prompts configurables
├── config/
│   └── analyzer_config.yaml # Configuration centralisée
└── gui/                     # Interface graphique

Base de données SQLite
Le module génère une base SQLite avec :

Table fichiers : Métadonnées SMBeagle + état traitement
Table reponses_llm : Analyses structurées par domaine
Table cache_prompts : Cache basé sur une clé FastHash+Taille
Table metriques : Monitoring performance

Stack technique
Dépendances externes (2 uniquement) :

tenacity : Retry logic robuste pour appels API
circuitbreaker : Protection surcharge API

Bibliothèques standard :

sqlite3, requests, pandas, yaml, jinja2

Format d'entrée
Le module traite les fichiers CSV générés par SMBeagle enrichi avec les colonnes :

Name,Host,Extension,Username,Hostname,UNCDirectory,CreationTime,LastWriteTime,
Readable,Writeable,Deletable,DirectoryType,Base,FileSize,Owner,FastHash,
AccessTime,FileAttributes,FileSignature

Format de sortie
Les analyses LLM sont stockées en JSON structuré :
{
  "security": {"classification": "C2", "confidence": 85},
  "rgpd": {"risk_level": "medium", "data_types": ["email", "phone"]},
  "finance": {"document_type": "invoice", "amounts": [{"value": "1500€"}]},
  "legal": {"contract_type": "service_agreement", "parties": [...]}
}
Configuration
API-DOC-IA
api_config:
  url: "http://localhost:8080"
  token: "sk-XXXXXXXXX"
  max_tokens: 32000
  timeout_seconds: 300

Exclusions
exclusions:
  extensions:
    blocked: [".tmp", ".log", ".bak"]
    # Extensions parsed from CSV are normalized with a leading dot so
    # that values like "zip" match the blocked list [".zip"].
  file_size:
    min_bytes: 100
    max_bytes: 104857600
  file_attributes:
    skip_system: true
    skip_hidden: false

templates:
  comprehensive:
    system_prompt: "Tu es un expert en analyse de documents..."
    user_template: "Fichier: {{ file_name }}..."

Monitoring
Métriques collectées

Performance API (temps réponse, taux succès)
Efficacité cache (hit rate, économies)
Qualité analyse (confiance, cohérence)
Utilisation ressources (mémoire, stockage)

Logs
Logs détaillés dans logs/content_analyzer.log avec niveaux INFO, WARN, ERROR.
Tests
# Tests unitaires
python -m pytest content_analyzer/tests/ -v

Intégration Pipeline
Avec SMBeagle enrichi (Brique 1)
# 1. Scan avec SMBeagle enrichi
SMBeagle.exe -c scan_results.csv --sizefile --ownerfile --fasthash

# 2. Analyse avec llm-content-analyzer
python content_analyzer/content_analyzer.py scan_results.csv analysis.db

Vers reports-generator (Brique 3)
La base SQLite générée (analysis_results.db) contient les données structurées pour la génération de rapports Excel/PDF par la brique 3.
Limitations

Dépend de la disponibilité de l'API-DOC-IA
Qualité d'analyse liée à la qualité des prompts et du modèle LLM
Formats de fichiers limités à ceux supportés par l'API
Cache basé sur FastHash+Taille : moins de faux positifs mais modifications mineures non détectées

Support
Les logs détaillés facilitent le diagnostic des problèmes. Les erreurs courantes sont documentées dans la configuration et l'interface GUI fournit des messages d'erreur explicites.
Licence
Apache License 2.0. Voir LICENSE pour les détails.

## Performance Benchmarks

La méthode `parse_csv_optimized` utilise des insertions SQLite en batch et des
PRAGMA adaptés pour accélérer l'import des CSV SMBeagle. Sur le fichier de test
`scan_local_mini.csv` (63 lignes), le temps de chargement passe d'environ
0.5&nbsp;s à moins de 0.1&nbsp;s sur la même machine.


