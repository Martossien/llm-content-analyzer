# llm-content-analyzer

![Python](https://img.shields.io/badge/python-3.9%2B-blue?logo=python) ![License](https://img.shields.io/badge/license-Apache--2.0-green) ![Build](https://img.shields.io/badge/build-passing-brightgreen)

**Analyse intelligente de documents avec tableau de bord BI intégré**

> Solution complète de classification et de veille documentaire pour les entreprises.

- Classification multi-domaines (Sécurité, RGPD, Finance, Juridique)
- Interface graphique moderne avec gestion des templates de prompt
- Tableau de bord Analytics pour la business intelligence
- Tests avancés de fiabilité LLM et stabilité de l'API

## Sommaire
1. [Aperçu et Architecture](#aperçu-et-architecture)
2. [Vitrine Fonctionnalités](#vitrine-fonctionnalités)
3. [Guide de Démarrage Rapide](#guide-de-démarrage-rapide)
4. [Utilisation Détaillée](#utilisation-détaillée)
5. [Documentation Technique](#documentation-technique)
6. [Business Intelligence Intégrée](#business-intelligence-intégrée)
7. [Développement et Contribution](#développement-et-contribution)
8. [Déploiement Production](#déploiement-production)
9. [Roadmap Future](#roadmap-future)

---

## Aperçu et Architecture

```text
SMBeagle Enriched ─▶ llm-content-analyzer ─▶ Actions Automatisées
   (Brique 1)            (Briques 2+3)           (Brique 4 prévue)
Découverte fichiers   Analyse IA + BI       Renommage / déplacement
```

llm-content-analyzer est la brique principale d'analyse de contenu. Les fichiers détectés par SMBeagle sont évalués via l'API-DOC-IA, puis stockés dans une base SQLite exploitable par le tableau de bord Analytics.

### Cas d'usage clés
- Audit de conformité RGPD
- Classification sécurité niveau C0 à C3
- Identification automatique de documents financiers et juridiques
- Visualisation BI et reporting temps réel

## Vitrine Fonctionnalités

### Classification Multi-Domaines
| Domaine   | Valeurs possibles |
|-----------|------------------|
| Sécurité  | C0 à C3 + N/A    |
| RGPD      | none, low, medium, high, critical |
| Finance   | none, invoice, contract, budget, accounting, payment + N/A |
| Juridique | none, employment, lease, sale, NDA, compliance, litigation + N/A |

### Interface Graphique
- Import et gestion de fichiers CSV
- Configuration API et test de connexion
- Système complet de templates Jinja2 pour les prompts
- Analyse par lot ou fichier unique
- Export CSV/JSON/Excel

### Test API & Fiabilité LLM
- Lancement de tests de charge multi-workers
- Mesure du taux de réponses corrompues ou tronquées
- Statistiques de variance de classification
- Détection de la cohérence des réponses et métriques de confiance

### Bibliothèque de Prompts
- Création et édition via l'interface
- Prévisualisation temps réel
- Versioning et sauvegarde dans `analyzer_config.yaml`
- Modes spécialisés (sécurité, RGPD, finance, juridique)

### Tableau de Bord Analytics
Six onglets spécialisés :
1. **Vue Globale** – volumes, tailles, progrès traitement
2. **Analyse Thématique** – répartitions Sécurité/RGPD/Finance/Juridique
3. **Analyse Temporelle** – distribution par âge de fichier
4. **Métriques Étendues** – doublons, top utilisateurs, tailles
5. **Focus Sécurité** – combinaisons C3 et RGPD critique
6. **Performance** – statistiques API et taux de cache

Chaque métrique est cliquable : un drill‑down affiche la liste détaillée des fichiers dans une fenêtre modale. Les données peuvent être exportées pour générer des rapports exécutifs.

### Moteur Haute Performance
- Import CSV optimisé avec inserts batch
- Filtrage avancé et scoring de priorité
- Cache SQLite intelligent (FastHash + taille)
- Traitement parallèle configurable

## Guide de Démarrage Rapide

### Prérequis
- Python 3.9+
- API-DOC-IA disponible (par défaut `localhost:8080`)
- CSV SMBeagle enrichi

```bash
pip install -r requirements.txt
cp content_analyzer/config/analyzer_config.yaml content_analyzer/config/local_config.yaml
```

### Première Analyse
```bash
python content_analyzer/content_analyzer.py scan.csv analysis.db
```
Lancez ensuite l'interface graphique :
```bash
python gui/main.py
```

## Utilisation Détaillée

### Interface CLI
```bash
# Analyse avec cache
python content_analyzer/content_analyzer.py --input scan.csv --output results.db --enable-cache
```

### Parcours GUI
1. Importer un CSV
2. Configurer l'API et sélectionner un template de prompt
3. Lancer l'analyse (unique ou par lot)
4. Consulter les résultats et exporter
5. Ouvrir le **Tableau de Bord Analytics** pour la BI

### Gestion des Templates
Des boutons permettent d'ajouter, éditer ou prévisualiser les prompts. Les limites de taille sont vérifiées automatiquement et un code couleur indique la marge disponible.

### Test API Avancé
Dans le menu « Test API », définissez le nombre d'itérations et de workers. Le tableau de résultats en temps réel affiche :
- Réponses réussies / corrompues
- Débit (req/min)
- Variance de classification
- Score de fiabilité global
Les rapports peuvent être exportés en JSON ou CSV.

## Documentation Technique

### Modules Principaux
- `csv_parser.py` – import CSV vers SQLite
- `file_filter.py` – règles de filtrage et scoring
- `cache_manager.py` – gestion cache et TTL
- `api_client.py` – client HTTP robuste (retry + circuit breaker)
- `db_manager.py` – accès SQLite thread‑safe
- `prompt_manager.py` – génération de prompts Jinja2
- `duplicate_detector.py` – détection de copies et statistiques
- `age_analyzer.py` / `size_analyzer.py` – analyses complémentaires

### Schéma Base de Données
```
fichiers(id, name, path, size, hash, status, ...)
reponses_llm(file_id, security, rgpd, finance, legal, confidence)
cache_prompts(cache_key, response_content, hits_count, ttl_expiry)
metriques(key, value, timestamp)
```

### Optimisation
- Connexions SQLite en pool
- Inserts batch et PRAGMA optimisés
- Cache avec TTL paramétrable
- Monitoring détaillé dans `logs/content_analyzer.log`

## Business Intelligence Intégrée

Le tableau de bord est accessible via la fenêtre principale. Chaque onglet propose filtres, graphiques et exports. Les mises à jour sont dynamiques et un bouton permet de générer un **rapport exécutif** au format CSV ou Excel. Les top utilisateurs (de 1 à 10) sont affichés par catégorie avec possibilité de cliquer pour afficher les fichiers correspondants.

## Développement et Contribution

```bash
# Lancer les tests
pytest -q
```
Les contributions sont les bienvenues via pull request. Merci de respecter la configuration Black et Flake8 fournie.

## Déploiement Production

1. Copier le projet sur le serveur cible
2. Installer les dépendances
3. Configurer `analyzer_config.yaml` (URL API, chemin base, paramètres cache)
4. Lancer `python gui/main.py`

Pour la supervision, consultez les logs et activez la rotation selon vos besoins.

## Roadmap Future

La prochaine **brique 4** ajoutera des actions automatisées : renommage, déplacement intelligent et application de permissions selon les résultats d'analyse.

---
Licence Apache 2.0.
