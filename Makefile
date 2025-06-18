# Makefile Brique 2 Content Analyzer
.PHONY: help install test lint format clean

help:  ## Affiche cette aide
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install:  ## Installe les dépendances
	pip3 install --user -r requirements.txt

test:  ## Lance les tests
	python3 -m pytest content_analyzer/tests/ -v

lint:  ## Vérifie le code avec flake8
	python3 -m flake8 content_analyzer/

format:  ## Formate le code avec black
	python3 -m black content_analyzer/

typecheck:  ## Vérifie les types avec mypy
	python3 -m mypy content_analyzer/

clean:  ## Nettoie les fichiers temporaires
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/

run:  ## Lance l'analyseur principal
	python3 content_analyzer/content_analyzer.py

dev-setup: install  ## Setup complet développement
	@echo "✅ Environnement développement configuré"
	@echo "🎯 Stack: tenacity + circuitbreaker + bibliothèques standard"
	@echo "📋 Commandes disponibles: make help"
