"""
llm-content-analyzer - Brique 2
Module d'analyse de contenu de fichiers par LLM
Architecture modulaire V2.3 avec stack minimal
"""

__version__ = "2.3.0"
__author__ = "Équipe Développement IA"

__all__ = ["ContentAnalyzer"]

try:
    from .content_analyzer import ContentAnalyzer
except Exception:
    ContentAnalyzer = None
