"""Dataclasses partagées entre ingestion, base, blocs, LLM et pipeline.

Aucune logique ici : ce sont les contrats d'interface entre modules (voir
`docs/DESIGN_V3.md`).
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path


class FileStatus(StrEnum):
    """Statut d'un fichier dans la base."""

    PENDING = "pending"
    """À analyser (nouveau, ou contenu modifié depuis la dernière analyse)."""
    EXCLUDED = "excluded"
    """Écarté par les règles (extension, taille, dossier système…) — `exclusion_reason`."""
    QUEUED = "queued"
    """Placé dans un bloc en cours d'envoi."""
    DONE = "done"
    """Analyse persistée pour la version de contenu courante."""
    ERROR = "error"
    """Introuvable, extraction en erreur, absent de la réponse… — `exclusion_reason`."""


class BlockStatus(StrEnum):
    """Cycle de vie d'un bloc : construit, envoyé, terminé, en erreur."""

    BUILT = "built"
    SENT = "sent"
    DONE = "done"
    ERROR = "error"


_WINDOWS_PATH = re.compile(r"^(\\\\|[A-Za-z]:)")


def path_key(path: str | os.PathLike[str]) -> str:
    """Clé d'unicité d'un chemin.

    Les chemins SMBeagle sont des chemins Windows (`\\\\srv\\part\\x`, `D:\\x`),
    insensibles à la casse **quel que soit l'OS qui exécute l'analyzer** : ils
    sont normalisés en minuscules avec `\\` comme séparateur. Un chemin POSIX
    (tests, dev Linux) suit `os.path.normcase` de la plateforme.
    """
    text = str(path)
    if _WINDOWS_PATH.match(text):
        return text.replace("/", "\\").lower()
    return os.path.normcase(text)


@dataclass(frozen=True)
class SmbeagleRow:
    """Une ligne du CSV SMBeagle (19 colonnes), typée. `path` = UNCDirectory + Name."""

    name: str
    host: str
    extension: str
    username: str
    hostname: str
    unc_directory: str
    creation_time: str
    last_write_time: str
    readable: bool
    writeable: bool
    deletable: bool
    directory_type: str
    base: str
    file_size: int
    access_time: str
    file_attributes: str
    owner: str
    fast_hash: str
    file_signature: str
    size_unreadable: bool = False
    """Vrai si `FileSize` était illisible et que `file_size` est une valeur de repli
    (0) et non la taille réelle — le fichier serait sinon exclu « trop petit » sans
    que personne ne sache que sa taille n'a jamais été lue. Compté par `import_csv`
    (`ImportReport.size_defaulted`)."""

    @property
    def path(self) -> str:
        """Chemin complet (`UNCDirectory` + `Name`).

        Un `Name` ou un `UNCDirectory` vide ne donne pas un chemin identifiant :
        les deux vides donnent `"\\"`, et un dossier vide donne `"\\facture.pdf"`
        pour tous les fichiers de ce nom, quel que soit leur dossier réel. Toutes
        ces lignes fusionneraient sur la même `path_key`, en un seul enregistrement
        compté « inchangé ». `identity_error` les rend visibles ; `parse_line` les
        rejette (`CsvLineError`) au lieu de les fondre en silence.
        """
        sep = "/" if "/" in self.unc_directory and "\\" not in self.unc_directory else "\\"
        directory = self.unc_directory.rstrip("\\/")
        return f"{directory}{sep}{self.name}"

    def identity_error(self) -> str:
        """Raison pour laquelle `path` n'identifie pas un fichier, sinon `""`."""
        if not self.name.strip():
            return "Name vide"
        if not self.unc_directory.strip():
            return "UNCDirectory vide"
        return ""


@dataclass(frozen=True)
class FileRow:
    """Un fichier tel que stocké dans `files`."""

    id: int
    path: str
    name: str
    extension: str
    size_bytes: int
    fast_hash: str
    last_write_time: str
    content_version: int
    status: FileStatus
    exclusion_reason: str | None = None
    priority_score: int = 0
    owner: str = ""
    host: str = ""
    unc_directory: str = ""


@dataclass(frozen=True)
class BlockFile:
    """Un fichier placé dans un bloc, avec la clé de corrélation `file_ref`
    (= valeur exacte de la ligne `## SOURCE:` du bloc)."""

    file_id: int
    file_ref: str
    content_version: int
    oversized: bool = False
    segment_index: int = 0
    """> 0 : ce `BlockFile` est le segment n° i (1..K) d'un fichier trop grand pour le
    contexte, découpé en K morceaux **complets** analysés séparément puis agrégés
    (jamais de troncature)."""
    segment_count: int = 1

    @property
    def is_segment(self) -> bool:
        """Vrai si ce fichier est une partie d'un gros fichier découpé (K > 1)."""
        return self.segment_count > 1


@dataclass
class BlockSpec:
    """Un bloc construit par DocFuse, prêt à être envoyé."""

    path: Path
    files: list[BlockFile]
    tokens_estimated: int
    tokens_with_margin: int
    oversized: bool = False
    block_id: int | None = None

    @property
    def text(self) -> str:
        """Contenu du bloc `.md` relu depuis le disque (jamais gardé en mémoire)."""
        return self.path.read_text(encoding="utf-8")


@dataclass(frozen=True)
class DomainAnalysis:
    """Un des quatre domaines d'une analyse."""

    label: str
    confidence: int
    details: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class FileAnalysis:
    """Résultat validé pour un fichier, prêt à persister."""

    file_ref: str
    resume: str
    security: DomainAnalysis
    rgpd: DomainAnalysis
    finance: DomainAnalysis
    legal: DomainAnalysis
    raw: dict[str, object]
    retention: DomainAnalysis = field(
        default_factory=lambda: DomainAnalysis("none", 0, {"required": False, "years": 0})
    )
    """Conservation : `label` = fondement (`basis`), `details` = {required, years, justification}."""


@dataclass(frozen=True)
class LLMUsage:
    """Consommation d'une requête : tokens de prompt et de sortie, latence, modèle."""

    prompt_tokens: int
    completion_tokens: int
    latency_ms: int
    model: str


@dataclass(frozen=True)
class LLMResult:
    """Réponse brute d'un transport, avant validation."""

    content: str
    usage: LLMUsage
    finish_reason: str | None = None
    reasoning_chars: int = 0
    """Longueur du raisonnement renvoyé à part (`reasoning_content`, vLLM avec
    `--reasoning-parser`) ; 0 si absent. ≈ tokens × 3,5 pour du texte anglais."""
