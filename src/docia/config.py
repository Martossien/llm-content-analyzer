"""Configuration `docia.toml` (tomllib, stdlib) → `Config`.

Un seul fichier, des valeurs par défaut sûres, une validation explicite. Les
secrets (clé API) peuvent venir de l'environnement (`DOCIA_API_KEY`) pour ne
pas traîner dans un fichier versionné.
"""

from __future__ import annotations

import json
import os
import re
import tomllib
from collections.abc import Callable
from dataclasses import asdict, dataclass, field, fields
from pathlib import Path
from typing import Any

DEFAULT_CONFIG_NAME = "docia.toml"

DEFAULT_BATCH_BYTES = 64 * 1024 * 1024
"""Budget mémoire par défaut d'un lot d'extraction, en octets de fichiers source (64 Mio).

Le nombre de fichiers seul (`blocks.batch_files`) ne borne **rien** en mémoire :
`filter.max_size_bytes` laisse passer des fichiers de 100 Mo, et DocFuse garde le texte
extrait de *tout* le lot avant le découpage.

Mesuré le 01/09 sur ce dépôt — `build_blocks`, 12 fichiers texte de 50 Mo (600 Mo au
total), `tokenizer_engine = "approx"`, pic de RSS du processus :

| `batch_bytes`      | pic RSS | durée   |
|--------------------|---------|---------|
| 0 (avant, aucun)   | 2 135 Mo| 285 s   |
| 256 Mio            | 1 311 Mo| 285 s   |
| **64 Mio (défaut)**| **360 Mo** | 284 s |

Le pic vaut environ **cinq fois** la taille du sous-lot : c'est ce facteur, et non le
cumul du lot, qu'il faut multiplier. 64 Mio tiennent donc dans ~360 Mo sur un serveur
Windows de 8 Go partagé, et le découpage ne coûte **rien** en temps (285 s dans les
trois cas). Un lot ordinaire de documents bureautiques (200 fichiers de ~200 Ko = 40 Mo)
passe d'un seul tenant : le budget ne mord que sur les gros fichiers.

Un fichier isolé plus gros que ce budget est traité **seul**, jamais écarté."""


@dataclass
class LLMConfig:
    transport: str = "vllm"
    """`vllm` (OpenAI-compatible direct) ou `openwebui` (API native, fichiers texte inline)."""
    base_url: str = "http://127.0.0.1:8000/v1"
    """vLLM : `http://host:8000/v1` — open-webui : `http://host:8080/api`."""
    api_key: str = ""
    """Vide = lue dans `DOCIA_API_KEY` ; `dummy` suffit pour vLLM sans `--api-key`."""
    model: str = "qwen38"
    max_in_flight: int = 8
    timeout_s: int = 900
    max_retries: int = 3
    temperature: float = 0.0
    max_tokens_per_file: int = 700
    """Budget de sortie par fichier (5 domaines + justifications ≈ 500–600 tokens)."""
    max_tokens_floor: int = 800
    max_tokens_cap: int = 32000
    max_context_tokens: int = 262_144
    """Plafond du modèle servi (tokens avec marge, prompt compris) — aligner sur
    `--max-model-len` du serveur (contexte natif du modèle, 262144 pour Qwen3.8) ; le pipeline garde en dessous la place du prompt système et de la réponse (raisonnement compris). Un fichier seul au-delà n'est ni
    tronqué ni mis en erreur : il est découpé en segments complets analysés
    séparément puis agrégés (sévérité = max des segments)."""
    enable_thinking: bool = True
    """Raisonnement activé par défaut (décision du 30/08 : c'est le point fort du
    modèle, et le même serveur sert d'autres usages) : envoie
    `chat_template_kwargs.enable_thinking`, réserve `thinking_budget_tokens` en plus
    dans `max_tokens` **et impose ce budget** (`thinking_token_budget`, vLLM avec
    `--reasoning-parser`) : au-delà, vLLM force `</think>` et le JSON garde sa place."""
    thinking_budget_tokens: int = 6_000
    """Budget de raisonnement imposé par requête. Banc du 30/08 (Qwen3.8-27B, 8 blocs
    de 2 K à 84 K tokens) : effort `medium` raisonne 500–4 400 tokens ; `xhigh` 1 500–18 000
    et, sans budget, rend parfois une réponse **vide** après `</think>` ; avec 6 000
    imposés, 18/18 JSON valides."""
    reasoning_effort: str = "medium"
    """Effort de raisonnement demandé au modèle (Qwen3.8 : `low` / `medium` / `xhigh`,
    vide = défaut du modèle, `xhigh`). `medium` : mêmes classifications que `xhigh`
    sur le banc, 2–3× plus rapide, corrige des sur-classifications de `low`
    (bulletins de paie C3 → C2). `xhigh` n'est sûr qu'avec le budget imposé."""

    def resolved_api_key(self) -> str:
        return self.api_key or os.environ.get("DOCIA_API_KEY", "") or "dummy"


@dataclass
class BlocksConfig:
    block_tokens: int = 32_000
    """Plafond par bloc (tokens avec marge). 16–64K recommandé (banc du 30/08)."""
    margin: float = 0.15
    tokenizer_engine: str = "openai"
    """`openai` (tiktoken o200k, embarqué dans l'exe) | `mistral` (tekken) | `approx`
    (octets/4). Défaut `openai` depuis le banc du 30/08 : `approx` sous-estime Qwen de
    ~30 % sur du texte français chiffré et faisait dépasser le contexte aux segments."""
    batch_files: int = 200
    """Fichiers passés à DocFuse par appel (extraction parallèle interne). **Plafond
    de rythme, pas de mémoire** : c'est `batch_bytes` qui borne la RAM."""
    batch_bytes: int = DEFAULT_BATCH_BYTES
    """Budget mémoire d'un lot d'extraction, en octets **de fichiers source** cumulés.

    Le lot se ferme dès que le cumul dépasse ce budget, même si `batch_files` n'est
    pas atteint : le builder appelle alors DocFuse une fois par sous-lot et libère le
    texte extrait entre deux. Un fichier seul plus gros que le budget forme son propre
    sous-lot — il est traité, jamais écarté. Voir `DEFAULT_BATCH_BYTES` pour la mesure
    qui fixe le défaut. `0` = aucun plafond (comportement d'avant le 01/09, déconseillé
    dès que `filter.max_size_bytes` laisse passer de gros fichiers)."""
    work_dir: str = ""
    """Dossier des blocs `.md` ; vide = `<db>.blocks/` à côté de la base."""
    max_file_tokens: int = 0
    """Budget (tokens avec marge) au-delà duquel un fichier seul est découpé en
    segments. 0 = dérivé du pipeline : `llm.max_context_tokens` moins une réserve
    pour le prompt et la réponse."""
    keep_blocks: bool = True
    """Conserve les blocs `.md` après le run — **ils contiennent le texte intégral
    des documents analysés**, en clair, sur le disque du poste.

    À lire avant de laisser le défaut : un bloc est la concaténation du texte extrait
    (OCR compris) des fichiers du partage. Un bulletin de paie, un compte rendu
    médical, un fichier de mots de passe scanné : leur contenu est recopié tel quel
    dans `work_dir` (vide = `<db>.blocks/`, à côté du fichier de campagne), sans
    chiffrement, et **y reste** tant que personne n'efface le dossier. Les droits du
    dossier sont ceux de son parent : quiconque lit la campagne lit les documents.

    `true` (défaut) sert la reprise et le diagnostic : après une interruption, un bloc
    déjà envoyé est relu au lieu d'être reconstruit, et on peut vérifier ce qui a
    réellement été soumis à la LLM quand une classification surprend. `false` efface
    le `.md` de chaque bloc dès qu'il est `done`/`error` (jamais un bloc resté `built`,
    qui bloquerait la reprise) : à préférer dès que le partage audité contient des
    données sensibles et que la campagne vit ailleurs que sur un poste d'administrateur.
    Le dossier reste à supprimer à la main à la fin de l'audit dans les deux cas."""


@dataclass
class FilterConfig:
    excluded_extensions: list[str] = field(
        default_factory=lambda: [
            ".tmp",
            ".temp",
            ".log",
            ".bak",
            ".cache",
            ".zip",
            ".7z",
            ".rar",
            ".gz",
            ".iso",
            ".exe",
            ".dll",
            ".sys",
            ".msi",
            ".lnk",
            ".db",
            ".sqlite",
            ".mdb",
            ".ldb",
            # Les images **matricielles** ne sont plus exclues : DocFuse les océrise
            # depuis D-109. Un courrier scanné sort en .tif ou .jpg d'un copieur, pas
            # en PDF, et ces fichiers étaient jusqu'ici absents de l'audit — ni classés,
            # ni signalés. Restent exclus `.ico` (icône d'interface) et `.svg`
            # (vectoriel : aucun moteur OCR ne le lit).
            ".ico",
            ".svg",
            ".mp3",
            ".mp4",
            ".avi",
            ".mov",
            ".wav",
            ".mkv",
        ]
    )
    min_size_bytes: int = 100
    max_size_bytes: int = 100 * 1024 * 1024
    excluded_dir_markers: list[str] = field(
        default_factory=lambda: [
            "\\admin$\\",
            "\\Windows\\",
            "\\Program Files",
            "\\$RECYCLE.BIN\\",
            "\\System Volume Information\\",
            "\\AppData\\",
        ]
    )


@dataclass
class ScanConfig:
    """Étape 0 : scanner SMBeagle_enriched piloté par docia (`docia scan`, onglet Accueil)."""

    smbeagle_path: str = ""
    """Chemin de `SMBeagle.exe` ; vide = à côté de l'exécutable docia, puis PATH."""
    preserve_access_time: bool = True
    """Restaure la date d'accès après lecture (hachage/signature) — sinon l'audit
    « rajeunit » tous les fichiers pour la statistique « non accédé depuis N ans »."""
    skip_acls: bool = False
    """Sans énumération des ACL (plus rapide ; colonnes lecture/écriture vides)."""
    exclude_hidden_shares: bool = True
    domain: str = ""
    username: str = ""
    """Compte SMB explicite (hors Windows ou compte de service) ; le mot de passe
    n'est jamais écrit dans docia.toml : variable `DOCIA_SMB_PASSWORD` ou saisie."""


@dataclass
class Config:
    db_path: str = "docia.sqlite"
    llm: LLMConfig = field(default_factory=LLMConfig)
    blocks: BlocksConfig = field(default_factory=BlocksConfig)
    filter: FilterConfig = field(default_factory=FilterConfig)
    scan: ScanConfig = field(default_factory=ScanConfig)
    prompt_path: str = ""
    """Vide = prompt embarqué `docia/prompts/docia_v3.md`."""

    def validate(self) -> list[str]:
        errors: list[str] = []
        if self.llm.transport not in ("vllm", "openwebui"):
            errors.append(
                f"llm.transport doit être 'vllm' ou 'openwebui' (valeur: {self.llm.transport})"
            )
        if not self.llm.base_url.startswith(("http://", "https://")):
            errors.append(
                f"llm.base_url doit commencer par http(s):// (valeur: {self.llm.base_url})"
            )
        if not (1 <= self.llm.max_in_flight <= 256):
            errors.append(
                f"llm.max_in_flight doit être entre 1 et 256 (valeur: {self.llm.max_in_flight})"
            )
        if self.llm.timeout_s < 10:
            errors.append("llm.timeout_s doit être >= 10")
        if not (1_000 <= self.blocks.block_tokens <= 1_000_000):
            errors.append(
                f"blocks.block_tokens hors plage 1000–1000000 (valeur: {self.blocks.block_tokens})"
            )
        if not (0.0 <= self.blocks.margin <= 1.0):
            errors.append("blocks.margin doit être entre 0 et 1")
        if self.blocks.tokenizer_engine not in ("approx", "mistral", "openai"):
            errors.append(f"blocks.tokenizer_engine inconnu : {self.blocks.tokenizer_engine}")
        if self.blocks.batch_files < 1:
            errors.append("blocks.batch_files doit être >= 1")
        if self.blocks.batch_bytes < 0:
            errors.append(
                "blocks.batch_bytes doit être >= 0 (0 = aucun plafond mémoire) "
                f"(valeur: {self.blocks.batch_bytes})"
            )
        if self.llm.thinking_budget_tokens < 0:
            errors.append("llm.thinking_budget_tokens doit être >= 0")
        if self.llm.reasoning_effort not in ("", "low", "medium", "xhigh"):
            errors.append(f"llm.reasoning_effort inconnu : {self.llm.reasoning_effort}")
        if self.llm.max_context_tokens < self.blocks.block_tokens:
            errors.append(
                "llm.max_context_tokens doit être >= blocks.block_tokens "
                f"({self.llm.max_context_tokens} < {self.blocks.block_tokens})"
            )
        if self.llm.max_retries < 0:
            errors.append(f"llm.max_retries doit être >= 0 (valeur: {self.llm.max_retries})")
        if self.llm.max_tokens_per_file < 1:
            errors.append(
                f"llm.max_tokens_per_file doit être >= 1 (valeur: {self.llm.max_tokens_per_file})"
            )
        # `[filter]` n'était pas validé du tout : `docia plan` excluait les 60 000
        # fichiers en annonçant « 0 à analyser » sans que rien ne pointe la config,
        # que `docia.toml` soit édité à la main ou produit par `docia init`.
        if self.filter.min_size_bytes < 0:
            errors.append(
                f"filter.min_size_bytes doit être >= 0 (valeur: {self.filter.min_size_bytes})"
            )
        if self.filter.max_size_bytes < 1:
            errors.append(
                f"filter.max_size_bytes doit être >= 1 (valeur: {self.filter.max_size_bytes})"
            )
        if self.filter.min_size_bytes > self.filter.max_size_bytes:
            errors.append(
                "filter.min_size_bytes doit être <= filter.max_size_bytes "
                f"({self.filter.min_size_bytes} > {self.filter.max_size_bytes})"
            )
        for name, values in (
            ("filter.excluded_extensions", self.filter.excluded_extensions),
            ("filter.excluded_dir_markers", self.filter.excluded_dir_markers),
        ):
            wrong = [v for v in values if not isinstance(v, str)]
            if wrong:
                errors.append(f"{name} ne doit contenir que du texte (valeur : {wrong[0]!r})")
        if not self.db_path.strip():
            errors.append("db_path ne peut pas être vide")
        return errors

    def work_dir(self) -> Path:
        if self.blocks.work_dir:
            return Path(self.blocks.work_dir)
        db = Path(self.db_path)
        return db.with_name(f"{db.stem}.blocks")


def _merge(target: Any, data: dict[str, Any], section: str) -> None:
    known = {f.name: f for f in fields(target)}
    for key, value in data.items():
        if key not in known:
            raise ValueError(f"[{section}] clé inconnue : {key}")
        current = getattr(target, key)
        # `bool` est une sous-classe de `int` : sans le rejet explicite,
        # `max_in_flight = true` passait pour l'entier 1, en silence.
        if isinstance(current, bool):
            if not isinstance(value, bool):
                raise ValueError(f"[{section}] {key} doit être un booléen")
        elif isinstance(current, int) and (isinstance(value, bool) or not isinstance(value, int)):
            raise ValueError(f"[{section}] {key} doit être un entier")
        elif isinstance(current, float) and (
            isinstance(value, bool) or not isinstance(value, int | float)
        ):
            raise ValueError(f"[{section}] {key} doit être un nombre")
        elif isinstance(current, str) and not isinstance(value, str):
            raise ValueError(f"[{section}] {key} doit être une chaîne")
        elif isinstance(current, list) and not isinstance(value, list):
            raise ValueError(f"[{section}] {key} doit être une liste")
        setattr(target, key, float(value) if isinstance(current, float) else value)


def load_config(path: Path | None, *, on_missing: Callable[[str], None] | None = None) -> Config:
    """Charge `docia.toml` ; `None` ou fichier absent → défauts.

    Args:
        on_missing: appelé avec un message en clair quand `path` est donné mais
            n'existe pas. Sans lui, une faute de frappe dans `--config` faisait
            tourner toute la campagne sur les réglages par défaut sans un mot —
            base, seuils de taille et modèle compris. Le message nomme le fichier
            absent ; le chargement se poursuit sur les défauts, comme avant.

    Raises:
        ValueError: clé inconnue ou type invalide (on ne dégrade pas en silence :
            une config fausse doit arrêter un batch de 50 000 fichiers avant de
            partir).
    """
    config = Config()
    if path is None:
        return config
    if not path.exists():
        if on_missing is not None:
            on_missing(
                f"configuration absente : {path} — réglages par défaut appliqués "
                f"(base « {config.db_path} », taille {config.filter.min_size_bytes}"
                f"–{config.filter.max_size_bytes} o, modèle « {config.llm.model} »)"
            )
        return config
    # `utf-8-sig` et non `utf-8` : sous Windows, le Bloc-notes et une redirection
    # PowerShell (`>`) écrivent un BOM en tête. `tomllib` échouait alors dès le
    # premier caractère (« Invalid statement at line 1, column 1 ») — toute la
    # configuration était perdue, la campagne repartait sur les valeurs par défaut,
    # et « Enregistrer » écrasait ensuite le fichier de l'administrateur. C'est le
    # naufrage le plus probable d'un déploiement en exécutable autonome, pour un
    # suffixe d'encodage. Le BOM est retiré s'il est là, ignoré sinon.
    return config_from_data(tomllib.loads(path.read_text(encoding="utf-8-sig")))


def config_from_data(data: dict[str, Any]) -> Config:
    """`Config` issue d'un `docia.toml` déjà analysé (défauts pour ce qui manque)."""
    data = dict(data)  # `_merge` consomme les sections : ne pas vider le dict de l'appelant
    config = Config()
    for section_name, target in (
        ("llm", config.llm),
        ("blocks", config.blocks),
        ("filter", config.filter),
        ("scan", config.scan),
    ):
        section = data.pop(section_name, None)
        if section is not None:
            if not isinstance(section, dict):
                raise ValueError(f"[{section_name}] doit être une table")
            _merge(target, section, section_name)
    _merge(config, data, "racine")
    return config


def default_toml() -> str:
    """Contenu d'un `docia.toml` de départ (commande `docia init`)."""
    return """# Doc-IA analyzer v3 — configuration
db_path = "docia.sqlite"
# prompt_path = "mon_prompt.md"   # vide = prompt embarqué

[llm]
transport = "vllm"                 # "vllm" (direct) ou "openwebui" (API native, auth par clé sk-)
base_url = "http://127.0.0.1:8000/v1"   # open-webui : "http://serveur:8080/api"
api_key = ""                       # vide = variable DOCIA_API_KEY (ou "dummy" pour vLLM)
model = "qwen38"
max_in_flight = 8
timeout_s = 900
max_retries = 3
max_context_tokens = 262144        # = --max-model-len du serveur (262144 = natif Qwen3.8) ; la sortie est réservée en dessous ; fichier plus grand → segments agrégés
enable_thinking = true             # raisonnement activé (qualité) ; false pour du volume pur
thinking_budget_tokens = 6000      # budget de raisonnement imposé par requête (vLLM thinking_token_budget) et réservé dans max_tokens
reasoning_effort = "medium"        # low | medium | xhigh — Qwen3.8 (xhigh : seulement avec budget imposé, 2–3× plus lent)

[blocks]
block_tokens = 32000               # 16–64K recommandé
margin = 0.15
tokenizer_engine = "openai"        # openai (o200k, précis) | mistral | approx (octets/4, sous-estime ~30 %)
batch_files = 200                  # rythme d'extraction (nombre de fichiers par appel DocFuse)
batch_bytes = 67108864             # 64 Mio : plafond MÉMOIRE d'un lot (cumul des tailles source ; pic ≈ 5× ce budget) ; 0 = aucun
work_dir = ""                      # vide = <db>.blocks/
keep_blocks = true                 # ATTENTION : les blocs .md contiennent le TEXTE INTÉGRAL des documents, en clair, dans work_dir — false les efface au fil du run

[filter]
min_size_bytes = 100
max_size_bytes = 104857600

[scan]
smbeagle_path = ""                 # vide = SMBeagle.exe à côté de Docia.exe, puis PATH
preserve_access_time = true        # ne pas « rajeunir » les fichiers lus par l'audit (statistique non accédés)
skip_acls = false                  # true = plus rapide, sans colonnes lecture/écriture
exclude_hidden_shares = true       # ignore les partages administratifs (C$, ADMIN$…)
domain = ""                        # compte SMB explicite (facultatif ; mot de passe : variable DOCIA_SMB_PASSWORD)
username = ""
"""


# --------------------------------------------------------------- réécriture en place
SECTIONS = ("llm", "blocks", "filter", "scan")
"""Tables de `docia.toml`, dans l'ordre où elles y figurent."""

ROOT_KEYS = ("db_path", "prompt_path")
"""Clés hors table, écrites avant la première `[section]`."""


class TomlRewriteError(ValueError):
    """`update_toml` n'a pas pu modifier le fichier sans risque — ne rien écrire.

    Levée quand le texte de départ n'est pas du TOML lisible, ou quand le texte
    produit ne rendrait pas exactement les valeurs demandées (clé pointée, table
    en ligne, mise en forme que le petit analyseur ci-dessous ne sait pas suivre).
    L'appelant retombe alors sur une regénération complète : il perd les
    commentaires, mais n'écrit jamais une configuration fausse.
    """


def toml_value(value: object) -> str:
    """Rend une valeur simple en TOML (`true`, `12`, `"texte"`, `["a", "b"]`).

    Les chaînes passent par `json.dumps` : les guillemets et les échappements de
    JSON sont ceux des chaînes TOML de base, et un `#` s'y retrouve **entre
    guillemets**, donc jamais pris pour un commentaire.
    """
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float):
        return str(value)
    if isinstance(value, list):
        return "[" + ", ".join(json.dumps(str(x), ensure_ascii=False) for x in value) + "]"
    return json.dumps(str(value), ensure_ascii=False)


_TABLE_RE = re.compile(r"^[ \t]*\[[ \t]*(?P<name>[^\[\]]+?)[ \t]*\][ \t]*(?:#.*)?$")
_KEY_RE = re.compile(r"^[ \t]*(?P<key>[A-Za-z0-9_-]+|\"[^\"]*\"|'[^']*')[ \t]*=[ \t]*")


def _end_of_string(text: str, start: int) -> int:
    """Position juste après la chaîne qui commence en `start` (guillemet compris)."""
    quote = text[start]
    index = start + 1
    while index < len(text):
        char = text[index]
        if char == "\n":
            return index  # chaîne non fermée : on ne dépasse pas la ligne
        if quote == '"' and char == "\\":
            index += 2
            continue
        if char == quote:
            return index + 1
        index += 1
    return len(text)


def _end_of_value(text: str, start: int) -> int:
    """Position de fin de la valeur qui commence en `start`.

    Suit les chaînes (un `#` entre guillemets n'ouvre pas un commentaire), les
    tableaux et tables en ligne — y compris sur plusieurs lignes — et s'arrête,
    pour une valeur nue, au commentaire ou à la fin de ligne.
    """
    index, depth, size = start, 0, len(text)
    while index < size:
        char = text[index]
        if char in "\"'":
            triple = text[index : index + 3]
            if triple in ('"""', "'''"):
                closing = text.find(triple, index + 3)
                index = size if closing < 0 else closing + 3
            else:
                index = _end_of_string(text, index)
            continue
        if char == "#":
            if depth == 0:
                break
            newline = text.find("\n", index)  # commentaire dans un tableau multiligne
            index = size if newline < 0 else newline
            continue
        if char in "[{":
            depth += 1
        elif char in "]}":
            depth -= 1
            if depth <= 0:
                index += 1
                break
        elif char == "\n" and depth == 0:
            break
        index += 1
    while index > start and text[index - 1] in " \t\r\n":
        index -= 1
    return index


def _scan_toml(text: str) -> tuple[dict[tuple[str, str], tuple[int, int]], dict[str, int]]:
    """Repère les affectations et la fin de chaque section.

    Rend `{(section, clé): (début, fin) de la valeur}` et `{section: position où
    insérer une clé manquante}` — après la dernière ligne non vide de la section,
    pour que l'ajout ne saute pas par-dessus la ligne blanche qui la sépare de la
    suivante. La section hors table porte le nom `""`.
    """
    spans: dict[tuple[str, str], tuple[int, int]] = {}
    ends: dict[str, int] = {"": 0}
    section, position = "", 0
    while position < len(text):
        newline = text.find("\n", position)
        line_end = len(text) if newline < 0 else newline
        line = text[position:line_end]
        if not line.strip():
            position = line_end + 1
            continue
        table = _TABLE_RE.match(line)
        if table:
            section = table.group("name").strip().strip("\"'")
            ends[section] = line_end
            position = line_end + 1
            continue
        assignment = None if line.lstrip().startswith("#") else _KEY_RE.match(line)
        if assignment is None:
            ends[section] = line_end  # commentaire ou ligne inconnue : la section s'étend
            position = line_end + 1
            continue
        key = assignment.group("key").strip("\"'")
        value_start = position + assignment.end()
        value_end = _end_of_value(text, value_start)
        spans[(section, key)] = (value_start, value_end)
        newline = text.find("\n", value_end)
        line_end = len(text) if newline < 0 else newline
        ends[section] = line_end
        position = line_end + 1
    return spans, ends


def _flat_config(config: Config) -> dict[tuple[str, str], Any]:
    """`{(section, clé): valeur}` de toute la configuration, dans l'ordre du fichier."""
    data = asdict(config)
    flat: dict[tuple[str, str], Any] = {("", key): data[key] for key in ROOT_KEYS}
    for section in SECTIONS:
        for key, value in data[section].items():
            flat[(section, key)] = value
    return flat


def _read_value(data: dict[str, Any], section: str, key: str) -> Any:
    table = data if section == "" else data.get(section)
    if not isinstance(table, dict):
        return None
    return table.get(key)


def update_toml(text: str, config: Config) -> str:
    """Réécrit dans `text` les **seules valeurs qui changent** — commentaires intacts.

    Regénérer le fichier à chaque « Enregistrer » effaçait ses 21 commentaires, dont
    l'avertissement qui prévient que `<campagne>.blocks/` garde le texte intégral des
    documents analysés, en clair, sur le disque : un administrateur cliquait une fois,
    et le suivant ne lisait plus rien. On modifie donc le texte existant au lieu de le
    reconstruire — les commentaires, l'ordre des clés, l'alignement et jusqu'aux clés
    inconnues de `Config` survivent, et une valeur inchangée n'est **pas** retouchée.

    Une clé absente du fichier n'y est ajoutée que si sa valeur **s'écarte du défaut**
    (`Config()`) : absente vaut défaut, l'écrire n'apprendrait rien et allongerait le
    fichier de tout ce que `docia init` a délibérément laissé de côté. Elle va à la fin
    de sa section, ou dans une nouvelle section en fin de fichier. Conséquence : réécrire
    sans rien changer un `docia.toml` produit par `docia init` le rend à l'identique,
    octet pour octet. Le résultat est relu avant d'être rendu : s'il ne redonne pas
    exactement les valeurs demandées, `TomlRewriteError` est levée et rien n'est écrit.

    Raises:
        TomlRewriteError: texte de départ illisible, ou réécriture non fidèle.
    """
    try:
        current = tomllib.loads(text)
    except tomllib.TOMLDecodeError as exc:
        raise TomlRewriteError(f"configuration illisible : {exc}") from exc

    wanted = _flat_config(config)
    defaults = _flat_config(Config())
    spans, ends = _scan_toml(text)
    edits: list[tuple[int, int, str]] = []  # (début, fin, remplacement)
    inserted: dict[str, list[str]] = {}  # clés à ajouter, section présente ou non
    for (section, key), value in wanted.items():
        if (section, key) in spans:
            if _read_value(current, section, key) == value:
                continue  # valeur inchangée : la ligne n'est même pas retouchée
            start, end = spans[(section, key)]
            edits.append((start, end, toml_value(value)))
        elif value != defaults[(section, key)]:
            inserted.setdefault(section, []).append(f"{key} = {toml_value(value)}")

    for section, lines in inserted.items():
        if section not in ends:
            continue  # section absente : ajoutée en fin de fichier, plus bas
        position = ends[section]  # fin de la dernière ligne de la section
        block = "\n".join(lines)
        # `0` = rien avant la première `[table]` : les clés hors table ouvrent le fichier.
        edits.append((0, 0, block + "\n") if position == 0 else (position, position, "\n" + block))
    result = text
    for start, end, replacement in sorted(edits, reverse=True):
        result = result[:start] + replacement + result[end:]
    for section in SECTIONS:  # section entièrement absente : ajoutée en fin de fichier
        if section in inserted and section not in ends:
            result = (
                result.rstrip("\n") + f"\n\n[{section}]\n" + "\n".join(inserted[section]) + "\n"
            )
    if not result.endswith("\n"):
        result += "\n"

    # Relecture par le vrai chargeur : ce que `load_config` lira doit être exactement
    # ce qu'on voulait écrire, sinon on n'écrit rien du tout.
    try:
        written = _flat_config(config_from_data(tomllib.loads(result)))
    except (tomllib.TOMLDecodeError, ValueError) as exc:
        raise TomlRewriteError(f"réécriture invalide : {exc}") from exc
    for entry, value in wanted.items():
        if written[entry] != value:
            raise TomlRewriteError(f"réécriture infidèle : {entry[0] or 'racine'}.{entry[1]}")
    return result
