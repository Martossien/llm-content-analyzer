"""Journal de docia : console lisible, fichier `docia.log` détaillé avec rotation.

Posé une seule fois par processus par `setup_logging` (la fenêtre rappelle `cli.main`
pour produire ses documents : ni doublon ni réouverture). Tout ce qui parle de
l'écran ou du fichier journal vit ici ; `docia.cli` n'en fait qu'usage.
"""

from __future__ import annotations

import argparse
import contextlib
import logging
import logging.handlers
import os
import sqlite3
import sys
from pathlib import Path


def utf8_console() -> None:
    """Console Windows en cp1252 par défaut : les accents des messages sortent en `�`.
    On passe stdout/stderr en UTF-8 (sans jamais planter si le flux ne le permet pas)."""
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is not None:
            with contextlib.suppress(ValueError, OSError):  # flux fermé ou redirigé
                reconfigure(encoding="utf-8", errors="replace")


class ConsoleFormatter(logging.Formatter):
    """Console : une ligne par événement, **sans la pile d'appels**.

    Une campagne réelle rencontre toujours quelques fichiers illisibles (mails
    corrompus, PDF protégés…). Déverser une trace Python de vingt lignes par fichier
    rend la console inutilisable et alarme l'utilisateur pour un incident bénin :
    la pile complète part dans le fichier journal, la console garde le message.
    """

    def format(self, record: logging.LogRecord) -> str:
        """Ligne console sans la pile d'appels (elle reste dans le fichier journal)."""
        saved = (record.exc_info, record.exc_text, record.stack_info)
        record.exc_info = record.exc_text = record.stack_info = None
        try:
            return super().format(record)
        finally:
            record.exc_info, record.exc_text, record.stack_info = saved


JOURNAL_ONLY = "journal_seul"
"""Attribut d'enregistrement (`extra={JOURNAL_ONLY: True}`) qui réserve un message au
fichier : le garde-fou de `main()` y dépose la pile complète, mais parle à
l'utilisateur par un `print`, sans le préfixe `ERROR docia.cli :` ni doublon."""


def log_file(config_path: Path | None) -> Path:
    """Emplacement de `docia.log` : à côté de `Docia.exe`, sinon à côté du `--config`.

    Le guide promet « `docia.log`, à côté de `Docia.exe` ». Dériver le chemin du
    seul `--config` ne tenait pas cette promesse : sa valeur par défaut est le
    **relatif** `docia.toml`, donc le journal atterrissait dans le répertoire
    courant — celui d'où l'utilisateur a lancé l'exe, pas celui de l'exe.
    """
    if getattr(sys, "frozen", False):  # exécutable empaqueté (PyInstaller)
        return Path(sys.executable).resolve().parent / "docia.log"
    base = Path(config_path).resolve().parent if config_path else Path.cwd()
    return base / "docia.log"


_JOURNAL: Path | None = None
"""Fichier journal en cours (configuré une seule fois par processus)."""

_LOGGING_CONFIGURED = False
"""Vrai dès que `setup_logging` a posé ses gestionnaires."""


def current_journal() -> Path | None:
    """Chemin du journal ouvert par `setup_logging`, ou None (console seule, ou pas encore)."""
    return _JOURNAL


def reset() -> None:
    """Oublie la configuration posée — réservé aux tests, qui rejouent `setup_logging`."""
    global _JOURNAL, _LOGGING_CONFIGURED
    _JOURNAL, _LOGGING_CONFIGURED = None, False


_LOG_MAX_BYTES = 4_000_000
"""Taille de `docia.log` au-delà de laquelle il est mis en rotation."""

_LOG_BACKUPS = 3
"""Sauvegardes de rotation conservées (`docia.log.1` … `docia.log.3`)."""

NOISY_DEBUG_LOGGERS: tuple[str, ...] = (
    "httpx",
    "httpcore",
    "openai",
    "urllib3",
    "asyncio",
    "PIL",
    "matplotlib",
    "charset_normalizer",
    "filelock",
)
"""Bibliothèques tierces muselées à `INFO` : en `DEBUG` elles écrivent une ligne
par requête HTTP, par image ouverte ou par police chargée, et noieraient le
journal — qui doit rester lisible pour diagnostiquer *notre* code."""


class RotatingFileHandler(logging.handlers.RotatingFileHandler):
    """`RotatingFileHandler` qui survit à une rotation impossible.

    Sous Windows, un fichier ouvert par un autre processus ne peut pas être
    renommé : deux `Docia.exe` (la fenêtre et un `scan` en parallèle, ou deux
    sessions RDS) et `doRollover()` lève `PermissionError` à *chaque*
    enregistrement une fois les 4 Mo atteints. Le gestionnaire standard déverse
    alors une trace par message sur stderr — exactement ce que
    `ConsoleFormatter` a été écrit pour empêcher — et **perd** tous les
    enregistrements suivants.

    Ici, le premier échec est signalé une seule fois, la rotation est abandonnée
    pour ce processus (`maxBytes = 0` : le journal continue de grossir, ce qui
    vaut infiniment mieux que de ne plus rien écrire) et le flux est rouvert.
    """

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)  # type: ignore[arg-type]
        self.rollover_failed = False

    def doRollover(self) -> None:  # noqa: N802 - nom imposé par logging
        """Rotation ; si elle est impossible (fichier tenu par un autre processus), le journal continue sans rotation."""
        try:
            super().doRollover()
        except OSError as exc:
            self.maxBytes = 0  # plus de tentative : un journal trop gros > un journal muet
            if self.stream is None:  # `doRollover` ferme avant de renommer
                self.stream = self._open()
            if not self.rollover_failed:
                self.rollover_failed = True
                logging.getLogger(__name__).warning(
                    "rotation de %s impossible (%s) — journal poursuivi sans rotation "
                    "(une autre instance de Docia le tient ouvert ?)",
                    self.baseFilename,
                    exc,
                )


def open_journal(target: Path) -> RotatingFileHandler | None:
    """Ouvre `docia.log`, ou `docia-<pid>.log` si le premier est inaccessible.

    Sous Windows, `docia.log` peut être verrouillé par une autre instance : plutôt
    que de renoncer au journal, on en ouvre un par processus, au même endroit.
    """
    per_process = target.with_name(f"{target.stem}-{os.getpid()}{target.suffix}")
    for candidate in (target, per_process):
        try:
            return RotatingFileHandler(
                candidate, maxBytes=_LOG_MAX_BYTES, backupCount=_LOG_BACKUPS, encoding="utf-8"
            )
        except OSError as exc:  # dossier en lecture seule, disque plein, fichier verrouillé
            logging.getLogger(__name__).warning(
                "journal sur disque impossible (%s) : %s", candidate, exc
            )
    return None


def setup_logging(args: argparse.Namespace) -> Path | None:
    """Console lisible + journal détaillé sur disque (traces complètes, rotation).

    Les niveaux sont distincts, sans quoi le fichier ne contiendrait rien de plus
    que l'écran alors qu'il s'annonce « journal détaillé » : la racine est en
    `DEBUG` (elle laisse tout passer), la console filtre à `INFO` (`DEBUG` avec
    `-v`) et le fichier prend tout. Les bibliothèques tierces bavardes sont
    muselées nommément (`NOISY_DEBUG_LOGGERS`).

    Idempotent : la fenêtre rappelle `main()` pour produire ses documents, il ne faut
    ni doubler les messages ni rouvrir le journal.
    """
    global _JOURNAL, _LOGGING_CONFIGURED
    if _LOGGING_CONFIGURED:
        return _JOURNAL
    _LOGGING_CONFIGURED = True
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    console = logging.StreamHandler(stream=sys.stderr)
    console.setLevel(logging.DEBUG if args.verbose else logging.INFO)
    console.setFormatter(ConsoleFormatter("%(levelname)s %(name)s : %(message)s"))
    console.addFilter(lambda record: not getattr(record, JOURNAL_ONLY, False))
    root.addHandler(console)
    for name in NOISY_DEBUG_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING if name == "httpx" else logging.INFO)
    journal = open_journal(log_file(getattr(args, "config", None)))
    if journal is None:  # la console suffit : jamais de plantage pour un journal
        return None
    journal.setLevel(logging.DEBUG)
    journal.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    root.addHandler(journal)
    _JOURNAL = Path(journal.baseFilename)
    return _JOURNAL


def silence_third_party_warnings() -> None:
    """Coupe les avertissements openpyxl sans conséquence (« Data Validation extension
    is not supported »), qui inquiétaient l'utilisateur dans la console de l'exe.

    DocFuse ne les filtre volontairement plus à l'import : une bibliothèque n'a pas à
    modifier la politique d'avertissements de son hôte. C'est donc au point d'entrée
    applicatif de le faire — ici, une fois, au démarrage.
    """
    try:
        from docfuse.extractors.xlsx import silence_openpyxl_warnings
    except (ImportError, AttributeError):  # DocFuse absent ou antérieur à cette API
        return
    with contextlib.suppress(Exception):
        silence_openpyxl_warnings()


def expected_failure(exc: BaseException) -> bool:
    """Vrai pour les pannes que l'utilisateur peut comprendre et corriger seul.

    L'import de `docia.service` n'a lieu qu'ici, sur le chemin d'erreur : `docia
    init` n'a pas à payer le chargement du pipeline pour un garde-fou.

    `UnicodeDecodeError` en fait partie : un prompt enregistré en « ANSI » par le
    Bloc-notes, un CSV exporté en Latin-1, et l'utilisateur recevait vingt-deux
    lignes de trace Python pour un fichier qu'il lui suffit de réenregistrer en
    UTF-8. C'est bien une panne qu'il peut comprendre et corriger seul.
    (Il hérite de `ValueError`, pas d'`OSError` : il n'était donc pas couvert.)
    """
    from docia.service import ServiceError

    return isinstance(exc, OSError | sqlite3.Error | ServiceError | UnicodeDecodeError)


def report_failure(command: str, exc: BaseException) -> None:
    """Une ligne pour l'utilisateur, la trace complète pour `docia.log` — et rien d'autre."""
    logging.getLogger(__name__).error(
        "échec de « docia %s »", command, exc_info=exc, extra={JOURNAL_ONLY: True}
    )
    detail = str(exc).strip().splitlines()
    message = detail[0] if detail else type(exc).__name__
    suffix = f" — détail dans {_JOURNAL}" if _JOURNAL is not None else ""
    print(f"échec de « docia {command} » : {message}{suffix}", file=sys.stderr)
