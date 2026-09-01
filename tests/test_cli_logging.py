"""Journalisation de la CLI : console lisible, détail complet sur disque.

Une campagne réelle rencontre toujours des fichiers illisibles ; sans ce partage,
l'utilisateur voit défiler des dizaines de traces Python et croit à un plantage.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

import docia.cli as cli


@pytest.fixture
def racine_propre() -> Iterator[logging.Logger]:
    """Isole le logger racine (pytest y installe ses propres gestionnaires)."""
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    root.handlers = []
    cli._JOURNAL, cli._LOGGING_CONFIGURED = None, False
    try:
        yield root
    finally:
        for h in root.handlers:
            h.close()
        root.handlers, root.level = handlers, level
        cli._JOURNAL, cli._LOGGING_CONFIGURED = None, False


def _args(tmp_path: Path) -> argparse.Namespace:
    return argparse.Namespace(verbose=False, config=tmp_path / "docia.toml")


def test_console_sans_pile_journal_avec_pile(
    racine_propre: logging.Logger, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    journal = cli._setup_logging(_args(tmp_path))
    assert journal == tmp_path / "docia.log"

    try:
        raise ValueError("mail illisible")
    except ValueError:
        logging.getLogger("docfuse.extractors.msg").warning(
            "Erreur extraction MSG %s", "D:\\part\\note.msg", exc_info=True
        )
    for handler in racine_propre.handlers:
        handler.flush()

    console = capsys.readouterr().err
    assert "Erreur extraction MSG D:\\part\\note.msg" in console
    assert "Traceback" not in console, "la pile d'appels n'a rien à faire dans la console"
    assert console.strip().count("\n") == 0, "un incident = une ligne"

    contenu = journal.read_text(encoding="utf-8")
    assert "Erreur extraction MSG" in contenu
    assert "Traceback (most recent call last)" in contenu
    assert "ValueError: mail illisible" in contenu


def test_configuration_idempotente(racine_propre: logging.Logger, tmp_path: Path) -> None:
    """La fenêtre rappelle `main()` pour produire ses documents : pas de doublons."""
    first = cli._setup_logging(_args(tmp_path))
    count = len(racine_propre.handlers)
    assert cli._setup_logging(_args(tmp_path)) == first
    assert len(racine_propre.handlers) == count


def test_journal_impossible_ne_bloque_pas(
    racine_propre: logging.Logger, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Dossier en lecture seule : on garde la console, on ne plante pas."""
    monkeypatch.setattr(
        cli, "_log_file", lambda _c: tmp_path / "inexistant" / "sous-dossier" / "docia.log"
    )
    monkeypatch.setattr(Path, "resolve", Path.absolute)
    assert cli._setup_logging(_args(tmp_path)) is None
    assert racine_propre.handlers, "la console reste branchée"


def test_journal_verrouille_bascule_sur_un_fichier_par_processus(
    racine_propre: logging.Logger, tmp_path: Path
) -> None:
    """`docia.log` inaccessible (verrou Windows) : on écrit `docia-<pid>.log`, pas rien."""
    (tmp_path / "docia.log").mkdir()  # ouverture impossible, dossier pourtant inscriptible
    journal = cli._setup_logging(_args(tmp_path))
    assert journal == tmp_path / f"docia-{os.getpid()}.log"
    logging.getLogger("docia.essai").info("import terminé")
    for handler in racine_propre.handlers:
        handler.flush()
    assert "import terminé" in journal.read_text(encoding="utf-8")


def test_rotation_impossible_nentraine_ni_perte_ni_traces(
    racine_propre: logging.Logger,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Deux instances sous Windows : le fichier ne peut pas être renommé.

    Reproduction fidèle (sémantique Windows simulée sur le renommage) : avec le
    gestionnaire standard, 20 des 60 messages seulement étaient écrits et
    80 traces `PermissionError` étaient déversées sur stderr. Ici : les 60
    messages sont dans le fichier, la console reste propre, et l'incident n'est
    signalé qu'une fois.
    """
    monkeypatch.setattr(cli, "_LOG_MAX_BYTES", 2_000)
    journal = cli._setup_logging(_args(tmp_path))
    assert journal is not None
    handler = next(h for h in racine_propre.handlers if isinstance(h, cli._RotatingFileHandler))

    def renommage_refuse(_source: str, _dest: str) -> None:
        raise PermissionError(
            32, "The process cannot access the file because it is being used by another process"
        )

    handler.rotator = renommage_refuse
    for i in range(60):
        logging.getLogger("docia.essai").info("message %02d %s", i, "x" * 120)
    for h in racine_propre.handlers:
        h.flush()

    console = capsys.readouterr().err
    assert "Traceback" not in console, "une rotation impossible ne déverse pas de pile"
    contenu = journal.read_text(encoding="utf-8")
    manquants = [i for i in range(60) if f"message {i:02d}" not in contenu]
    assert manquants == [], f"enregistrements perdus : {manquants}"
    assert contenu.count("rotation de") == 1, "l'incident n'est signalé qu'une fois"
    assert handler.maxBytes == 0, "la rotation est abandonnée pour ce processus"
    assert list(tmp_path.glob("docia.log.*")) == []


def test_journal_a_cote_de_l_executable_quand_il_est_fige(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Exe empaqueté : `docia.log` est à côté de `Docia.exe`, pas dans le répertoire courant.

    `--config` vaut par défaut le **relatif** `docia.toml` : en dériver le chemin
    du journal le plaçait là d'où l'utilisateur a lancé l'exe.
    """
    exe = tmp_path / "programme" / "Docia.exe"
    exe.parent.mkdir()
    exe.write_bytes(b"")
    ailleurs = tmp_path / "cwd"
    ailleurs.mkdir()
    monkeypatch.chdir(ailleurs)
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    monkeypatch.setattr(sys, "executable", str(exe))
    assert cli._log_file(Path("docia.toml")) == exe.parent / "docia.log"


def test_journal_a_cote_de_la_config_hors_executable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Hors exe : le journal suit le `--config` résolu, même relatif."""
    monkeypatch.delattr(sys, "frozen", raising=False)
    monkeypatch.chdir(tmp_path)
    assert cli._log_file(Path("docia.toml")) == tmp_path / "docia.log"
    autre = tmp_path / "campagne"
    autre.mkdir()
    assert cli._log_file(autre / "docia.toml") == autre / "docia.log"


def test_journal_plus_detaille_que_la_console(
    racine_propre: logging.Logger, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Le fichier s'annonce « journal détaillé » : il doit contenir plus que l'écran."""
    journal = cli._setup_logging(_args(tmp_path))
    assert journal is not None
    logging.getLogger("docia.essai").debug("détail interne %s", 42)
    logging.getLogger("docia.essai").info("visible partout")
    for handler in racine_propre.handlers:
        handler.flush()

    console = capsys.readouterr().err
    assert "visible partout" in console
    assert "détail interne 42" not in console, "le DEBUG n'a rien à faire à l'écran sans -v"
    contenu = journal.read_text(encoding="utf-8")
    assert "détail interne 42" in contenu, "le journal n'est pas plus détaillé que la console"
    assert "visible partout" in contenu


def test_verbose_descend_le_debug_a_la_console(
    racine_propre: logging.Logger,  # noqa: ARG001 - remet la journalisation à zéro
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    args = _args(tmp_path)
    args.verbose = True
    cli._setup_logging(args)
    logging.getLogger("docia.essai").debug("détail interne")
    assert "détail interne" in capsys.readouterr().err


def test_bibliotheques_bavardes_muselees(
    racine_propre: logging.Logger,  # noqa: ARG001 - remet la journalisation à zéro
    tmp_path: Path,
) -> None:
    """Racine en DEBUG : sans muselière, un import de 934 028 lignes noierait le journal."""
    cli._setup_logging(_args(tmp_path))
    for nom in cli.NOISY_DEBUG_LOGGERS:
        assert logging.getLogger(nom).level >= logging.INFO, nom


def test_main_rend_une_ligne_sur_base_en_lecture_seule(
    racine_propre: logging.Logger,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Scénario « exe posé dans C:\\Program Files » : la base ne peut pas être écrite.

    Sans garde-fou, `main()` laissait remonter le
    `sqlite3.OperationalError: attempt to write a readonly database` levé par
    `PRAGMA journal_mode=WAL` — vingt lignes de trace Python pour l'utilisateur.
    """
    import sqlite3

    import docia.db

    dossier_journal = tmp_path / "journal"
    dossier_journal.mkdir()
    monkeypatch.setattr(cli, "_log_file", lambda _c: dossier_journal / "docia.log")

    # On reproduit l'erreur que SQLite lève réellement, au lieu de verrouiller un
    # dossier : `chmod(0o555)` n'interdit rien sous Windows, qui ignore les bits
    # POSIX sur les répertoires. Le test passait donc sous Linux et échouait sur la
    # plateforme cible du produit — alors que ce qu'il vérifie, le garde-fou de
    # `main()`, ne dépend pas du système de fichiers.
    def refuse(_self: object, _path: object) -> None:
        raise sqlite3.OperationalError("attempt to write a readonly database")

    monkeypatch.setattr(docia.db.Database, "__init__", refuse)
    code = cli.main(["--db", str(tmp_path / "programme" / "docia.sqlite"), "status"])
    for handler in racine_propre.handlers:
        handler.flush()

    assert code == 1
    erreur = capsys.readouterr().err
    utiles = [ligne for ligne in erreur.splitlines() if "échec" in ligne or "readonly" in ligne]
    assert "Traceback" not in erreur, "la pile n'a rien à faire à l'écran"
    assert len(utiles) == 1, f"une seule ligne attendue, reçu : {utiles}"
    assert utiles[0].startswith("échec de « docia status » : attempt to write a readonly database")
    assert str(dossier_journal / "docia.log") in utiles[0], "l'utilisateur sait où regarder"
    contenu = (dossier_journal / "docia.log").read_text(encoding="utf-8")
    assert "Traceback (most recent call last)" in contenu
    assert "sqlite3.OperationalError" in contenu


def test_main_laisse_remonter_ce_quil_ne_comprend_pas(
    racine_propre: logging.Logger,  # noqa: ARG001 - remet la journalisation à zéro
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Le garde-fou couvre les pannes prévisibles, pas les anomalies : rien n'est masqué."""

    def explose(_args: argparse.Namespace) -> int:
        raise RuntimeError("anomalie inattendue")

    monkeypatch.setattr(cli, "_dispatch", explose)
    monkeypatch.setattr(cli, "_log_file", lambda _c: tmp_path / "docia.log")
    with pytest.raises(RuntimeError, match="anomalie inattendue"):
        cli.main(["status"])


def test_avertissements_openpyxl_filtres_au_demarrage() -> None:
    """DocFuse ne filtre plus à l'import : c'est au point d'entrée de le faire.

    Sans cet appel, « Data Validation extension is not supported » revient polluer
    la console de l'exe à chaque classeur lu — un des bruits signalés en production.
    """
    import warnings

    with warnings.catch_warnings():
        warnings.resetwarnings()
        cli._silence_third_party_warnings()
        assert any(
            f[0] == "ignore"
            and f[2] is UserWarning
            and f[3] is not None
            and f[3].pattern.startswith("openpyxl")
            for f in warnings.filters
        ), warnings.filters
