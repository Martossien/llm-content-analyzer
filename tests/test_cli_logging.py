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

    # La campagne doit **exister** : depuis que les commandes de lecture exigent une
    # base présente (`cli._require_existing_campaign`), une base absente sort en
    # amont, sur un tout autre message. Ce que ce test vérifie est le garde-fou du
    # cas « la base est là mais refuse d'être écrite ».
    base = tmp_path / "programme" / "docia.sqlite"
    docia.db.Database(base).close()

    # On reproduit l'erreur que SQLite lève réellement, au lieu de verrouiller un
    # dossier : `chmod(0o555)` n'interdit rien sous Windows, qui ignore les bits
    # POSIX sur les répertoires. Le test passait donc sous Linux et échouait sur la
    # plateforme cible du produit — alors que ce qu'il vérifie, le garde-fou de
    # `main()`, ne dépend pas du système de fichiers.
    def refuse(_self: object, _path: object) -> None:
        raise sqlite3.OperationalError("attempt to write a readonly database")

    monkeypatch.setattr(docia.db.Database, "__init__", refuse)
    code = cli.main(["--db", str(base), "status"])
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


# ------------------- une base absente ne se fabrique pas toute seule


def test_une_faute_de_frappe_dans_db_ne_fabrique_pas_une_campagne(
    racine_propre: logging.Logger,  # noqa: ARG001 - remet la journalisation à zéro
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`--db /chemin/qui/nexiste/pas` : code 1, et **rien** n'est créé sur le disque.

    Avant : `Database` créait le dossier *et* une base vide de 180 Ko, puis
    `docia status` annonçait « 0 fichier » et `docia report` « 0 fichier sensible »
    — en code retour **0**, sur une campagne inventée de toutes pièces. Pour un
    outil dont la sortie justifie des suppressions de fichiers dans un organisme
    public, un rapport rassurant livré en succès sur une base qui n'existe pas est
    le pire résultat possible.
    """
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli, "_log_file", lambda _c: tmp_path / "docia.log")
    fantome = tmp_path / "nexistepas_audit" / "abc.sqlite"

    for commande in (
        ["status"],
        ["status", "--json"],
        ["report", "--format", "html", "--out", str(tmp_path / "rapport.html")],
        ["export", "--format", "csv", "--out", str(tmp_path / "export.csv")],
        ["plan"],
        ["retry"],
        ["review", "1", "--status", "validated"],
        ["backup"],
    ):
        capsys.readouterr()
        assert cli.main(["--db", str(fantome), *commande]) == 1, commande
        erreur = capsys.readouterr().err
        assert "campagne introuvable" in erreur, commande
        assert str(fantome) in erreur, "l'utilisateur voit le chemin qu'il a tapé"
        assert not fantome.exists(), f"{commande} a fabriqué la base"
        assert not fantome.parent.exists(), f"{commande} a fabriqué le dossier"
    assert not (tmp_path / "rapport.html").exists(), "aucun rapport rassurant n'est écrit"
    assert not (tmp_path / "export.csv").exists()


def test_une_base_etrangere_nest_pas_greffee_de_tables_docia(
    racine_propre: logging.Logger,  # noqa: ARG001 - remet la journalisation à zéro
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Un SQLite d'un autre logiciel reste intact : refus, pas de greffe."""
    import sqlite3

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli, "_log_file", lambda _c: tmp_path / "docia.log")
    etrangere = tmp_path / "contacts.sqlite"
    con = sqlite3.connect(etrangere)
    con.execute("CREATE TABLE contacts (nom TEXT)")
    con.execute("INSERT INTO contacts VALUES ('Dupont')")
    con.commit()
    con.close()
    avant = etrangere.read_bytes()

    assert cli.main(["--db", str(etrangere), "status"]) == 1
    assert "n'est pas une campagne docia" in capsys.readouterr().err
    assert etrangere.read_bytes() == avant, "la base d'un autre logiciel n'a pas bougé"


def test_les_commandes_qui_creent_la_base_gardent_ce_droit(
    racine_propre: logging.Logger,  # noqa: ARG001 - remet la journalisation à zéro
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Le contrôle ne vise que la lecture : `init` crée toujours sa campagne.

    `ingest`, `scan` et `restore` sont dispensés pour la même raison (voir
    `cli.UNCHECKED_COMMANDS`) ; `campaigns` n'ouvre pas `cfg.db_path` du tout.
    """
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli, "_log_file", lambda _c: tmp_path / "docia.log")
    neuve = tmp_path / "neuve" / "campagne.sqlite"
    assert cli.main(["--db", str(neuve), "init"]) == 0
    assert neuve.exists()
    assert cli.main(["--db", str(tmp_path / "absente.sqlite"), "campaigns"]) == 0

    # et une fois la campagne créée, les commandes de lecture repassent
    assert cli.main(["--db", str(neuve), "status"]) == 0


def test_unicode_decode_error_est_une_panne_previsible() -> None:
    """Un prompt enregistré en « ANSI » par le Bloc-notes : une ligne, pas 22.

    `UnicodeDecodeError` hérite de `ValueError`, pas d'`OSError` : il échappait au
    garde-fou de `main()` et sortait en trace Python complète.
    """
    faute = UnicodeDecodeError("utf-8", b"\xe9", 0, 1, "invalid continuation byte")
    assert cli._expected_failure(faute)
    assert not cli._expected_failure(ValueError("autre chose")), "on ne masque pas tout"


def test_le_journal_nomme_la_campagne(
    racine_propre: logging.Logger,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`docia.log` est unique pour tout le poste : chaque ligne dit de quelle base elle parle.

    `grep campagne.sqlite docia.log` ne rendait rien — relire un incident revenait
    à deviner à laquelle des campagnes du poste appartenaient les lignes.
    """
    monkeypatch.chdir(tmp_path)
    journal = tmp_path / "docia.log"
    monkeypatch.setattr(cli, "_log_file", lambda _c: journal)
    campagne = tmp_path / "campagne.sqlite"

    assert cli.main(["--db", str(campagne), "init"]) == 0
    assert cli.main(["--db", str(campagne), "status"]) == 0
    for handler in racine_propre.handlers:
        handler.flush()

    lignes = [
        ligne for ligne in journal.read_text(encoding="utf-8").splitlines() if "campagne" in ligne
    ]
    assert lignes, "le journal ne nomme aucune campagne"
    assert any(str(campagne) in ligne for ligne in lignes), lignes
    assert any("docia status" in ligne for ligne in lignes), lignes


# ------------------- la panne la plus grave doit rendre le code le plus dur


def test_serveur_llm_eteint_sort_en_code_1_et_non_2(
    racine_propre: logging.Logger,  # noqa: ARG001 - remet la journalisation à zéro
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Serveur LLM éteint : 0 fichier traité — c'est un run **raté**, pas partiel.

    `cli` documente « 1 erreur (config, base, LLM injoignable), 2 erreurs partielles
    — les résultats obtenus sont persistés ». Mesuré serveur éteint : **2**. La
    condition du code 1 exigeait `blocks_built`, précisément nul dans cette
    panne-là, puisque le contrôle de santé coupe avant toute construction de bloc.
    La panne totale rendait donc le code le plus doux, et une supervision qui
    tolère le 2 acceptait en silence un run qui n'avait rien fait.
    """
    import socket

    from docia.config import Config
    from docia.db import Database
    from docia.models import SmbeagleRow

    # Un port fermé : le contrôle de santé échoue tout de suite, sans attente réseau.
    with socket.socket() as sonde:
        sonde.bind(("127.0.0.1", 0))
        port = sonde.getsockname()[1]

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli, "_log_file", lambda _c: tmp_path / "docia.log")
    cfg = Config(db_path=str(tmp_path / "campagne.sqlite"))
    cfg.llm.base_url = f"http://127.0.0.1:{port}/v1"
    cfg.llm.timeout_s = 5
    cfg.llm.max_retries = 1
    with Database(cfg.db_path) as db:
        scan = db.start_scan("scan.csv")
        db.upsert_files(
            [
                SmbeagleRow(
                    name="note.txt",
                    host="srv",
                    extension="txt",
                    username="u",
                    hostname="srv.dom",
                    unc_directory="\\\\srv\\part\\dossier",
                    creation_time="01/01/2020 10:00:00",
                    last_write_time="01/01/2026 10:00:00",
                    readable=True,
                    writeable=False,
                    deletable=False,
                    directory_type="SMB",
                    base="\\\\srv\\part",
                    file_size=1000,
                    access_time="02/01/2026 10:00:00",
                    file_attributes="Archive",
                    owner="DOM\\alice",
                    fast_hash="h1",
                    file_signature="unknown",
                )
            ],
            scan,
        )
        db.finish_scan(scan, total=1, new=1, updated=0, unchanged=0, invalid=0)

    args = argparse.Namespace(limit=None, dry_run=False)
    code = cli.cmd_run(args, cfg)
    sortie = capsys.readouterr()
    assert "injoignable" in sortie.err, sortie.err
    assert "0 analysés" in sortie.out, sortie.out
    assert code == 1, "un run qui n'a rien fait ne s'annonce pas comme partiellement réussi"


# ------------------- `docia backup --out` : ne pas compter dans le mauvais dossier


def test_backup_out_nannonce_pas_le_compte_dun_autre_dossier(
    racine_propre: logging.Logger,  # noqa: ARG001 - remet la journalisation à zéro
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`--out AUTRE` : la sauvegarde et la rotation vont dans AUTRE — le compte aussi.

    `list_backups` ne sait regarder que `<base>.backups` : après `--out`, le
    « N sauvegarde(s) conservée(s) » annoncé était celui d'un **autre** dossier, et
    nommait un dossier où l'utilisateur ne trouverait pas la copie qu'il vient de
    prendre. Mieux vaut ne rien compter que compter faux.
    """
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli, "_log_file", lambda _c: tmp_path / "docia.log")
    base = tmp_path / "campagne.sqlite"
    assert cli.main(["--db", str(base), "init"]) == 0

    # deux copies dans le dossier par défaut, pour que le mauvais compte soit visible
    capsys.readouterr()
    assert cli.main(["--db", str(base), "backup"]) == 0
    assert cli.main(["--db", str(base), "backup"]) == 0
    defaut = capsys.readouterr().out
    assert "2 sauvegarde(s) conservée(s)" in defaut
    assert str(base.with_name(base.name + ".backups")) in defaut

    ailleurs = tmp_path / "coffre"
    assert cli.main(["--db", str(base), "backup", "--out", str(ailleurs)]) == 0
    sortie = capsys.readouterr().out
    assert str(ailleurs) in sortie, "l'utilisateur sait où la copie est partie"
    assert "sauvegarde(s) conservée(s)" not in sortie, "aucun compte d'un autre dossier"
    assert len(list(ailleurs.glob("*.sqlite"))) == 1


def test_backup_keep_laisse_la_rotation_au_service(
    racine_propre: logging.Logger,  # noqa: ARG001 - remet la journalisation à zéro
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`--keep` non précisé : c'est `service.DEFAULT_KEEP_BACKUPS` qui décide, lui seul.

    La CLI redéfinissait sa propre constante. Deux valeurs pour la rotation, c'est
    une campagne effacée le jour où l'une des deux bouge sans l'autre.
    """
    from docia.service import DEFAULT_KEEP_BACKUPS

    assert not hasattr(cli, "DEFAULT_KEEP_BACKUPS"), "plus de seconde source de vérité"
    assert cli.build_parser().parse_args(["backup"]).keep is None

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli, "_log_file", lambda _c: tmp_path / "docia.log")
    base = tmp_path / "campagne.sqlite"
    assert cli.main(["--db", str(base), "init"]) == 0
    capsys.readouterr()
    for _ in range(DEFAULT_KEEP_BACKUPS + 2):
        assert cli.main(["--db", str(base), "backup"]) == 0
    gardees = list((base.with_name(base.name + ".backups")).glob("*.sqlite"))
    assert len(gardees) == DEFAULT_KEEP_BACKUPS

    # et `--keep` explicite reste respecté
    assert cli.main(["--db", str(base), "backup", "--keep", "3"]) == 0
    assert len(list((base.with_name(base.name + ".backups")).glob("*.sqlite"))) == 3
