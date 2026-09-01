"""Tests du parseur CSV SMBeagle et de l'import en base."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

import pytest

from docia.db import FILES_INDEXES, Database
from docia.ingest.smbeagle_csv import (
    SUSPECT_ZERO_MIN,
    CsvLineError,
    ImportProgress,
    import_csv,
    parse_line,
    parse_smbeagle_datetime,
    read_smbeagle_csv,
    split_csv_line,
    validate_header,
)
from docia.models import FileStatus, SmbeagleRow

FIXTURE = Path(__file__).parent / "fixtures" / "scan_local_mini.csv"

HEADER_LINE = (
    "Name,Host,Extension,Username,Hostname,UNCDirectory,CreationTime,LastWriteTime,"
    "Readable,Writeable,Deletable,DirectoryType,Base,FileSize,AccessTime,FileAttributes,"
    "Owner,FastHash,FileSignature"
)

REAL_LINE = (
    r"fxsext.ecf,192.168.1.72,ecf,martos,DESKTOP-N3NNKHT.WORKGROUP,\\192.168.1.72\admin$\addins,"
    r"07/12/2019 15:51:42,18/04/2019 20:49:00,True,False,False,SMB,\\192.168.1.72\admin$\,802,"
    r"15/08/2022 10:28:52,Archive,NT SERVICE\TrustedInstaller,c3fa820ff5e877f1,unknown"
)


def quoted_line(name: str, *, file_size: str = "802", base: str = r'"\\srv\part$\"') -> str:
    """Ligne 19 colonnes avec les guillemets sélectifs réels de SMBeagle."""
    return ",".join(
        [
            name,
            '"192.168.1.72"',
            '"pdf"',
            '"martos"',
            '"DESKTOP-N3NNKHT.WORKGROUP"',
            r'"\\srv\part$\docs"',
            "07/12/2019 15:51:42",
            "18/04/2019 20:49:00",
            "True",
            "False",
            "False",
            "SMB",
            base,
            file_size,
            "15/08/2022 10:28:52",
            '"Archive"',
            r'"NT SERVICE\TrustedInstaller"',
            '"c3fa820ff5e877f1"',
            '"unknown"',
        ]
    )


# ------------------------------------------------------------------- découpage


def test_parse_line_ligne_reelle() -> None:
    row = parse_line(REAL_LINE, 2)
    assert row.name == "fxsext.ecf"
    assert row.host == "192.168.1.72"
    assert row.extension == "ecf"  # minuscules, sans point
    assert row.unc_directory == r"\\192.168.1.72\admin$\addins"
    assert row.last_write_time == "18/04/2019 20:49:00"
    assert row.readable is True
    assert row.writeable is False
    assert row.deletable is False
    assert row.file_size == 802
    assert row.owner == r"NT SERVICE\TrustedInstaller"
    assert row.fast_hash == "c3fa820ff5e877f1"
    assert row.file_signature == "unknown"
    assert row.path == r"\\192.168.1.72\admin$\addins\fxsext.ecf"


def test_virgule_dans_champ_quote() -> None:
    row = parse_line(quoted_line('"rapport, final, v2.pdf"'), 2)
    assert row.name == "rapport, final, v2.pdf"
    assert row.file_size == 802
    assert row.file_signature == "unknown"


def test_guillemet_echappe_serilog() -> None:
    row = parse_line(quoted_line(r'"note \"urgente\".pdf"'), 2)
    assert row.name == 'note "urgente".pdf'


def test_guillemet_echappe_rfc4180() -> None:
    row = parse_line(quoted_line('"note ""urgente"".pdf"'), 2)
    assert row.name == 'note "urgente".pdf'


def test_antislash_final_avant_guillemet_fermant() -> None:
    """`"...part$\\"` est un chemin, pas un guillemet échappé."""
    row = parse_line(quoted_line('"a.pdf"'), 2)
    assert row.base == r"\\srv\part$" + "\\"


def test_champ_quote_vide() -> None:
    fields = split_csv_line('"a","","c"')
    assert fields == ["a", "", "c"]


def test_ligne_18_champs() -> None:
    tronquee = ",".join(REAL_LINE.split(",")[:-1])
    with pytest.raises(ValueError, match="18 champs au lieu de 19"):
        parse_line(tronquee, 7)


def test_filesize_non_entier() -> None:
    ligne = quoted_line('"a.pdf"', file_size="n/a")
    assert parse_line(ligne, 2).file_size == 0
    with pytest.raises(ValueError, match="FileSize"):
        parse_line(ligne, 2, strict=True)


def test_booleens_insensibles_a_la_casse() -> None:
    ligne = REAL_LINE.replace("True,False,False", "TRUE,true,FALSE")
    row = parse_line(ligne, 2)
    assert (row.readable, row.writeable, row.deletable) == (True, True, False)


# ---------------------------------------------------------------------- en-tête


def test_validate_header_ok() -> None:
    assert validate_header(HEADER_LINE) == []


def test_validate_header_invalide() -> None:
    errors = validate_header(HEADER_LINE.replace("Username", "User"))
    assert len(errors) == 1
    assert "colonne 3" in errors[0]


def test_validate_header_colonnes_manquantes() -> None:
    errors = validate_header("Name,Host,Extension")
    assert errors == ["en-tête : 3 colonnes au lieu de 19"]


def test_header_invalide_stoppe_la_lecture_dans_les_deux_modes(tmp_path: Path) -> None:
    """Le mode tolérant l'est pour les lignes, jamais pour la structure."""
    csv = tmp_path / "mauvais.csv"
    csv.write_text("a,b,c\n" + REAL_LINE + "\n", encoding="utf-8")
    for strict in (True, False):
        items = list(read_smbeagle_csv(csv, strict=strict))
        assert len(items) == 1
        assert isinstance(items[0], CsvLineError)
        assert items[0].line_number == 1


def test_entete_decale_arrete_l_import_tolerant(tmp_path: Path) -> None:
    """MOYEN 11b : deux colonnes interverties ne doivent pas être lues « au mieux ».

    Avant : l'import tolérant (défaut de la fenêtre et de `docia scan`) continuait
    avec les colonnes aux mauvaises positions — `AccessTime` lu comme `FileSize`,
    donc toutes les tailles à 0 et toutes les dates dans les tailles, pour le seul
    signal d'« une ligne invalide » noyée dans le journal.
    """
    decale = HEADER_LINE.replace("FileSize,AccessTime", "AccessTime,FileSize")
    ligne = quoted_line('"a.pdf"').replace(",802,15/08/2022 10:28:52,", ",15/08/2022 10:28:52,802,")
    csv = tmp_path / "decale.csv"
    csv.write_text(decale + "\n" + ligne + "\n" + ligne + "\n", encoding="utf-8")
    with Database(tmp_path / "docia.sqlite") as db:
        report = import_csv(db, csv, strict=False)
    assert report.total == 0  # aucune ligne lue avec des colonnes décalées
    assert report.invalid == 1
    assert "colonne 13" in report.errors[0].reason
    assert report.errors[0].line_number == 1


# ------------------------------------------------------------------- lecture


def test_bom_et_crlf(tmp_path: Path) -> None:
    csv = tmp_path / "bom.csv"
    contenu = HEADER_LINE + "\r\n" + REAL_LINE + "\r\n\r\n"
    csv.write_bytes(b"\xef\xbb\xbf" + contenu.encode("utf-8"))
    items = list(read_smbeagle_csv(csv))
    assert len(items) == 1  # la ligne vide est ignorée
    row = items[0]
    assert isinstance(row, SmbeagleRow)
    assert row.name == "fxsext.ecf"


def test_lecture_fixture_complete() -> None:
    items = list(read_smbeagle_csv(FIXTURE))
    assert len(items) == 63
    assert all(isinstance(item, SmbeagleRow) for item in items)


def test_lignes_invalides_signalees(tmp_path: Path) -> None:
    csv = tmp_path / "mixte.csv"
    csv.write_text(
        HEADER_LINE + "\n" + REAL_LINE + "\n" + "a,b,c\n" + REAL_LINE + "\n", encoding="utf-8"
    )
    items = list(read_smbeagle_csv(csv))
    erreurs = [item for item in items if isinstance(item, CsvLineError)]
    assert len(erreurs) == 1
    assert erreurs[0].line_number == 3
    assert "3 champs au lieu de 19" in erreurs[0].reason


# ------------------------------------------------------------------- datetime


@pytest.mark.parametrize(
    ("texte", "attendu"),
    [
        ("18/04/2019 20:49:00", datetime(2019, 4, 18, 20, 49, 0)),
        ("12/25/2024 08:30:00", datetime(2024, 12, 25, 8, 30, 0)),
        ("2024-12-12 17:10:23", datetime(2024, 12, 12, 17, 10, 23)),
        ("2024-12-12T17:10:23", datetime(2024, 12, 12, 17, 10, 23)),
    ],
)
def test_parse_smbeagle_datetime(texte: str, attendu: datetime) -> None:
    assert parse_smbeagle_datetime(texte) == attendu


@pytest.mark.parametrize("texte", ["", "   ", "n/a", "32/13/2024 00:00:00"])
def test_parse_smbeagle_datetime_illisible(texte: str) -> None:
    assert parse_smbeagle_datetime(texte) is None


# --------------------------------------------------------------------- import


def test_import_csv_nouveaux(tmp_path: Path) -> None:
    with Database(tmp_path / "docia.sqlite") as db:
        report = import_csv(db, FIXTURE)
        assert (report.total, report.new, report.updated, report.unchanged) == (63, 63, 0, 0)
        assert report.invalid == 0
        assert report.errors == []
        assert db.counts()["files"] == 63


def test_reimport_inchange(tmp_path: Path) -> None:
    with Database(tmp_path / "docia.sqlite") as db:
        import_csv(db, FIXTURE)
        report = import_csv(db, FIXTURE)
        assert (report.new, report.updated, report.unchanged) == (0, 0, 63)
        assert db.counts()["files"] == 63


def test_import_path_reconstruit(tmp_path: Path) -> None:
    with Database(tmp_path / "docia.sqlite") as db:
        import_csv(db, FIXTURE)
        paths = {row.path for row in list(db.iter_files())}
        assert r"\\192.168.1.72\admin$\addins\fxsext.ecf" in paths


def test_fast_hash_modifie_incremente_content_version(tmp_path: Path) -> None:
    modifie = tmp_path / "scan_v2.csv"
    lignes = FIXTURE.read_text(encoding="utf-8").splitlines()
    lignes[1] = lignes[1].replace("c3fa820ff5e877f1", "0000000000000000")
    modifie.write_text("\n".join(lignes) + "\n", encoding="utf-8")

    with Database(tmp_path / "docia.sqlite") as db:
        import_csv(db, FIXTURE)
        fichiers = list(db.iter_files())
        cible = next(row for row in fichiers if row.path.endswith(r"\addins\fxsext.ecf"))
        db.set_file_status(cible.id, FileStatus.DONE)

        report = import_csv(db, modifie)
        assert (report.new, report.updated, report.unchanged) == (0, 1, 62)

        apres = db.get_file(cible.id)
        assert apres is not None
        assert apres.content_version == 2
        assert apres.status == FileStatus.PENDING
        assert apres.fast_hash == "0000000000000000"


# ---------------------------------------------------- progression et index


def test_import_appelle_le_rappel_de_progression(tmp_path: Path) -> None:
    """Le rappel reçoit des valeurs croissantes et finit sur le total du rapport."""
    vus: list[ImportProgress] = []
    with Database(tmp_path / "docia.sqlite") as db:
        report = import_csv(db, FIXTURE, progress=vus.append, progress_every=1)

    assert len(vus) >= 2  # au moins le démarrage et la fin
    assert [p.rows for p in vus] == sorted(p.rows for p in vus)
    assert [p.bytes_read for p in vus] == sorted(p.bytes_read for p in vus)
    assert vus[0].rows == 0
    assert vus[-1].rows == report.total
    assert vus[-1].total_bytes == FIXTURE.stat().st_size
    assert vus[-1].percent == 100.0
    assert all(0.0 <= p.percent <= 100.0 for p in vus)
    assert all(p.elapsed_s >= 0.0 for p in vus)


def test_progression_par_lots(tmp_path: Path) -> None:
    """Un gros CSV donne plusieurs points d'avancement, pas un seul à la fin."""
    gros = tmp_path / "gros.csv"
    lignes = FIXTURE.read_text(encoding="utf-8").splitlines()
    entete, donnees = lignes[0], lignes[1:]
    with gros.open("w", encoding="utf-8") as fh:
        fh.write(entete + "\n")
        for i in range(300):  # ~18 900 lignes, chemins distincts
            for ligne in donnees:
                fh.write(ligne.replace("\\admin$\\", f"\\admin$\\copie{i}\\", 1) + "\n")

    vus: list[ImportProgress] = []
    with Database(tmp_path / "docia.sqlite") as db:
        report = import_csv(db, gros, strict=False, progress=vus.append, progress_every=2)
    assert report.new + report.unchanged == report.total
    assert len(vus) >= 5
    assert 0.0 < vus[len(vus) // 2].percent < 100.0
    assert vus[-1].rows == report.total


def test_import_laisse_tous_les_index_en_place(tmp_path: Path) -> None:
    """Après l'import, `files` a retrouvé ses index secondaires (chargement en masse)."""
    with Database(tmp_path / "docia.sqlite") as db:
        import_csv(db, FIXTURE)
        noms = {
            str(r[0])
            for r in db.query_values(
                "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='files'"
                " AND sql IS NOT NULL"
            )
        }
        assert noms == set(FILES_INDEXES)


def test_import_interrompu_puis_reouverture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Import interrompu en plein vol : la base rouverte a de nouveau tous ses index."""
    path = tmp_path / "docia.sqlite"
    reel = Database.upsert_files

    def explose(*_args: object, **_kwargs: object) -> tuple[int, int, int]:
        """Le processus est tué au premier lot écrit."""
        raise KeyboardInterrupt("import interrompu")

    with Database(path) as db:
        monkeypatch.setattr(Database, "upsert_files", explose)
        with pytest.raises(KeyboardInterrupt):
            import_csv(db, FIXTURE)
    monkeypatch.setattr(Database, "upsert_files", reel)

    with Database(path) as db:  # le filet d'ouverture a reconstruit ce qui manquait
        noms = {
            str(r[0])
            for r in db.query_values(
                "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='files'"
                " AND sql IS NOT NULL"
            )
        }
        assert noms == set(FILES_INDEXES)


def test_rescan_garde_les_memes_compteurs(tmp_path: Path) -> None:
    """Trois imports d'affilée : les compteurs et le contenu ne bougent plus."""
    with Database(tmp_path / "docia.sqlite") as db:
        premier = import_csv(db, FIXTURE)
        deuxieme = import_csv(db, FIXTURE)
        troisieme = import_csv(db, FIXTURE)
        assert (premier.new, premier.updated, premier.unchanged) == (63, 0, 0)
        assert (deuxieme.new, deuxieme.updated, deuxieme.unchanged) == (0, 0, 63)
        assert (troisieme.new, troisieme.updated, troisieme.unchanged) == (0, 0, 63)
        versions = {int(r[0]) for r in db.query_values("SELECT content_version FROM files")}
        assert versions == {1}
        assert db.counts()["files"] == 63


def test_rappel_de_progression_defaillant_ne_casse_pas_l_import(tmp_path: Path) -> None:
    """Un tube fermé ou une fenêtre détruite ne doit pas faire perdre l'import.

    Avant, l'exception du rappel remontait : la base restait à moitié remplie et la
    ligne `scans` orpheline, `finish_scan` n'étant jamais atteint.
    """
    appels = 0

    def rappel(_progress: object) -> None:
        nonlocal appels
        appels += 1
        raise BrokenPipeError("console fermée")

    with Database(tmp_path / "docia.sqlite") as db:
        report = import_csv(db, FIXTURE, progress=rappel)
        assert appels >= 1, "le rappel doit bien avoir été tenté"
        assert (report.total, report.new, report.invalid) == (63, 63, 0)
        assert db.counts()["files"] == 63
        scans = db.query_values("SELECT rows_total FROM scans")
        assert [int(r[0]) for r in scans] == [63], "le scan doit être clôturé, pas orphelin"


# ------------------------------------------- robustesse : une ligne ne fait pas tomber le million


def test_filesize_hors_plage_sqlite(tmp_path: Path) -> None:
    """GRAVE 2 : un `FileSize` non stockable est refusé à la lecture, pas à l'écriture.

    `int()` n'avait aucune borne : la valeur remontait intacte jusqu'à `sqlite3`,
    qui levait `OverflowError` en écrivant le **lot** — donc très loin de la ligne
    fautive, en emportant l'import entier.
    """
    enorme = quoted_line('"enorme.pdf"', file_size=str(2**63))
    with pytest.raises(ValueError, match="FileSize"):
        parse_line(enorme, 2, strict=True)
    ligne = parse_line(enorme, 2)
    assert (ligne.file_size, ligne.size_unreadable) == (0, True)

    csv = tmp_path / "overflow.csv"
    saines = [quoted_line(f'"bon{i}.pdf"') for i in range(4)]
    csv.write_text(
        HEADER_LINE + "\n" + "\n".join([*saines[:2], enorme, *saines[2:]]) + "\n", encoding="utf-8"
    )
    with Database(tmp_path / "docia.sqlite") as db:
        report = import_csv(db, csv, strict=True)
        assert report.total == 4, "les lignes saines ne doivent pas partir avec la fautive"
        assert report.invalid == 1
        assert "FileSize" in report.errors[0].reason
        assert db.counts()["files"] == 4
        scans = db.query_values("SELECT rows_total, rows_invalid FROM scans")
        assert [tuple(int(v) for v in r) for r in scans] == [(4, 1)]


def test_lot_refuse_par_la_base_est_rejoue_ligne_a_ligne(tmp_path: Path) -> None:
    """GRAVE 2 (2ᵉ moitié) : un lot refusé ne doit plus annuler tout l'import.

    Le refus est simulé au niveau d'`upsert_files` — n'importe quelle valeur qu'une
    version future de SQLite refuserait produirait le même effet qu'un `FileSize`
    démesuré : la transaction du lot est annulée et les lignes saines perdues.
    """
    csv = tmp_path / "piege.csv"
    noms = ["a.pdf", "b.pdf", "poison.pdf", "c.pdf", "d.pdf"]
    csv.write_text(
        HEADER_LINE + "\n" + "\n".join(quoted_line(f'"{n}"') for n in noms) + "\n",
        encoding="utf-8",
    )
    with Database(tmp_path / "docia.sqlite") as db:
        vrai_upsert = db.upsert_files

        def piege(rows: list[SmbeagleRow], scan_id: int) -> tuple[int, int, int]:
            if any(row.name == "poison.pdf" for row in rows):
                raise OverflowError("Python int too large to convert to SQLite INTEGER")
            return vrai_upsert(rows, scan_id)

        db.upsert_files = piege  # type: ignore[method-assign]
        report = import_csv(db, csv, strict=False)
        assert report.total == 4, "les 4 lignes saines doivent être conservées"
        assert report.new == 4
        assert report.invalid == 1
        assert "écriture refusée" in report.errors[0].reason
        assert "poison.pdf" in report.errors[0].raw
        db.upsert_files = vrai_upsert  # type: ignore[method-assign]
        assert db.counts()["files"] == 4
        scans = db.query_values("SELECT rows_total, rows_invalid FROM scans")
        assert [tuple(int(v) for v in r) for r in scans] == [(4, 1)]


def test_taille_illisible_comptee_dans_le_bilan(tmp_path: Path) -> None:
    """MOYEN 11a : une taille illisible ramenée à 0 laisse une trace chiffrée.

    Sans ce compteur, le fichier était simplement exclu « trop petit » : un fichier
    dont le scanner n'a pas pu lire la taille sortait de l'audit sans un mot.
    """
    csv = tmp_path / "tailles.csv"
    csv.write_text(
        HEADER_LINE
        + "\n"
        + "\n".join(
            [
                quoted_line('"bon.pdf"'),
                quoted_line('"cassee.pdf"', file_size="n/a"),
                quoted_line('"absente.pdf"', file_size=""),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    with Database(tmp_path / "docia.sqlite") as db:
        report = import_csv(db, csv, strict=False)
    assert (report.total, report.invalid) == (3, 0)
    assert report.size_defaulted == 2


def _csv_tailles(chemin: Path, tailles: list[str]) -> Path:
    """CSV de N lignes valides, une taille imposée par ligne."""
    chemin.write_text(
        HEADER_LINE
        + "\n"
        + "\n".join(quoted_line(f'"doc{i:03d}.pdf"', file_size=t) for i, t in enumerate(tailles))
        + "\n",
        encoding="utf-8",
    )
    return chemin


def test_partage_entier_a_zero_octet_est_signale(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """CRITIQUE : un CSV produit sans `--sizefile` vidait la campagne en silence.

    Les scanners antérieurs au 01/09 écrivaient `0` — et non un champ vide — quand
    la taille n'était pas collectée. `size_unreadable` ne se déclenchait donc pas,
    `size_defaulted` restait à 0, l'avertissement ne sortait pas, et `plan` excluait
    les 100 % « fichier trop petit » sans dire pourquoi. Le guide invite pourtant à
    importer un CSV « fait ailleurs ».
    """
    csv = _csv_tailles(tmp_path / "sans_sizefile.csv", ["0"] * SUSPECT_ZERO_MIN)
    with caplog.at_level(logging.WARNING), Database(tmp_path / "docia.sqlite") as db:
        report = import_csv(db, csv, strict=False)

    assert (report.total, report.invalid, report.size_defaulted) == (SUSPECT_ZERO_MIN, 0, 0)
    assert report.size_zero == SUSPECT_ZERO_MIN
    assert "--sizefile" in caplog.text
    assert "trop petit" in caplog.text


def test_un_seul_fichier_non_vide_suffit_a_taire_le_soupcon(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Le soupçon ne porte que sur le partage *entièrement* à zéro.

    Un partage réel contient des fichiers vides ; les compter n'autorise pas à
    crier au scan raté. Un seul octet quelque part et l'hypothèse tombe.
    """
    csv = _csv_tailles(tmp_path / "presque.csv", ["0"] * (SUSPECT_ZERO_MIN - 1) + ["4096"])
    with caplog.at_level(logging.WARNING), Database(tmp_path / "docia.sqlite") as db:
        report = import_csv(db, csv, strict=False)

    assert report.size_zero == SUSPECT_ZERO_MIN - 1
    assert "--sizefile" not in caplog.text


def test_petit_lot_tout_a_zero_ne_declenche_pas_l_alerte(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Sous le seuil, « tout à zéro » reste un dossier de test plausible."""
    csv = _csv_tailles(tmp_path / "poignee.csv", ["0"] * (SUSPECT_ZERO_MIN - 1))
    with caplog.at_level(logging.WARNING), Database(tmp_path / "docia.sqlite") as db:
        report = import_csv(db, csv, strict=False)

    assert report.size_zero == SUSPECT_ZERO_MIN - 1
    assert "--sizefile" not in caplog.text


def test_taille_absente_et_taille_nulle_ne_se_confondent_pas(tmp_path: Path) -> None:
    """Champ vide et `0` sont deux faits différents, comptés séparément.

    C'est toute la correction du 01/09 côté scanner : « je n'ai pas collecté la
    taille » ne doit plus s'écrire comme « ce fichier est vide ».
    """
    csv = _csv_tailles(tmp_path / "mixte.csv", ["", "0", "12"])
    with Database(tmp_path / "docia.sqlite") as db:
        report = import_csv(db, csv, strict=False)

    assert (report.size_defaulted, report.size_zero) == (1, 1)


def _csv_cp1252(chemin: Path) -> Path:
    """CSV réenregistré depuis Excel en page de codes Windows — cas courant."""
    ligne = quoted_line('"présentation.pdf"').replace(r"\\srv\part$\docs", r"\\srv\Compta été")
    chemin.write_bytes((HEADER_LINE + "\n" + ligne + "\n").encode("cp1252"))
    return chemin


def test_chemin_non_utf8_est_compte_et_annonce(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """GRAVE : `errors="replace"` acceptait des chemins qui ne désignent rien.

    « Compta été » lu en UTF-8 donne « Compt� �t� ». La ligne était acceptée,
    `invalid=0`, et le chemin ressortait dans les exports comme candidat à la
    suppression — sans qu'aucun compteur n'existe pour le signaler.
    """
    csv = _csv_cp1252(tmp_path / "excel.csv")
    with caplog.at_level(logging.WARNING), Database(tmp_path / "docia.sqlite") as db:
        report = import_csv(db, csv, strict=False)

    assert (report.total, report.invalid, report.mojibake) == (1, 0, 1)
    assert "n'est pas en UTF-8" in caplog.text


def test_chemin_non_utf8_est_refuse_en_mode_strict(tmp_path: Path) -> None:
    """Le mode strict existe pour ça : ne rien laisser entrer d'invérifiable."""
    csv = _csv_cp1252(tmp_path / "excel.csv")
    with Database(tmp_path / "docia.sqlite") as db:
        report = import_csv(db, csv, strict=True)

    assert (report.total, report.invalid) == (0, 1)
    assert "non décodable en UTF-8" in report.errors[0].reason


def test_fichier_utf16_nomme_sa_cause_au_lieu_du_mojibake(tmp_path: Path) -> None:
    """Un CSV UTF-16 était bien refusé, mais sur 1 400 caractères de mojibake.

    Le message énumérait 19 colonnes illisibles (« ��N a m e » au lieu de
    « Name ») sans jamais dire la seule chose utile : ce fichier n'est pas en
    UTF-8. L'utilisateur n'avait aucun moyen d'en tirer quoi que ce soit.
    """
    csv = tmp_path / "utf16.csv"
    csv.write_bytes((HEADER_LINE + "\n" + quoted_line('"note.pdf"') + "\n").encode("utf-16"))

    items = list(read_smbeagle_csv(csv, strict=False))

    assert len(items) == 1, "la lecture s'arrête : les colonnes ne sont plus à leur place"
    erreur = items[0]
    assert isinstance(erreur, CsvLineError)
    assert "n'est pas encodé en UTF-8" in erreur.reason
    assert len(erreur.reason) < 300, "un message de 1 400 caractères n'est pas un message"


def test_ligne_sans_nom_ni_dossier_est_rejetee(tmp_path: Path) -> None:
    """MINEUR 14 : `Name` et `UNCDirectory` vides donnaient tous le chemin `\\`.

    Ces lignes fusionnaient en un seul enregistrement, comptées « inchangées » et
    `invalid=0` — un fichier fantôme, et la fusion de deux vrais fichiers de même
    nom dès qu'un `UNCDirectory` manque (erreur d'ACL).
    """
    vide = "," * 18
    with pytest.raises(ValueError, match="Name vide"):
        parse_line(vide, 2)
    with pytest.raises(ValueError, match="UNCDirectory vide"):
        parse_line(REAL_LINE.replace(r"\\192.168.1.72\admin$\addins", ""), 2)

    csv = tmp_path / "fantome.csv"
    csv.write_text(
        HEADER_LINE + "\n" + "\n".join([quoted_line('"vrai.pdf"'), vide, vide, vide]) + "\n",
        encoding="utf-8",
    )
    with Database(tmp_path / "docia.sqlite") as db:
        report = import_csv(db, csv, strict=False)
        assert (report.total, report.new, report.unchanged) == (1, 1, 0)
        assert report.invalid == 3
        assert db.counts()["files"] == 1
