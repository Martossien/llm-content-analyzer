"""Tests du parseur CSV SMBeagle et de l'import en base."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest

from docia.db import Database
from docia.ingest.smbeagle_csv import (
    CsvLineError,
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


def test_header_invalide_stoppe_la_lecture_en_strict(tmp_path: Path) -> None:
    csv = tmp_path / "mauvais.csv"
    csv.write_text("a,b,c\n" + REAL_LINE + "\n", encoding="utf-8")
    items = list(read_smbeagle_csv(csv, strict=True))
    assert len(items) == 1
    assert isinstance(items[0], CsvLineError)
    assert items[0].line_number == 1
    items_tolerants = list(read_smbeagle_csv(csv, strict=False))
    assert len(items_tolerants) == 2
    assert isinstance(items_tolerants[1], SmbeagleRow)


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
