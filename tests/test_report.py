"""Rapport HTML / Markdown, classeur Excel et export Power BI."""

from __future__ import annotations

import csv
import re
from collections.abc import Iterator
from html.parser import HTMLParser
from pathlib import Path

import pytest
from openpyxl import load_workbook

from docia import views
from docia.cli import main
from docia.db import Database
from docia.models import FileStatus
from docia.report import excel, powerbi
from docia.report.data import collect
from docia.report.html import render_html
from docia.report.markdown import render_markdown
from tests.test_views import TODAY, _analysis, _row

_VOID = {"meta", "br", "hr", "img", "input", "link", "rect", "path"}


class _Checker(HTMLParser):
    """Vérifie que les balises sont bien imbriquées et toutes refermées."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.stack: list[str] = []
        self.errors: list[str] = []
        self.tags: set[str] = set()

    def handle_starttag(self, tag: str, _attrs: list[tuple[str, str | None]]) -> None:
        self.tags.add(tag)
        if tag not in _VOID:
            self.stack.append(tag)

    def handle_endtag(self, tag: str) -> None:
        if tag in _VOID:
            return
        if not self.stack or self.stack[-1] != tag:
            self.errors.append(f"</{tag}> inattendu (pile : {self.stack[-3:]})")
            return
        self.stack.pop()


@pytest.fixture
def db(tmp_path: Path) -> Iterator[Database]:
    """Petite base représentative (doublons, ancien, sensible, conservation, revue)."""
    database = Database(tmp_path / "rapport.sqlite")
    scan = database.start_scan("scan.csv")
    database.upsert_files(
        [
            _row("contrat.pdf", fast_hash="dup", size=4096),
            _row("contrat-copie.pdf", fast_hash="dup", size=4096, directory="\\\\srv\\part\\bis"),
            _row(
                "archive.txt",
                fast_hash="old",
                size=1_500_000,
                lwt="01/02/2017 08:00:00",
                access="02/02/2017 08:00:00",
                owner="DOM\\bob",
            ),
            _row("paye.xlsx", fast_hash="rh", size=250_000, owner="DOM\\rh"),
        ],
        scan,
    )
    files = {f.name: f for f in database.iter_files()}
    database.store_analysis(
        files["contrat.pdf"].id,
        None,
        1,
        prompt_hash="ph0123456789abcd",
        model="qwen38",
        analysis=_analysis(
            "contrat.pdf", security="C3", rgpd="critical", retention=True, years=10, basis="legal"
        ),
    )
    database.store_analysis(
        files["archive.txt"].id,
        None,
        1,
        prompt_hash="ph0123456789abcd",
        model="qwen38",
        analysis=_analysis("archive.txt", security="C0", rgpd="none"),
    )
    database.store_analysis(
        files["paye.xlsx"].id,
        None,
        1,
        prompt_hash="ph0123456789abcd",
        model="qwen38",
        analysis=_analysis(
            "paye.xlsx", security="C2", rgpd="high", retention=True, years=5, basis="rh"
        ),
    )
    database.set_review(files["contrat.pdf"].id, "corrected", corrected_security="C2")
    yield database
    database.close()


# ------------------------------------------------------------------ HTML


def test_html_is_self_contained_and_well_formed(db: Database) -> None:
    page = render_html(db, today=TODAY)
    checker = _Checker()
    checker.feed(page)
    checker.close()
    assert checker.errors == []
    assert checker.stack == []
    # autonome : aucun script, aucune ressource externe
    assert "<script" not in page.lower()
    assert "<script src=" not in page.lower()
    assert "http://" not in page.replace("http://www.w3.org/2000/svg", "")
    assert "https://" not in page
    assert "<link" not in page.lower()
    # les graphiques sont du SVG écrit en Python
    assert "svg" in checker.tags
    assert "<rect" in page


def test_html_contains_all_sections_and_key_numbers(db: Database) -> None:
    page = render_html(db, today=TODAY)
    for anchor in ("synthese", "hygiene", "risque", "verification", "execution"):
        assert f'id="{anchor}"' in page
    for title in (
        "Synthèse",
        "Hygiène du stockage",
        "Risque et conformité",
        "Vérification humaine",
        "Exécution",
        "Doublons",
        "Ancienneté",
        "Plan de conservation",
        "Candidats au nettoyage",
    ):
        assert title in page
    # 4 fichiers, 3 analysés, 1 famille de doublons de 4 096 octets récupérables
    assert views.format_int(4) in page
    assert views.format_bytes(4096) in page
    assert "contrat.pdf" in page
    assert "C3" in page
    assert "critical" in page
    # date de fin de conservation : 01/01/2026 + 10 ans
    assert "01/01/2036" in page


def test_html_escapes_dangerous_content(tmp_path: Path) -> None:
    with Database(tmp_path / "x.sqlite") as database:
        scan = database.start_scan("s.csv")
        database.upsert_files(
            [_row("a.txt", fast_hash="h", owner="<script>alert(1)</script>")], scan
        )
        files = list(database.iter_files())
        database.set_file_status(files[0].id, FileStatus.EXCLUDED, "raison <b>brute</b>")
        page = render_html(database, today=TODAY)
    assert "<script>alert(1)</script>" not in page
    assert "&lt;script&gt;" in page
    assert "&lt;b&gt;brute&lt;/b&gt;" in page


# ------------------------------------------------------------------ Markdown


def test_markdown_contains_tables_and_sections(db: Database) -> None:
    text = render_markdown(db, today=TODAY)
    assert text.startswith("# Doc-IA — rapport d'analyse")
    for heading in (
        "## 1. Synthèse",
        "## 2. Hygiène du stockage",
        "## 3. Risque et conformité",
        "## 4. Vérification humaine",
        "## 5. Exécution",
    ):
        assert heading in text
    # tableaux GFM : au moins une ligne de séparation
    assert re.search(r"^\|---\|", text, re.MULTILINE)
    assert "| Exemplaire de référence | Copies |" in text
    assert "contrat.pdf" in text
    assert "01/01/2036" in text


def test_html_and_markdown_share_the_same_data(db: Database) -> None:
    data = collect(db, today=TODAY)
    page = render_html(db, data=data)
    text = render_markdown(db, data=data)
    assert data.overview.total_files == 4
    assert data.overview.analyzed == 3
    for rendering in (page, text):
        assert views.format_bytes(data.duplicates.total_reclaimable_bytes) in rendering


# ------------------------------------------------------------------ Excel


def test_excel_workbook_sheets_and_rows(db: Database, tmp_path: Path) -> None:
    path = excel.write_workbook(db, tmp_path / "rapport.xlsx", today=TODAY)
    assert path.exists()
    workbook = load_workbook(path)
    assert workbook.sheetnames == list(excel.SHEETS)
    files_sheet = workbook["Fichiers"]
    assert files_sheet.max_row == 5  # 4 fichiers + en-tête
    assert files_sheet["A1"].font.bold is True
    assert files_sheet.freeze_panes == "A2"
    assert files_sheet.auto_filter.ref is not None
    doublons = workbook["Doublons"]
    assert doublons.max_row == 2
    assert doublons.cell(row=2, column=5).value == 4096  # octets récupérables
    anciennete = workbook["Ancienneté"]
    assert anciennete.max_row == 5  # 4 seuils
    conservation = workbook["Conservation"]
    assert conservation.max_row == 3
    ends = {conservation.cell(row=r, column=8).value for r in (2, 3)}
    assert any(str(value).startswith("2036-01-01") for value in ends)
    assert workbook["Sensibles"].max_row == 3
    assert workbook["Synthèse"].max_row > 10


# ------------------------------------------------------------------ Power BI


def _read_csv(path: Path) -> tuple[list[str], list[list[str]]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.reader(handle, delimiter=";"))
    return rows[0], rows[1:]


def test_powerbi_export_files_and_encoding(db: Database, tmp_path: Path) -> None:
    target = tmp_path / "powerbi"
    written = powerbi.export_powerbi(db, target, today=TODAY)
    names = {p.name for p in written}
    assert names == set(powerbi.POWERBI_COLUMNS) | {powerbi.README_NAME}
    for name in powerbi.POWERBI_COLUMNS:
        raw = (target / name).read_bytes()
        assert raw.startswith(b"\xef\xbb\xbf"), f"{name} : BOM UTF-8 manquant"
        assert b";" in raw, f"{name} : séparateur ; manquant"

    header, rows = _read_csv(target / "files.csv")
    assert header == [c[0] for c in powerbi.POWERBI_COLUMNS["files.csv"]]
    assert len(rows) == 4
    by_name = {row[header.index("name")]: row for row in rows}
    assert by_name["archive.txt"][header.index("access_date")] == "2017-02-02"
    assert by_name["archive.txt"][header.index("share")] == "\\\\srv\\part"
    assert int(by_name["archive.txt"][header.index("age_days_access")]) > 3000

    header, rows = _read_csv(target / "analyses.csv")
    assert header == [c[0] for c in powerbi.POWERBI_COLUMNS["analyses.csv"]]
    assert len(rows) == 3
    ends = {row[header.index("retention_end_date")] for row in rows}
    assert "2036-01-01" in ends
    assert "identite" in {row[header.index("rgpd_data_types")] for row in rows}

    header, rows = _read_csv(target / "duplicates.csv")
    assert len(rows) == 2  # une famille de deux exemplaires
    assert rows[0][header.index("reclaimable_bytes")] == "4096"

    header, rows = _read_csv(target / "reviews.csv")
    assert len(rows) == 1
    assert rows[0][header.index("corrected_security")] == "C2"

    header, _ = _read_csv(target / "runs.csv")
    assert header == [c[0] for c in powerbi.POWERBI_COLUMNS["runs.csv"]]


def test_powerbi_readme_documents_exactly_the_real_columns(db: Database, tmp_path: Path) -> None:
    target = tmp_path / "powerbi"
    powerbi.export_powerbi(db, target, today=TODAY)
    readme = (target / powerbi.README_NAME).read_text(encoding="utf-8")
    for name in powerbi.POWERBI_COLUMNS:
        assert f"### `{name}`" in readme
        documented = re.findall(rf"### `{re.escape(name)}`.*?(?=\n### |\Z)", readme, re.S)[0]
        columns = re.findall(r"^\| `([a-z_]+)` \|", documented, re.MULTILINE)
        real, _ = _read_csv(target / name)
        assert columns == real, f"{name} : README et CSV divergent"
    assert "utf-8" in readme.lower()
    assert "file_id" in readme
    assert "Rafraîchissement" in readme


def test_powerbi_export_is_idempotent(db: Database, tmp_path: Path) -> None:
    target = tmp_path / "powerbi"
    first = powerbi.export_powerbi(db, target, today=TODAY)
    second = powerbi.export_powerbi(db, target, today=TODAY)
    assert [p.name for p in first] == [p.name for p in second]
    assert (target / "files.csv").read_bytes() == (target / "files.csv").read_bytes()


# ------------------------------------------------------------------ CLI


def test_cli_report_and_export_formats(db: Database, tmp_path: Path) -> None:
    db_path = str(db.path)
    db.close()
    out_html = tmp_path / "r.html"
    assert main(["--db", db_path, "report", "--format", "html", "--out", str(out_html)]) == 0
    assert "Doc-IA" in out_html.read_text(encoding="utf-8")

    out_md = tmp_path / "r.md"
    assert main(["--db", db_path, "report", "--format", "md", "--out", str(out_md)]) == 0
    assert out_md.read_text(encoding="utf-8").startswith("# Doc-IA")

    out_xlsx = tmp_path / "r.xlsx"
    assert main(["--db", db_path, "export", "--format", "xlsx", "--out", str(out_xlsx)]) == 0
    assert load_workbook(out_xlsx).sheetnames == list(excel.SHEETS)

    out_pbi = tmp_path / "pbi"
    assert main(["--db", db_path, "export", "--format", "powerbi", "--out", str(out_pbi)]) == 0
    assert (out_pbi / "files.csv").exists()

    # les formats existants restent intacts
    out_csv = tmp_path / "r.csv"
    assert main(["--db", db_path, "export", "--format", "csv", "--out", str(out_csv)]) == 0
    assert out_csv.read_text(encoding="utf-8-sig").startswith("path;")
    out_json = tmp_path / "r.json"
    assert main(["--db", db_path, "export", "--format", "json", "--out", str(out_json)]) == 0
    assert out_json.read_text(encoding="utf-8").lstrip().startswith("[")


def test_cli_report_default_output_next_to_database(db: Database) -> None:
    db_path = Path(db.path)
    db.close()
    assert main(["--db", str(db_path), "report"]) == 0
    assert db_path.with_name(f"{db_path.stem}_rapport.html").exists()
