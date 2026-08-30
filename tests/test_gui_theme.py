"""Fonctions pures de l'interface v3.2 : thème, formats, avancement, campagnes récentes."""

from __future__ import annotations

from pathlib import Path

import pytest

from docia.gui import service_shim
from docia.gui.helpers import (
    campaign_title,
    eta_seconds,
    pretty_amounts,
    pretty_list,
    progress_fraction,
    rate_per_hour,
)
from docia.gui.theme import (
    folder_of,
    format_bytes,
    format_duration,
    format_int,
    severity_color,
    shorten_path,
)


def test_severity_color_known_and_unknown() -> None:
    assert severity_color("C3") == "#b91c1c"
    assert severity_color("critical") == severity_color("C3")
    assert severity_color(None) == severity_color("inconnu")


def test_shorten_path_keeps_head_and_tail() -> None:
    p = r"\\srv\partage\direction\finance\2024\budget\rapport-annuel.pdf"
    short = shorten_path(p, 40)
    assert len(short) <= 40
    assert short.startswith((r"\\srv\partage", "srv"))
    assert short.endswith("rapport-annuel.pdf")
    assert shorten_path("court.txt", 40) == "court.txt"


def test_folder_of_windows_and_posix() -> None:
    assert folder_of(r"\\srv\share\a\b.txt") == r"\\srv\share\a"
    assert folder_of("/home/u/a/b.txt") == "/home/u/a"


def test_formats() -> None:
    assert format_bytes(512) == "512 o"
    assert format_bytes(3 * 1024**2) == "3.0 Mo"
    assert format_duration(45) == "45 s"
    assert format_duration(600) == "10 min"
    assert format_duration(3900) == "1 h 05"
    assert format_duration(None) == "—"
    assert format_int(1234567) == "1 234 567"


def test_progress_fraction_and_eta() -> None:
    counts = {"pending": 50, "queued": 0, "done": 40, "error": 10}
    assert progress_fraction(counts) == pytest.approx(0.5)
    assert progress_fraction({}) == 0.0
    assert eta_seconds(done_delta=10, elapsed_s=100.0, remaining=20) == pytest.approx(200.0)
    assert eta_seconds(0, 100.0, 20) is None
    assert rate_per_hour(10, 3600.0) == pytest.approx(10.0)


def test_campaign_title() -> None:
    assert campaign_title(r"C:\audit\finance-2026.sqlite") == "finance-2026"
    assert campaign_title("/tmp/x/audit.sqlite") == "audit"


def test_recent_campaigns_roundtrip(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from docia import service

    monkeypatch.setattr(service, "docia_home", lambda: tmp_path)
    assert service_shim.load_recent() == []
    (tmp_path / "a.sqlite").touch()
    (tmp_path / "b.sqlite").touch()
    service_shim.remember_recent(str(tmp_path / "a.sqlite"))
    service_shim.remember_recent(str(tmp_path / "b.sqlite"))
    service_shim.remember_recent(str(tmp_path / "a.sqlite"))
    names = [r.db_path.name for r in service_shim.load_recent()]
    assert names == ["a.sqlite", "b.sqlite"]


def test_gui_service_backup_and_restore(tmp_path: Path) -> None:
    from docia.db import Database

    db_path = tmp_path / "camp.sqlite"
    with Database(db_path) as db:
        db.save_prompt("p1", "x" * 60)
    svc = service_shim.GuiService(lambda: Database(db_path))
    out = svc.backup(db_path, tmp_path / "sauvegardes")
    assert out.exists()
    assert out.parent.name == "sauvegardes"
    with Database(db_path) as db:
        db.delete_prompt("p1")
    svc.restore(db_path, out)
    with Database(db_path) as db:
        assert db.get_prompt("p1") is not None
    assert service_shim.default_backup_dir(db_path).name.endswith(".backups")


def test_pretty_list_and_amounts() -> None:
    assert pretty_list('["identite", "rh"]') == "identite, rh"
    assert pretty_list("") == "—"
    assert pretty_list("texte libre") == "texte libre"
    raw = '[{"value": 3766.65, "currency": "EUR", "context": "Salaire brut"}, {"value": 12}]'
    assert pretty_amounts(raw) == "3 766,65 EUR (Salaire brut) ; 12,00"
    assert pretty_amounts(None) == "—"
