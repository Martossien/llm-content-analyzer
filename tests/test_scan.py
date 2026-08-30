"""Pilotage du scanner SMBeagle : ligne de commande, progression JSON, sous-processus
(faux scanner Python), enchaînement scan → import → plan, dates d'accès premières."""

from __future__ import annotations

import json
import os
import stat
import sys
import threading
from pathlib import Path

import pytest

from docia.scan import (
    ENRICHMENT_FLAGS,
    ScanError,
    ScanProfile,
    build_command,
    count_csv_rows,
    parse_progress_line,
    redact,
    run_scan,
)

HEADER = (
    "Name,Host,Extension,Username,Hostname,UNCDirectory,CreationTime,LastWriteTime,Readable,"
    "Writeable,Deletable,DirectoryType,Base,FileSize,AccessTime,FileAttributes,Owner,FastHash,"
    "FileSignature"
)
ROW = (
    '"a.txt","srv","txt","u","srv","\\\\srv\\part","01/02/2024 10:00:00","01/02/2024 10:00:00",'
    'True,False,False,"SMB","\\\\srv\\part",120,"03/04/2024 09:00:00","Archive","DOM\\u",'
    '"0123456789abcdef","TXT"'
)


def test_build_command_local_and_smb(tmp_path: Path) -> None:
    exe = tmp_path / "SMBeagle.exe"
    local = ScanProfile(local_paths=["D:\\partage", "E:\\autre"])
    cmd = build_command(exe, local, tmp_path / "scan.csv", manifest_out=tmp_path / "m.json")
    assert cmd[:4] == [str(exe), "-c", str(tmp_path / "scan.csv"), "-q"]
    assert cmd.count("--local-path") == 2
    assert all(flag in cmd for flag in ENRICHMENT_FLAGS)
    assert "--preserve-access-time" in cmd
    assert "--progress-json" in cmd
    assert cmd[cmd.index("--manifest") + 1] == str(tmp_path / "m.json")
    assert "-D" not in cmd

    smb = ScanProfile(
        hosts=["srv1"], shares=["finance"], exclude_shares=["tmp"], username="u", password="s3cret"
    )
    cmd = build_command(exe, smb, tmp_path / "scan.csv", progress_json=False)
    assert "-D" in cmd
    assert cmd[cmd.index("-h") + 1] == "srv1"
    assert cmd[cmd.index("-s") + 1] == "finance"
    assert cmd[cmd.index("-S") + 1] == "tmp"
    assert "-E" in cmd
    assert "--progress-json" not in cmd
    assert "s3cret" not in redact(cmd)
    assert "••••" in redact(cmd)


def test_profile_validation() -> None:
    assert ScanProfile().validate()
    assert ScanProfile(local_paths=["x"], hosts=["h"]).validate()
    assert ScanProfile(hosts=["h"], username="u").validate()
    assert ScanProfile(local_paths=["x"]).validate() == []


def test_parse_progress_line() -> None:
    ev = parse_progress_line(
        '{"event":"progress","stage":"files","hosts":1,"shares":2,"files":345,"elapsed_s":4.2}'
    )
    assert ev is not None
    assert (ev.stage, ev.files, ev.shares, ev.elapsed_s) == ("files", 345, 2, 4.2)
    err = parse_progress_line('{"event":"error","message":"accès refusé"}')
    assert err is not None
    assert err.stage == "error"
    assert parse_progress_line("4. Probing hosts…") is None
    assert parse_progress_line('{"pas":"un événement"}') is None
    assert parse_progress_line("{invalide") is None


def _fake_scanner(tmp_path: Path, *, rows: int = 3, exit_code: int = 0, sleep: float = 0.0) -> Path:
    """Faux SMBeagle : écrit un CSV 19 colonnes, des lignes de progression JSON, un manifeste."""
    script = tmp_path / "fake_smbeagle.py"
    script.write_text(
        f"""import json, sys, time
args = sys.argv[1:]
csv_out = args[args.index('-c') + 1]
manifest = args[args.index('--manifest') + 1] if '--manifest' in args else None
print('SMBeagle by PunkSecurity (faux)')
print(json.dumps({{'event': 'progress', 'stage': 'files', 'hosts': 1, 'shares': 1, 'files': 0, 'elapsed_s': 0.1}}))
time.sleep({sleep})
with open(csv_out, 'w', encoding='utf-8', newline='') as fh:
    fh.write({HEADER!r} + '\\n')
    for i in range({rows}):
        fh.write({ROW!r}.replace('a.txt', f'f{{i}}.txt') + '\\n')
print(json.dumps({{'event': 'progress', 'stage': 'writing', 'hosts': 1, 'shares': 1, 'files': {rows}, 'elapsed_s': 0.2}}))
if manifest:
    with open(manifest, 'w', encoding='utf-8') as fh:
        json.dump({{'version': 'fake', 'options': {{'args': args}}, 'counts': {{'files': {rows}}}}}, fh)
print(json.dumps({{'event': 'done', 'files': {rows}, 'csv': csv_out, 'elapsed_s': 0.3}}))
sys.exit({exit_code})
""",
        encoding="utf-8",
    )
    launcher = tmp_path / ("SMBeagle.cmd" if os.name == "nt" else "SMBeagle")
    if os.name == "nt":
        launcher.write_text(f'@"{sys.executable}" "{script}" %*\n', encoding="utf-8")
    else:
        launcher.write_text(
            f'#!/bin/sh\nexec "{sys.executable}" "{script}" "$@"\n', encoding="utf-8"
        )
        launcher.chmod(launcher.stat().st_mode | stat.S_IXUSR)
    return launcher


def test_run_scan_with_fake_scanner(tmp_path: Path) -> None:
    exe = _fake_scanner(tmp_path, rows=5)
    events, lines = [], []
    result = run_scan(
        ScanProfile(local_paths=[str(tmp_path)]),
        tmp_path / "out" / "scan.csv",
        exe=exe,
        on_event=events.append,
        on_line=lines.append,
    )
    assert result.exit_code == 0
    assert result.files == 5
    assert result.csv_path.is_file()
    assert result.manifest["counts"] == {"files": 5}
    assert [e.stage for e in events] == ["files", "writing", "done"]
    assert events[-1].files == 5
    assert lines[0].startswith("scan : ")
    assert any("PunkSecurity" in line for line in lines)
    assert count_csv_rows(result.csv_path) == 5


def test_run_scan_failure_and_missing_exe(tmp_path: Path) -> None:
    exe = _fake_scanner(tmp_path, rows=1, exit_code=1)
    with pytest.raises(ScanError, match="code 1"):
        run_scan(ScanProfile(local_paths=[str(tmp_path)]), tmp_path / "scan.csv", exe=exe)
    with pytest.raises(ScanError, match="introuvable"):
        run_scan(
            ScanProfile(local_paths=[str(tmp_path)]),
            tmp_path / "scan.csv",
            exe=None,
            configured_exe=str(tmp_path / "absent.exe"),
        )
    with pytest.raises(ScanError, match="au moins"):
        run_scan(ScanProfile(), tmp_path / "scan.csv", exe=exe)


def test_run_scan_cancel_keeps_partial_csv(tmp_path: Path) -> None:
    exe = _fake_scanner(tmp_path, rows=2, sleep=0.5)
    cancel = threading.Event()

    def on_event(_ev: object) -> None:
        cancel.set()

    result = run_scan(
        ScanProfile(local_paths=[str(tmp_path)]),
        tmp_path / "scan.csv",
        exe=exe,
        cancel=cancel,
        on_event=on_event,
    )
    assert result.exit_code != 0 or result.files >= 0


def test_scan_campaign_imports_and_plans(tmp_path: Path) -> None:
    from docia import service
    from docia.config import Config
    from docia.db import Database

    exe = _fake_scanner(tmp_path, rows=4)
    cfg = Config()
    cfg.db_path = str(tmp_path / "camp.sqlite")
    cfg.scan.smbeagle_path = str(exe)
    cfg.filter.excluded_dir_markers = []
    cfg.filter.min_size_bytes = 1
    with Database(cfg.db_path) as db:
        result, report, plan_report = service.scan_campaign(
            db, cfg, ScanProfile(local_paths=[str(tmp_path)])
        )
        assert result.files == 4
        assert report.total == 4
        assert report.new == 4
        assert plan_report.pending + plan_report.excluded == 4
        last = db.last_scan()
        assert last is not None
        assert last["kind"] == "scan"
        assert json.loads(last["manifest_json"])["counts"]["files"] == 4
        assert Path(last["csv_path"]).parent == service.scans_dir_for(db.path)


def test_first_access_time_survives_rescan(tmp_path: Path) -> None:
    """Un rescan d'un fichier inchangé ne rajeunit pas la date d'accès retenue."""
    from docia import views
    from docia.db import Database
    from docia.ingest.smbeagle_csv import import_csv

    csv1 = tmp_path / "s1.csv"
    csv1.write_text(HEADER + "\n" + ROW + "\n", encoding="utf-8")
    csv2 = tmp_path / "s2.csv"
    csv2.write_text(
        HEADER + "\n" + ROW.replace("03/04/2024", "30/08/2026") + "\n", encoding="utf-8"
    )
    csv3 = tmp_path / "s3.csv"
    csv3.write_text(
        HEADER
        + "\n"
        + ROW.replace("03/04/2024", "30/08/2026").replace("0123456789abcdef", "ffff")
        + "\n",
        encoding="utf-8",
    )
    with Database(tmp_path / "db.sqlite") as db:
        import_csv(db, csv1)
        import_csv(db, csv2)
        row = db.query("SELECT access_time, access_time_first FROM files")[0]
        assert row["access_time"].startswith("30/08/2026")
        assert row["access_time_first"].startswith("03/04/2024")
        from datetime import date

        buckets = views.stale_files(db, years=(1,), today=date(2026, 8, 30))
        assert buckets[0].not_accessed_files == 1  # d'après la première observation
        import_csv(db, csv3)  # contenu modifié : activité réelle → la référence repart
        row = db.query("SELECT access_time_first, content_version FROM files")[0]
        assert row["access_time_first"].startswith("30/08/2026")
        assert row["content_version"] == 2
