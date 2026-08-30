import sqlite3
import subprocess
import sys
from pathlib import Path
from unittest import mock

from content_analyzer.content_analyzer import ContentAnalyzer

ROOT_DIR = Path(__file__).resolve().parents[1]
CFG = ROOT_DIR / "config" / "analyzer_config.yaml"


def create_sample_csv(tmp_path: Path, rows: int) -> Path:
    data = {
        "Name": [f"file{i}.txt" for i in range(rows)],
        "Host": ["host"] * rows,
        "Extension": ["TXT"] * rows,
        "Username": ["user"] * rows,
        "Hostname": ["host"] * rows,
        "UNCDirectory": ["/share"] * rows,
        "CreationTime": ["2020-01-01"] * rows,
        "LastWriteTime": ["2020-01-02"] * rows,
        "Readable": [True] * rows,
        "Writeable": [True] * rows,
        "Deletable": [True] * rows,
        "DirectoryType": ["file"] * rows,
        "Base": ["base"] * rows,
        "FileSize": [100] * rows,
        "Owner": ["owner"] * rows,
        "FastHash": ["abc"] * rows,
        "AccessTime": ["2020-01-03"] * rows,
        "FileAttributes": ["fa"] * rows,
        "FileSignature": ["sig"] * rows,
    }
    import pandas as pd

    df = pd.DataFrame(data)
    csv_path = tmp_path / "sample.csv"
    df.to_csv(csv_path, index=False)
    return csv_path


def test_cli_help():
    result = subprocess.run(
        [sys.executable, "content_analyzer/content_analyzer.py", "--help"],
        capture_output=True,
        text=True,
    )
    assert "Content Analyzer Brique 2" in result.stdout


def test_analyze_batch(tmp_path):
    csv_file = create_sample_csv(tmp_path, 3)
    out_db = tmp_path / "out.db"
    analyzer = ContentAnalyzer(CFG)
    analyzer.csv_parser.validation_strict = False
    analyzer.enable_cache = True
    with mock.patch.object(analyzer.api_client, "analyze_file") as mapi:
        mapi.return_value = {
            "status": "completed",
            "result": {"content": "{}"},
            "task_id": "t1",
        }
        result = analyzer.analyze_batch(csv_file, out_db)
    assert result["status"] == "completed"
    assert result["files_processed"] == 3
    conn = sqlite3.connect(out_db)
    completed = conn.execute(
        "SELECT COUNT(*) FROM fichiers WHERE status='completed'"
    ).fetchone()[0]
    cached = conn.execute(
        "SELECT COUNT(*) FROM fichiers WHERE status='cached'"
    ).fetchone()[0]
    conn.close()
    assert completed + cached == 3
