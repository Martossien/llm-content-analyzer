import sqlite3
from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from content_analyzer.modules.csv_parser import CSVParser

ROOT_DIR = Path(__file__).resolve().parents[2]
CONFIG_PATH = ROOT_DIR / "content_analyzer" / "config" / "analyzer_config.yaml"


def create_sample_csv(tmp_path: Path, rows: int, drop_cols=None) -> Path:
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
    df = pd.DataFrame(data)
    if drop_cols:
        df = df.drop(columns=drop_cols)
    csv_path = tmp_path / "sample.csv"
    df.to_csv(csv_path, index=False)
    return csv_path


def test_parse_valid_csv(tmp_path):
    csv_file = create_sample_csv(tmp_path, 5)
    db_file = tmp_path / "out.db"
    parser = CSVParser(CONFIG_PATH)
    result = parser.parse_csv(csv_file, db_file, chunk_size=2)
    assert result["total_files"] == 5
    assert result["imported_files"] == 5
    assert result["errors"] == []
    conn = sqlite3.connect(db_file)
    count = conn.execute("SELECT COUNT(*) FROM fichiers").fetchone()[0]
    conn.close()
    assert count == 5


def test_parse_invalid_format(tmp_path):
    csv_file = create_sample_csv(tmp_path, 3, drop_cols=["FastHash"])
    db_file = tmp_path / "out.db"
    parser = CSVParser(CONFIG_PATH)
    result = parser.parse_csv(csv_file, db_file, chunk_size=2)
    assert result["imported_files"] == 0
    assert any("FastHash" in e for e in result["errors"])


def test_chunked_processing(tmp_path):
    csv_file = create_sample_csv(tmp_path, 25)
    db_file = tmp_path / "out.db"
    parser = CSVParser(CONFIG_PATH)
    result = parser.parse_csv(csv_file, db_file, chunk_size=10)
    assert result["imported_files"] == 25
    conn = sqlite3.connect(db_file)
    count = conn.execute("SELECT COUNT(*) FROM fichiers").fetchone()[0]
    conn.close()
    assert count == 25


def test_metadata_transformation():
    parser = CSVParser(CONFIG_PATH)
    df = pd.DataFrame({
        "Name": ["file0.txt"],
        "UNCDirectory": ["/share"],
        "FileSize": [100],
        "Owner": ["owner"],
        "FastHash": ["abc"],
        "AccessTime": ["2020-01-03"],
        "FileAttributes": ["fa"],
        "FileSignature": ["sig"],
        "LastWriteTime": ["2020-01-02"],
        "CreationTime": ["2020-01-01"],
    })
    meta = parser.transform_metadata(df.iloc[0])
    assert meta["path"] == "/share/file0.txt"
    assert meta["file_size"] == 100
    assert meta["fast_hash"] == "abc"


def test_parse_scan_local_mini(tmp_path):
    csv_file = ROOT_DIR / "content_analyzer" / "scan_local_mini.csv"
    db_file = tmp_path / "out.db"
    parser = CSVParser(CONFIG_PATH)
    result = parser.parse_csv(csv_file, db_file, chunk_size=20)
    assert result["total_files"] == 63
    assert result["imported_files"] == 63
    assert result["errors"] == []
