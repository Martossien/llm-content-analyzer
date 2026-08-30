from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from content_analyzer.modules.db_manager import SafeDBManager

def test_safe_db_manager_checkpoint(tmp_path):
    db_file = tmp_path / "test.db"
    mgr = SafeDBManager(db_file)
    success = mgr._force_wal_checkpoint()
    mgr.close()
    assert success is True
