import os
import re
from pathlib import Path

def find_incorrect_db_usage(root_dir: str) -> list:
    """Find incorrect SQLiteConnectionPool usage patterns."""
    incorrect = []
    bad_patterns = [
        r"with\s+.*\._connect\(\)\s+as\s+\w+:",
        r"with\s+.*\.db_manager\._connect\(\)\s+as\s+\w+:",
    ]
    for py_file in Path(root_dir).rglob("*.py"):
        # Skip optimizer which uses SQLiteConnectionManager correctly
        if py_file.name == "sql_optimizer.py":
            continue
        with open(py_file, 'r', encoding='utf-8') as f:
            content = f.read().splitlines()
        for idx, line in enumerate(content, 1):
            if '.get()' in line:
                continue
            for pattern in bad_patterns:
                if re.search(pattern, line):
                    incorrect.append({'file': str(py_file), 'line': idx, 'content': line.strip()})
    return incorrect

if __name__ == "__main__":
    issues = find_incorrect_db_usage(".")
    if issues:
        print("\u274c REMAINING ISSUES FOUND:")
        for issue in issues:
            print(f"  {issue['file']}:{issue['line']} - {issue['content']}")
    else:
        print("\u2705 ALL DATABASE CONTEXT MANAGER USAGE IS CORRECT")
