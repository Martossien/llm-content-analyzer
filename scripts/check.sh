#!/usr/bin/env bash
# Contrôle complet avant commit : ruff (lint + format), mypy strict, pytest.
# Sort en erreur au premier échec et affiche un verdict explicite.
set -u
cd "$(dirname "$0")/.."
V=.venv/bin
fail=0
$V/ruff check src tests scripts || fail=1
$V/ruff format --check src tests scripts >/dev/null || { echo "ruff format : fichiers à formater"; fail=1; }
mypy_out=$($V/mypy --strict src/docia 2>&1); mypy_rc=$?; echo "$mypy_out" | tail -1; [ $mypy_rc -eq 0 ] || fail=1
out=$($V/python -m pytest tests -rfE -q -p no:cacheprovider 2>&1)
if echo "$out" | grep -qE "^(FAILED|ERROR)"; then echo "$out" | grep -E "^(FAILED|ERROR)"; fail=1; fi
n=$($V/python -m pytest tests --co -q -o addopts="" -p no:cacheprovider 2>/dev/null | tail -1 | grep -oE "^[0-9]+")
if [ $fail -eq 0 ]; then echo "VERDICT: OK ($n tests)"; else echo "VERDICT: ECHEC"; fi
exit $fail
