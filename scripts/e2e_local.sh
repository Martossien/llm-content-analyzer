#!/usr/bin/env bash
# Test complet sur la machine de développement : scanner → DocFuse (OCR) → LLM → base → rapports.
#
#   scripts/e2e_local.sh [DOSSIER_DE_TRAVAIL]      (défaut : /tmp/docia-e2e-<date>)
#
# Pré-requis : ~/Doc-IA/smbeagle_enriched (dotnet), ~/Doc-IA/bench_vllm/serve_qwen38.sh (vLLM),
# .venv de ce dépôt avec DocFuse. vLLM est démarré puis TOUJOURS arrêté (trap), même en échec.
# Verdict explicite en dernière ligne : E2E: OK / E2E: ECHEC.
set -u
cd "$(dirname "$0")/.."
ROOT=$PWD
V=$ROOT/.venv/bin
WORK=${1:-/tmp/docia-e2e-$(date +%Y%m%d-%H%M%S)}
SMB_REPO=${SMB_REPO:-$HOME/Doc-IA/smbeagle_enriched}
SERVE=${SERVE:-$HOME/Doc-IA/bench_vllm/serve_qwen38.sh}
FIXTURES=${FIXTURES:-$HOME/DocFuse/tests/fixtures}
fail=0
say() { printf '\n== %s\n' "$*"; }

cleanup() {
  say "arrêt de vLLM"
  bash "$SERVE" stop >/dev/null 2>&1 || true
}
trap cleanup EXIT

say "1. scanner SMBeagle (build Release)"
( cd "$SMB_REPO" && dotnet build -c Release -nologo -v q >/dev/null ) || { echo "build smbeagle impossible"; exit 1; }
SMB_BIN=$(find "$SMB_REPO/bin/Release" -type f -name SMBeagle | head -1)
[ -x "$SMB_BIN" ] || { echo "binaire SMBeagle introuvable"; exit 1; }
echo "scanner : $SMB_BIN"

say "2. corpus de test → $WORK/partage"
mkdir -p "$WORK/partage/archives" "$WORK/partage/RH"
cp -r "$FIXTURES"/. "$WORK/partage/archives/"
$V/python scripts/make_scanned_pdf.py "$WORK/partage/RH/bulletin_scanne.pdf" \
  "BULLETIN DE PAIE|Salarie : Jean DUPONT|Numero de securite sociale 1 85 03 75 123 456 78|Salaire net 2 345,67 EUR|Periode : mars 2026"
cp "$WORK/partage/RH/bulletin_scanne.pdf" "$WORK/partage/archives/copie_bulletin.pdf"   # doublon exact
$V/python - "$WORK/partage/archives/gros_registre.txt" <<'PY'
import sys
lines = [f"Ligne {i:06d} — registre des decisions du conseil, seance du {i % 28 + 1:02d}/{i % 12 + 1:02d}/2019, montant {i * 13 % 9973},{i % 100:02d} EUR" for i in range(60000)]
open(sys.argv[1], "w", encoding="utf-8").write("\n".join(lines))
PY
# Ancienneté : sans cela tout le corpus est daté de l'instant et les vues « non
# accédé depuis N ans », « candidats au nettoyage » et l'onglet Ancienneté restent
# vides par construction — le banc les traversait sans rien prouver.
touch -a -m -d "6 years ago" "$WORK/partage/archives/sample.doc" "$WORK/partage/archives/sample.xls" "$WORK/partage/archives/sample.ppt"
touch -a -m -d "2 years ago" "$WORK/partage/archives/sample.rtf" "$WORK/partage/archives/sample.odt"
ls -la "$WORK/partage" "$WORK/partage/RH" | head -20

say "3. configuration"
cat > "$WORK/docia.toml" <<TOML
db_path = "$WORK/campagne.sqlite"
[llm]
base_url = "http://127.0.0.1:8000/v1"
model = "qwen38"
max_in_flight = 4
[blocks]
block_tokens = 32000
[filter]
excluded_dir_markers = []
min_size_bytes = 1
[scan]
smbeagle_path = "$SMB_BIN"
TOML
$V/docia --config "$WORK/docia.toml" doctor || fail=1

say "4. vLLM"
bash "$SERVE" start >/dev/null 2>&1 &
for i in $(seq 1 90); do curl -sf http://127.0.0.1:8000/v1/models >/dev/null 2>&1 && break; sleep 10; done
curl -sf http://127.0.0.1:8000/v1/models >/dev/null || { echo "vLLM ne répond pas"; exit 1; }
echo "vLLM prêt"

say "5. scan → import → préparation"
$V/docia --config "$WORK/docia.toml" scan --local-path "$WORK/partage" || fail=1

say "6. analyse LLM"
$V/docia --config "$WORK/docia.toml" run || fail=1
$V/docia --config "$WORK/docia.toml" status

say "7. rapports et exports"
$V/docia --config "$WORK/docia.toml" report --format html --out "$WORK/rapport.html" || fail=1
$V/docia --config "$WORK/docia.toml" export --format xlsx --out "$WORK/resultats.xlsx" || fail=1
$V/docia --config "$WORK/docia.toml" export --format powerbi --out "$WORK/powerbi" || fail=1

say "8. vérifications"
$V/python scripts/e2e_check.py "$WORK" || fail=1

if [ $fail -eq 0 ]; then echo "E2E: OK ($WORK)"; else echo "E2E: ECHEC ($WORK)"; fi
exit $fail
