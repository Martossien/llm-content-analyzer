"""Vérifications du test complet (`scripts/e2e_local.sh`) : ce que la base, les blocs et
les rapports doivent contenir quand toute la chaîne fonctionne. Chaque contrôle est
nommé et explicite ; sortie : une ligne par contrôle, puis le bilan."""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path


def main(work: Path) -> int:
    db = sqlite3.connect(work / "campagne.sqlite")
    db.row_factory = sqlite3.Row
    checks: list[tuple[str, bool, str]] = []

    def check(name: str, ok: bool, detail: str = "") -> None:
        checks.append((name, ok, detail))

    # -- scan
    scan = db.execute("SELECT * FROM scans ORDER BY id DESC LIMIT 1").fetchone()
    check(
        "scan importé avec manifeste",
        scan is not None and scan["kind"] == "scan" and scan["manifest_json"] != "",
        f"kind={scan['kind'] if scan else None}",
    )
    if scan and scan["manifest_json"]:
        manifest = json.loads(scan["manifest_json"])
        check(
            "manifeste : 19 colonnes",
            len(manifest.get("columns", [])) == 19,
            str(len(manifest.get("columns", []))),
        )
    files = db.execute("SELECT * FROM files").fetchall()
    by_name = {f["name"]: f for f in files}
    check("fichiers scannés", len(files) >= 12, f"{len(files)} fichiers")
    check(
        "hash présent sur les OLE (.doc/.xls/.ppt)",
        all(
            by_name[n]["fast_hash"]
            for n in ("sample.doc", "sample.xls", "sample.ppt")
            if n in by_name
        ),
        ", ".join(
            f"{n}={by_name[n]['fast_hash'][:6] if n in by_name else '?'}"
            for n in ("sample.doc", "sample.xls", "sample.ppt")
        ),
    )
    check(
        "dates d'accès : première observation renseignée",
        all(f["access_time_first"] for f in files),
    )

    # -- analyse
    counts = {
        r["status"]: r["n"]
        for r in db.execute("SELECT status, COUNT(*) n FROM files GROUP BY status")
    }
    check("aucun fichier en erreur", counts.get("error", 0) == 0, str(counts))
    blocks = {
        r["status"]: r["n"]
        for r in db.execute("SELECT status, COUNT(*) n FROM blocks GROUP BY status")
    }
    check(
        "aucun bloc en erreur (comptage exact + re-découpage)",
        blocks.get("error", 0) == 0,
        str(blocks),
    )
    check(
        "tout analysé (hors exclus)",
        counts.get("pending", 0) == 0 and counts.get("queued", 0) == 0,
        str(counts),
    )
    latest = {
        r["name"]: r
        for r in db.execute(
            "SELECT f.name, f.path, a.* FROM files f JOIN analyses a ON a.file_id=f.id AND a.content_version=f.content_version"
        )
    }

    # -- OCR relu par la LLM : le bulletin scanné doit être compris comme de la paie / RGPD sensible
    b = latest.get("bulletin_scanne.pdf")
    resume = (b["resume"] if b else "") or ""
    check("PDF scanné analysé (OCR → LLM)", b is not None)
    if b is not None:
        rgpd_ok = b["rgpd_risk_level"] in ("high", "critical")
        content_ok = any(w in resume.lower() for w in ("paie", "salaire", "bulletin", "dupont"))
        check(
            "PDF scanné : contenu compris (paie/salaire dans le résumé)", content_ok, resume[:120]
        )
        check("PDF scanné : RGPD élevé/critique", rgpd_ok, str(b["rgpd_risk_level"]))
        check(
            "PDF scanné : sécurité C2/C3",
            b["security_classification"] in ("C2", "C3"),
            str(b["security_classification"]),
        )
        check(
            "PDF scanné : conservation renseignée (paie)",
            bool(b["retention_required"]) and (b["retention_years"] or 0) > 0,
            f"{b['retention_basis']} {b['retention_years']} ans",
        )
    # -- doublon exact hérité
    c = latest.get("copie_bulletin.pdf")
    check(
        "doublon exact : analyse héritée",
        c is not None
        and b is not None
        and c["security_classification"] == b["security_classification"],
        f"{c['security_classification'] if c else None} vs {b['security_classification'] if b else None}",
    )
    dup = db.execute(
        "SELECT COUNT(*) n FROM files WHERE fast_hash=(SELECT fast_hash FROM files WHERE name='bulletin_scanne.pdf')"
    ).fetchone()["n"]
    check("doublon exact : même empreinte", dup == 2, str(dup))
    # -- gros fichier découpé en segments
    g = latest.get("gros_registre.txt")
    check(
        "gros fichier : segments agrégés",
        g is not None and (g["segments"] or 0) >= 2,
        f"segments={g['segments'] if g else None}",
    )
    # -- formats bureautiques (extracteurs) analysés
    for name in (
        "sample.docx",
        "sample.xlsx",
        "sample.pptx",
        "sample.pdf",
        "sample.doc",
        "sample.xls",
        "sample.odt",
        "sample.rtf",
        "sample.eml",
    ):
        a = latest.get(name)
        check(
            f"{name} analysé",
            a is not None and bool(a["security_classification"]),
            str(a["security_classification"]) if a else "absent",
        )

    # -- restitution
    html = work / "rapport.html"
    check(
        "rapport HTML produit",
        html.is_file()
        and html.stat().st_size > 20_000
        and "doublon" in html.read_text(encoding="utf-8", errors="replace").lower(),
    )
    check(
        "classeur Excel produit",
        (work / "resultats.xlsx").is_file() and (work / "resultats.xlsx").stat().st_size > 5_000,
    )
    pbi = sorted((work / "powerbi").glob("*.csv"))
    check(
        "dossier Power BI (CSV)",
        len(pbi) >= 5 and all(p.stat().st_size > 0 for p in pbi),
        f"{len(pbi)} CSV",
    )

    width = max(len(n) for n, _, _ in checks)
    failed = 0
    for name, ok, detail in checks:
        failed += 0 if ok else 1
        print(f"{'OK ' if ok else 'KO '} {name:{width}}  {detail}")
    print(f"\nvérifications : {len(checks) - failed}/{len(checks)} OK")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main(Path(sys.argv[1])))
