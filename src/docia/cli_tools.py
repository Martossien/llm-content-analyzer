"""Sous-commandes « outils » de la CLI : `docia bench`, `docia quick` et `docia scan`.

Le module s'enregistre dans le parseur principal (`cli.py`) via `register()`,
qui rend les gestionnaires associés — même contrat que les autres commandes :
`(args, cfg) -> code retour` (0 OK, 1 erreur, 2 erreurs partielles).
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

from docia.config import Config

Handler = Callable[[argparse.Namespace, Config], int]


def register(
    sub: argparse._SubParsersAction[argparse.ArgumentParser],
) -> dict[str, Handler]:
    """Ajoute `bench`, `quick` et `scan` au parseur et rend leurs gestionnaires."""
    s = sub.add_parser(
        "scan", help="étape 0 : lance SMBeagle_enriched sur un périmètre, importe et prépare"
    )
    s.add_argument(
        "--local-path", action="append", default=[], help="dossier local ou UNC monté (répétable)"
    )
    s.add_argument("--host", action="append", default=[], help="serveur SMB (répétable)")
    s.add_argument("--share", action="append", default=[], help="partage à retenir (répétable)")
    s.add_argument("--exclude-share", action="append", default=[], help="partage à ignorer")
    s.add_argument("--csv", type=Path, default=None, help="CSV de sortie (défaut : <base>.scans/)")
    s.add_argument(
        "--username", default="", help="compte SMB explicite (mot de passe : DOCIA_SMB_PASSWORD)"
    )
    s.add_argument("--domain", default="")
    s.add_argument("--no-plan", action="store_true", help="importer sans préparer")
    s.add_argument("--json", action="store_true", help="bilan en JSON")
    d = sub.add_parser(
        "doctor", help="état du poste : DocFuse, OCR (Tesseract), pdfium, scanner SMBeagle, serveur"
    )
    d.add_argument("--json", action="store_true", help="résultat en JSON")
    p = sub.add_parser("bench", help="mesure la vitesse du serveur LLM (blocs synthétiques)")
    p.add_argument("--blocks", type=int, default=6, help="nombre de blocs envoyés (défaut 6)")
    p.add_argument(
        "--block-tokens", type=int, default=8_000, help="taille visée d'un bloc (défaut 8000)"
    )
    p.add_argument("--files-per-block", type=int, default=4, help="documents par bloc (défaut 4)")
    p.add_argument(
        "--in-flight",
        type=int,
        default=None,
        help="requêtes en parallèle (défaut llm.max_in_flight)",
    )
    p.add_argument("--json", action="store_true", help="rapport JSON au lieu du résumé")

    p = sub.add_parser("quick", help="analyse immédiate de fichiers ou dossiers, sans CSV")
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="extraction et blocs seulement, sans LLM (contrôle d'un exécutable empaqueté)",
    )
    p.add_argument("paths", type=Path, nargs="+", metavar="PATH")
    p.add_argument(
        "--keep-db", type=Path, default=None, help="base à conserver (historique et reprise)"
    )
    p.add_argument("--json", action="store_true", help="rapport JSON au lieu du tableau")
    return {"bench": cmd_bench, "quick": cmd_quick}


def cmd_bench(args: argparse.Namespace, cfg: Config) -> int:
    """`docia bench` : débit du serveur LLM, en tokens/s et en fichiers/heure."""
    from docia.bench import run_bench

    report = run_bench(
        cfg,
        blocks=args.blocks,
        block_tokens=args.block_tokens,
        files_per_block=args.files_per_block,
        in_flight=args.in_flight,
        progress=None if args.json else print,
    )
    if args.json:
        print(json.dumps(report.as_dict(), ensure_ascii=False, indent=2))
    else:
        for line in report.as_lines():
            print(line)
    if not report.ok:
        print(report.message, file=sys.stderr)
        return 1
    return 2 if report.errors else 0


def cmd_quick(args: argparse.Namespace, cfg: Config) -> int:
    """`docia quick` : analyse immédiate de fichiers ou dossiers locaux."""
    from docia.quick import quick_analyze

    report = quick_analyze(
        cfg,
        args.paths,
        db_path=args.keep_db,
        progress=None if args.json else print,
        dry_run=bool(args.dry_run),
    )
    if args.json:
        print(json.dumps(report.as_dict(), ensure_ascii=False, indent=2))
    else:
        for line in report.as_lines():
            print(line)
    if not report.ok:
        print(report.message, file=sys.stderr)
        return 1
    if report.dry_run:
        return 2 if report.extraction_errors else 0
    return 2 if report.errors or report.llm_errors else 0


def cmd_scan(args: argparse.Namespace, cfg: Config) -> int:
    """`docia scan` : scanner → import → préparation, progression sur stderr."""
    from docia import service
    from docia.db import Database
    from docia.filter import plan_progress_logger
    from docia.scan import ScanProfile

    profile = ScanProfile(
        local_paths=list(args.local_path),
        hosts=list(args.host),
        shares=list(args.share),
        exclude_shares=list(args.exclude_share),
        domain=args.domain,
        username=args.username,
    )
    errors = profile.validate()
    if errors:
        print("scan : " + " ; ".join(errors), file=sys.stderr)
        return 2

    def on_event(ev: object) -> None:
        stage = getattr(ev, "stage", "")
        files = getattr(ev, "files", 0)
        print(f"scan [{stage}] {files} fichiers", file=sys.stderr)

    try:
        with Database(cfg.db_path) as db:
            result, report, plan_report = service.scan_campaign(
                db,
                cfg,
                profile,
                csv_out=args.csv,
                on_event=on_event,
                on_line=None if args.json else lambda line: print(line, file=sys.stderr),
                on_import_progress=service.import_progress_logger(
                    lambda line: print(line, file=sys.stderr)
                ),
                on_plan_progress=plan_progress_logger(lambda line: print(line, file=sys.stderr)),
                do_plan=not args.no_plan,
            )
    except service.ServiceError as exc:
        print(f"scan : {exc}", file=sys.stderr)
        return 1
    summary = {
        "csv": str(result.csv_path),
        "files": result.files,
        "elapsed_s": round(result.elapsed_s, 1),
        "new": report.new,
        "updated": report.updated,
        "unchanged": report.unchanged,
        "invalid": report.invalid,
        "pending": plan_report.pending,
        "excluded": plan_report.excluded,
    }
    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    else:
        # Le bilan d'import est formulé une seule fois, dans `service` : il était
        # recopié ici, dans `cli.py` et dans la fenêtre, et les trois divergeaient.
        print(
            f"scan : {result.files} fichiers en {result.elapsed_s:.0f} s → {result.csv_path}\n"
            f"{service.format_import_report(report)} — "
            f"préparation : {plan_report.pending} à analyser, {plan_report.excluded} exclus"
        )
    return 0


def doctor_report(cfg: Config) -> dict[str, Any]:
    """Diagnostic du poste (pur : aucun affichage). Chaque entrée dit ce qui marche
    et pourquoi ça ne marche pas — c'est ce que l'administrateur lit quand un PDF
    scanné sort vide ou qu'un exe « plante au lancement »."""
    import platform

    from docia import __version__
    from docia.scan import find_smbeagle

    report: dict[str, Any] = {
        "docia": __version__,
        "python": platform.python_version(),
        "frozen": bool(getattr(sys, "frozen", False)),
        "platform": platform.platform(),
    }
    try:
        import docfuse

        report["docfuse"] = getattr(docfuse, "__version__", "?")
    except Exception as exc:  # noqa: BLE001
        report["docfuse"] = f"ABSENT : {exc}"
    try:
        import pypdfium2

        info = getattr(pypdfium2, "PDFIUM_INFO", None)
        report["pdfium"] = str(
            getattr(info, "build", None) or getattr(pypdfium2, "__version__", "ok")
        )
        try:
            import io

            from PIL import Image

            buf = io.BytesIO()
            Image.new("RGB", (40, 40), "white").save(buf, "PDF")
            doc = pypdfium2.PdfDocument(buf.getvalue())
            size = doc[0].render(scale=1).to_pil().size
            report["pdfium_raster"] = "ok" if size == (40, 40) else f"taille inattendue {size}"
        except Exception as exc:  # noqa: BLE001
            report["pdfium_raster"] = f"ÉCHEC : {exc}"
    except Exception as exc:  # noqa: BLE001
        report["pdfium"] = f"ABSENT : {exc} — aucun raster, donc aucun OCR"
    try:
        from docfuse.core.ocr import tesseract as tess
        from docfuse.core.ocr.registry import list_ocr_engines

        report["ocr_engines"] = [e.id for e in list_ocr_engines()]
        # Essai OCR réel sur une image fabriquée ici : c'est le seul contrôle qui prouve
        # que Tesseract lit vraiment quelque chose sur CE poste, et le seul qui rapporte
        # le `stderr` du binaire (« Error opening data file… ») quand il sort en code 1.
        probe = tess.self_test()
        report["tesseract"] = probe.get("binary") or "introuvable (ni embarqué, ni dans le PATH)"
        report["tesseract_version"] = probe.get("version", "")
        report["tesseract_langs"] = probe.get("available_languages", [])
        report["tesseract_lang_utilisee"] = probe.get("effective_lang", "")
        report["tessdata_prefix"] = probe.get("tessdata_prefix") or "(hérité du système)"
        # Le booléen fait foi, pas la phrase : le code retour de `doctor` en dépend,
        # et la CI Windows s'en sert. Le reformuler ne doit jamais rendre 1 en silence.
        report["ocr_ok"] = bool(probe.get("ocr_ok"))
        report["ocr_essai"] = (
            f"ok — « {probe.get('ocr_text', '')} » relu"
            if report["ocr_ok"]
            else f"ÉCHEC (code {probe.get('returncode')}) : {probe.get('stderr') or 'sans message'}"
        )
    except Exception as exc:  # noqa: BLE001
        report["ocr_ok"] = False
        report["ocr"] = f"ÉCHEC : {exc} — OCR indisponible, les PDF scannés sortiront vides"
    if cfg.llm.transport == "vllm":
        try:
            import httpx

            data = httpx.get(cfg.llm.base_url.rstrip("/") + "/models", timeout=5).json()
            served = next(
                (
                    m.get("max_model_len")
                    for m in data.get("data", [])
                    if isinstance(m, dict) and m.get("id") == cfg.llm.model
                ),
                None,
            )
            if served is not None:
                match = (
                    "identique"
                    if served == cfg.llm.max_context_tokens
                    else (f"≠ config {cfg.llm.max_context_tokens} — la valeur du serveur fait foi")
                )
                report["llm_contexte_servi"] = f"{served} ({match})"
        except Exception as exc:  # noqa: BLE001 — serveur éteint : information, pas une erreur
            report["llm_contexte_servi"] = f"serveur injoignable ({type(exc).__name__})"
    scanner = find_smbeagle(cfg.scan.smbeagle_path)
    report["smbeagle"] = (
        str(scanner) if scanner else "introuvable (à côté de l'exe ou scan.smbeagle_path)"
    )
    report["llm"] = f"{cfg.llm.transport} {cfg.llm.base_url} modèle {cfg.llm.model}"
    return report


def cmd_doctor(args: argparse.Namespace, cfg: Config) -> int:
    """`docia doctor` : état du poste ; code 1 si l'OCR ne lit pas réellement une image.

    Le contrôle porte sur un OCR **réellement exécuté** (`ocr_essai`), pas seulement sur
    la présence du binaire : sur un serveur, Tesseract peut être là et refuser de lire
    (fichier de langue mis en quarantaine, TESSDATA_PREFIX détourné…).
    """
    report = doctor_report(cfg)
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        for key, value in report.items():
            print(f"{key:24} {value}")
    ok = bool(report.get("ocr_ok")) and report.get("pdfium_raster") == "ok"
    if not ok:
        detail = report.get("ocr_essai") or report.get("ocr") or "OCR indisponible"
        print(
            f"doctor : les PDF scannés sortiront vides — {detail}",
            file=sys.stderr,
        )
    return 0 if ok else 1
