"""Vérification humaine et bilan des runs."""

from __future__ import annotations

from docia.db import Database
from docia.ingest.smbeagle_csv import parse_smbeagle_datetime
from docia.views._common import _FROM_LATEST, _IS_LATEST, Discrepancy, ReviewProgress, RunStat


def _review_counts(db: Database) -> dict[str, int]:
    """Fichiers par statut de vérification humaine, **parmi les fichiers analysés**.

    Le comptage portait sur toute la table `reviews`, sans lien avec l'ensemble qui
    sert de dénominateur (`_analyzed_files`). `set_review` accepte n'importe quel
    identifiant, y compris un fichier jamais analysé ou dont l'analyse a été invalidée
    par une modification du contenu : l'avancement affiché montait alors au-dessus de
    100 % — mesuré à **400 %** avec un analysé et quatre revues — et `not_reviewed`
    était ramené à 0 par un `max(..., 0)` qui masquait l'incohérence au lieu de la
    signaler. Les deux nombres se rapportent maintenant au même ensemble.
    """
    return {
        str(r[0]): int(r[1])
        for r in db.query_values(
            "SELECT r.status, COUNT(DISTINCT r.file_id) FROM reviews r"
            " WHERE EXISTS (SELECT 1 FROM analyses a JOIN files f ON f.id = a.file_id"
            "               AND a.content_version = f.content_version"
            "               WHERE a.file_id = r.file_id)"
            " GROUP BY r.status"
        )
    }


def _analyzed_files(db: Database) -> int:
    """Nombre de fichiers ayant au moins une analyse.

    Délégué à `Database.count_analyzed_files` : c'est le même chiffre que
    `counts()["analyses"]` de la fenêtre et de `docia status`, et il n'existe
    donc plus qu'une seule façon de le calculer.
    """
    return db.count_analyzed_files()


def review_progress(db: Database, *, limit: int | None = None) -> ReviewProgress:
    """Avancement des revues et écarts entre classe LLM et classe corrigée."""
    counts = _review_counts(db)
    analyzed = _analyzed_files(db)
    reviewed = sum(counts.values())
    if limit == 0:  # seuls les compteurs sont demandés : pas de recherche d'écarts
        return ReviewProgress(
            to_review=counts.get("to_review", 0),
            validated=counts.get("validated", 0),
            corrected=counts.get("corrected", 0),
            not_reviewed=max(analyzed - reviewed, 0),
            analyzed=analyzed,
        )
    rows = db.query(
        "SELECT f.id AS id, f.path AS path, a.security_classification AS sec,"
        " a.rgpd_risk_level AS rgpd, COALESCE(r.corrected_security,'') AS csec,"
        " COALESCE(r.corrected_rgpd,'') AS crgpd"
        f"{_FROM_LATEST} JOIN reviews r ON r.file_id = f.id"
        f" WHERE {_IS_LATEST} AND ("
        "       (r.corrected_security IS NOT NULL AND r.corrected_security <> ''"
        "        AND r.corrected_security <> a.security_classification)"
        "    OR (r.corrected_rgpd IS NOT NULL AND r.corrected_rgpd <> ''"
        "        AND r.corrected_rgpd <> a.rgpd_risk_level))"
        " ORDER BY f.path"
    )
    gaps = [
        Discrepancy(
            file_id=int(r["id"]),
            path=str(r["path"]),
            llm_security=str(r["sec"]),
            corrected_security=str(r["csec"]),
            llm_rgpd=str(r["rgpd"]),
            corrected_rgpd=str(r["crgpd"]),
        )
        for r in rows
    ]
    return ReviewProgress(
        to_review=counts.get("to_review", 0),
        validated=counts.get("validated", 0),
        corrected=counts.get("corrected", 0),
        not_reviewed=max(analyzed - reviewed, 0),
        analyzed=analyzed,
        discrepancies=gaps if limit is None else gaps[:limit],
        total_discrepancies=len(gaps),
    )


def runs_summary(db: Database) -> list[RunStat]:
    """Un enregistrement par run : blocs, tokens, durée, tokens moyens par fichier."""
    rows = db.query(
        "SELECT r.id AS id, r.started_at AS started, COALESCE(r.finished_at,'') AS finished,"
        " r.status AS status, r.model AS model, r.prompt_hash AS ph,"
        " COUNT(b.id) AS blocks,"
        " SUM(CASE WHEN b.status='done' THEN 1 ELSE 0 END) AS done,"
        " SUM(CASE WHEN b.status='error' THEN 1 ELSE 0 END) AS err,"
        " COALESCE(SUM(b.file_count),0) AS files,"
        " COALESCE(SUM(b.usage_prompt_tokens),0) AS ptok,"
        " COALESCE(SUM(b.usage_completion_tokens),0) AS ctok,"
        " COALESCE(AVG(b.latency_ms),0) AS lat"
        " FROM runs r LEFT JOIN blocks b ON b.run_id = r.id"
        " GROUP BY r.id ORDER BY r.id DESC"
    )
    out: list[RunStat] = []
    for r in rows:
        started = parse_smbeagle_datetime(str(r["started"]))
        finished = parse_smbeagle_datetime(str(r["finished"]))
        duration = (finished - started).total_seconds() if started and finished else 0.0
        files = int(r["files"])
        tokens = int(r["ptok"]) + int(r["ctok"])
        out.append(
            RunStat(
                run_id=int(r["id"]),
                started_at=str(r["started"]),
                finished_at=str(r["finished"]),
                status=str(r["status"]),
                model=str(r["model"]),
                prompt_hash=str(r["ph"]),
                blocks=int(r["blocks"]),
                blocks_done=int(r["done"] or 0),
                blocks_error=int(r["err"] or 0),
                files=files,
                prompt_tokens=int(r["ptok"]),
                completion_tokens=int(r["ctok"]),
                duration_s=round(max(duration, 0.0), 1),
                avg_latency_ms=round(float(r["lat"] or 0.0), 1),
                tokens_per_file=round(tokens / files, 1) if files else 0.0,
            )
        )
    return out
