"""Risque et conformité : matrices par axe, répartition par répertoire, fichiers sensibles."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any

from docia.db import Database
from docia.views._common import (
    _FROM_LATEST,
    _IS_LATEST,
    _RGPD_AT_RISK,
    _SENSITIVE,
    RGPD_LEVELS,
    SECURITY_CLASSES,
    AxisRow,
    SensitiveFile,
    _count_latest,
)
from docia.views.axes import AXES, RiskTally, _axis_group, _axis_labeller, _axis_risk, _axis_volumes


def _new_tally() -> RiskTally:
    return (dict.fromkeys(SECURITY_CLASSES, 0), dict.fromkeys(RGPD_LEVELS, 0), [0])


def _fold_risk(
    rows: Iterable[tuple[Any, ...]], width: int, label_of: Callable[[tuple[Any, ...]], str]
) -> dict[str, RiskTally]:
    """Répartition sécurité/RGPD par étiquette, **cumulée au fil des lignes**.

    Ce que retient ce repli ne dépend que du nombre d'**étiquettes**, jamais du
    nombre de lignes lues : chaque ligne est ajoutée aux compteurs de son
    étiquette puis oubliée. La version précédente empilait un tuple Python par
    ligne source dans une liste par étiquette, pour les cumuler ensuite : sur une
    campagne de 934 028 fichiers tous analysés, réduits à 5 862 étiquettes,
    c'étaient 934 028 tuples retenus — 444 Mo — pour rendre exactement le même
    résultat. `tests/test_views.py` mesure que la taille du repli ne bouge pas
    quand le nombre de lignes est multiplié par cinquante mille.
    """
    tallies: dict[str, RiskTally] = {}
    for row in rows:
        # Une analyse sans classe de sécurité tombe dans « N/A », elle n'est pas jetée.
        # Le `continue` d'avant supprimait la ligne **entière** — son niveau RGPD compris :
        # la synthèse annonçait 3 analysés et 2 RGPD à risque, la matrice du même rapport
        # 2 et 1. Le cas existe (analyse aboutie mais champ vide, ou échec partiel du
        # modèle), et c'est précisément celui qu'il faut voir : un fichier au risque RGPD
        # critique disparaissait du tableau parce que sa sécurité n'était pas renseignée.
        classification = row[width] or "N/A"
        label = label_of(row)
        entry = tallies.get(label)
        if entry is None:
            entry = _new_tally()
            tallies[label] = entry
        security, rgpd, analyzed = entry
        number = int(row[width + 2])
        security[classification] = security.get(classification, 0) + number
        niveau = row[width + 1] or "N/A"  # même règle pour le RGPD non renseigné
        rgpd[niveau] = rgpd.get(niveau, 0) + number
        analyzed[0] += number
    return tallies


def classification_matrix(
    db: Database, *, axis: str = "share", depth: int = 2, limit: int | None = None
) -> list[AxisRow]:
    """Classification par valeur d'axe : `share`, `owner`, `directory` ou `extension`.

    Deux requêtes ciblées sur l'axe demandé : la volumétrie sur l'index couvrant
    des fichiers, la répartition sécurité/RGPD sur les seuls fichiers analysés.

    Raises:
        ValueError: axe inconnu.
    """
    if axis not in AXES:
        raise ValueError(f"axe inconnu : {axis}")
    keys = _axis_group(db, axis)
    width = len(keys)
    risk = _fold_risk(_axis_risk(db, keys), width, _axis_labeller(axis, depth))
    label_of = _axis_labeller(axis, depth)
    totals: dict[str, list[int]] = {}
    for row in _axis_volumes(db, keys):
        label = label_of(row)
        entry_totals = totals.get(label)
        if entry_totals is None:
            totals[label] = [row[width], row[width + 1]]
        else:
            entry_totals[0] += row[width]
            entry_totals[1] += row[width + 1]
    out: list[AxisRow] = []
    for label, (count, size) in totals.items():
        security, rgpd, analyzed = risk.get(label) or _new_tally()
        out.append(
            AxisRow(
                label=label,
                files=count,
                bytes=size,
                analyzed=analyzed[0],
                security=security,
                rgpd=rgpd,
            )
        )
    out.sort(key=lambda r: (-r.sensitive, -r.analyzed, -r.bytes, r.label))
    return out if limit is None else out[:limit]


def by_directory(db: Database, *, depth: int = 2, limit: int | None = None) -> list[AxisRow]:
    """Répertoires (partage + `depth` niveaux) : volume et répartition sécurité."""
    rows = classification_matrix(db, axis="directory", depth=depth)
    rows.sort(key=lambda r: (-r.bytes, -r.files, r.label))
    return rows if limit is None else rows[:limit]


def count_sensitive(db: Database) -> int:
    """Nombre de fichiers que `top_sensitive` classerait, **sans la borne**.

    Ce que le « top 50 » ne montre pas doit pouvoir être annoncé : un simple
    `COUNT`, pas la liste.
    """
    return _count_latest(db, f"({_SENSITIVE} OR {_RGPD_AT_RISK})")


def top_sensitive(db: Database, *, limit: int | None = 50) -> list[SensitiveFile]:
    """Les `limit` fichiers les plus sensibles : C2/C3, ou RGPD `high`/`critical`.

    C'est un **classement borné**, pas la liste exhaustive : `count_sensitive`
    donne le total, l'onglet « Fichiers » du classeur et `analyses.csv` de
    l'export Power BI donnent la totalité des analyses. `limit=None` lève la
    borne (`LIMIT -1` : la convention SQLite pour « pas de limite »).
    """
    rows = db.query(
        "SELECT f.id AS id, f.path AS path, f.owner AS owner, f.size_bytes AS size,"
        " a.security_classification AS sec, a.security_confidence AS secc,"
        " a.security_justification AS just, a.rgpd_risk_level AS rgpd,"
        " a.rgpd_confidence AS rgpdc, a.resume AS resume,"
        " COALESCE(r.status,'') AS review"
        f"{_FROM_LATEST} LEFT JOIN reviews r ON r.file_id = f.id"
        f" WHERE {_IS_LATEST} AND ({_SENSITIVE} OR {_RGPD_AT_RISK})"
        " ORDER BY CASE a.security_classification WHEN 'C3' THEN 0 WHEN 'C2' THEN 1"
        " WHEN 'C1' THEN 2 WHEN 'C0' THEN 3 ELSE 4 END,"
        " CASE a.rgpd_risk_level WHEN 'critical' THEN 0 WHEN 'high' THEN 1 WHEN 'medium' THEN 2"
        " WHEN 'low' THEN 3 ELSE 4 END, f.size_bytes DESC, f.path LIMIT ?",
        (-1 if limit is None else limit,),
    )
    return [
        SensitiveFile(
            file_id=int(r["id"]),
            path=str(r["path"]),
            owner=str(r["owner"]),
            size_bytes=int(r["size"]),
            security=str(r["sec"]),
            security_confidence=int(r["secc"]),
            rgpd=str(r["rgpd"]),
            rgpd_confidence=int(r["rgpdc"]),
            resume=str(r["resume"]),
            justification=str(r["just"]),
            review_status=str(r["review"]),
        )
        for r in rows
    ]
