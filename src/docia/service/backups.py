"""Sauvegardes horodatées d'une campagne : écriture atomique, rotation, restauration."""

from __future__ import annotations

import os
import re
import shutil
import sqlite3
from contextlib import suppress
from pathlib import Path

from docia.db import Database, backup_dir_for
from docia.service._common import (
    BACKUP_SUFFIX,
    DEFAULT_KEEP_BACKUPS,
    SAFETY_LABEL_PREFIX,
    ServiceError,
    _slug,
    _stamp,
    logger,
)


def _unique_backup_path(directory: Path, stem: str, label: str) -> Path:
    """Chemin libre `<stem>_<horodatage>[_label][_n].sqlite` (deux appels dans la même seconde).

    Le `.sqlite.tmp` correspondant compte comme occupé : la copie n'apparaît sous
    son nom définitif qu'au `os.replace` final, et deux sauvegardes lancées dans la
    même seconde choisiraient sinon le même nom — la seconde écraserait la première.
    """
    suffix = f"_{_slug(label)}" if _slug(label) else ""
    base = f"{stem}_{_stamp()}{suffix}"
    candidate = directory / f"{base}{BACKUP_SUFFIX}"
    counter = 2
    while candidate.exists() or candidate.with_name(candidate.name + ".tmp").exists():
        candidate = directory / f"{base}_{counter}{BACKUP_SUFFIX}"
        counter += 1
    return candidate


def backup_database(
    db_path: Path,
    *,
    out_dir: Path | None = None,
    label: str = "",
    keep: int = DEFAULT_KEEP_BACKUPS,
    db: Database | None = None,
) -> Path:
    """Sauvegarde horodatée d'une base, cohérente même pendant un run.

    Écrit `<base>.backups/<nom>_AAAAMMJJ-HHMMSS[_étiquette].sqlite` via l'API
    `sqlite3` de sauvegarde, puis ne garde que les `keep` copies courantes les
    plus récentes **de cette campagne** (`keep <= 0` : aucune rotation ; les
    copies de sûreté, voir `SAFETY_LABEL_PREFIX`, ne sont jamais tournées).
    `db` évite d'ouvrir une seconde connexion quand la base est déjà ouverte.

    La copie est écrite dans `<nom>.sqlite.tmp` puis renommée par `os.replace`,
    comme `restore_database` et `_write_recent`. Sans cela, une machine éteinte
    au milieu d'une sauvegarde de 932 Mo laissait un fichier tronqué que
    `list_backups` présentait comme « la plus récente » — donc celle qu'un
    utilisateur restaure. Un `.tmp` abandonné, lui, n'est jamais listé.
    """
    source = Path(db_path)
    if db is None and not source.exists():
        raise ServiceError(f"base introuvable : {source}")
    directory = Path(out_dir) if out_dir is not None else backup_dir_for(source)
    try:
        directory.mkdir(parents=True, exist_ok=True)
        target = _unique_backup_path(directory, source.stem, label)
        temporary = target.with_name(target.name + ".tmp")
        try:
            if db is not None:
                db.backup_to(temporary)
            else:
                src = sqlite3.connect(str(source))
                try:
                    dest = sqlite3.connect(str(temporary))
                    try:
                        src.backup(dest)
                    finally:
                        dest.close()
                finally:
                    src.close()
            os.replace(temporary, target)
        except BaseException:
            with suppress(OSError):  # une copie partielle ne protège rien
                temporary.unlink(missing_ok=True)
            raise
    except (OSError, sqlite3.Error) as exc:
        raise ServiceError(f"sauvegarde impossible dans {directory} : {exc}") from exc
    logger.info("sauvegarde → %s", target)
    _rotate(directory, source.stem, keep)
    return target


def _rotate(directory: Path, stem: str, keep: int) -> None:
    """Supprime les sauvegardes **courantes** de cette campagne au-delà de `keep`.

    Ne voit ni les sauvegardes d'une autre campagne du même dossier, ni les
    copies de sûreté : voir `_backups_in` et `SAFETY_LABEL_PREFIX`.
    """
    if keep <= 0:
        return
    for old in _rotatable_in(directory, stem)[keep:]:
        try:
            old.unlink()
            logger.info("sauvegarde éliminée par rotation : %s", old.name)
        except OSError as exc:  # pragma: no cover - fichier verrouillé
            logger.warning("suppression impossible de %s : %s", old, exc)


_CURRENT_TAIL = re.compile(r"^\d{8}-\d{6}(?:_(?P<label>.+))?$")
"""Ce qui suit `<campagne>_` dans une sauvegarde de `backup_database` :
l'horodatage de `_stamp()`, puis l'étiquette et le rang éventuels."""

_MIGRATION_TAIL = re.compile(r"^avant_migration_v\d+_\d{8}T\d{6}(?:_\d+)?$")
"""Idem pour une copie d'avant-migration, écrite par `Database._backup_before_migration`
(horodatage `AAAAMMJJTHHMMSS`, sans tiret)."""


def _backup_tail(path: Path, stem: str) -> str | None:
    """Ce qui suit `<stem>_` si `path` est une sauvegarde de **cette** campagne, sinon `None`.

    Le nom complet est exigé : `<stem>_` suivi d'un horodatage reconnu. Un simple
    `glob("audit_*.sqlite")` réclamait aussi les sauvegardes de la campagne
    `audit_2024_direction` — que l'écran Rapports invite précisément à ranger dans
    le même dossier. `list_backups` les présentait comme siennes et la rotation
    d'`audit` les supprimait. Le `.tmp` d'une sauvegarde en cours (ou abandonnée)
    n'est pas non plus une sauvegarde : il n'a pas le suffixe attendu.
    """
    if path.suffix != BACKUP_SUFFIX:
        return None
    prefix = f"{stem}_"
    name = path.stem
    if not name.startswith(prefix):
        return None
    tail = name[len(prefix) :]
    if not (_CURRENT_TAIL.match(tail) or _MIGRATION_TAIL.match(tail)):
        return None
    return tail if path.is_file() else None


def _is_safety_copy(tail: str) -> bool:
    """Une copie de sûreté (`avant_migration_*`, `avant_restauration`, `avant_reanalyse_*`) ?"""
    if _MIGRATION_TAIL.match(tail):
        return True
    match = _CURRENT_TAIL.match(tail)
    label = str(match.group("label") or "") if match else ""
    return label.startswith(SAFETY_LABEL_PREFIX)


def _backups_in(directory: Path, stem: str) -> list[Path]:
    """Sauvegardes de **cette** campagne, de la plus récente à la plus ancienne.

    Copies de sûreté comprises : elles sont restaurables comme les autres, et
    l'utilisateur doit les voir. Seule la rotation les ignore (`_rotatable_in`).
    """
    if not directory.is_dir():
        return []
    found = [p for p in directory.iterdir() if _backup_tail(p, stem) is not None]
    return sorted(found, key=lambda p: (p.stat().st_mtime_ns, p.name), reverse=True)


def _rotatable_in(directory: Path, stem: str) -> list[Path]:
    """Vivier de la rotation : les sauvegardes courantes, hors copies de sûreté."""
    out: list[Path] = []
    for path in _backups_in(directory, stem):
        tail = _backup_tail(path, stem)
        if tail is not None and not _is_safety_copy(tail):
            out.append(path)
    return out


def list_backups(db_path: Path) -> list[Path]:
    """Sauvegardes d'une base, de la plus récente à la plus ancienne.

    Seulement celles de cette campagne, même si le dossier en abrite d'autres, et
    jamais une copie en cours d'écriture (`.sqlite.tmp`).
    """
    source = Path(db_path)
    return _backups_in(backup_dir_for(source), source.stem)


def restore_database(db_path: Path, backup_path: Path) -> Path:
    """Restaure une sauvegarde par-dessus la base et rend le chemin restauré.

    La sauvegarde est **d'abord** recopiée dans `<base>.tmp`, ensuite seulement la
    base courante est mise de côté (étiquette `avant_restauration`), et enfin le
    `.tmp` prend la place de la base par `os.replace` (atomique sous Windows comme
    sous POSIX). Les journaux `-wal`/`-shm` de l'ancienne base sont retirés pour ne
    pas être rejoués sur la nouvelle.

    Cet ordre n'est pas un détail : la sauvegarde préalable déclenche une rotation,
    et la rotation supprimait le fichier que l'on est en train de restaurer dès
    qu'il était le plus ancien des dix. La restauration échouait (« No such file or
    directory ») **et** la copie visée était perdue — juste au moment où l'on comptait
    dessus. Copier avant de tourner met la source à l'abri quoi qu'il advienne.

    Aucun verrou n'est posé : c'est à l'appelant de s'assurer qu'aucun run ni
    aucune interface n'a la base ouverte (sinon le remplacement échoue sous
    Windows, et les lecteurs en cours voient l'ancienne base sous POSIX).
    """
    source = Path(backup_path)
    target = Path(db_path)
    if not source.is_file():
        raise ServiceError(f"sauvegarde introuvable : {source}")
    try:
        probe = sqlite3.connect(f"file:{source}?mode=ro", uri=True)
        try:
            probe.execute("SELECT COUNT(*) FROM sqlite_master").fetchone()
        finally:
            probe.close()
    except sqlite3.Error as exc:
        raise ServiceError(f"sauvegarde illisible ({source.name}) : {exc}") from exc

    temporary = target.with_name(target.name + ".tmp")
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, temporary)  # avant toute rotation : voir la docstring
        if target.exists():
            backup_database(target, label="avant_restauration")
        os.replace(temporary, target)
        for sidecar in (
            target.with_name(target.name + "-wal"),
            target.with_name(target.name + "-shm"),
        ):
            sidecar.unlink(missing_ok=True)
    except OSError as exc:
        with suppress(OSError):
            temporary.unlink(missing_ok=True)
        raise ServiceError(f"restauration impossible vers {target} : {exc}") from exc
    except BaseException:  # `ServiceError` de la sauvegarde préalable, interruption…
        with suppress(OSError):
            temporary.unlink(missing_ok=True)
        raise
    logger.info("restauration : %s → %s", source, target)
    return target


# ------------------------------------------------------------------ campagnes
