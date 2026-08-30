#!/usr/bin/env python3
"""Fabrique un CSV SMBeagle (19 colonnes, guillemets sélectifs) à partir d'un dossier local.

Outil de développement : permet d'exercer `docia` sans SMBeagle (ex. sous Linux)
sur n'importe quel dossier. Le format imite `smbeagle --local-path` : `Host` =
`localhost`, `DirectoryType` = `LOCAL_FIXED`, `Base` = `\\localhost\\LOCAL_SCAN\\`,
`FastHash` = xxHash64 des 64 premiers Ko si `xxhash` est installé, sinon
SHA-256 tronqué à 16 hex (même rôle : détecter un changement de contenu).

Usage : python scripts/csv_from_dir.py DOSSIER [-o scan.csv] [--max-files N]
"""

from __future__ import annotations

import argparse
import getpass
import hashlib
import os
import socket
import sys
from datetime import datetime
from pathlib import Path

HEADER = (
    "Name,Host,Extension,Username,Hostname,UNCDirectory,CreationTime,LastWriteTime,Readable,"
    "Writeable,Deletable,DirectoryType,Base,FileSize,AccessTime,FileAttributes,Owner,FastHash,FileSignature"
)


def fast_hash(path: Path) -> str:
    head = path.read_bytes()[: 64 * 1024]
    try:
        import xxhash  # type: ignore[import-not-found]

        return str(xxhash.xxh64(head).hexdigest())
    except ImportError:
        return hashlib.sha256(head).hexdigest()[:16]


def fmt(ts: float) -> str:
    return datetime.fromtimestamp(ts).strftime("%d/%m/%Y %H:%M:%S")


def quote(text: str) -> str:
    return '"' + text.replace('"', '\\"') + '"'


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("folder", type=Path)
    ap.add_argument("-o", "--out", type=Path, default=Path("scan.csv"))
    ap.add_argument("--max-files", type=int, default=0)
    args = ap.parse_args(argv)
    root = args.folder.resolve()
    if not root.is_dir():
        print(f"dossier introuvable : {root}", file=sys.stderr)
        return 1
    user, host = getpass.getuser(), socket.gethostname()
    lines = [HEADER]
    count = 0
    skipped = 0
    for path in sorted(p for p in root.rglob("*") if p.is_file()):
        try:
            st = path.stat()
            digest = fast_hash(path)
        except OSError as exc:  # fichier illisible (montage cassé, verrou, E/S) → ignoré, compté
            print(f"ignoré ({exc.__class__.__name__}) : {path}", file=sys.stderr)
            skipped += 1
            continue
        lines.append(
            ",".join(
                [
                    quote(path.name),
                    quote("localhost"),
                    quote(path.suffix.lstrip(".").lower()),
                    quote(user),
                    quote(host),
                    quote(str(path.parent)),
                    fmt(st.st_ctime),
                    fmt(st.st_mtime),
                    "True",
                    str(os.access(path, os.W_OK)),
                    str(os.access(path.parent, os.W_OK)),
                    "LOCAL_FIXED",
                    quote("\\\\localhost\\LOCAL_SCAN\\"),
                    str(st.st_size),
                    fmt(st.st_atime),
                    quote("Archive"),
                    quote(user),
                    quote(digest),
                    quote("unknown"),
                ]
            )
        )
        count += 1
        if args.max_files and count >= args.max_files:
            break
    args.out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(
        f"{count} fichier(s) → {args.out}"
        + (f" ({skipped} illisible(s) ignoré(s))" if skipped else "")
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
