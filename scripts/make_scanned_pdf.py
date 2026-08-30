"""Fabrique un PDF « scanné » (une image, aucun texte natif) pour tester l'OCR embarqué.

    python scripts/make_scanned_pdf.py sortie.pdf "TEXTE À RECONNAÎTRE"

Pillow suffit (dépendance de DocFuse) : le texte est dessiné en grand, noir sur
blanc, puis la page est enregistrée comme image PDF — exactement ce que produit
un copieur. Tesseract doit relire le texte ; un PDF natif ne testerait rien.
"""

from __future__ import annotations

import sys
from pathlib import Path


def make_scanned_pdf(out: Path, text: str) -> None:
    from PIL import Image, ImageDraw, ImageFont

    page = Image.new("RGB", (1654, 2339), "white")  # A4 à 200 dpi
    draw = ImageDraw.Draw(page)
    font = None
    for candidate in (
        "arial.ttf",
        "DejaVuSans.ttf",
        "/usr/share/fonts/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ):
        try:
            font = ImageFont.truetype(candidate, 72)
            break
        except OSError:
            continue
    if font is None:
        font = ImageFont.load_default(size=72)
    y = 300
    for line in text.split("|"):
        draw.text((150, y), line, fill="black", font=font)
        y += 140
    out.parent.mkdir(parents=True, exist_ok=True)
    page.save(out, "PDF", resolution=200.0)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(2)
    make_scanned_pdf(Path(sys.argv[1]), sys.argv[2])
    print(f"PDF scanné écrit : {sys.argv[1]}")
