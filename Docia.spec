# -*- mode: python ; coding: utf-8 -*-
"""Spec PyInstaller pour Doc-IA analyzer (`Docia.exe`, Windows x64, --onefile).

Reprend les recettes éprouvées de DocFuse.spec (D-054/D-055 : ratisser les DLL
de Python pour les extensions natives ; hiddenimports des extracteurs chargés
dynamiquement ; données i18n/assets de DocFuse embarquées). `console=True` :
le même exécutable sert la CLI (`Docia.exe run …`) et la GUI (`Docia.exe`
sans argument) — une console reste ouverte derrière la fenêtre, c'est voulu.

    pip install -e ".[dev,gui]" pyinstaller
    pyinstaller --noconfirm Docia.spec          → dist/Docia.exe

Le nom suit DOCIA_APP_NAME (défaut Docia).

**OCR embarqué** (décision utilisateur du 30/08 : sans OCR, les PDF scannés — courriers,
factures numérisées — sortent vides et sont mal classés) : Tesseract (binaire, DLL,
tessdata fra+eng) est embarqué exactement comme dans `DocFuse-OCR.spec`, sous
`tesseract/tesseract.exe` + `tesseract/tessdata/` — l'arborescence que
`docfuse.core.ocr.tesseract._bundled_binary_path` attend dans `sys._MEIPASS`.
Le build exige `TESSERACT_HOME` (défaut `C:\Program Files\Tesseract-OCR`, préparé
par la CI : `choco install tesseract` + `fra.traineddata`) et échoue sinon ;
`DOCIA_NO_OCR=1` permet un build local sans OCR, explicitement.
"""

import os
import sys
from pathlib import Path

from PyInstaller.utils.hooks import collect_all, collect_data_files, collect_submodules

_APP_NAME = (os.environ.get("DOCIA_APP_NAME") or "Docia").strip() or "Docia"
_ROOT = Path(SPECPATH)
_SRC = _ROOT / "src"

# DLL natives de Python (tcl/tk, sqlite3, ssl, ffi…) — non auto-collectées.
_extra_binaries: list[tuple[str, str]] = []
_python_dlls_dir = Path(getattr(sys, "base_prefix", sys.prefix)) / "DLLs"
if _python_dlls_dir.is_dir():
    for _dll_path in sorted(_python_dlls_dir.glob("*.dll")):
        _extra_binaries.append((str(_dll_path), "."))

_datas = [
    (str(_SRC / "docia" / "prompts"), "docia/prompts"),
]

# Tesseract embarqué (même recette que DocFuse-OCR.spec : tout le dossier
# d'installation, les noms de DLL variant selon la version de build).
_tesseract_home = Path(os.environ.get("TESSERACT_HOME", r"C:\Program Files\Tesseract-OCR"))
if os.environ.get("DOCIA_NO_OCR") == "1":
    print("Docia.spec : build SANS OCR (DOCIA_NO_OCR=1) — les PDF scannés ne seront pas lus")
else:
    if not _tesseract_home.is_dir():
        raise FileNotFoundError(
            f"Tesseract introuvable dans TESSERACT_HOME={_tesseract_home} — l'OCR fait partie "
            "de Docia.exe : installez Tesseract (+ fra.traineddata) avant le build, "
            "ou DOCIA_NO_OCR=1 pour un build local sans OCR."
        )
    for _f in sorted(_tesseract_home.glob("*.exe")) + sorted(_tesseract_home.glob("*.dll")):
        _extra_binaries.append((str(_f), "tesseract"))
    _tessdata = sorted((_tesseract_home / "tessdata").glob("*.traineddata"))
    if not any(t.name == "fra.traineddata" for t in _tessdata):
        raise FileNotFoundError(
            f"fra.traineddata absent de {_tesseract_home / 'tessdata'} — l'OCR français est requis."
        )
    for _t in _tessdata:
        _datas.append((str(_t), "tesseract/tessdata"))
_datas += collect_data_files("docfuse")  # i18n/*.json, assets/*.ttf, vocabulaires tokenizers
_datas += collect_data_files("customtkinter")
_datas += collect_data_files("tiktoken_ext")

# Bibliothèques importées PARESSEUSEMENT par les extracteurs DocFuse (dans les
# fonctions, pas en tête de module) : PyInstaller ne les voit pas → l'exe
# démarre puis plante au premier .docx/.pdf/.xlsx. On les ratisse toutes,
# données et binaires compris (pypdfium2, lxml, office_oxide sont natifs).
_LAZY_LIBS = (
    "pypdf",
    "pdfminer",
    "pypdfium2",
    "docx",
    "pptx",
    "openpyxl",
    "lxml",
    "bs4",
    "striprtf",
    "ftfy",
    "oxmsg",
    "office_oxide",
    "charset_normalizer",
    "odf",
    "olefile",
    "xlrd",
)
_lazy_hidden: list[str] = []
for _lib in _LAZY_LIBS:
    try:
        _d, _b, _h = collect_all(_lib)
    except Exception:  # noqa: BLE001 — bibliothèque absente de cet environnement : ignorée
        continue
    _datas += _d
    _extra_binaries += _b
    _lazy_hidden += _h

_hidden = (
    collect_submodules("docfuse.extractors")
    + collect_submodules("docfuse.core")
    + collect_submodules("tiktoken_ext")
    + _lazy_hidden
    + [
        "tkinter",
        "tkinter.ttk",
        "tkinter.filedialog",
        "tkinter.messagebox",
        "customtkinter",
        "charset_normalizer",
        "tiktoken",
        "sqlite3",
        "httpx",
        "anyio",
        "h11",
        "email.parser",
        "email.policy",
    ]
)

block_cipher = None

a = Analysis(
    [str(_SRC / "docia" / "__main__.py")],
    pathex=[str(_SRC)],
    binaries=_extra_binaries,
    datas=_datas,
    hiddenimports=_hidden,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=["pytest", "mypy", "ruff", "tkinterdnd2"],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)
pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name=_APP_NAME,
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
