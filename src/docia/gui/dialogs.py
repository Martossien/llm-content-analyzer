"""Boîtes de dialogue de la fenêtre : ce qui a besoin de Tk *et* de la CLI.

`produce_document` demande un emplacement (`filedialog`), fait produire le document
par `docia.cli` et ouvre le rapport HTML (`webbrowser`). Rien de tout cela n'a sa
place dans `service_shim`, dont la promesse est de ne rien savoir de Tk.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any


def produce_document(
    app: Any,
    fmt: str,
    kind: str,
    *,
    on_done: Callable[[Path], None] | None = None,
) -> None:
    """Demande l'emplacement, produit le document, puis **ouvre le rapport HTML**.

    Partagé par l'onglet Rapports et par le bouton « Rapport HTML… » des Statistiques :
    un bouton qui promet un document doit produire ce document, pas seulement changer
    d'onglet. Le chemin complet est écrit au journal — c'est la réponse à « où est le
    fichier ? ».

    `--config` est passé explicitement : sans lui, la CLI relit le `docia.toml` du
    répertoire courant — pas celui que la fenêtre a ouvert. Un fichier invalide dans ce
    répertoire faisait alors répondre « configuration refusée (1) » à tous les boutons
    d'export, sans dire ce qui clochait ni où.
    """
    from tkinter import filedialog

    if not app.db_path().exists():
        app.log("aucune campagne ouverte")
        return
    stem = app.db_path().stem
    if fmt == "powerbi":
        chosen = filedialog.askdirectory(title="Dossier de sortie Power BI")
    else:
        chosen = filedialog.asksaveasfilename(
            title=f"Enregistrer le document {fmt.upper()}",
            defaultextension=f".{fmt}",
            initialfile=f"{stem}-rapport.{fmt}",
            filetypes=[(fmt.upper(), f"*.{fmt}")],
        )
    if not chosen:
        return
    out = Path(chosen)
    db_path = str(app.db_path())
    config_path = str(app.config_path)

    def work() -> None:
        from docia.cli import main as cli_main

        argv = ["--config", config_path, "--db", db_path, kind, "--format", fmt, "--out", str(out)]
        try:
            code = cli_main(argv)
        except SystemExit:  # `_load` sort par SystemExit sur une config invalide
            app.log(f"{fmt} : configuration refusée — {config_path}")
            for line in config_problems(config_path):
                app.log(f"   {line}")
            return
        if code != 0:
            app.log(f"{fmt} : échec de la production du document")
            return
        app.log(f"document {fmt} écrit : {out}")
        if fmt == "html":
            import webbrowser

            webbrowser.open(out.as_uri())
            app.log("le rapport s'ouvre dans le navigateur")
        if on_done is not None:
            app.ui(lambda: on_done(out))

    app.run_in_thread(work, f"document {fmt}")


def config_problems(config_path: str) -> list[str]:
    """Ce que la CLI reproche à la configuration, en clair — jamais un code de retour.

    « configuration refusée (1) » n'apprend rien à personne : on relit le fichier et on
    recopie au journal les lignes de `Config.validate()` (ou l'erreur de lecture).
    """
    from docia.config import load_config

    path = Path(config_path)
    try:
        return load_config(path if path.exists() else None).validate() or ["aucune erreur relevée"]
    except (ValueError, OSError) as exc:
        return [str(exc)]
