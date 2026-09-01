"""Écran Accueil (`docia.gui.tab_home`) — **sans écran**.

Le seul comportement testé ici est celui qui ment le plus cher : la ligne d'état
de l'étape « source » en fin de scan. Elle annonçait « scan terminé : N fichiers »
même quand une cible demandée n'avait pas été parcourue ou que l'utilisateur avait
arrêté le scan. Aucun Tk n'est construit : l'onglet est instancié nu et son
étiquette remplacée par une doublure qui retient ce qu'on lui a demandé d'afficher.
"""

from __future__ import annotations

from typing import Any

from docia.gui.theme import ACCENT_OK, ACCENT_STOP


class _EtiquetteFactice:
    """Le strict nécessaire d'un `CTkLabel` : ce qu'on lui a demandé d'afficher."""

    def __init__(self) -> None:
        self.etat: dict[str, Any] = {}

    def configure(self, **options: Any) -> None:
        self.etat.update(options)


def _accueil_nu() -> Any:
    """Un `HomeTab` non construit, muni de la seule étiquette d'état de l'étape 1."""
    from docia.gui.tab_home import HomeTab

    onglet = object.__new__(HomeTab)
    onglet.source_status = _EtiquetteFactice()
    return onglet


class _ResultatFactice:
    """Un `ScanResult` réduit à ses faits de périmètre."""

    def __init__(self, **faits: Any) -> None:
        self.files = faits.get("files", 0)
        self.skipped = faits.get("skipped", [])
        self.cancelled = faits.get("cancelled", False)
        self.expected_files = faits.get("expected_files", 0)

    @property
    def complete(self) -> bool:
        return not self.skipped and not self.cancelled and self.expected_files <= self.files


def test_accueil_annonce_un_perimetre_complet() -> None:
    """Cas normal : le compte de fichiers, et la mention explicite « périmètre complet »."""
    onglet = _accueil_nu()
    onglet._show_scan_scope(_ResultatFactice(files=7, expected_files=7))
    assert "périmètre complet" in onglet.source_status.etat["text"]
    assert onglet.source_status.etat["text_color"] == ACCENT_OK


def test_accueil_annonce_une_cible_non_parcourue() -> None:
    """Un partage refusé par une ACL sortait en « succès » : la cible était écartée,
    le scan se terminait, et l'accueil affichait un simple compte de fichiers. La
    ligne d'état dit maintenant ce qui manque et quoi faire, en rouge."""
    onglet = _accueil_nu()
    onglet._show_scan_scope(
        _ResultatFactice(files=7, expected_files=7, skipped=["\\\\srv\\finance"])
    )
    texte = onglet.source_status.etat["text"]
    assert "PÉRIMÈTRE INCOMPLET" in texte
    assert "srv\\finance" in texte
    assert "relancez le scan" in texte
    assert onglet.source_status.etat["text_color"] == ACCENT_STOP


def test_accueil_distingue_un_scan_arrete() -> None:
    """Un scan annulé n'est pas un scanner tombé : le texte le dit tel quel."""
    onglet = _accueil_nu()
    onglet._show_scan_scope(_ResultatFactice(files=4901, expected_files=10_000, cancelled=True))
    texte = onglet.source_status.etat["text"]
    assert "arrêté en cours de route" in texte
    assert "à refaire" not in texte
