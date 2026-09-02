"""Écran Résultats & vérification : filtres lisibles, tableau coloré par sévérité,
fiche du fichier avec pastilles, vérification en deux boutons (Valider / Corriger)."""

from __future__ import annotations

from typing import Any

from docia.db import Database
from docia.gui.helpers import pretty_amounts, pretty_list, result_rows_v31
from docia.gui.lazy import LazyScreen
from docia.gui.theme import (
    ACCENT_OK,
    FONT_FAMILY,
    FONT_SIZE,
    FONT_SIZE_SMALL,
    REVIEW_LABELS,
    RGPD_LABELS,
    SECURITY_LABELS,
    STATUS_LABELS,
    folder_of,
    format_bytes,
    shorten_path,
)
from docia.gui.widgets import Badge, Card, Table

_COLUMNS = (
    "nom",
    "dossier",
    "sécu",
    "rgpd",
    "finance",
    "juridique",
    "conservation",
    "revue",
    "résumé",
)
_WIDTHS = {
    "nom": 220,
    "dossier": 260,
    "sécu": 60,
    "rgpd": 70,
    "finance": 110,
    "juridique": 110,
    "conservation": 120,
    "revue": 80,
    "résumé": 420,
}
_ALL = "(tous)"
_SEC_FILTERS = {
    _ALL: None,
    "C3 secret": "C3",
    "C2 confidentiel": "C2",
    "C1 interne": "C1",
    "C0 public": "C0",
    "non évalué": "N/A",
}
_RGPD_FILTERS = {
    _ALL: None,
    "critique": "critical",
    "élevé": "high",
    "moyen": "medium",
    "faible": "low",
    "aucune": "none",
}
_REVIEW_FILTERS = {
    _ALL: None,
    "non vérifié": "",
    "à vérifier": "to_review",
    "validé": "validated",
    "corrigé": "corrected",
}
_LIMIT = 1000
TAB_NAME = "Résultats"


class ResultsTab(LazyScreen):
    """Onglet Résultats : tableau filtrable, fiche d'un fichier, validation/correction."""

    TAB_NAME = "Résultats"

    def __init__(self, app: Any, parent: Any) -> None:
        self.app = app
        self.parent = parent
        self.ctk = app.ctk
        self._rows: list[dict[str, str]] = []
        self._selected_id: int | None = None
        self._lazy_setup()

    def build(self) -> None:
        """Construit les widgets de l'onglet (une fois)."""
        ctk, p = self.ctk, self.parent
        filters = ctk.CTkFrame(p, fg_color="transparent")
        filters.pack(fill="x", padx=8, pady=(8, 4))
        self.sec_var = self._filter(filters, "Sécurité", list(_SEC_FILTERS), 140)
        self.rgpd_var = self._filter(filters, "RGPD", list(_RGPD_FILTERS), 110)
        self.review_var = self._filter(filters, "Vérification", list(_REVIEW_FILTERS), 120)
        ctk.CTkLabel(filters, text="Recherche").pack(side="left", padx=(8, 4))
        self.search_var = ctk.StringVar(value="")
        entry = ctk.CTkEntry(
            filters,
            textvariable=self.search_var,
            width=220,
            placeholder_text="nom, dossier, propriétaire…",
        )
        entry.pack(side="left")
        entry.bind("<Return>", lambda _e: self.refresh())
        ctk.CTkButton(filters, text="Filtrer", width=70, command=self.refresh).pack(
            side="left", padx=6
        )
        self.count_label = ctk.CTkLabel(
            filters, text="", font=ctk.CTkFont(family=FONT_FAMILY, size=FONT_SIZE_SMALL)
        )
        self.count_label.pack(side="right")

        self.table = Table(
            ctk, p, columns=_COLUMNS, on_select=self._select, height=300, widths=_WIDTHS
        )
        self.table.pack(fill="both", expand=True, padx=8, pady=4)

        card = Card(ctk, p, "Fiche du fichier", subtitle="sélectionne une ligne")
        card.pack(fill="x", padx=8, pady=(4, 8))
        self.card = card
        head = ctk.CTkFrame(card.body, fg_color="transparent")
        head.pack(fill="x")
        self.path_label = ctk.CTkLabel(
            head,
            text="",
            anchor="w",
            font=ctk.CTkFont(family=FONT_FAMILY, size=FONT_SIZE, weight="bold"),
        )
        self.path_label.pack(side="left", fill="x", expand=True)
        badges = ctk.CTkFrame(card.body, fg_color="transparent")
        badges.pack(fill="x", pady=(4, 2))
        self.sec_badge = Badge(ctk, badges)
        self.sec_badge.pack(side="left", padx=(0, 6))
        self.rgpd_badge = Badge(ctk, badges)
        self.rgpd_badge.pack(side="left", padx=(0, 6))
        self.ret_badge = Badge(ctk, badges)
        self.ret_badge.pack(side="left", padx=(0, 6))
        self.review_badge = Badge(ctk, badges)
        self.review_badge.pack(side="left", padx=(0, 6))
        self.meta_label = ctk.CTkLabel(
            badges, text="", font=ctk.CTkFont(family=FONT_FAMILY, size=FONT_SIZE_SMALL)
        )
        self.meta_label.pack(side="left", padx=8)
        self.detail_label = ctk.CTkLabel(
            card.body, text="", anchor="w", justify="left", wraplength=1100
        )
        self.detail_label.pack(fill="x", pady=(2, 6))

        review = ctk.CTkFrame(card.body, fg_color="transparent")
        review.pack(fill="x")
        self.validate_button = ctk.CTkButton(
            review, text="✔ Valider", width=110, fg_color=ACCENT_OK, command=self._validate
        )
        self.validate_button.pack(side="left")
        self.correct_button = ctk.CTkButton(
            review, text="✎ Corriger…", width=110, command=self._toggle_correct
        )
        self.correct_button.pack(side="left", padx=(6, 0))
        self.reviewer_var = ctk.StringVar(value="")
        ctk.CTkLabel(review, text="vérificateur").pack(side="left", padx=(16, 4))
        ctk.CTkEntry(
            review, textvariable=self.reviewer_var, width=120, placeholder_text="initiales"
        ).pack(side="left")

        self.correct_frame = ctk.CTkFrame(card.body, fg_color="transparent")
        ctk.CTkLabel(self.correct_frame, text="sécurité").pack(side="left")
        self.corr_sec_var = ctk.StringVar(value="")
        ctk.CTkOptionMenu(
            self.correct_frame,
            variable=self.corr_sec_var,
            values=["", "C0", "C1", "C2", "C3"],
            width=70,
        ).pack(side="left", padx=(4, 10))
        ctk.CTkLabel(self.correct_frame, text="RGPD").pack(side="left")
        self.corr_rgpd_var = ctk.StringVar(value="")
        ctk.CTkOptionMenu(
            self.correct_frame,
            variable=self.corr_rgpd_var,
            values=["", "none", "low", "medium", "high", "critical"],
            width=90,
        ).pack(side="left", padx=(4, 10))
        ctk.CTkLabel(self.correct_frame, text="commentaire").pack(side="left")
        self.comment_var = ctk.StringVar(value="")
        ctk.CTkEntry(self.correct_frame, textvariable=self.comment_var, width=320).pack(
            side="left", padx=(4, 10)
        )
        ctk.CTkButton(
            self.correct_frame,
            text="Enregistrer la correction",
            width=180,
            command=self._save_correction,
        ).pack(side="left")
        ctk.CTkButton(
            self.correct_frame,
            text="Marquer « à vérifier »",
            width=160,
            fg_color="#6b7280",
            command=self._to_review,
        ).pack(side="left", padx=(6, 0))
        self._correct_visible = False

        self.app.on_refresh(self.refresh)
        self._clear_card()

    def _filter(self, parent: Any, label: str, values: list[str], width: int) -> Any:
        ctk = self.ctk
        ctk.CTkLabel(parent, text=label).pack(side="left", padx=(0, 4))
        var = ctk.StringVar(value=_ALL)
        ctk.CTkOptionMenu(
            parent, variable=var, values=values, width=width, command=lambda _v: self.refresh()
        ).pack(side="left", padx=(0, 10))
        return var

    # ---- table
    def _current_review_filter(self) -> str | None:
        return _REVIEW_FILTERS[self.review_var.get()]

    def refresh_if_needed(self) -> None:
        """Recharge la liste hors du thread Tk (une grande campagne demande des secondes)."""
        if not self._dirty or not self.visible():
            return
        if not self.app.db_path().exists():
            self._rows = []
            self.table.set_rows([])
            self.count_label.configure(text="")
            self._clear_card()
            self._dirty = False
            return
        self.count_label.configure(text="chargement…")
        sec = _SEC_FILTERS[self.sec_var.get()]
        rgpd = _RGPD_FILTERS[self.rgpd_var.get()]
        rev = self._current_review_filter()
        # Transmis tel quel : c'est `LIKE` qui replie la casse, et replier ici
        # ferait perdre les majuscules accentuées que SQLite, lui, ne replie pas.
        search = self.search_var.get().strip() or None

        def compute(db: Database) -> tuple[list[dict[str, str]], list[list[str]], list[str], int]:
            """Filtres, tri et limite en SQL — l'écran ne voit plus que ses 1 000 lignes.

            Le tri SQL est **approché** (`LOWER()` de SQLite ignore les accents) :
            les lignes rendues, au plus `_LIMIT`, sont re-triées ici avec la clé
            exacte `_display_order`. Le prix de ce compromis : à la frontière de la
            millième ligne, deux noms que seul un accent sépare peuvent s'échanger.
            Les lignes affichées, elles, sont toujours dans l'ordre exact.
            """
            total = db.count_latest_analyses(security=sec, rgpd=rgpd, review=rev, search=search)
            page = sorted(
                db.latest_analyses(
                    security=sec,
                    rgpd=rgpd,
                    review=rev,
                    search=search,
                    limit=_LIMIT,
                    display_order=True,
                ),
                key=_display_order,
            )
            shown = result_rows_v31(page, limit=_LIMIT)
            table_rows: list[list[str]] = []
            tags: list[str] = []
            for row in shown:
                values, tag = _row_cells(row)
                table_rows.append(values)
                tags.append(tag)
            return shown, table_rows, tags, total

        def apply(result: tuple[list[dict[str, str]], list[list[str]], list[str], int]) -> None:
            self._rows, table_rows, tags, total = result
            # `keys` : l'identité du fichier voyage avec la ligne, donc survit à un tri.
            self.table.set_rows(table_rows, tags, keys=[int(r["id"] or 0) for r in self._rows])
            shown = min(total, _LIMIT)
            self.count_label.configure(
                text=f"{total} fichier(s)" + (f" — {shown} affichés" if shown < total else "")
            )

        self._start(compute, apply, name="résultats")

    def _clear_card(self) -> None:
        self._selected_id = None
        self.path_label.configure(text="")
        for b in (self.sec_badge, self.rgpd_badge, self.ret_badge, self.review_badge):
            b.set("—", None)
        self.meta_label.configure(text="")
        self.detail_label.configure(text="")
        self.card.subtitle.configure(text="sélectionne une ligne")

    def _select(self, file_id: int) -> None:
        """`file_id` vient du tableau (`keys`) : l'identité de la ligne, jamais son rang.

        Interroger la base avec cette identité supprime toute indirection par une liste
        que la fenêtre garderait de son côté — la source d'un tri qui ouvrait la fiche
        d'un autre fichier.
        """
        rec = self.app.service.latest_analysis(int(file_id))
        if rec is None:
            self._clear_card()
            return
        self._show(rec)

    def _show(self, rec: Any) -> None:
        """Remplit la fiche depuis une ligne de `latest_analyses` déjà lue."""
        self._selected_id = int(rec["id"])
        self.card.subtitle.configure(text="")
        self.path_label.configure(text=str(rec["path"]))
        sec = rec["security_classification"] or "N/A"
        rg = rec["rgpd_risk_level"] or "N/A"
        self.sec_badge.set(SECURITY_LABELS.get(sec, sec), sec)
        self.rgpd_badge.set("RGPD " + RGPD_LABELS.get(rg, rg), rg)
        if rec["retention_required"]:
            self.ret_badge.set(
                f"conserver {rec['retention_years'] or 0} ans · {rec['retention_basis'] or ''}",
                "pending",
            )
        else:
            self.ret_badge.set("pas de conservation imposée", "N/A")
        status = rec["review_status"] or ""
        self.review_badge.set(
            REVIEW_LABELS.get(status, status),
            {"validated": "done", "corrected": "C2", "to_review": "C1"}.get(status, "N/A"),
        )
        self.meta_label.configure(
            text=f"{rec['owner'] or '—'} · {format_bytes(rec['size_bytes'] or 0)}"
            + (
                f" · analysé le {str(rec['created_at'])[:16]}"
                if rec["created_at"]
                else " · pas encore analysé"
            )
            + (f" · {rec['segments']} segments" if (rec["segments"] or 0) > 1 else "")
        )
        lines = detail_lines(rec)
        self.detail_label.configure(text="\n".join(lines))
        self.corr_sec_var.set(rec["corrected_security"] or "")
        self.corr_rgpd_var.set(rec["corrected_rgpd"] or "")
        self.comment_var.set(rec["review_comment"] or "")

    # ---- vérification
    def _toggle_correct(self) -> None:
        self._correct_visible = not self._correct_visible
        if self._correct_visible:
            self.correct_frame.pack(fill="x", pady=(6, 0))
        else:
            self.correct_frame.pack_forget()

    def _save(self, status: str, **kwargs: Any) -> None:
        """Enregistre la vérification, puis rafraîchit **la seule ligne concernée**.

        `refresh()` relisait toute la campagne après chaque clic : un relecteur qui
        validait cent fichiers payait cent fois les 9,3 s et les 950 Mo d'une
        campagne de 934 028 lignes. Rien d'autre n'a changé en base — seule cette
        ligne peut avoir changé d'aspect.

        Seule exception : le filtre « Vérification » est actif et la ligne vient d'en
        sortir (ou d'y entrer). La liste et le total ne seraient plus ceux du filtre
        affiché : là, et là seulement, on recharge.
        """
        if self._selected_id is None:
            self.app.log("sélectionne un fichier dans le tableau")
            return
        rec = self.app.service.set_review(
            self._selected_id, status, reviewer=self.reviewer_var.get().strip(), **kwargs
        )
        self.app.log(f"vérification « {REVIEW_LABELS.get(status, status)} » enregistrée")
        review_filter = self._current_review_filter()
        if rec is None or (review_filter is not None and status != review_filter):
            self.refresh()
            return
        self._replace_row(rec)
        self._show(rec)

    def _replace_row(self, rec: Any) -> None:
        """Réécrit en place la ligne du tableau qui porte l'identité `rec["id"]`.

        Le tableau porte l'identité de ses lignes (`Table.keys`) : on la retrouve
        même après un tri par en-tête, et on n'écrit que la case concernée — la
        sélection de l'utilisateur et son tri survivent.
        """
        file_id = int(rec["id"])
        rows = result_rows_v31([rec], limit=1)
        if not rows:
            return
        row = rows[0]
        for index, known in enumerate(self._rows):
            if int(known["id"] or 0) == file_id:
                self._rows[index] = row
                break
        table = self.table
        if file_id not in table.keys:
            return  # ligne hors de la page affichée : rien à réécrire
        index = table.keys.index(file_id)
        values, tag = _row_cells(row)
        table.rows[index] = values
        table.row_tags[index] = tag
        # Même écrêtage que `Table._render` : le tableau ne peint jamais plus long.
        table.tree.item(
            str(index),
            values=[c if len(c) <= 200 else c[:197] + "…" for c in values],
            tags=(tag,),
        )

    def _validate(self) -> None:
        self._save("validated", comment=self.comment_var.get().strip())

    def _save_correction(self) -> None:
        self._save(
            "corrected",
            comment=self.comment_var.get().strip(),
            corrected_security=self.corr_sec_var.get() or None,
            corrected_rgpd=self.corr_rgpd_var.get() or None,
        )

    def _to_review(self) -> None:
        self._save("to_review", comment=self.comment_var.get().strip())


_SEVERITY_ORDER = {"C3": 0, "C2": 1, "C1": 2, "C0": 3, "N/A": 4}


def _row_cells(row: dict[str, str]) -> tuple[list[str], str]:
    """(cellules du tableau, couleur de la ligne) pour une ligne de `result_rows_v31`.

    Une seule définition : le rechargement complet et la réécriture d'une ligne
    après validation peignent forcément la même chose.
    """
    sec_value = row["sécu"]
    tag = (
        sec_value
        if sec_value in ("C3", "C2", "C1")
        else ("error" if sec_value == "error" else "ok")
    )
    return (
        [
            row["nom"],
            shorten_path(folder_of(row["chemin"]), 48),
            STATUS_LABELS.get(sec_value, sec_value),
            row["rgpd"],
            row["finance"],
            row["juridique"],
            row["conservation"],
            REVIEW_LABELS.get(row["revue"], row["revue"]),
            row["résumé"],
        ],
        tag,
    )


def _display_order(r: Any) -> tuple[int, int, str]:
    """Analysés d'abord (du plus sensible au moins sensible), puis en erreur, puis à analyser."""
    sec = r["security_classification"]
    status = r["status"] or ""
    if sec:
        return (0, _SEVERITY_ORDER.get(sec, 5), str(r["name"]).lower())
    return ({"error": 1, "done": 2}.get(status, 3), 0, str(r["name"]).lower())


def detail_lines(rec: Any) -> list[str]:
    """Lignes de la fiche d'un fichier (résumé, justifications, revue) — fonction pure."""
    lines = [f"Résumé : {rec['resume'] or '—'}"]
    if rec["security_justification"]:
        lines.append(f"Sécurité : {rec['security_justification']}")
    if rec["rgpd_data_types"]:
        lines.append(f"Données personnelles : {pretty_list(rec['rgpd_data_types'])}")
    if rec["finance_document_type"] and rec["finance_document_type"] != "none":
        lines.append(
            f"Finance : {rec['finance_document_type']} — {pretty_amounts(rec['finance_amounts'])}"
        )
    if rec["legal_contract_type"] and rec["legal_contract_type"] != "none":
        lines.append(
            f"Juridique : {rec['legal_contract_type']} — {pretty_list(rec['legal_parties'])}"
        )
    if rec["retention_justification"]:
        lines.append(f"Conservation : {rec['retention_justification']}")
    if rec["review_comment"]:
        lines.append(
            f"Commentaire de vérification : {rec['review_comment']} ({rec['reviewer'] or ''})"
        )
    if rec["exclusion_reason"]:
        lines.append(f"Motif : {rec['exclusion_reason']}")
    return lines
