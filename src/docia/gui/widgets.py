"""Widgets partagés : tuiles chiffrées, badges, cartes, graphique en barres (Canvas),
tableau `ttk.Treeview`, zone de texte en lecture seule. Aucune logique métier."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import asdict, is_dataclass
from typing import Any

from docia.gui.theme import (
    ACCENT,
    FONT_FAMILY,
    FONT_SIZE,
    FONT_SIZE_KPI,
    FONT_SIZE_SMALL,
    severity_color,
)


class KpiTile:
    """Tuile « chiffre clé » : valeur en gros, libellé, couleur d'accent, clic optionnel."""

    def __init__(
        self,
        ctk: Any,
        parent: Any,
        *,
        label: str,
        value: str = "—",
        color: str = ACCENT,
        on_click: Callable[[], None] | None = None,
        width: int = 190,
    ) -> None:
        self.frame = ctk.CTkFrame(parent, width=width, height=88, corner_radius=10)
        self.frame.pack_propagate(False)
        self.value_label = ctk.CTkLabel(
            self.frame,
            text=value,
            font=ctk.CTkFont(family=FONT_FAMILY, size=FONT_SIZE_KPI, weight="bold"),
            text_color=color,
        )
        self.value_label.pack(pady=(10, 0))
        self.text_label = ctk.CTkLabel(
            self.frame, text=label, font=ctk.CTkFont(family=FONT_FAMILY, size=FONT_SIZE_SMALL)
        )
        self.text_label.pack()
        if on_click is not None:
            for w in (self.frame, self.value_label, self.text_label):
                w.configure(cursor="hand2")
                w.bind("<Button-1>", lambda _e: on_click())

    def pack(self, **kwargs: Any) -> None:
        """Place le widget (`pack`)."""
        self.frame.pack(**kwargs)

    def grid(self, **kwargs: Any) -> None:
        """Place le widget (`grid`)."""
        self.frame.grid(**kwargs)

    def set(self, value: str, color: str | None = None) -> None:
        """Met à jour la valeur affichée."""
        self.value_label.configure(text=value)
        if color:
            self.value_label.configure(text_color=color)


class Badge:
    """Pastille colorée (classe de sécurité, niveau RGPD, statut)."""

    def __init__(self, ctk: Any, parent: Any, text: str = "", key: str | None = None) -> None:
        self.ctk = ctk
        self.label = ctk.CTkLabel(
            parent,
            text=f" {text} ",
            corner_radius=6,
            fg_color=severity_color(key),
            text_color="white",
            font=ctk.CTkFont(family=FONT_FAMILY, size=FONT_SIZE_SMALL, weight="bold"),
        )

    def set(self, text: str, key: str | None = None) -> None:
        """Met à jour la valeur affichée."""
        self.label.configure(text=f" {text} ", fg_color=severity_color(key))

    def pack(self, **kwargs: Any) -> None:
        """Place le widget (`pack`)."""
        self.label.pack(**kwargs)

    def grid(self, **kwargs: Any) -> None:
        """Place le widget (`grid`)."""
        self.label.grid(**kwargs)


class Card:
    """Cadre titré (section d'un écran)."""

    def __init__(self, ctk: Any, parent: Any, title: str, *, subtitle: str = "") -> None:
        self.frame = ctk.CTkFrame(parent, corner_radius=10)
        head = ctk.CTkFrame(self.frame, fg_color="transparent")
        head.pack(fill="x", padx=12, pady=(10, 2))
        self.title = ctk.CTkLabel(
            head,
            text=title,
            font=ctk.CTkFont(family=FONT_FAMILY, size=FONT_SIZE + 2, weight="bold"),
        )
        self.title.pack(side="left")
        self.subtitle = ctk.CTkLabel(
            head, text=subtitle, font=ctk.CTkFont(family=FONT_FAMILY, size=FONT_SIZE_SMALL)
        )
        self.subtitle.pack(side="left", padx=10)
        self.body = ctk.CTkFrame(self.frame, fg_color="transparent")
        self.body.pack(fill="both", expand=True, padx=12, pady=(0, 10))

    def pack(self, **kwargs: Any) -> None:
        """Place le widget (`pack`)."""
        self.frame.pack(**kwargs)

    def grid(self, **kwargs: Any) -> None:
        """Place le widget (`grid`)."""
        self.frame.grid(**kwargs)


class BarChart:
    """Graphique en barres horizontales sur un `tkinter.Canvas` (aucune dépendance)."""

    def __init__(self, ctk: Any, parent: Any, *, width: int = 520, height: int = 220) -> None:
        import tkinter as tk

        self.ctk = ctk
        self.width, self.height = width, height
        self.canvas = tk.Canvas(parent, width=width, height=height, highlightthickness=0)
        self._bg = "#ffffff"

    def pack(self, **kwargs: Any) -> None:
        """Place le widget (`pack`)."""
        self.canvas.pack(**kwargs)

    def grid(self, **kwargs: Any) -> None:
        """Place le widget (`grid`)."""
        self.canvas.grid(**kwargs)

    def draw(
        self,
        items: Sequence[tuple[str, float, str | None]],
        *,
        title: str = "",
        unit: str = "",
    ) -> None:
        """`items` = (libellé, valeur, clé de couleur ou None), dans l'ordre d'affichage."""
        c = self.canvas
        c.delete("all")
        c.configure(bg=self._bg)
        if title:
            c.create_text(
                8,
                10,
                anchor="nw",
                text=title,
                font=(FONT_FAMILY, FONT_SIZE, "bold"),
                fill="#111827",
            )
        if not items:
            c.create_text(
                8,
                40,
                anchor="nw",
                text="aucune donnée",
                fill="#6b7280",
                font=(FONT_FAMILY, FONT_SIZE_SMALL),
            )
            return
        top = 34 if title else 8
        label_w = 170
        max_value = max(v for _, v, _ in items) or 1.0
        row_h = max(16, min(28, (self.height - top - 8) // max(1, len(items))))
        bar_w = self.width - label_w - 90
        for i, (label, value, key) in enumerate(items):
            y = top + i * row_h
            c.create_text(
                label_w - 8,
                y + row_h / 2,
                anchor="e",
                text=label[:26],
                fill="#111827",
                font=(FONT_FAMILY, FONT_SIZE_SMALL),
            )
            w = int(bar_w * value / max_value)
            color = severity_color(key) if key else ACCENT
            c.create_rectangle(
                label_w, y + 3, label_w + max(w, 2), y + row_h - 3, fill=color, outline=""
            )
            text = f"{value:,.0f}".replace(",", " ") + (f" {unit}" if unit else "")
            c.create_text(
                label_w + max(w, 2) + 6,
                y + row_h / 2,
                anchor="w",
                text=text,
                fill="#111827",
                font=(FONT_FAMILY, FONT_SIZE_SMALL),
            )


def sort_rows(
    rows: Sequence[Sequence[str]],
    tags: Sequence[str],
    keys: Sequence[Any],
    col: int,
    *,
    desc: bool = False,
) -> tuple[list[list[str]], list[str], list[Any]]:
    """Trie lignes, couleurs **et identités** ensemble (fonction pure, testable sans écran).

    Trier les lignes sans emporter les identités, c'était afficher la fiche d'un autre
    fichier que celui qu'on venait de cliquer — et enregistrer la vérification humaine
    dessus. Les nombres se trient comme des nombres, le reste sans tenir compte de la casse.
    """
    triples = list(zip(rows, tags, keys, strict=False))

    def key(triple: tuple[Sequence[str], str, Any]) -> tuple[int, float, str]:
        value = str(triple[0][col]) if col < len(triple[0]) else ""
        try:
            return (0, float(value.replace(" ", "").replace(" ", "").replace(",", ".")), "")
        except ValueError:
            return (1, 0.0, value.lower())

    triples.sort(key=key, reverse=desc)
    return (
        [list(r) for r, _, _ in triples],
        [t for _, t, _ in triples],
        [k for _, _, k in triples],
    )


class Table:
    """Tableau sur `ttk.Treeview` (des milliers de lignes), tri par clic sur l'en-tête,
    callback au clic sur une ligne, lignes colorées par sévérité (`tags`).

    C'est le tableau qui porte l'identité de ses lignes (`keys`) : `on_select` reçoit
    l'identité de la ligne cliquée, jamais son rang — un tri par en-tête réordonne le
    tableau et l'appelant n'a rien à resynchroniser.
    """

    def __init__(
        self,
        ctk: Any,
        parent: Any,
        *,
        columns: Sequence[str],
        on_select: Callable[[Any], None] | None = None,
        height: int = 280,
        widths: dict[str, int] | None = None,
    ) -> None:
        import tkinter as tk
        from tkinter import ttk

        self.ctk = ctk
        self.columns = list(columns)
        self.widths = dict(widths or {})
        self.on_select = on_select
        self.rows: list[list[str]] = []
        self.row_tags: list[str] = []
        self.keys: list[Any] = []
        self._sort_col: int | None = None
        self._sort_desc = False

        self.frame = ctk.CTkFrame(parent, fg_color="transparent")
        style = ttk.Style()
        style.theme_use("clam")
        style.configure(
            "Docia.Treeview",
            background="#ffffff",
            fieldbackground="#ffffff",
            foreground="#111827",
            rowheight=24,
            borderwidth=0,
            font=(FONT_FAMILY, FONT_SIZE_SMALL + 1),
        )
        style.configure(
            "Docia.Treeview.Heading",
            background="#e5e7eb",
            foreground="#111827",
            relief="flat",
            font=(FONT_FAMILY, FONT_SIZE_SMALL + 1, "bold"),
        )
        style.map(
            "Docia.Treeview",
            background=[("selected", "#bfdbfe")],
            foreground=[("selected", "#111827")],
        )
        box = tk.Frame(self.frame, bg="#ffffff")
        box.pack(fill="both", expand=True)
        self.tree = ttk.Treeview(
            box,
            columns=list(range(len(self.columns))),
            show="headings",
            style="Docia.Treeview",
            height=max(4, height // 24),
        )
        for key, color in (
            ("C3", "#fee2e2"),
            ("C2", "#ffedd5"),
            ("C1", "#fef9c3"),
            ("error", "#fee2e2"),
            ("ok", "#ffffff"),
        ):
            self.tree.tag_configure(key, background=color)
        vsb = ttk.Scrollbar(box, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(box, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        box.grid_rowconfigure(0, weight=1)
        box.grid_columnconfigure(0, weight=1)
        self.tree.bind("<<TreeviewSelect>>", self._on_tree_select)
        self._apply_columns()

    def pack(self, **kwargs: Any) -> None:
        """Place le widget (`pack`)."""
        self.frame.pack(**kwargs)

    def _sorter(self, col: int) -> Callable[[], None]:
        def run() -> None:
            self._sort(col)

        return run

    def _apply_columns(self) -> None:
        self.tree.configure(columns=list(range(len(self.columns))))
        stretch_cols = ("dossier", "résumé", "chemin", "chemins", "justification")
        for idx, name in enumerate(self.columns):
            arrow = ""
            if self._sort_col == idx:
                arrow = " ▼" if self._sort_desc else " ▲"
            self.tree.heading(str(idx), text=name + arrow, command=self._sorter(idx))
            width = self.widths.get(name, 110 if len(name) < 10 else 150)
            self.tree.column(str(idx), width=width, minwidth=50, stretch=name in stretch_cols)

    def set_rows(
        self,
        rows: Sequence[Sequence[str]],
        tags: Sequence[str] | None = None,
        keys: Sequence[Any] | None = None,
    ) -> None:
        """`keys` : identité métier de chaque ligne, rendue telle quelle à `on_select`.

        Optionnel : sans elle, `on_select` reçoit le rang de la ligne — ce qui suffit
        aux tableaux qui ne servent qu'à lire (Statistiques).
        """
        self.rows = [[str(c) for c in r] for r in rows]
        self.row_tags = list(tags) if tags else ["ok"] * len(self.rows)
        self.keys = list(keys) if keys is not None else list(range(len(self.rows)))
        self._sort_col, self._sort_desc = None, False  # nouvelles données : tri remis à zéro
        self._apply_columns()
        self._render()

    def _render(self) -> None:
        self.tree.delete(*self.tree.get_children())
        for i, row in enumerate(self.rows):
            values = [c if len(c) <= 200 else c[:197] + "…" for c in row]
            tag = self.row_tags[i] if i < len(self.row_tags) else "ok"
            self.tree.insert("", "end", iid=str(i), values=values, tags=(tag,))

    def _sort(self, col: int) -> None:
        if self._sort_col == col:
            self._sort_desc = not self._sort_desc
        else:
            self._sort_col, self._sort_desc = col, False
        self.rows, self.row_tags, self.keys = sort_rows(
            self.rows, self.row_tags, self.keys, col, desc=self._sort_desc
        )
        self._apply_columns()
        self._render()

    def _on_tree_select(self, _event: object) -> None:
        selection = self.tree.selection()
        if not selection or self.on_select is None:
            return
        index = int(selection[0])
        self.on_select(self.keys[index] if index < len(self.keys) else index)


def rows_from_records(
    records: Sequence[Any], columns: Sequence[str] | None = None
) -> tuple[list[str], list[list[str]]]:
    """(colonnes, lignes de texte) depuis des dataclasses / dicts / `sqlite3.Row` / tuples."""
    if not records:
        return list(columns or []), []
    first = records[0]
    if is_dataclass(first) and not isinstance(first, type):
        cols = list(columns or asdict(first).keys())
        return cols, [[_fmt(asdict(r).get(c, "")) for c in cols] for r in records]
    if isinstance(first, dict):
        cols = list(columns or first.keys())
        return cols, [[_fmt(r.get(c, "")) for c in cols] for r in records]
    if hasattr(first, "keys"):
        cols = list(columns or first.keys())
        return cols, [[_fmt(r[c]) for c in cols] for r in records]
    cols = list(columns or [f"col{i + 1}" for i in range(len(first))])
    return cols, [[_fmt(v) for v in r] for r in records]


def _fmt(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:,.1f}".replace(",", " ")
    if isinstance(value, int) and not isinstance(value, bool):
        return f"{value:,}".replace(",", " ")
    if isinstance(value, list | tuple):
        return ", ".join(str(v) for v in value[:6]) + (" …" if len(value) > 6 else "")
    return str(value)


class ReadOnlyText:
    """Zone de texte en lecture seule."""

    def __init__(self, ctk: Any, parent: Any, *, height: int = 200) -> None:
        self.box = ctk.CTkTextbox(
            parent, height=height, wrap="word", font=ctk.CTkFont(family=FONT_FAMILY, size=FONT_SIZE)
        )
        self.box.configure(state="disabled")

    def pack(self, **kwargs: Any) -> None:
        """Place le widget (`pack`)."""
        self.box.pack(**kwargs)

    def set(self, text: str) -> None:
        """Met à jour la valeur affichée."""
        self.box.configure(state="normal")
        self.box.delete("1.0", "end")
        self.box.insert("end", text)
        self.box.configure(state="disabled")
