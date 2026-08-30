"""Widgets partagés : tableau simple, zone de texte en lecture seule, étiquettes clés."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import asdict, is_dataclass
from typing import Any


class Table:
    """Tableau sur `ttk.Treeview` (rapide : des milliers de lignes), tri par clic sur
    l'en-tête, callback au clic sur une ligne, thème sombre accordé à CustomTkinter.

    Une grille d'étiquettes CustomTkinter coûte ~10 ms par cellule : 500 lignes ×
    9 colonnes bloquaient l'interface plusieurs secondes — d'où Treeview.
    """

    def __init__(
        self,
        ctk: Any,
        parent: Any,
        *,
        columns: Sequence[str],
        on_select: Callable[[int], None] | None = None,
        height: int = 280,
        label_text: str = "",
    ) -> None:
        import tkinter as tk
        from tkinter import ttk

        self.ctk = ctk
        self.columns = list(columns)
        self.on_select = on_select
        self.rows: list[list[str]] = []
        self._sort_col: int | None = None
        self._sort_desc = False

        self.frame = ctk.CTkFrame(parent, fg_color="transparent")
        if label_text:
            ctk.CTkLabel(self.frame, text=label_text).pack(anchor="w")
        style = ttk.Style()
        style.theme_use("clam")
        style.configure(
            "Docia.Treeview",
            background="#1f2937",
            fieldbackground="#1f2937",
            foreground="#e5e7eb",
            rowheight=22,
            borderwidth=0,
        )
        style.configure(
            "Docia.Treeview.Heading", background="#111827", foreground="#e5e7eb", relief="flat"
        )
        style.map("Docia.Treeview", background=[("selected", "#2563eb")])
        box = tk.Frame(self.frame, bg="#1f2937")
        box.pack(fill="both", expand=True)
        self.tree = ttk.Treeview(
            box,
            columns=list(range(len(self.columns))),
            show="headings",
            style="Docia.Treeview",
            height=max(4, height // 22),
        )
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
        self.frame.pack(**kwargs)

    def _apply_columns(self) -> None:
        self.tree.configure(columns=list(range(len(self.columns))))
        for idx, name in enumerate(self.columns):
            arrow = ""
            if self._sort_col == idx:
                arrow = " ▼" if self._sort_desc else " ▲"
            self.tree.heading(str(idx), text=name + arrow, command=self._sorter(idx))
            width = 90 if len(name) < 8 else 140
            if name in ("chemin", "résumé", "chemins", "justification", "path"):
                width = 420
            self.tree.column(
                str(idx), width=width, minwidth=60, stretch=name in ("chemin", "résumé", "chemins")
            )

    def _sorter(self, col: int) -> Callable[[], None]:
        def run() -> None:
            self._sort(col)

        return run

    def set_rows(self, rows: Sequence[Sequence[str]]) -> None:
        self.rows = [[str(c) for c in r] for r in rows]
        self._apply_columns()
        self._render()

    def _render(self) -> None:
        self.tree.delete(*self.tree.get_children())
        for i, row in enumerate(self.rows):
            values = [c if len(c) <= 200 else c[:197] + "…" for c in row]
            self.tree.insert("", "end", iid=str(i), values=values)

    def _sort(self, col: int) -> None:
        if self._sort_col == col:
            self._sort_desc = not self._sort_desc
        else:
            self._sort_col, self._sort_desc = col, False

        def key(row: Sequence[str]) -> tuple[int, float, str]:
            value = str(row[col]) if col < len(row) else ""
            try:
                return (
                    0,
                    float(value.replace("\u202f", "").replace(" ", "").replace(",", ".")),
                    "",
                )
            except ValueError:
                return (1, 0.0, value.lower())

        self.rows.sort(key=key, reverse=self._sort_desc)
        self._apply_columns()
        self._render()

    def _on_tree_select(self, _event: object) -> None:
        selection = self.tree.selection()
        if selection and self.on_select is not None:
            self.on_select(int(selection[0]))


def rows_from_records(
    records: Sequence[Any], columns: Sequence[str] | None = None
) -> tuple[list[str], list[list[str]]]:
    """Convertit une liste de dataclasses / dicts / tuples en (colonnes, lignes de texte).

    Sert à afficher n'importe quelle vue de `docia.views` sans connaître sa forme.
    """
    if not records:
        return list(columns or []), []
    first = records[0]
    if is_dataclass(first) and not isinstance(first, type):
        cols = list(columns or asdict(first).keys())
        return cols, [[_fmt(asdict(r).get(c, "")) for c in cols] for r in records]
    if isinstance(first, dict):
        cols = list(columns or first.keys())
        return cols, [[_fmt(r.get(c, "")) for c in cols] for r in records]
    if hasattr(first, "keys"):  # sqlite3.Row
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
    """Zone de texte en lecture seule (détail d'un fichier, JSON, prompt affiché)."""

    def __init__(self, ctk: Any, parent: Any, *, height: int = 200) -> None:
        self.box = ctk.CTkTextbox(parent, height=height, wrap="word")
        self.box.configure(state="disabled")

    def pack(self, **kwargs: Any) -> None:
        self.box.pack(**kwargs)

    def set(self, text: str) -> None:
        self.box.configure(state="normal")
        self.box.delete("1.0", "end")
        self.box.insert("end", text)
        self.box.configure(state="disabled")
