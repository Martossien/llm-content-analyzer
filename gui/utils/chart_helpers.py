from __future__ import annotations

try:
    from matplotlib.figure import Figure
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
except Exception:  # pragma: no cover - optional dependency
    Figure = None
    FigureCanvasTkAgg = None


def create_empty_chart(parent, title: str = ""):
    """Return a simple matplotlib canvas if matplotlib is available."""
    if Figure is None:
        return None
    fig = Figure(figsize=(5, 3), dpi=100)
    ax = fig.add_subplot(111)
    ax.set_title(title)
    canvas = FigureCanvasTkAgg(fig, parent)
    canvas.get_tk_widget().pack(fill="both", expand=True)
    return canvas, ax
