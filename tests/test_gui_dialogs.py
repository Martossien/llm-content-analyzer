"""Production d'un document depuis la fenêtre (`docia.gui.dialogs`).

Sans écran : `filedialog` est remplacé, `run_in_thread` exécute tout de suite, et on
regarde la ligne de commande réellement passée à la CLI.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from docia.gui.dialogs import config_problems, produce_document


class FakeApp:
    def __init__(self, db_path: Path, config_path: Path) -> None:
        self._db_path = db_path
        self.config_path = config_path
        self.logs: list[str] = []
        self.differe: list[Any] = []

    def db_path(self) -> Path:
        return self._db_path

    def log(self, message: str) -> None:
        self.logs.append(message)

    def ui(self, action: Any) -> None:
        self.differe.append(action)

    def run_in_thread(self, work: Any, _name: str) -> bool:
        work()  # le travail est joué tout de suite : pas de thread dans ce test
        return True


@pytest.fixture
def app(tmp_path: Path) -> FakeApp:
    db_path = tmp_path / "camp.sqlite"
    db_path.touch()
    config_path = tmp_path / "ailleurs" / "docia.toml"
    config_path.parent.mkdir()
    config_path.write_text('db_path = "camp.sqlite"\n', encoding="utf-8")
    return FakeApp(db_path, config_path)


def test_produce_document_passe_le_config_de_la_fenetre(
    app: FakeApp, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Sans `--config`, la CLI relisait le `docia.toml` du répertoire courant."""
    import docia.cli

    out = tmp_path / "rapport.html"
    monkeypatch.setattr("tkinter.filedialog.asksaveasfilename", lambda **_kw: str(out))
    vu: list[list[str]] = []

    def faux_main(argv: list[str] | None = None) -> int:
        vu.append(list(argv or []))
        out.write_text("<html></html>", encoding="utf-8")
        return 0

    monkeypatch.setattr(docia.cli, "main", faux_main)
    monkeypatch.setattr("webbrowser.open", lambda *_a, **_k: True)

    produce_document(app, "html", "report")

    assert vu, "la CLI doit être appelée"
    argv = vu[0]
    assert argv[:2] == ["--config", str(app.config_path)]
    assert "--db" in argv
    assert str(app.db_path()) in argv
    assert any("document html écrit" in ligne for ligne in app.logs)


def test_produce_document_dit_ce_que_la_config_a_de_faux(
    app: FakeApp, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Une config refusée doit expliquer pourquoi, pas afficher un code de retour."""
    import docia.cli

    app.config_path.write_text(
        'db_path = "camp.sqlite"\n\n[llm]\nbase_url = "pas-une-url"\n', encoding="utf-8"
    )
    monkeypatch.setattr(
        "tkinter.filedialog.asksaveasfilename", lambda **_kw: str(tmp_path / "r.html")
    )

    def faux_main(_argv: list[str] | None = None) -> int:
        raise SystemExit(1)

    monkeypatch.setattr(docia.cli, "main", faux_main)

    produce_document(app, "html", "report")

    journal = "\n".join(app.logs)
    assert "configuration refusée" in journal
    assert str(app.config_path) in journal
    assert "llm.base_url" in journal, f"le motif doit être écrit au journal : {journal}"


def test_config_problems_lit_le_fichier_indique(tmp_path: Path) -> None:
    bon = tmp_path / "bon.toml"
    bon.write_text('db_path = "x.sqlite"\n', encoding="utf-8")
    assert config_problems(str(bon)) == ["aucune erreur relevée"]

    casse = tmp_path / "casse.toml"
    casse.write_text("ceci n'est pas du TOML = = =\n", encoding="utf-8")
    assert config_problems(str(casse)) != []
