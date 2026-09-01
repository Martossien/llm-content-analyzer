"""Configuration `docia.toml` : validation complète et absence de dégradation muette.

`docia.toml` est édité à la main par un administrateur et `docia init` expose
justement les réglages qui décident du volume analysé : une virgule mal placée
suffit. Ce qui n'était pas contrôlé faisait exclure les 60 000 fichiers d'une
campagne en annonçant « 0 à analyser », sans un mot sur la configuration.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from docia.config import Config, load_config


def _write(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "docia.toml"
    path.write_text(text, encoding="utf-8")
    return path


# ------------------------------------------------------------- section [filter]


@pytest.mark.parametrize(
    ("toml", "attendu"),
    [
        ("[filter]\nmin_size_bytes = -100\n", "filter.min_size_bytes doit être >= 0"),
        ("[filter]\nmax_size_bytes = 0\n", "filter.max_size_bytes doit être >= 1"),
        (
            "[filter]\nmin_size_bytes = 999999999\nmax_size_bytes = 100\n",
            "filter.min_size_bytes doit être <= filter.max_size_bytes",
        ),
        ('db_path = ""\n', "db_path ne peut pas être vide"),
        ("[llm]\nmax_retries = -5\n", "llm.max_retries doit être >= 0"),
        ("[llm]\nmax_tokens_per_file = 0\n", "llm.max_tokens_per_file doit être >= 1"),
        (
            "[filter]\nexcluded_extensions = [1, 2]\n",
            "filter.excluded_extensions ne doit contenir que du texte",
        ),
    ],
)
def test_validate_refuse_les_reglages_absurdes(tmp_path: Path, toml: str, attendu: str) -> None:
    """Chacun de ces `docia.toml` passait sans un mot et faussait toute la campagne."""
    errors = load_config(_write(tmp_path, toml)).validate()
    assert any(attendu in e for e in errors), errors


def test_validate_accepte_la_configuration_par_defaut() -> None:
    assert Config().validate() == []


def test_validate_accepte_les_bornes_de_taille_egales() -> None:
    cfg = Config()
    cfg.filter.min_size_bytes = cfg.filter.max_size_bytes = 4096
    assert cfg.validate() == []


def test_un_booleen_nest_pas_un_entier(tmp_path: Path) -> None:
    """`bool` est une sous-classe de `int` : `max_in_flight = true` valait 1 en silence."""
    with pytest.raises(ValueError, match="max_in_flight doit être un entier"):
        load_config(_write(tmp_path, "[llm]\nmax_in_flight = true\n"))


def test_un_booleen_nest_pas_un_nombre(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="margin doit être un nombre"):
        load_config(_write(tmp_path, "[blocks]\nmargin = true\n"))


def test_un_vrai_booleen_reste_accepte(tmp_path: Path) -> None:
    cfg = load_config(_write(tmp_path, "[llm]\nenable_thinking = false\n"))
    assert cfg.llm.enable_thinking is False


# ------------------------------------------------------- fichier de config absent


def test_fichier_absent_est_signale_en_nommant_le_fichier(tmp_path: Path) -> None:
    """Une faute de frappe dans `--config` ne doit plus passer inaperçue.

    La campagne continuait sur les défauts — base, seuils de taille et modèle
    compris — sans que rien ne soit dit.
    """
    messages: list[str] = []
    absent = tmp_path / "docai.toml"  # coquille : « docai » pour « docia »
    cfg = load_config(absent, on_missing=messages.append)
    assert cfg.db_path == Config().db_path  # les défauts s'appliquent, comme avant
    assert len(messages) == 1
    assert str(absent) in messages[0]
    assert "défaut" in messages[0]


def test_fichier_present_ne_signale_rien(tmp_path: Path) -> None:
    messages: list[str] = []
    load_config(_write(tmp_path, 'db_path = "x.sqlite"\n'), on_missing=messages.append)
    assert messages == []


def test_aucun_chemin_ne_signale_rien() -> None:
    messages: list[str] = []
    load_config(None, on_missing=messages.append)
    assert messages == []
