"""Configuration `docia.toml` : validation complète et absence de dégradation muette.

`docia.toml` est édité à la main par un administrateur et `docia init` expose
justement les réglages qui décident du volume analysé : une virgule mal placée
suffit. Ce qui n'était pas contrôlé faisait exclure les 60 000 fichiers d'une
campagne en annonçant « 0 à analyser », sans un mot sur la configuration.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from docia.config import (
    Config,
    TomlRewriteError,
    default_toml,
    load_config,
    update_toml,
)


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


# ------------------------------------------------- réécriture en place (`update_toml`)


def _commentaires(texte: str) -> list[str]:
    """Les commentaires du fichier — un `#` entre guillemets n'en est pas un."""
    trouves: list[str] = []
    for ligne in texte.splitlines():
        chaine = False
        for index, car in enumerate(ligne):
            if car == '"':
                chaine = not chaine
            elif car == "#" and not chaine:
                trouves.append(ligne[index:])
    return trouves


def test_enregistrer_preserve_les_commentaires_de_docia_init(tmp_path: Path) -> None:
    """Le défaut le plus grave de la fenêtre : « Enregistrer » effaçait les 21 commentaires.

    Dont l'avertissement — une mention RSSI — qui prévient que `<campagne>.blocks/`
    conserve le texte intégral des documents analysés, OCR compris, en clair sur le
    disque. Un administrateur cliquait une fois, et le suivant ne le lisait jamais.
    """
    gabarit = default_toml()
    assert len(_commentaires(gabarit)) == 21
    cfg = load_config(_write(tmp_path, gabarit))
    cfg.llm.model = "un-autre-modele"

    sortie = update_toml(gabarit, cfg)

    assert _commentaires(sortie) == _commentaires(gabarit)
    assert "TEXTE INTÉGRAL des documents" in sortie, "l'avertissement RSSI doit survivre"
    # et la disposition ne bouge pas : une seule ligne change, celle du modèle
    changees = set(sortie.splitlines()) - set(gabarit.splitlines())
    assert changees == {'model = "un-autre-modele"'}
    assert load_config(_write(tmp_path, sortie)).llm.model == "un-autre-modele"


def test_reecrire_sans_rien_changer_rend_le_fichier_a_lidentique(tmp_path: Path) -> None:
    """Octet pour octet : une clé absente du fichier vaut son défaut, on ne l'ajoute pas."""
    gabarit = default_toml()
    assert update_toml(gabarit, load_config(_write(tmp_path, gabarit))) == gabarit


def test_une_valeur_contenant_un_diese_nest_pas_prise_pour_un_commentaire(
    tmp_path: Path,
) -> None:
    source = 'db_path = "a.sqlite"\n\n[llm]\napi_key = "sk-a#b"   # à ne pas versionner\n'
    cfg = load_config(_write(tmp_path, source))
    cfg.llm.model = "m2"

    sortie = update_toml(source, cfg)

    assert load_config(_write(tmp_path, sortie)).llm.api_key == "sk-a#b"
    assert 'api_key = "sk-a#b"   # à ne pas versionner' in sortie
    assert load_config(_write(tmp_path, sortie)).llm.model == "m2"


def test_une_cle_absente_du_fichier_est_ajoutee_a_sa_section(tmp_path: Path) -> None:
    source = 'db_path = "a.sqlite"\n\n[llm]\nmodel = "m"   # le modèle servi\n'
    cfg = load_config(_write(tmp_path, source))
    cfg.llm.timeout_s = 42

    sortie = update_toml(source, cfg)

    assert load_config(_write(tmp_path, sortie)).llm.timeout_s == 42
    assert "# le modèle servi" in sortie
    assert sortie.splitlines()[:4] == source.splitlines()[:4], "rien ne bouge avant l'ajout"


def test_une_section_absente_est_ajoutee_en_fin_de_fichier(tmp_path: Path) -> None:
    source = '# entête maison\ndb_path = "a.sqlite"\n'
    cfg = load_config(_write(tmp_path, source))
    cfg.scan.domain = "ACME"

    sortie = update_toml(source, cfg)

    assert load_config(_write(tmp_path, sortie)).scan.domain == "ACME"
    assert sortie.startswith(source)
    assert "[scan]" in sortie


def test_une_mise_en_forme_inhabituelle_est_respectee(tmp_path: Path) -> None:
    """Fichier édité à la main : tableau sur plusieurs lignes, commentaire à l'intérieur."""
    source = (
        'db_path = "a.sqlite"\n\n[filter]\nexcluded_extensions = [\n'
        '  ".tmp",   # temporaires\n  ".log",\n]\nmin_size_bytes = 100\n'
    )
    cfg = load_config(_write(tmp_path, source))
    cfg.filter.min_size_bytes = 5

    sortie = update_toml(source, cfg)

    relu = load_config(_write(tmp_path, sortie))
    assert relu.filter.excluded_extensions == [".tmp", ".log"], "le tableau n'est pas retouché"
    assert relu.filter.min_size_bytes == 5
    assert "  # temporaires" in sortie


def test_un_toml_casse_est_refuse_au_lieu_detre_ecrase() -> None:
    """Plutôt lever que réécrire n'importe quoi : l'appelant retombe sur la regénération."""
    with pytest.raises(TomlRewriteError):
        update_toml("db_path = \n[llm\n", Config())


def test_un_docia_toml_avec_bom_reste_lisible(tmp_path: Path) -> None:
    """Le Bloc-notes Windows écrit un BOM : sans `utf-8-sig`, toute la config est perdue.

    `tomllib` échouait dès le premier caractère (« Invalid statement at line 1,
    column 1 »). La campagne repartait alors sur les valeurs par défaut — seuils de
    taille, modèle, chemin de base — sans que rien ne le dise à l'écran, puis
    « Enregistrer » remplaçait le fichier de l'administrateur. Sur un exécutable
    autonome livré à des postes Windows, c'est le naufrage le plus probable, et il
    tient à un suffixe d'encodage.
    """
    chemin = tmp_path / "docia.toml"
    chemin.write_bytes(
        b"\xef\xbb\xbf" + b'db_path = "campagne.sqlite"\n\n[filter]\nmin_size_bytes = 4096\n'
    )

    config = load_config(chemin)

    assert config.db_path == "campagne.sqlite"
    assert config.filter.min_size_bytes == 4096, "les réglages de l'administrateur, pas les défauts"


def test_reecrire_un_fichier_avec_bom_ne_le_replique_pas_au_milieu(tmp_path: Path) -> None:
    """Le BOM lu doit disparaître, pas se retrouver inséré dans le texte réécrit."""
    chemin = tmp_path / "docia.toml"
    chemin.write_bytes(b"\xef\xbb\xbf" + default_toml().encode("utf-8"))

    source = chemin.read_text(encoding="utf-8-sig")
    resultat = update_toml(source, load_config(chemin))

    assert "﻿" not in resultat
