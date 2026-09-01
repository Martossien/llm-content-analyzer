"""Tests des exclusions, du score de priorité et du plan."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pytest

from docia.config import FilterConfig
from docia.db import Database
from docia.filter import (
    PlanProgress,
    PlanReport,
    exclusion_reason,
    plan_files,
    plan_progress_logger,
    priority_score,
)
from docia.ingest.smbeagle_csv import import_csv
from docia.models import FileRow, FileStatus
from tests.test_views import _row as _smbeagle_row

FIXTURE = Path(__file__).parent / "fixtures" / "scan_local_mini.csv"
NOW = datetime(2026, 8, 30, 12, 0, 0)


def make_row(
    *,
    name: str = "rapport.pdf",
    extension: str = "pdf",
    size_bytes: int = 50_000,
    path: str = r"\\srv\part$\docs\rapport.pdf",
    last_write_time: str = "01/08/2026 10:00:00",
    status: FileStatus = FileStatus.PENDING,
) -> FileRow:
    """Construit un `FileRow` minimal pour les fonctions pures."""
    return FileRow(
        id=1,
        path=path,
        name=name,
        extension=extension,
        size_bytes=size_bytes,
        fast_hash="deadbeef",
        last_write_time=last_write_time,
        content_version=1,
        status=status,
    )


def permissive_config() -> FilterConfig:
    """Config qui ne rejette rien : sert de base aux tests ciblés."""
    return FilterConfig(
        excluded_extensions=[],
        min_size_bytes=0,
        max_size_bytes=1 << 40,
        excluded_dir_markers=[],
    )


# ------------------------------------------------------------------ exclusions


def test_aucune_exclusion() -> None:
    assert exclusion_reason(make_row(), permissive_config()) is None


@pytest.mark.parametrize("extension", ["jpg", "JPG", ".Jpg"])
def test_exclusion_par_extension(extension: str) -> None:
    cfg = permissive_config()
    cfg.excluded_extensions = [".jpg"]
    reason = exclusion_reason(make_row(extension=extension), cfg)
    assert reason is not None
    assert "extension exclue" in reason


def test_exclusion_extension_sans_point_dans_la_config() -> None:
    cfg = permissive_config()
    cfg.excluded_extensions = ["PNG"]
    assert exclusion_reason(make_row(extension="png"), cfg) is not None


def test_exclusion_fichier_trop_petit() -> None:
    cfg = permissive_config()
    cfg.min_size_bytes = 100
    reason = exclusion_reason(make_row(size_bytes=4), cfg)
    assert reason is not None
    assert "trop petit" in reason


def test_exclusion_fichier_trop_volumineux() -> None:
    cfg = permissive_config()
    cfg.max_size_bytes = 1_000
    reason = exclusion_reason(make_row(size_bytes=2_000), cfg)
    assert reason is not None
    assert "trop volumineux" in reason


@pytest.mark.parametrize(
    "path",
    [
        r"\\srv\c$\Windows\System32\notes.txt",
        r"\\srv\c$\WINDOWS\System32\notes.txt",
        "//srv/c$/Windows/System32/notes.txt",
    ],
)
def test_exclusion_par_marqueur_de_dossier(path: str) -> None:
    cfg = permissive_config()
    cfg.excluded_dir_markers = ["\\Windows\\"]
    reason = exclusion_reason(make_row(path=path, extension="txt"), cfg)
    assert reason is not None
    assert "dossier exclu" in reason


def test_ordre_des_regles_extension_avant_taille() -> None:
    cfg = FilterConfig(
        excluded_extensions=[".log"],
        min_size_bytes=1_000_000,
        max_size_bytes=1 << 40,
        excluded_dir_markers=[],
    )
    reason = exclusion_reason(make_row(extension="log", size_bytes=1), cfg)
    assert reason is not None
    assert "extension exclue" in reason


def test_fichier_sans_extension_non_exclu_par_extension() -> None:
    cfg = permissive_config()
    cfg.excluded_extensions = [".log"]
    assert exclusion_reason(make_row(extension="", name="LICENCE"), cfg) is None


# ----------------------------------------------------------------------- score


def test_score_maximum() -> None:
    row = make_row(name="contrat_cadre.docx", extension="docx", size_bytes=200_000)
    assert priority_score(row, NOW) == 100  # 40 + 30 + 20 + 10


def test_score_famille_bureautique_sans_mot_cle() -> None:
    row = make_row(name="notes.docx", extension="docx", size_bytes=200_000)
    assert priority_score(row, NOW) == 90


def test_score_famille_texte() -> None:
    row = make_row(name="notes.txt", extension="txt", size_bytes=200_000)
    assert priority_score(row, NOW) == 75  # 25 + 30 + 20


def test_score_famille_inconnue() -> None:
    row = make_row(name="donnees.bin", extension="bin", size_bytes=200_000)
    assert priority_score(row, NOW) == 60  # 10 + 30 + 20


@pytest.mark.parametrize(
    ("size_bytes", "attendu"),
    [(500, 10), (10 * 1024, 30), (5 * 1024 * 1024, 30), (20 * 1024 * 1024, 15)],
)
def test_score_taille(size_bytes: int, attendu: int) -> None:
    base = priority_score(make_row(extension="bin", size_bytes=200_000, name="x.bin"), NOW)
    row = make_row(extension="bin", size_bytes=size_bytes, name="x.bin")
    assert priority_score(row, NOW) == base - 30 + attendu


@pytest.mark.parametrize(
    ("jours", "attendu"),
    [(30, 20), (400, 12), (2_000, 5)],
)
def test_score_age(jours: int, attendu: int) -> None:
    modifie = (NOW - timedelta(days=jours)).strftime("%d/%m/%Y %H:%M:%S")
    row = make_row(extension="bin", size_bytes=200_000, name="x.bin", last_write_time=modifie)
    assert priority_score(row, NOW) == 10 + 30 + attendu


def test_score_age_inconnu() -> None:
    row = make_row(extension="bin", size_bytes=200_000, name="x.bin", last_write_time="n/a")
    assert priority_score(row, NOW) == 10 + 30 + 10


@pytest.mark.parametrize(
    "name",
    ["facture_2026.bin", "MOT DE PASSE.bin", "mot_de_passe.bin", "Bilan.bin", "rgpd-note.bin"],
)
def test_score_mots_cles(name: str) -> None:
    row = make_row(extension="bin", size_bytes=200_000, name=name)
    assert priority_score(row, NOW) == 70


def test_score_horodatage_avec_fuseau() -> None:
    """Un `now` aware ne doit pas faire exploser la comparaison."""
    aware = NOW.astimezone()
    assert priority_score(make_row(), aware) > 0


# ------------------------------------------------------------------------ plan


def test_plan_fixture_tout_pending(tmp_path: Path) -> None:
    with Database(tmp_path / "docia.sqlite") as db:
        import_csv(db, FIXTURE)
        report = plan_files(db, permissive_config())
        assert report.pending == 63
        assert report.excluded == 0
        assert report.by_reason == {}
        assert db.counts()["pending"] == 63


def test_plan_fixture_exclusions_par_defaut(tmp_path: Path) -> None:
    """La fixture est presque entièrement sous `\\admin$\\`.

    D-109 : les images matricielles ne sont plus exclues par extension — DocFuse
    les océrise. Celles de la fixture qui vivent hors de `\\admin$\\` deviennent
    donc analysables ; le compte d'exclus baisse d'autant. C'est le comportement
    voulu : un courrier scanné en `.tif` n'a plus à sortir de l'audit.
    """
    with Database(tmp_path / "docia.sqlite") as db:
        import_csv(db, FIXTURE)
        report = plan_files(db, FilterConfig())
        assert report.pending + report.excluded == 63
        assert sum(report.by_reason.values()) == report.excluded
        # Le motif de loin le plus fréquent reste le partage administratif.
        assert report.by_reason["dossier exclu (\\admin$\\)"] == 50
        counts = db.counts()
        assert counts["excluded"] == report.excluded
        assert counts["pending"] == report.pending
        assert ".jpg" not in " ".join(report.by_reason), "une image n'est plus exclue par extension"


def test_plan_fixture_taille_minimale(tmp_path: Path) -> None:
    cfg = permissive_config()
    cfg.min_size_bytes = 100
    with Database(tmp_path / "docia.sqlite") as db:
        import_csv(db, FIXTURE)
        report = plan_files(db, cfg)
        assert report.pending + report.excluded == 63
        assert report.excluded > 0
        assert all("trop petit" in reason for reason in report.by_reason)
        counts = db.counts()
        assert counts["pending"] == report.pending
        assert counts["excluded"] == report.excluded


def test_plan_attribue_les_scores(tmp_path: Path) -> None:
    with Database(tmp_path / "docia.sqlite") as db:
        import_csv(db, FIXTURE)
        plan_files(db, permissive_config())
        assert all(row.priority_score > 0 for row in list(db.iter_files()))


def test_plan_ne_retrograde_pas_un_fichier_done(tmp_path: Path) -> None:
    with Database(tmp_path / "docia.sqlite") as db:
        import_csv(db, FIXTURE)
        plan_files(db, permissive_config())
        cible = next(
            row for row in list(db.iter_files()) if row.path.endswith(r"\addins\fxsext.ecf")
        )
        db.set_file_status(cible.id, FileStatus.DONE)

        plan_files(db, FilterConfig())  # tout serait exclu

        apres = db.get_file(cible.id)
        assert apres is not None
        assert apres.status == FileStatus.DONE


# --------------------------------------------------- plan en flux et progression


def _fingerprint(db: Database) -> list[tuple[int, str, str | None, int]]:
    """État exact que le plan écrit en base, pour comparer deux exécutions."""
    return [
        (int(r["id"]), str(r["status"]), r["exclusion_reason"], int(r["priority_score"]))
        for r in db.query(
            "SELECT id, status, exclusion_reason, priority_score FROM files ORDER BY id"
        )
    ]


@pytest.mark.parametrize("chunk_size", [1, 7, 10_000])
def test_plan_par_tranches_rend_les_memes_compteurs(tmp_path: Path, chunk_size: int) -> None:
    """La taille de tranche est un détail d'exécution : compteurs et base identiques."""
    reference: tuple[PlanReport, list[tuple[int, str, str | None, int]]] | None = None
    for taille in (chunk_size, 10_000):
        with Database(tmp_path / f"docia_{taille}_{chunk_size}.sqlite") as db:
            import_csv(db, FIXTURE)
            report = plan_files(db, FilterConfig(), chunk_size=taille)
            state = _fingerprint(db)
        if reference is None:
            reference = (report, state)
        else:
            assert report == reference[0]
            assert state == reference[1]


def test_plan_progression_croissante_et_complete(tmp_path: Path) -> None:
    """Le rappel voit des valeurs croissantes, démarre à 0 et finit au total exact."""
    vus: list[PlanProgress] = []
    with Database(tmp_path / "docia.sqlite") as db:
        import_csv(db, FIXTURE)
        report = plan_files(db, FilterConfig(), progress=vus.append, chunk_size=10)
    assert len(vus) > 3  # 63 fichiers par tranches de 10 : départ, 6 tranches, fin
    assert vus[0].files == 0
    assert [p.files for p in vus] == sorted(p.files for p in vus)
    assert vus[-1].files == report.pending + report.excluded == 63
    assert vus[-1].total == 63
    assert vus[-1].percent == pytest.approx(100.0)
    assert vus[0].elapsed_s <= vus[-1].elapsed_s


def test_plan_progress_logger_espace_les_lignes() -> None:
    """Une ligne au démarrage, puis seulement au-delà des seuils (comme l'import)."""
    lignes: list[str] = []
    emit = plan_progress_logger(lignes.append, min_seconds=3600.0, min_files=50)
    emit(PlanProgress(files=0, total=100, elapsed_s=0.0))
    emit(PlanProgress(files=10, total=100, elapsed_s=0.1))  # sous les deux seuils : muet
    emit(PlanProgress(files=60, total=100, elapsed_s=0.2))  # +60 fichiers : parle
    assert lignes == [
        "préparation : 0 fichiers — 0 % — 0 s",
        "préparation : 60 fichiers — 60 % — 0 s",
    ]


def test_plan_progress_logger_emet_toujours_la_ligne_finale(tmp_path: Path) -> None:
    """Avec des seuils inatteignables, la préparation doit **quand même** annoncer sa fin.

    Sans `PlanProgress.final`, l'étranglement ravalait le dernier appel : la
    préparation d'une grosse campagne s'arrêtait sur « 97 % » et l'utilisateur ne
    voyait jamais 100 %.
    """
    lignes: list[str] = []
    with Database(tmp_path / "docia.sqlite") as db:
        import_csv(db, FIXTURE)
        plan_files(
            db,
            FilterConfig(),
            progress=plan_progress_logger(lignes.append, min_seconds=3_600.0, min_files=1_000_000),
            chunk_size=10,
        )
    assert lignes == [
        "préparation : 0 fichiers — 0 % — 0 s",
        "préparation : 63 fichiers — 100 % — 0 s",
    ]


def test_plan_progress_logger_ne_repete_pas_la_ligne_finale() -> None:
    """La dernière tranche a déjà tout dit : ne pas écrire deux fois la même ligne."""
    lignes: list[str] = []
    emit = plan_progress_logger(lignes.append, min_seconds=0.0, min_files=0)
    emit(PlanProgress(files=0, total=2, elapsed_s=0.0))
    emit(PlanProgress(files=2, total=2, elapsed_s=0.1))
    emit(PlanProgress(files=2, total=2, elapsed_s=0.1, final=True))
    assert lignes == [
        "préparation : 0 fichiers — 0 % — 0 s",
        "préparation : 2 fichiers — 100 % — 0 s",
    ]


def test_plan_base_vide_annonce_quand_meme_cent_pour_cent() -> None:
    """Une base vide n'a pas de dénominateur : le travail est fini, donc 100 %."""
    assert PlanProgress(files=0, total=0, elapsed_s=0.0).percent == 0.0
    assert PlanProgress(files=0, total=0, elapsed_s=0.0, final=True).percent == 100.0


def test_plan_ne_charge_pas_toute_la_base(tmp_path: Path) -> None:
    """`plan_files` parcourt en flux : jamais de liste complète des fichiers."""
    with Database(tmp_path / "docia.sqlite") as db:
        import_csv(db, FIXTURE)
        vus = 0

        def compte(_progress: PlanProgress) -> None:
            nonlocal vus
            vus += 1

        plan_files(db, permissive_config(), progress=compte, chunk_size=1)
        # un rappel au départ, un par fichier, un à la fin
        assert vus == 63 + 2


# ------------------------------------- raisons d'exclusion : stables, donc groupables


def test_la_raison_de_taille_ne_contient_pas_la_taille_du_fichier() -> None:
    """Une raison par taille = une clé de regroupement par fichier.

    Chaque vidéo, chaque PST, chaque VHD a sa propre taille : la colonne
    `exclusion_reason` et `PlanReport.by_reason` explosaient en dizaines de
    milliers de valeurs distinctes, et le tableau « 5.2 Exclusions et erreurs »,
    borné au top 10, n'affichait que dix tailles arbitraires.
    """
    cfg = permissive_config()
    cfg.min_size_bytes = 100
    cfg.max_size_bytes = 1_000
    petites = {exclusion_reason(make_row(size_bytes=n), cfg) for n in range(100)}
    grosses = {exclusion_reason(make_row(size_bytes=n), cfg) for n in range(1_001, 1_100)}
    assert petites == {"fichier trop petit"}
    assert grosses == {"fichier trop volumineux"}


def test_plan_regroupe_les_exclusions_de_taille(tmp_path: Path) -> None:
    """Sur une base entière : deux raisons, pas une par fichier."""
    cfg = permissive_config()
    cfg.min_size_bytes = 100
    cfg.max_size_bytes = 200_000
    with Database(tmp_path / "docia.sqlite") as db:
        scan = db.start_scan("x.csv")
        db.upsert_files(
            [_smbeagle_row(f"f{i}.dat", size=i) for i in range(60)]
            + [_smbeagle_row(f"g{i}.dat", size=300_000 + i) for i in range(60)],
            scan,
        )
        db.finish_scan(scan, total=120, new=120, updated=0, unchanged=0, invalid=0)
        report = plan_files(db, cfg)
        distinctes = db.query(
            "SELECT COUNT(DISTINCT exclusion_reason) AS n FROM files WHERE status='excluded'"
        )
        assert int(distinctes[0]["n"]) == 2
    assert report.excluded == 120
    assert report.by_reason == {"fichier trop petit": 60, "fichier trop volumineux": 60}


def test_plan_un_rappel_qui_echoue_ne_perd_pas_la_preparation(tmp_path: Path) -> None:
    """Une fenêtre fermée pendant la préparation ne doit pas faire tout recommencer.

    `import_csv.notify` protège déjà le sien : « une fenêtre détruite ne doit pas
    faire perdre un import de dix minutes ». La préparation d'un million de
    fichiers mérite la même garde.
    """

    def fenetre_detruite(_progress: PlanProgress) -> None:
        raise RuntimeError("main thread is not in main loop")

    with Database(tmp_path / "docia.sqlite") as db:
        import_csv(db, FIXTURE)
        report = plan_files(db, permissive_config(), progress=fenetre_detruite, chunk_size=1)
        assert report.pending + report.excluded == 63
        assert all(row.priority_score > 0 for row in db.iter_files())
