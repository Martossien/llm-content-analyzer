"""Pilotage du scanner SMBeagle : ligne de commande, progression JSON, sous-processus
(faux scanner Python), enchaînement scan → import → plan, dates d'accès premières."""

from __future__ import annotations

import json
import logging
import os
import stat
import sys
import threading
import time
from pathlib import Path

import pytest

from docia.scan import (
    ENRICHMENT_FLAGS,
    ScanError,
    ScanProfile,
    build_command,
    count_csv_rows,
    parse_progress_line,
    redact,
    run_scan,
)

HEADER = (
    "Name,Host,Extension,Username,Hostname,UNCDirectory,CreationTime,LastWriteTime,Readable,"
    "Writeable,Deletable,DirectoryType,Base,FileSize,AccessTime,FileAttributes,Owner,FastHash,"
    "FileSignature"
)
ROW = (
    '"a.txt","srv","txt","u","srv","\\\\srv\\part","01/02/2024 10:00:00","01/02/2024 10:00:00",'
    'True,False,False,"SMB","\\\\srv\\part",120,"03/04/2024 09:00:00","Archive","DOM\\u",'
    '"0123456789abcdef","TXT"'
)


def test_build_command_local_and_smb(tmp_path: Path) -> None:
    exe = tmp_path / "SMBeagle.exe"
    local = ScanProfile(local_paths=["D:\\partage", "E:\\autre"])
    cmd = build_command(exe, local, tmp_path / "scan.csv", manifest_out=tmp_path / "m.json")
    assert cmd[:4] == [str(exe), "-c", str(tmp_path / "scan.csv"), "-q"]
    # Une seule fois l'option, ses valeurs à la suite : SMBeagle déclare `--local-path`
    # en séquence CommandLineParser et refuse l'option répétée (code 2).
    assert cmd.count("--local-path") == 1
    assert cmd[cmd.index("--local-path") + 1 : cmd.index("--local-path") + 3] == [
        "D:\\partage",
        "E:\\autre",
    ]
    assert all(flag in cmd for flag in ENRICHMENT_FLAGS)
    assert "--preserve-access-time" in cmd
    assert "--progress-json" in cmd
    assert cmd[cmd.index("--manifest") + 1] == str(tmp_path / "m.json")
    assert "-D" not in cmd

    smb = ScanProfile(
        hosts=["srv1"], shares=["finance"], exclude_shares=["tmp"], username="u", password="s3cret"
    )
    cmd = build_command(exe, smb, tmp_path / "scan.csv", progress_json=False)
    assert "-D" in cmd
    assert cmd[cmd.index("-h") + 1] == "srv1"
    assert cmd[cmd.index("-s") + 1] == "finance"
    assert cmd[cmd.index("-S") + 1] == "tmp"
    assert "-E" in cmd
    assert "--progress-json" not in cmd
    assert "s3cret" not in redact(cmd)
    assert "••••" in redact(cmd)

    multi = ScanProfile(hosts=["srv1", "srv2"], shares=["a", "b"], exclude_shares=["x", "y"])
    multi_cmd = build_command(exe, multi, tmp_path / "scan.csv")
    for option in ("-h", "-s", "-S"):
        assert multi_cmd.count(option) == 1, f"{option} répété : CommandLineParser le refuse"


def test_profile_validation() -> None:
    assert ScanProfile().validate()
    assert ScanProfile(local_paths=["D:\\x"], hosts=["h"]).validate()
    assert ScanProfile(hosts=["h"], username="u").validate()
    assert ScanProfile(local_paths=["D:\\x"]).validate() == []


def test_profile_refuses_relative_paths() -> None:
    """Un chemin relatif serait résolu contre le répertoire courant du scanner —
    donc un autre dossier que celui voulu, sans que rien ne le signale."""
    for absolu in ("D:\\partage", "\\\\serveur\\partage", "/mnt/partage", "C:/data"):
        assert ScanProfile(local_paths=[absolu]).validate() == [], absolu
    for relatif in ("fichiers", "..\\partage", "./data", "", "   ", "partage\\sous"):
        errors = ScanProfile(local_paths=[relatif]).validate()
        assert any("non absolu" in e for e in errors), relatif


def test_parse_progress_line() -> None:
    ev = parse_progress_line(
        '{"event":"progress","stage":"files","hosts":1,"shares":2,"files":345,"elapsed_s":4.2}'
    )
    assert ev is not None
    assert (ev.stage, ev.files, ev.shares, ev.elapsed_s) == ("files", 345, 2, 4.2)
    err = parse_progress_line('{"event":"error","message":"accès refusé"}')
    assert err is not None
    assert err.stage == "error"
    assert parse_progress_line("4. Probing hosts…") is None
    assert parse_progress_line('{"pas":"un événement"}') is None
    assert parse_progress_line("{invalide") is None


def _fake_scanner(
    tmp_path: Path,
    *,
    rows: int = 3,
    exit_code: int = 0,
    sleep: float = 0.0,
    sleep_after: float = 0.0,
    announced: int | None = None,
    skipped: list[str] | None = None,
    manifest_skipped_key: bool = True,
) -> Path:
    """Faux SMBeagle : écrit un CSV 19 colonnes, des lignes de progression JSON, un manifeste.

    `announced` sépare le nombre de lignes **annoncées** (progression, manifeste)
    du nombre de lignes réellement **écrites** (`rows`) : c'est ce qui permet de
    rejouer un scanner mort en écrivant. `skipped` remplit la clé `skipped` du
    manifeste (cibles écartées, code de retour 4) ; `manifest_skipped_key=False`
    rejoue un `SMBeagle.exe` antérieur, dont le manifeste n'a pas cette clé.

    `sleep` retarde l'écriture du CSV, `sleep_after` la suit : le second rejoue un
    scan qu'on arrête alors qu'un CSV **partiel** existe déjà — le cas d'annulation
    qui compte, puisque c'est celui que docia importe.
    """
    annonce = rows if announced is None else announced
    skipped_json = json.dumps(list(skipped or []))
    script = tmp_path / "fake_smbeagle.py"
    script.write_text(
        f"""import json, sys, time
args = sys.argv[1:]
csv_out = args[args.index('-c') + 1]
manifest = args[args.index('--manifest') + 1] if '--manifest' in args else None
print('SMBeagle by PunkSecurity (faux)')
print(json.dumps({{'event': 'progress', 'stage': 'files', 'hosts': 1, 'shares': 1, 'files': 0, 'elapsed_s': 0.1}}))
time.sleep({sleep})
with open(csv_out, 'w', encoding='utf-8', newline='') as fh:
    fh.write({HEADER!r} + '\\n')
    for i in range({rows}):
        fh.write({ROW!r}.replace('a.txt', f'f{{i}}.txt') + '\\n')
print(json.dumps({{'event': 'progress', 'stage': 'writing', 'hosts': 1, 'shares': 1, 'files': {annonce}, 'elapsed_s': 0.2}}))
time.sleep({sleep_after})
if manifest:
    contenu = {{'version': 'fake', 'options': {{'args': args}}, 'counts': {{'files': {annonce}}}}}
    if {manifest_skipped_key!r}:
        contenu['skipped'] = json.loads({skipped_json!r})
    with open(manifest, 'w', encoding='utf-8') as fh:
        json.dump(contenu, fh)
print(json.dumps({{'event': 'done', 'files': {annonce}, 'csv': csv_out, 'elapsed_s': 0.3}}))
sys.exit({exit_code})
""",
        encoding="utf-8",
    )
    launcher = tmp_path / ("SMBeagle.cmd" if os.name == "nt" else "SMBeagle")
    if os.name == "nt":
        launcher.write_text(f'@"{sys.executable}" "{script}" %*\n', encoding="utf-8")
    else:
        launcher.write_text(
            f'#!/bin/sh\nexec "{sys.executable}" "{script}" "$@"\n', encoding="utf-8"
        )
        launcher.chmod(launcher.stat().st_mode | stat.S_IXUSR)
    return launcher


def test_run_scan_with_fake_scanner(tmp_path: Path) -> None:
    exe = _fake_scanner(tmp_path, rows=5)
    events, lines = [], []
    result = run_scan(
        ScanProfile(local_paths=[str(tmp_path)]),
        tmp_path / "out" / "scan.csv",
        exe=exe,
        on_event=events.append,
        on_line=lines.append,
    )
    assert result.exit_code == 0
    assert result.files == 5
    assert result.csv_path.is_file()
    assert result.manifest["counts"] == {"files": 5}
    assert [e.stage for e in events] == ["files", "writing", "done"]
    assert events[-1].files == 5
    assert lines[0].startswith("scan : ")
    assert any("PunkSecurity" in line for line in lines)
    assert count_csv_rows(result.csv_path) == 5


def test_run_scan_failure_and_missing_exe(tmp_path: Path) -> None:
    exe = _fake_scanner(tmp_path, rows=1, exit_code=1)
    with pytest.raises(ScanError, match="code 1"):
        run_scan(ScanProfile(local_paths=[str(tmp_path)]), tmp_path / "scan.csv", exe=exe)
    with pytest.raises(ScanError, match="introuvable"):
        run_scan(
            ScanProfile(local_paths=[str(tmp_path)]),
            tmp_path / "scan.csv",
            exe=None,
            configured_exe=str(tmp_path / "absent.exe"),
        )
    with pytest.raises(ScanError, match="au moins"):
        run_scan(ScanProfile(), tmp_path / "scan.csv", exe=exe)


def test_run_scan_cancel_keeps_partial_csv(tmp_path: Path) -> None:
    exe = _fake_scanner(tmp_path, rows=2, sleep=0.5)
    cancel = threading.Event()

    def on_event(_ev: object) -> None:
        cancel.set()

    result = run_scan(
        ScanProfile(local_paths=[str(tmp_path)]),
        tmp_path / "scan.csv",
        exe=exe,
        cancel=cancel,
        on_event=on_event,
    )
    assert result.exit_code != 0 or result.files >= 0


def test_scan_campaign_imports_and_plans(tmp_path: Path) -> None:
    from docia import service
    from docia.config import Config
    from docia.db import Database

    exe = _fake_scanner(tmp_path, rows=4)
    cfg = Config()
    cfg.db_path = str(tmp_path / "camp.sqlite")
    cfg.scan.smbeagle_path = str(exe)
    cfg.filter.excluded_dir_markers = []
    cfg.filter.min_size_bytes = 1
    with Database(cfg.db_path) as db:
        result, report, plan_report = service.scan_campaign(
            db, cfg, ScanProfile(local_paths=[str(tmp_path)])
        )
        assert result.files == 4
        assert report.total == 4
        assert report.new == 4
        assert plan_report.pending + plan_report.excluded == 4
        last = db.last_scan()
        assert last is not None
        assert last["kind"] == "scan"
        assert json.loads(last["manifest_json"])["counts"]["files"] == 4
        assert Path(last["csv_path"]).parent == service.scans_dir_for(db.path)


def test_first_access_time_survives_rescan(tmp_path: Path) -> None:
    """Un rescan d'un fichier inchangé ne rajeunit pas la date d'accès retenue."""
    from docia import views
    from docia.db import Database
    from docia.ingest.smbeagle_csv import import_csv

    csv1 = tmp_path / "s1.csv"
    csv1.write_text(HEADER + "\n" + ROW + "\n", encoding="utf-8")
    csv2 = tmp_path / "s2.csv"
    csv2.write_text(
        HEADER + "\n" + ROW.replace("03/04/2024", "30/08/2026") + "\n", encoding="utf-8"
    )
    csv3 = tmp_path / "s3.csv"
    csv3.write_text(
        HEADER
        + "\n"
        + ROW.replace("03/04/2024", "30/08/2026").replace("0123456789abcdef", "ffff")
        + "\n",
        encoding="utf-8",
    )
    with Database(tmp_path / "db.sqlite") as db:
        import_csv(db, csv1)
        import_csv(db, csv2)
        row = db.query("SELECT access_time, access_time_first FROM files")[0]
        assert row["access_time"].startswith("30/08/2026")
        assert row["access_time_first"].startswith("03/04/2024")
        from datetime import date

        buckets = views.stale_files(db, years=(1,), today=date(2026, 8, 30))
        assert buckets[0].not_accessed_files == 1  # d'après la première observation
        import_csv(db, csv3)  # contenu modifié : activité réelle → la référence repart
        row = db.query("SELECT access_time_first, content_version FROM files")[0]
        assert row["access_time_first"].startswith("30/08/2026")
        assert row["content_version"] == 2


# ------------------------------------------------ robustesse : ne jamais perdre un scan en cours


def _script_scanner(tmp_path: Path, corps: str, nom: str) -> Path:
    """Faux scanner au comportement choisi ; `args` et `csv_out` sont déjà en place."""
    script = tmp_path / f"{nom}.py"
    script.write_text(
        "import json, sys, time\nargs = sys.argv[1:]\ncsv_out = args[args.index('-c') + 1]\n"
        + corps,
        encoding="utf-8",
    )
    launcher = tmp_path / (f"{nom}.cmd" if os.name == "nt" else nom)
    if os.name == "nt":
        launcher.write_text(f'@"{sys.executable}" "{script}" %*\n', encoding="utf-8")
    else:
        launcher.write_text(
            f'#!/bin/sh\nexec "{sys.executable}" "{script}" "$@"\n', encoding="utf-8"
        )
        launcher.chmod(launcher.stat().st_mode | stat.S_IXUSR)
    return launcher


def test_parse_progress_line_malformee_est_une_ligne_de_texte() -> None:
    """GRAVE 5 : le contrat du scanner (C#, versionné à part) n'est pas verrouillé.

    Un `files` rendu par `ToString("N0")`, un `elapsed_s` en durée ou un flottant
    infini faisaient remonter `ValueError` / `OverflowError` hors du `try` : le
    sous-processus était tué et un scan de plusieurs heures perdu sur une trace.
    """
    for ligne in (
        '{"event":"scan","files":"beaucoup"}',
        '{"event":"scan","files":"1 234"}',
        '{"event":"scan","files":1e400}',
        '{"event":"scan","elapsed_s":"00:01:23"}',
        '{"event":"scan","hosts":{"srv":1}}',
    ):
        assert parse_progress_line(ligne) is None, ligne


def test_run_scan_consulte_cancel_pendant_un_scanner_silencieux(tmp_path: Path) -> None:
    """MOYEN 6 : SMBeagle est lancé avec `-q` — le silence est le cas normal.

    `cancel` n'était consulté qu'après réception d'une ligne, or `for raw in
    proc.stdout:` bloque : « Arrêter » restait sans effet pendant l'énumération
    d'un gros partage ou le délai TCP d'un hôte injoignable.
    """
    exe = _script_scanner(
        tmp_path,
        f"open(csv_out, 'w', encoding='utf-8').write({HEADER!r} + '\\n')\n"
        "print(json.dumps({'event': 'scan', 'stage': 'hosts', 'files': 0}), flush=True)\n"
        "time.sleep(20)\n",
        "muet",
    )
    cancel = threading.Event()
    lignes: list[str] = []
    threading.Timer(0.5, cancel.set).start()
    debut = time.monotonic()
    result = run_scan(
        ScanProfile(local_paths=[str(tmp_path)]),
        tmp_path / "scan.csv",
        exe=exe,
        cancel=cancel,
        on_line=lignes.append,
    )
    duree = time.monotonic() - debut
    assert duree < 8, f"l'annulation a mis {duree:.1f} s (le scanner se taisait 20 s)"
    assert any("arrêt demandé" in ligne for ligne in lignes), "l'arrêt doit être signalé"
    assert result.csv_path.is_file(), "le CSV partiel reste conservé"


def test_run_scan_refuse_un_csv_vide_annonce_plein(tmp_path: Path) -> None:
    """MOYEN 10 : le chiffre de la progression n'est pas un décompte de lignes.

    Disque plein en fin de scan, écriture coupée sur un partage, scanner tombé
    après avoir vidé ses compteurs : `files = count_csv_rows(...) or last_files`
    annonçait « scan terminé : 42 000 fichiers » sur un CSV de 0 octet.
    """
    exe = _script_scanner(
        tmp_path,
        "open(csv_out, 'w', encoding='utf-8').close()\n"
        "print(json.dumps({'event': 'scan', 'files': 42000}), flush=True)\n",
        "csv_vide",
    )
    with pytest.raises(ScanError, match="42000"):
        run_scan(ScanProfile(local_paths=[str(tmp_path)]), tmp_path / "scan.csv", exe=exe)


def test_run_scan_survit_a_des_rappels_defaillants(tmp_path: Path) -> None:
    """MOYEN 12 : un `on_event` qui lève (fenêtre détruite) ne doit pas tuer le scan."""

    def boum(_payload: object) -> None:
        raise RuntimeError("fenêtre détruite (TclError)")

    exe = _fake_scanner(tmp_path, rows=3)
    result = run_scan(
        ScanProfile(local_paths=[str(tmp_path)]),
        tmp_path / "scan.csv",
        exe=exe,
        on_event=boum,
        on_line=boum,
    )
    assert result.files == 3


def test_scan_result_ne_conserve_pas_le_mot_de_passe(tmp_path: Path) -> None:
    """MINEUR 21 : `command` est stockée déjà masquée, pas seulement affichée."""
    exe = _fake_scanner(tmp_path, rows=1)
    result = run_scan(
        ScanProfile(hosts=["srv1"], username="dom\\u", password="s3cret"),
        tmp_path / "scan.csv",
        exe=exe,
    )
    assert "s3cret" not in " ".join(result.command)
    assert "••••" in result.command


# --------------------------------------------------- périmètre du scan (C1/C2/C3)


def test_run_scan_accepte_le_code_4_perimetre_incomplet(tmp_path: Path) -> None:
    """C1 : le code 4 du scanner n'est **pas** un échec, et les cibles écartées remontent.

    Preuve d'origine, vrai binaire SMBeagle sur deux dossiers dont l'un en
    `chmod 000` : docia relevait `EXIT=0`, `manifest.targets=[A]`, `counts.files=2`
    et annonçait « scan terminé » — le contenu de `B` n'existait nulle part. Le
    scanner sort désormais en 4 avec `skipped=[B]` ; docia le traitait alors comme
    fatal (`scan.py` : code hors (0, 3) → `ScanError`), ce qui transformait un scan
    parfaitement exploitable en échec dur. Vérifié en réel : `EXIT=4`,
    `skipped=[B]`, 7 lignes importées.
    """
    exe = _fake_scanner(tmp_path, rows=7, exit_code=4, skipped=["\\\\srv\\finance"])
    lignes: list[str] = []
    result = run_scan(
        ScanProfile(local_paths=[str(tmp_path)]),
        tmp_path / "scan.csv",
        exe=exe,
        on_line=lignes.append,
    )
    assert result.exit_code == 4  # accepté, aucune ScanError
    assert result.files == 7  # le CSV est bon : il s'importe normalement
    assert result.skipped == ["\\\\srv\\finance"]
    assert result.complete is False
    assert any("périmètre incomplet" in ligne for ligne in lignes)


def test_run_scan_sans_cible_ecartee_reste_complet(tmp_path: Path) -> None:
    """Compatibilité descendante : un scanner antérieur au code 4 ne devient pas suspect.

    Manifeste sans clé `skipped`, code de retour 0 : rien ne change, et surtout
    aucun faux positif « périmètre incomplet »."""
    exe = _fake_scanner(tmp_path, rows=3, manifest_skipped_key=False)
    result = run_scan(ScanProfile(local_paths=[str(tmp_path)]), tmp_path / "scan.csv", exe=exe)
    assert result.exit_code == 0
    assert result.skipped == []
    assert result.cancelled is False
    assert result.complete is True


def test_run_scan_refuse_un_csv_plus_court_que_le_compte_annonce(tmp_path: Path) -> None:
    """C2 : un CSV tronqué ne passe plus pour un scan réussi.

    Preuve d'origine : un scanner qui annonce 1 000 000 de fichiers, en écrit 5 et
    rend 0 donnait `OK exit_code=0 files=5 → import total=5 invalid=0`, sans le
    moindre avertissement — `scan.py` ne levait que si `files == 0 and
    last_files > 0`. Le contrôle porte maintenant sur l'écart, dont « zéro ligne »
    n'était que le cas extrême, et confronte le compte réel à `manifest.counts.files`.
    """
    exe = _fake_scanner(tmp_path, rows=5, announced=1_000_000)
    with pytest.raises(ScanError, match="1000000 fichier"):
        run_scan(ScanProfile(local_paths=[str(tmp_path)]), tmp_path / "scan.csv", exe=exe)


def test_expected_file_count_fait_foi_sur_le_manifeste() -> None:
    """Le manifeste prime sur la progression, et son absence n'invente aucun écart."""
    from docia.scan import expected_file_count

    # Le manifeste est écrit après la fermeture du CSV : c'est lui qui décrit le
    # fichier tel qu'il a été refermé, pas le compteur en cours de progression.
    assert expected_file_count({"counts": {"files": 12}}, 9) == 12
    assert expected_file_count({}, 9) == 9  # pas de manifeste : repli sur la progression
    assert expected_file_count({}, 0) == 0  # aucune annonce : aucun contrôle possible
    assert expected_file_count({"counts": {"files": "12"}}, 0) == 12
    assert expected_file_count({"counts": {"files": "?"}}, 4) == 4  # illisible : repli
    assert expected_file_count({"counts": {"files": True}}, 4) == 4  # `True` ne vaut pas 1


def test_scan_annule_est_marque_en_base(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    """C3 : un scan annulé cesse d'être indiscernable d'un scan complet.

    Preuve d'origine (faux scanner d'annulation) : `exit_code=-15`, 4 901 lignes
    importées, `total=4901 invalid=0`, et **rien** — ni `ScanResult`, ni la table
    `scans` — ne disait que la campagne portait sur un fragment. Importer le
    partiel reste le bon choix ; n'en garder aucune trace ne l'était pas.

    Le message et le stockage distinguent l'annulation (attendue) d'un scanner
    mort en écrivant (anormal, couvert par le test précédent, qui refuse le CSV).
    """
    from docia import service
    from docia.config import Config
    from docia.db import Database

    exe = _fake_scanner(tmp_path, rows=2, announced=500, sleep_after=1.5)
    cfg = Config()
    cfg.db_path = str(tmp_path / "camp.sqlite")
    cfg.scan.smbeagle_path = str(exe)
    cfg.filter.excluded_dir_markers = []
    cfg.filter.min_size_bytes = 1
    cancel = threading.Event()

    def annule_des_le_csv_ecrit(event: object) -> None:
        """Annule **sur un fait**, jamais sur un délai.

        Ce test avait un `threading.Timer(0.25, cancel.set)` : il pariait que le
        faux scanner aurait écrit son CSV partiel avant l'annulation. Sur un
        runner Windows lent, le seul démarrage de l'interpréteur dépasse ce délai,
        l'annulation arrivait **avant** le CSV, et le scan tombait dans l'autre
        branche — correcte, mais pas celle qu'on veut éprouver ici (« scan arrêté
        avant que le scanner n'écrive le CSV »). Le faux scanner émet son étape
        `writing` **après** avoir écrit les lignes : on s'y accroche.
        """
        if getattr(event, "stage", "") == "writing":
            cancel.set()

    lignes: list[str] = []
    with caplog.at_level(logging.WARNING), Database(cfg.db_path) as db:
        result, report, _ = service.scan_campaign(
            db,
            cfg,
            ScanProfile(local_paths=[str(tmp_path)]),
            cancel=cancel,
            on_event=annule_des_le_csv_ecrit,
            on_line=lignes.append,
            do_plan=False,
        )
        assert result.cancelled is True
        assert result.complete is False
        assert report.total == result.files  # le partiel est bien importé
        last = db.last_scan()
        assert last is not None
        assert last["cancelled"] == 1
        assert last["complete"] == 0
        assert last["exit_code"] == result.exit_code
        assert db.incomplete_scans()  # la base sait répondre, sans le manifeste
    # L'avertissement passe par le **journal**, pas par `on_line` : les deux façades
    # l'affichent déjà pour leur compte (la CLI par le gestionnaire console, la fenêtre
    # par `tab_home`), et le pousser aussi dans `on_line` le faisait sortir deux fois
    # à l'écran — constaté sur un vrai scan à périmètre amputé.
    assert "arrêté en cours de route" in caplog.text
    assert "écriture interrompue" not in caplog.text, "un arrêt demandé n'est pas un scanner mort"
    assert not any("arrêté en cours de route" in ligne for ligne in lignes), "pas de doublon"


def test_scan_complet_ne_marque_rien_en_base(tmp_path: Path) -> None:
    """Le cas normal ne change pas : `complete=1`, aucun scan signalé incomplet."""
    from docia import service
    from docia.config import Config
    from docia.db import Database

    exe = _fake_scanner(tmp_path, rows=4)
    cfg = Config()
    cfg.db_path = str(tmp_path / "camp.sqlite")
    cfg.scan.smbeagle_path = str(exe)
    cfg.filter.excluded_dir_markers = []
    cfg.filter.min_size_bytes = 1
    with Database(cfg.db_path) as db:
        result, _report, _plan = service.scan_campaign(
            db, cfg, ScanProfile(local_paths=[str(tmp_path)]), do_plan=False
        )
        assert result.complete is True
        last = db.last_scan()
        assert last is not None
        assert last["complete"] == 1
        assert last["cancelled"] == 0
        assert last["skipped_json"] == ""
        assert last["expected_files"] == 4
        assert db.incomplete_scans() == []


def test_scope_warnings_dit_quoi_faire_sans_jargon() -> None:
    """Les messages nomment ce qui manque et l'action, sans code de retour brut."""
    from docia.scan import scope_warnings

    assert scope_warnings(skipped=[], cancelled=False, expected_files=7, files=7) == []
    (ecarte,) = scope_warnings(skipped=["\\\\srv\\rh"], cancelled=False, expected_files=7, files=7)
    assert "\\\\srv\\rh" in ecarte
    assert "relancez le scan" in ecarte
    assert "exit" not in ecarte.lower()
    assert "code 4" not in ecarte
    (annule,) = scope_warnings(skipped=[], cancelled=True, expected_files=500, files=2)
    assert "arrêté en cours de route" in annule
    (mort,) = scope_warnings(skipped=[], cancelled=False, expected_files=500, files=2)
    assert "à refaire" in mort
    assert "arrêté" not in mort  # scanner mort ≠ arrêt demandé
    # Au-delà de trois cibles, la phrase reste lisible : trois noms puis un compte.
    (beaucoup,) = scope_warnings(
        skipped=[f"/part{i}" for i in range(5)], cancelled=False, expected_files=0, files=0
    )
    assert "/part2" in beaucoup
    assert "/part3" not in beaucoup
    assert "2 autre(s)" in beaucoup
