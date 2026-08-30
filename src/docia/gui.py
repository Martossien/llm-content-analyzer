"""Interface graphique CustomTkinter (extra `docia[gui]`), calquée sur celle de DocFuse.

Un seul écran, trois zones :
1. **Source** — CSV SMBeagle, base SQLite, dossier des blocs ; boutons Importer / Planifier.
2. **LLM et blocs** — transport, URL, clé, modèle, requêtes en vol, taille de bloc,
   moteur de comptage ; « Tester la connexion » ; « Enregistrer la config ».
3. **Exécution** — Lancer / Arrêter (annulation propre : les blocs déjà construits
   sont repris au run suivant), journal, compteurs, export CSV/JSON.

Toute la logique métier reste dans `pipeline`/`db` : la fenêtre ne fait que
lire la config, lancer un thread et afficher. `customtkinter` n'est importé
qu'à la construction de la fenêtre (`launch()`), pour que le cœur s'importe
sans Tk. Les fonctions de bas de module sont pures (testables sans fenêtre).
"""

from __future__ import annotations

import json
import logging
import queue
import threading
import tomllib
from collections.abc import Iterable
from dataclasses import asdict
from pathlib import Path
from typing import Any

from docia import __version__
from docia.config import DEFAULT_CONFIG_NAME, Config, load_config

logger = logging.getLogger(__name__)

TRANSPORTS = ("vllm", "openwebui")
TOKENIZERS = ("approx", "mistral", "openai")
_POLL_MS = 200
_MAX_LOG_LINES = 2000


# ---------------------------------------------------------------- helpers purs
def config_to_toml(cfg: Config) -> str:
    """Sérialise une `Config` en TOML lisible (tomllib ne sait qu'écrire → on
    formate à la main, champs simples uniquement)."""

    def value(v: object) -> str:
        if isinstance(v, bool):
            return "true" if v else "false"
        if isinstance(v, int | float):
            return str(v)
        if isinstance(v, list):
            return "[" + ", ".join(json.dumps(str(x), ensure_ascii=False) for x in v) + "]"
        return json.dumps(str(v), ensure_ascii=False)

    data = asdict(cfg)
    lines = [f"# docia.toml — écrit par l'interface docia {__version__}"]
    for key in ("db_path", "prompt_path"):
        lines.append(f"{key} = {value(data[key])}")
    for section in ("llm", "blocks", "filter"):
        lines.append("")
        lines.append(f"[{section}]")
        for key, v in data[section].items():
            lines.append(f"{key} = {value(v)}")
    text = "\n".join(lines) + "\n"
    tomllib.loads(text)  # garantit qu'on écrit du TOML valide
    return text


def status_lines(counts: dict[str, int], classes: dict[str, dict[str, int]]) -> list[str]:
    """Lignes du panneau de compteurs (pur)."""
    lines = [
        f"fichiers : {counts.get('files', 0)} — à analyser {counts.get('pending', 0)}, "
        f"en cours {counts.get('queued', 0)}, analysés {counts.get('done', 0)}, "
        f"exclus {counts.get('excluded', 0)}, en erreur {counts.get('error', 0)}",
        f"blocs : construits {counts.get('blocks_built', 0)}, envoyés {counts.get('blocks_sent', 0)}, "
        f"terminés {counts.get('blocks_done', 0)}, en erreur {counts.get('blocks_error', 0)} — "
        f"analyses : {counts.get('analyses', 0)}",
    ]
    for domain, label in (
        ("security", "sécurité"),
        ("rgpd", "RGPD"),
        ("finance", "finance"),
        ("legal", "juridique"),
    ):
        dist = classes.get(domain) or {}
        if dist:
            lines.append(f"{label} : " + ", ".join(f"{k} {v}" for k, v in sorted(dist.items())))
    return lines


def parse_int(raw: str, fallback: int, *, minimum: int = 1) -> int:
    """Entier saisi dans un champ texte, sinon `fallback` (pur)."""
    try:
        v = int(raw.strip().replace(" ", "").replace(" ", ""))
    except ValueError:
        return fallback
    return v if v >= minimum else fallback


def result_rows(rows: Iterable[Any], limit: int = 500) -> list[tuple[str, str, str, str, str, str]]:
    """Lignes du tableau de résultats : (nom, sécurité, RGPD, finance, juridique, résumé)."""
    out: list[tuple[str, str, str, str, str, str]] = []
    for r in rows:
        if len(out) >= limit:
            break
        out.append(
            (
                str(r["name"]),
                str(r["security_classification"] or (r["status"] if r["status"] != "done" else "")),
                str(r["rgpd_risk_level"] or ""),
                str(r["finance_document_type"] or ""),
                str(r["legal_contract_type"] or ""),
                (str(r["resume"] or r["exclusion_reason"] or ""))[:120],
            )
        )
    return out


# ---------------------------------------------------------------- fenêtre
class DociaGUI:
    """Fenêtre principale. Construite dans `launch()` uniquement."""

    def __init__(self, config_path: Path | None = None) -> None:
        import customtkinter as ctk

        self.ctk = ctk
        self.config_path = config_path or Path(DEFAULT_CONFIG_NAME)
        try:
            self.config = load_config(self.config_path if self.config_path.exists() else None)
        except ValueError as exc:
            logger.warning("config invalide, défauts utilisés : %s", exc)
            self.config = Config()
        self._log_queue: queue.Queue[str] = queue.Queue()
        self._worker: threading.Thread | None = None
        self._cancel = threading.Event()

        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")
        self.root = ctk.CTk()
        self.root.title(f"Doc-IA analyzer {__version__}")
        self.root.geometry("1100x820")
        self.root.minsize(900, 700)
        self._build_ui()
        self.root.after(_POLL_MS, self._poll)

    # ---- construction
    def _build_ui(self) -> None:
        ctk = self.ctk
        root = self.root

        # 1. Source
        src = ctk.CTkFrame(root)
        src.pack(fill="x", padx=12, pady=(12, 6))
        ctk.CTkLabel(src, text="1. Source", font=ctk.CTkFont(size=14, weight="bold")).grid(
            row=0, column=0, columnspan=4, sticky="w", padx=10, pady=(8, 4)
        )
        self.csv_var = ctk.StringVar(value="")
        self.db_var = ctk.StringVar(value=self.config.db_path)
        self._row_path(src, 1, "CSV SMBeagle", self.csv_var, self._pick_csv)
        self._row_path(src, 2, "Base SQLite", self.db_var, self._pick_db)
        btns = ctk.CTkFrame(src, fg_color="transparent")
        btns.grid(row=3, column=0, columnspan=4, sticky="w", padx=10, pady=(4, 8))
        self.import_button = ctk.CTkButton(btns, text="Importer le CSV", command=self._import)
        self.import_button.pack(side="left", padx=(0, 8))
        self.plan_button = ctk.CTkButton(
            btns, text="Planifier (exclusions + priorité)", command=self._plan
        )
        self.plan_button.pack(side="left", padx=(0, 8))
        self.status_button = ctk.CTkButton(
            btns, text="Rafraîchir l'état", command=self._refresh_status
        )
        self.status_button.pack(side="left")

        # 2. LLM et blocs
        llm = ctk.CTkFrame(root)
        llm.pack(fill="x", padx=12, pady=6)
        ctk.CTkLabel(llm, text="2. LLM et blocs", font=ctk.CTkFont(size=14, weight="bold")).grid(
            row=0, column=0, columnspan=6, sticky="w", padx=10, pady=(8, 4)
        )
        c = self.config
        self.transport_var = ctk.StringVar(value=c.llm.transport)
        self.url_var = ctk.StringVar(value=c.llm.base_url)
        self.key_var = ctk.StringVar(value=c.llm.api_key)
        self.model_var = ctk.StringVar(value=c.llm.model)
        self.inflight_var = ctk.StringVar(value=str(c.llm.max_in_flight))
        self.block_tokens_var = ctk.StringVar(value=str(c.blocks.block_tokens))
        self.tokenizer_var = ctk.StringVar(value=c.blocks.tokenizer_engine)
        self.timeout_var = ctk.StringVar(value=str(c.llm.timeout_s))

        ctk.CTkLabel(llm, text="Transport").grid(row=1, column=0, sticky="w", padx=10)
        ctk.CTkOptionMenu(
            llm, variable=self.transport_var, values=list(TRANSPORTS), width=130
        ).grid(row=1, column=1, sticky="w", padx=4, pady=3)
        ctk.CTkLabel(llm, text="URL de base").grid(row=1, column=2, sticky="w", padx=10)
        ctk.CTkEntry(llm, textvariable=self.url_var, width=340).grid(
            row=1, column=3, columnspan=3, sticky="w", padx=4, pady=3
        )
        ctk.CTkLabel(llm, text="Clé API").grid(row=2, column=0, sticky="w", padx=10)
        ctk.CTkEntry(llm, textvariable=self.key_var, width=220, show="•").grid(
            row=2, column=1, columnspan=2, sticky="w", padx=4, pady=3
        )
        ctk.CTkLabel(llm, text="Modèle").grid(row=2, column=3, sticky="w", padx=10)
        ctk.CTkEntry(llm, textvariable=self.model_var, width=180).grid(
            row=2, column=4, sticky="w", padx=4, pady=3
        )
        ctk.CTkLabel(llm, text="Requêtes en vol").grid(row=3, column=0, sticky="w", padx=10)
        ctk.CTkEntry(llm, textvariable=self.inflight_var, width=70).grid(
            row=3, column=1, sticky="w", padx=4, pady=3
        )
        ctk.CTkLabel(llm, text="Tokens par bloc").grid(row=3, column=2, sticky="w", padx=10)
        ctk.CTkEntry(llm, textvariable=self.block_tokens_var, width=90).grid(
            row=3, column=3, sticky="w", padx=4, pady=3
        )
        ctk.CTkLabel(llm, text="Comptage").grid(row=3, column=4, sticky="w", padx=10)
        ctk.CTkOptionMenu(
            llm, variable=self.tokenizer_var, values=list(TOKENIZERS), width=110
        ).grid(row=3, column=5, sticky="w", padx=4, pady=3)
        ctk.CTkLabel(llm, text="Timeout (s)").grid(row=4, column=0, sticky="w", padx=10)
        ctk.CTkEntry(llm, textvariable=self.timeout_var, width=70).grid(
            row=4, column=1, sticky="w", padx=4, pady=3
        )
        b2 = ctk.CTkFrame(llm, fg_color="transparent")
        b2.grid(row=5, column=0, columnspan=6, sticky="w", padx=10, pady=(4, 8))
        ctk.CTkButton(b2, text="Tester la connexion", command=self._test_connection).pack(
            side="left", padx=(0, 8)
        )
        ctk.CTkButton(b2, text="Enregistrer la config", command=self._save_config).pack(side="left")

        # 3. Exécution
        run = ctk.CTkFrame(root)
        run.pack(fill="both", expand=True, padx=12, pady=(6, 12))
        ctk.CTkLabel(run, text="3. Exécution", font=ctk.CTkFont(size=14, weight="bold")).pack(
            anchor="w", padx=10, pady=(8, 4)
        )
        b3 = ctk.CTkFrame(run, fg_color="transparent")
        b3.pack(fill="x", padx=10)
        ctk.CTkLabel(b3, text="Limite de fichiers (0 = tous)").pack(side="left")
        self.limit_var = ctk.StringVar(value="0")
        ctk.CTkEntry(b3, textvariable=self.limit_var, width=80).pack(side="left", padx=(4, 12))
        self.dry_var = ctk.BooleanVar(value=False)
        ctk.CTkCheckBox(
            b3, text="Construire les blocs seulement (sans LLM)", variable=self.dry_var
        ).pack(side="left", padx=(0, 12))
        self.run_button = ctk.CTkButton(
            b3, text="Lancer l'analyse", command=self._start_run, fg_color="#16a34a"
        )
        self.run_button.pack(side="left", padx=(0, 8))
        self.stop_button = ctk.CTkButton(
            b3, text="Arrêter", command=self._stop_run, state="disabled", fg_color="#ef4444"
        )
        self.stop_button.pack(side="left", padx=(0, 8))
        ctk.CTkButton(b3, text="Exporter CSV", command=lambda: self._export("csv")).pack(
            side="left", padx=(0, 8)
        )
        ctk.CTkButton(b3, text="Exporter JSON", command=lambda: self._export("json")).pack(
            side="left"
        )

        self.status_label = ctk.CTkLabel(run, text="", justify="left", anchor="w")
        self.status_label.pack(fill="x", padx=10, pady=(6, 2))

        self.log_box = ctk.CTkTextbox(run, height=180)
        self.log_box.pack(fill="x", padx=10, pady=(2, 6))
        self.log_box.configure(state="disabled")

        self.results = ctk.CTkScrollableFrame(run, label_text="Derniers résultats (500 max)")
        self.results.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        self._refresh_status()

    def _row_path(self, parent: Any, row: int, label: str, var: Any, picker: Any) -> None:
        ctk = self.ctk
        ctk.CTkLabel(parent, text=label).grid(row=row, column=0, sticky="w", padx=10, pady=3)
        ctk.CTkEntry(parent, textvariable=var, width=620).grid(
            row=row, column=1, columnspan=2, sticky="w", padx=4
        )
        ctk.CTkButton(parent, text="…", width=36, command=picker).grid(
            row=row, column=3, sticky="w", padx=4
        )

    # ---- config
    def _collect_config(self) -> Config:
        cfg = self.config
        cfg.db_path = self.db_var.get().strip() or cfg.db_path
        cfg.llm.transport = self.transport_var.get()
        cfg.llm.base_url = self.url_var.get().strip()
        cfg.llm.api_key = self.key_var.get().strip()
        cfg.llm.model = self.model_var.get().strip() or cfg.llm.model
        cfg.llm.max_in_flight = parse_int(self.inflight_var.get(), cfg.llm.max_in_flight)
        cfg.llm.timeout_s = parse_int(self.timeout_var.get(), cfg.llm.timeout_s, minimum=10)
        cfg.blocks.block_tokens = parse_int(
            self.block_tokens_var.get(), cfg.blocks.block_tokens, minimum=1000
        )
        cfg.blocks.tokenizer_engine = self.tokenizer_var.get()
        errors = cfg.validate()
        for e in errors:
            self._log(f"config : {e}")
        return cfg

    def _save_config(self) -> None:
        cfg = self._collect_config()
        self.config_path.write_text(config_to_toml(cfg), encoding="utf-8")
        self._log(f"config enregistrée : {self.config_path}")

    # ---- actions
    def _pick_csv(self) -> None:
        from tkinter import filedialog

        path = filedialog.askopenfilename(filetypes=[("CSV SMBeagle", "*.csv"), ("Tous", "*.*")])
        if path:
            self.csv_var.set(path)
            if not self.db_var.get().strip() or self.db_var.get() == "docia.sqlite":
                self.db_var.set(str(Path(path).with_suffix(".sqlite")))

    def _pick_db(self) -> None:
        from tkinter import filedialog

        path = filedialog.asksaveasfilename(
            defaultextension=".sqlite", filetypes=[("SQLite", "*.sqlite")]
        )
        if path:
            self.db_var.set(path)

    def _import(self) -> None:
        csv_path = Path(self.csv_var.get().strip())
        if not csv_path.is_file():
            self._log("choisissez un CSV SMBeagle existant")
            return
        cfg = self._collect_config()

        def work() -> None:
            from docia.db import Database
            from docia.ingest.smbeagle_csv import import_csv

            with Database(cfg.db_path) as db:
                rep = import_csv(db, csv_path, strict=False)
            self._log(
                f"import : {rep.total} lignes — {rep.new} nouveaux, {rep.updated} modifiés, "
                f"{rep.unchanged} inchangés, {rep.invalid} invalides"
            )

        self._run_in_thread(work, "import")

    def _plan(self) -> None:
        cfg = self._collect_config()

        def work() -> None:
            from docia.db import Database
            from docia.filter import plan_files

            with Database(cfg.db_path) as db:
                rep = plan_files(db, cfg.filter)
            self._log(f"plan : {rep.pending} à analyser, {rep.excluded} exclus")
            for reason, n in sorted(rep.by_reason.items(), key=lambda kv: -kv[1])[:8]:
                self._log(f"   {n:>7}  {reason}")

        self._run_in_thread(work, "plan")

    def _test_connection(self) -> None:
        cfg = self._collect_config()

        def work() -> None:
            import asyncio

            from docia.llm.client import LLMClient

            async def probe() -> bool:
                async with LLMClient(cfg.llm, "") as client:
                    return await client.health()

            ok = asyncio.run(probe())
            self._log(f"connexion {cfg.llm.base_url} : {'OK' if ok else 'ÉCHEC'}")

        self._run_in_thread(work, "test")

    def _start_run(self) -> None:
        if self._worker and self._worker.is_alive():
            return
        cfg = self._collect_config()
        if cfg.validate():
            return
        limit = parse_int(self.limit_var.get(), 0, minimum=0) or None
        dry = bool(self.dry_var.get())
        self._cancel.clear()

        def work() -> None:
            from docia.db import Database
            from docia.pipeline import run_pipeline

            with Database(cfg.db_path) as db:
                rep = run_pipeline(
                    db, cfg, limit=limit, dry_run=dry, progress=self._log, cancel=self._cancel
                )
            self._log(
                f"run {rep.run_id} : {rep.files_selected} sélectionnés, {rep.files_done} analysés, "
                f"{rep.files_error} en erreur, blocs {rep.blocks_done}/{rep.blocks_built + rep.blocks_resumed}, "
                f"tokens {rep.prompt_tokens}/{rep.completion_tokens}"
            )
            for e in rep.errors[:10]:
                self._log(f"   {e}")

        self._run_in_thread(work, "run")

    def _stop_run(self) -> None:
        self._cancel.set()
        self._log("arrêt demandé — les requêtes en cours se terminent, rien n'est perdu")

    def _export(self, fmt: str) -> None:
        from tkinter import filedialog

        cfg = self._collect_config()
        path = filedialog.asksaveasfilename(
            defaultextension=f".{fmt}", filetypes=[(fmt.upper(), f"*.{fmt}")]
        )
        if not path:
            return
        from docia.cli import main as cli_main

        code = cli_main(["--db", cfg.db_path, "export", "--format", fmt, "--out", path])
        self._log(f"export {fmt} → {path} ({'OK' if code == 0 else 'échec'})")

    # ---- thread / journal / état
    def _run_in_thread(self, work: Any, name: str) -> None:
        if self._worker and self._worker.is_alive():
            self._log("une opération est déjà en cours")
            return

        def wrapped() -> None:
            try:
                work()
            except Exception as exc:  # noqa: BLE001 — affiché à l'utilisateur, jamais avalé
                logger.exception("échec %s", name)
                self._log(f"{name} : ERREUR {exc}")
            finally:
                self._log_queue.put("__done__")

        self._worker = threading.Thread(target=wrapped, name=f"docia-{name}", daemon=True)
        self._set_busy(True)
        self._worker.start()

    def _set_busy(self, busy: bool) -> None:
        state = "disabled" if busy else "normal"
        for b in (self.import_button, self.plan_button, self.run_button):
            b.configure(state=state)
        self.stop_button.configure(state="normal" if busy else "disabled")

    def _log(self, message: str) -> None:
        self._log_queue.put(message)

    def _poll(self) -> None:
        finished = False
        while True:
            try:
                msg = self._log_queue.get_nowait()
            except queue.Empty:
                break
            if msg == "__done__":
                finished = True
                continue
            self.log_box.configure(state="normal")
            self.log_box.insert("end", msg + "\n")
            if int(self.log_box.index("end-1c").split(".")[0]) > _MAX_LOG_LINES:
                self.log_box.delete("1.0", "200.0")
            self.log_box.see("end")
            self.log_box.configure(state="disabled")
        if finished:
            self._set_busy(False)
            self._refresh_status()
        self.root.after(_POLL_MS, self._poll)

    def _refresh_status(self) -> None:
        db_path = Path(self.db_var.get().strip() or self.config.db_path)
        if not db_path.exists():
            self.status_label.configure(text=f"base absente : {db_path}")
            return
        from docia.db import Database

        try:
            with Database(db_path) as db:
                counts, classes = db.counts(), db.classification_summary()
                rows = result_rows(db.latest_analyses())
        except Exception as exc:  # noqa: BLE001
            self.status_label.configure(text=f"base illisible : {exc}")
            return
        self.status_label.configure(text="\n".join(status_lines(counts, classes)))
        for w in self.results.winfo_children():
            w.destroy()
        header = ("Fichier", "Sécu", "RGPD", "Finance", "Juridique", "Résumé / raison")
        for col, text in enumerate(header):
            self.ctk.CTkLabel(self.results, text=text, font=self.ctk.CTkFont(weight="bold")).grid(
                row=0, column=col, sticky="w", padx=6
            )
        for i, row in enumerate(rows, 1):
            for col, text in enumerate(row):
                self.ctk.CTkLabel(self.results, text=text, anchor="w").grid(
                    row=i, column=col, sticky="w", padx=6
                )

    def run(self) -> None:
        self.root.mainloop()


def launch(config_path: Path | None = None) -> None:
    """Point d'entrée GUI (`python -m docia`, `docia gui`)."""
    DociaGUI(config_path).run()
