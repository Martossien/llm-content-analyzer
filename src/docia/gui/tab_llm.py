"""Onglet Serveur & performances (administrateur) : réglages du serveur LLM et des blocs,
test de connexion, banc de vitesse (fichiers/heure, JSON valides)."""

from __future__ import annotations

from typing import Any

from docia.config import Config
from docia.gui.helpers import parse_int
from docia.gui.widgets import ReadOnlyText

TRANSPORTS = ("vllm", "openwebui")
TOKENIZERS = ("approx", "mistral", "openai")
EFFORTS = ("low", "medium", "xhigh")


class LLMTab:
    def __init__(self, app: Any, parent: Any) -> None:
        self.app = app
        self.parent = parent
        self.ctk = app.ctk

    def build(self) -> None:
        ctk, p = self.ctk, self.parent
        c: Config = self.app.config
        ctk.CTkLabel(
            p,
            text="Réglages avancés — réservés à l'administrateur. « Enregistrer » écrit docia.toml.",
            anchor="w",
        ).pack(fill="x", padx=10, pady=(8, 0))
        grid = ctk.CTkFrame(p)
        grid.pack(fill="x", padx=10, pady=(6, 6))
        self.transport_var = ctk.StringVar(value=c.llm.transport)
        self.url_var = ctk.StringVar(value=c.llm.base_url)
        self.key_var = ctk.StringVar(value=c.llm.api_key)
        self.model_var = ctk.StringVar(value=c.llm.model)
        self.inflight_var = ctk.StringVar(value=str(c.llm.max_in_flight))
        self.timeout_var = ctk.StringVar(value=str(c.llm.timeout_s))
        self.context_var = ctk.StringVar(value=str(c.llm.max_context_tokens))
        self.thinking_var = ctk.BooleanVar(value=c.llm.enable_thinking)
        self.budget_var = ctk.StringVar(value=str(c.llm.thinking_budget_tokens))
        self.block_tokens_var = ctk.StringVar(value=str(c.blocks.block_tokens))
        self.tokenizer_var = ctk.StringVar(value=c.blocks.tokenizer_engine)
        self.batch_var = ctk.StringVar(value=str(c.blocks.batch_files))

        def cell(r: int, col: int, label: str, widget: Any) -> None:
            ctk.CTkLabel(grid, text=label).grid(
                row=r, column=col * 2, sticky="w", padx=(10, 4), pady=4
            )
            widget.grid(row=r, column=col * 2 + 1, sticky="w", padx=(0, 12), pady=4)

        cell(
            0,
            0,
            "Transport",
            ctk.CTkOptionMenu(
                grid, variable=self.transport_var, values=list(TRANSPORTS), width=120
            ),
        )
        cell(0, 1, "URL de base", ctk.CTkEntry(grid, textvariable=self.url_var, width=360))
        cell(1, 0, "Clé API", ctk.CTkEntry(grid, textvariable=self.key_var, width=220, show="•"))
        cell(1, 1, "Modèle", ctk.CTkEntry(grid, textvariable=self.model_var, width=200))
        cell(2, 0, "Requêtes en vol", ctk.CTkEntry(grid, textvariable=self.inflight_var, width=70))
        cell(2, 1, "Timeout (s)", ctk.CTkEntry(grid, textvariable=self.timeout_var, width=80))
        cell(
            3,
            0,
            "Contexte du modèle (tokens)",
            ctk.CTkEntry(grid, textvariable=self.context_var, width=100),
        )
        cell(
            3,
            1,
            "Tokens par bloc",
            ctk.CTkEntry(grid, textvariable=self.block_tokens_var, width=90),
        )
        cell(
            4,
            0,
            "Raisonnement (thinking)",
            ctk.CTkCheckBox(grid, text="activé — qualité", variable=self.thinking_var),
        )
        cell(
            4,
            1,
            "Budget de raisonnement (tokens, imposé)",
            ctk.CTkEntry(grid, textvariable=self.budget_var, width=90),
        )
        self.smbeagle_var = ctk.StringVar(value=c.scan.smbeagle_path)
        self.preserve_var = ctk.BooleanVar(value=c.scan.preserve_access_time)
        cell(
            7,
            0,
            "Scanner SMBeagle (chemin)",
            ctk.CTkEntry(
                grid,
                textvariable=self.smbeagle_var,
                width=360,
                placeholder_text="vide = à côté de Docia.exe, puis PATH",
            ),
        )
        cell(
            7,
            1,
            "Dates d'accès",
            ctk.CTkCheckBox(grid, text="préservées pendant le scan", variable=self.preserve_var),
        )
        self.effort_var = ctk.StringVar(value=c.llm.reasoning_effort or "xhigh")
        cell(
            6,
            0,
            "Effort de raisonnement",
            ctk.CTkOptionMenu(grid, variable=self.effort_var, values=list(EFFORTS), width=110),
        )
        cell(
            5,
            0,
            "Comptage des tokens",
            ctk.CTkOptionMenu(
                grid, variable=self.tokenizer_var, values=list(TOKENIZERS), width=110
            ),
        )
        cell(
            5,
            1,
            "Fichiers par lot DocFuse",
            ctk.CTkEntry(grid, textvariable=self.batch_var, width=80),
        )

        btns = ctk.CTkFrame(p, fg_color="transparent")
        btns.pack(fill="x", padx=10, pady=(0, 6))
        self.test_button = ctk.CTkButton(btns, text="Tester la connexion", command=self._test)
        self.test_button.pack(side="left", padx=(0, 8))
        ctk.CTkButton(btns, text="Enregistrer les réglages", command=self.app.save_config).pack(
            side="left", padx=(0, 16)
        )
        ctk.CTkLabel(btns, text="Bench : blocs").pack(side="left")
        self.bench_blocks_var = ctk.StringVar(value="6")
        ctk.CTkEntry(btns, textvariable=self.bench_blocks_var, width=50).pack(
            side="left", padx=(4, 8)
        )
        ctk.CTkLabel(btns, text="tokens/bloc").pack(side="left")
        self.bench_tokens_var = ctk.StringVar(value="8000")
        ctk.CTkEntry(btns, textvariable=self.bench_tokens_var, width=70).pack(
            side="left", padx=(4, 8)
        )
        self.bench_button = ctk.CTkButton(
            btns, text="Mesurer la vitesse de la LLM", command=self._bench, fg_color="#0e7490"
        )
        self.bench_button.pack(side="left")

        self.output = ReadOnlyText(ctk, p, height=260)
        self.output.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        self.app.on_busy(self._busy)

    def _busy(self, busy: bool) -> None:
        state = "disabled" if busy else "normal"
        self.test_button.configure(state=state)
        self.bench_button.configure(state=state)

    def apply_to_config(self, cfg: Config) -> None:
        """Recopie les champs de l'onglet dans la config (appelé par `collect_config`)."""
        cfg.llm.transport = self.transport_var.get()
        cfg.llm.base_url = self.url_var.get().strip()
        cfg.llm.api_key = self.key_var.get().strip()
        cfg.llm.model = self.model_var.get().strip() or cfg.llm.model
        cfg.llm.max_in_flight = parse_int(self.inflight_var.get(), cfg.llm.max_in_flight)
        cfg.llm.timeout_s = parse_int(self.timeout_var.get(), cfg.llm.timeout_s, minimum=10)
        cfg.llm.max_context_tokens = parse_int(
            self.context_var.get(), cfg.llm.max_context_tokens, minimum=1000
        )
        cfg.llm.enable_thinking = bool(self.thinking_var.get())
        cfg.llm.thinking_budget_tokens = parse_int(
            self.budget_var.get(), cfg.llm.thinking_budget_tokens, minimum=0
        )
        cfg.llm.reasoning_effort = self.effort_var.get()
        cfg.scan.smbeagle_path = self.smbeagle_var.get().strip()
        cfg.scan.preserve_access_time = bool(self.preserve_var.get())
        cfg.blocks.block_tokens = parse_int(
            self.block_tokens_var.get(), cfg.blocks.block_tokens, minimum=1000
        )
        cfg.blocks.tokenizer_engine = self.tokenizer_var.get()
        cfg.blocks.batch_files = parse_int(self.batch_var.get(), cfg.blocks.batch_files)

    def _test(self) -> None:
        app = self.app
        cfg = app.collect_config()

        def work() -> None:
            import asyncio

            from docia.llm.client import LLMClient

            async def probe() -> bool:
                async with LLMClient(cfg.llm, "") as client:
                    return await client.health()

            ok = asyncio.run(probe())
            msg = f"connexion {cfg.llm.base_url} ({cfg.llm.transport}) : {'OK' if ok else 'ÉCHEC'}"
            app.log(msg)
            self.output.set(msg)

        app.run_in_thread(work, "test de connexion")

    def _bench(self) -> None:
        app = self.app
        cfg = app.collect_config()
        if cfg.validate():
            return
        blocks = parse_int(self.bench_blocks_var.get(), 6)
        tokens = parse_int(self.bench_tokens_var.get(), 8000, minimum=500)

        def work() -> None:
            from docia.bench import run_bench

            rep = run_bench(cfg, blocks=blocks, block_tokens=tokens, progress=app.log)
            lines = rep.as_lines()
            self.output.set("\n".join(lines))
            for line in lines[:3]:
                app.log(line)

        app.run_in_thread(work, "bench")
