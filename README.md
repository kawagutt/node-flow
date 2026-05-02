# NodeFlow

**NodeFlow v1.6 (in progress)** — task-oriented nodes over external CLIs and HTTP APIs (see [specification](doc/nodeflow_spec.md)). The YAML / GraphSpec / RunnerFrame loader path has been removed; public wiring is moving to **JSON PipeSpec** + a dumb **Runner**.

## Overview

NodeFlow wires **routing**, **external execution**, and **summarization** into reusable **graphs**. The public model is strict:

- **Taxonomy (under `BaseNode`)**: `PipeNode` and `ActionNode` only.
- **Implementation kinds (under `ActionNode`)**: `PythonActionNode`, `CliActionNode`, `ApiActionNode`.
- **Role** (`route_by_task_type`, `summarize_result`, `exec`, …) is a **class attribute**, not an inheritance axis.
- **Runner** stays minimal: it only runs `execute` in graph order; **no routing, provider selection, or role interpretation** inside the Runner.

Runtime primitives (`BaseNode.execute`, limits, `_runtime` / `_usage`, `ExecutionContext`) and **taxonomy** (`PipeNode`, `ActionNode`, …) live in **`nodeflow/core/`**. Reusable building-block nodes live in **`nodeflow/nodes/`** under **role/purpose** folders (`routing/`, `summarize/`, `exec/`). Packaged composite workflows (`development_flow`, fixed-provider pipes) live in **`nodeflow/workflows/`**.

## Install

```bash
pip install -e .
```

Or with [uv](https://github.com/astral-sh/uv):

```bash
uv sync --extra dev
```

## Quick start

Run the core test slice (no public YAML samples under `examples/pipelines/`):

```bash
pytest -q tests/core tests/test_public_contract.py tests/test_registry.py tests/test_e2e.py
```

## Exec and API failure semantics

- **External call returned a response** (HTTP error, error JSON, logical failure): represent it inside the normal output, e.g. port `execution_result` with `ok: false` and details in `stderr` / `raw_response` — same key shape as success (Part V §9).
- **Precondition not met before a meaningful request** (e.g. missing required API key env var): raise or let `BaseNode` surface **fatal** + `read_error()` — this is a node/configuration error, not a domain `execution_result`.

See `ApiActionNode` docstring in code for the same contract.

## Pipes and the Runner

**`load_pipeline()` / YAML `graph` + `final` are removed.** They raise `NotImplementedError` intentionally. Executable wiring is **`SourceRef`** + **`PipeSpec`** validated in core, stepped by **`Runner`** (occupancy scheduling, no frame stack). **`PipeNode`** implementations own internal execution (`pipe_spec()` / delegated children). See core tests under `tests/core/` for the contract skeleton.

## Workspace

The CLI `-w` / `--workspace` option sets the workspace directory (not necessarily a folder named `workspace`). It is used for pipeline file resolution and workspace-relative params where applicable.

For CLI exec nodes (`codex_exec`, `claude_code_exec`), subprocess `cwd` follows this order:
- `params.cwd` (relative paths are resolved against workspace when available)
- otherwise workspace directory (`_workspace_dir`) when provided by top-level execution
- otherwise process default cwd

## `_usage` visibility

`_usage` is a reserved **runtime-internal accounting channel** consumed by the
execution template (`BaseNode._apply_usage`). It is not part of domain output and is
not exposed on final node outputs by default.

## Public pipelines

- **YAML graph samples** under `examples/pipelines/` were removed; v1.6 uses **JSON PipeSpec** (loader still evolving).
- **Built-in registry types** (for programmatic / future JSON use) include routing, summarize, exec nodes, and fixed-provider pipes **`review_with_claude`** and **`implement_with_codex`**. **`workflows.development_flow.*`** composite YAML types are **not** registered anymore; **`development_flow`** **ActionNodes** remain under [`nodeflow/workflows/development_flow/`](nodeflow/workflows/development_flow/) for reuse inside new PipeSpecs.

## Development flow (helpers)

Development flow tooling is intentionally split:

- **`nodeflow/nodes/`**: building blocks (`exec`, `routing`, …).
- **`nodeflow/workflows/development_flow/`**: **ActionNodes** (checkpoint, workspace prep, summaries, review parsing, etc.). The old **`workflows.development_flow.*`** **PipeNode** composites are dropped until they are rebuilt on v1.6 PipeSpec.
- **`nodeflow/workflows/`** also ships **`review_with_claude`** and **`implement_with_codex`** PipeNodes (still registered).

Field semantics for workspace / checkpoints / summaries are summarized in **`nodeflow/workflows/development_flow/README.md`** (being updated alongside JSON PipeSpec). `.nodeflow/` should usually be git-ignored.

**`nodeflow.core.loader.load_pipeline()`** confirms removal of YAML v1.5 by raising `NotImplementedError`.

## API keys (optional)

- **Kimi (Moonshot)**: `MOONSHOT_API_KEY` for `kimi_exec`.
- **Qwen (DashScope compatible mode)**: `DASHSCOPE_API_KEY` for `qwen_exec`.

## Layout

```
nodeflow/
├── core/
│   ├── base_node.py      # execute template, _runtime / _usage
│   ├── node_kinds/       # PipeNode, ActionNode, implementation kinds
│   ├── runner.py
│   ├── registry.py
│   ├── loader.py         # legacy YAML entry points removed / migration guards
│   ├── config.py         # YAML IO helpers (load_yaml, deep merge)
│   └── run.py            # load_and_kick_pipeline entry
├── nodes/                # reusable building-block nodes (by role / purpose)
│   ├── routing/
│   ├── summarize/
│   └── exec/
└── workflows/            # packaged composite workflows (development_flow, fixed-provider pipes)
    ├── development_flow/ # see workflows/development_flow/README.md
    ├── review_with_claude/
    └── implement_with_codex/
```

## Custom nodes

Register classes on `nodeflow.core.registry.registry` with `register("your_type", YourClass)` for use from **PipeSpec** / programmatic execution. Custom code typically subclasses `BaseNode` or `ActionNode` / `PythonActionNode` and lives in your workspace, not in this package.

## Upgrading from v1.4

- **CLI:** `nodeflow run …` is no longer used; pass the pipeline YAML as the first argument (see [CHANGELOG.md](CHANGELOG.md)).

## License

MIT
