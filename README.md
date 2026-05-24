# NodeFlow

**NodeFlow** — task-oriented nodes over external CLIs and HTTP APIs (see [specification](doc/nodeflow_spec.md)). NodeFlow uses **JSON PipeSpec** as the public pipeline format with a source-based dumb **Runner**, **PipeNode** composite wiring, **dict-only** port payloads, and **Common Output**. Legacy YAML / GraphSpec / RunnerFrame execution paths are removed.

## Overview

NodeFlow wires **routing**, **external execution**, and **summarization** into reusable **graphs**. The public model is strict:

- **Taxonomy (under `BaseNode`)**: `PipeNode` and `ActionNode` only.
- **Implementation kinds (under `ActionNode`)**: `PythonActionNode`, `CliActionNode`, `ApiActionNode`.
- **Role** (`route_by_task_type`, `summarize_result`, `exec`, …) is a **class attribute**, not an inheritance axis.
- **Runner** stays minimal: it only runs `execute` in graph order; **no routing, provider selection, or role interpretation** inside the Runner.

Runtime primitives (`BaseNode.execute`, limits, `_runtime` / `_usage`, `ExecutionContext`) and **taxonomy** (`PipeNode`, `ActionNode`, …) live in **`nodeflow/core/`**. Reusable building-block nodes live in **`nodeflow/nodes/`** under **role/purpose** folders (`routing/`, `summarize/`, `exec/`). Workflow helpers and ActionNodes live in **`nodeflow/workflows/`**.

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

- **External call returned a response** (HTTP error, error JSON, logical failure): represent it inside the normal output, e.g. port `execution_output` with `ok: false` and details in `stderr` / `raw_output` — same key shape as success (Common Output, spec §8).
- **Precondition not met before a meaningful request** (e.g. missing required API key env var): raise or let `BaseNode` surface **fatal** + `read_error()` — this is a node/configuration error, not a domain `execution_output` mishandling.

See `ApiActionNode` docstring in code for the same contract.

## Pipes and the Runner

**`load_pipeline()` / YAML `graph` + `final` are removed.** They raise `NotImplementedError` intentionally. Executable wiring is **`SourceRef`** + **`PipeSpec`** validated in core, stepped by **`Runner`** (occupancy scheduling, no frame stack). **`PipeNode`** implementations own internal execution (`pipe_spec()` / delegated children). See core tests under `tests/core/` for the contract skeleton.

## Workspace

The CLI `-w` / `--workspace` option sets the workspace directory (not necessarily a folder named `workspace`). It is reserved for workspace-relative resolution (e.g. `load_pipeline(workspace, path)` for `*.json` PipeSpec files) and workspace-relative params where applicable.

For CLI exec nodes (`codex_exec`, `claude_code_exec`), subprocess `cwd` follows this order:
- `params.cwd` (relative paths are resolved against workspace when available)
- otherwise workspace directory (`_workspace_dir`) when provided by top-level execution
- otherwise process default cwd

## `_usage` visibility

`_usage` is a reserved **runtime-internal accounting channel** consumed by the
execution template (`BaseNode._apply_usage`). It is not part of domain output and is
not exposed on final node outputs by default.

## Public pipelines

- **YAML graph samples** under `examples/pipelines/` were removed; public samples are **JSON PipeSpec**.
- **Built-in registry types** (for programmatic / future JSON use) include routing, summarize, and exec nodes. **`workflows.development_flow.*`** composite YAML types are **not** registered anymore; **`development_flow`** **ActionNodes** remain under [`nodeflow/workflows/development_flow/`](nodeflow/workflows/development_flow/) for reuse inside new PipeSpecs.

## Development flow (helpers)

Development flow tooling is intentionally split:

- **`nodeflow/nodes/`**: building blocks (`exec`, `routing`, …).
- **`nodeflow/workflows/development_flow/`**: **ActionNodes** (checkpoint, workspace prep, summaries, review parsing, etc.). The old **`workflows.development_flow.*`** **PipeNode** composites are dropped.
- **`nodeflow/workflows/`** ships workflow building utilities and ActionNodes; workflow-specific `PipeNode` subclasses are not part of the public model.

Field semantics for workspace / checkpoints / summaries are **`doc/nodeflow_spec.md`** (see also code and tests under `tests/workflows/`). `.nodeflow/` should usually be git-ignored.

**`nodeflow.core.loader.load_pipeline()`** confirms removal of YAML v1.5 by raising `NotImplementedError`.

## Dev-process (recommended CLI)

For the **dev-process** orchestration flow (spec → implement → review → merge), use the thin wrapper CLI — no manual checkpoint handling. **Recommended v1 entry point.**

```bash
nodeflow-dev-process --repo-root /path/to/target-repo start --task-prompt '...'
nodeflow-dev-process --repo-root /path/to/target-repo status
nodeflow-dev-process --repo-root /path/to/target-repo approve-spec
nodeflow-dev-process --repo-root /path/to/target-repo approve-final
nodeflow-dev-process --repo-root /path/to/target-repo merge
```

Merge policy on `start`: `record_only` (audit) or `git_merge_branch` (local merge). Docs: [`doc/dev_process_p7_wrapper.md`](doc/dev_process_p7_wrapper.md), [`doc/dev_process.md`](doc/dev_process.md), [`doc/dev_process_smoke_log.md`](doc/dev_process_smoke_log.md) (recorded real Codex runs). Manual PipeSpec path: [`doc/dev_process_real_codex_dry_run.md`](doc/dev_process_real_codex_dry_run.md).

## API keys (optional)

- **Kimi (Moonshot)**: `MOONSHOT_API_KEY` for `kimi_exec`.
- **Qwen (DashScope compatible mode)**: `DASHSCOPE_API_KEY` for `qwen_exec`.

## Layout

Each **descendant** directory under `nodeflow/nodes/` and `nodeflow/workflows/` (excluding the
package roots themselves and `__pycache__`) carries a `node_<dirname>.py` module. For container-only
trees (for example ``development_flow/``), that file may be a **layout marker** with minimal exports;
stage subpackages hold the concrete ActionNodes. See `tests/test_node_layout.py`.

Built-ins for **`nodeflow.nodes.*`** are registered in [`nodeflow/builtins.py`](nodeflow/builtins.py)
(single place). The **`nodeflow`** package import loads it; ``nodeflow/nodes/__init__.py`` does **not**
touch the registry.

```
nodeflow/
├── builtins.py          # built-in registry (nodes + packaged workflows)
├── cli.py
├── core/
│   ├── base_node.py
│   ├── node_kinds/       # PipeNode, ActionNode, Python/Cli/Api kinds
│   ├── runner.py
│   ├── registry.py
│   ├── loader.py         # JSON PipeSpec → in-memory PipeSpec
│   ├── pipe_spec.py
│   ├── pipe_runtime.py
│   ├── config.py         # workspace YAML (not PipeSpec)
│   └── run.py            # stub / not a YAML kick entrypoint
├── nodes/
│   ├── hello_demo.py
│   ├── routing/          # e.g. node_routing.py
│   ├── summarize/
│   ├── exec/             # e.g. node_exec.py + per-provider modules
│   └── git/collect_diff/
└── workflows/
    ├── development_flow/   # ActionNodes + helpers; node_development_flow.py
    ├── review_with_claude/
    └── implement_with_codex/
```

## Custom nodes

Register classes on `nodeflow.core.registry.registry` with `register("your_type", YourClass)` for use from **PipeSpec** / programmatic execution. Custom code typically subclasses `BaseNode` or `ActionNode` / `PythonActionNode` and lives in your workspace, not in this package.

## Upgrading from legacy YAML

- **CLI / file loading:** The old **YAML `graph` + `final`** pipeline is gone. Use **`nodeflow.core.loader.load_pipeline(workspace, "relative/or/abs/path/to/pipe.json")`** (JSON only) or construct a **`PipeNode`** and call **`execute(...)`** from Python. The `nodeflow` console script still wires to the removed `load_and_kick_pipeline` helper and always raises, so treat it as a compatibility stub (see [CHANGELOG.md](CHANGELOG.md)).

## License

MIT
