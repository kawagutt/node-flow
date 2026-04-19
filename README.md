# NodeFlow

**NodeFlow v1.5** — task-oriented **dispatcher** over external CLIs and HTTP APIs (Part V of the [specification](doc/nodeflow_spec.md)).

## Overview

NodeFlow wires **routing**, **external execution**, and **summarization** into reusable **graphs**. The public model is strict:

- **Taxonomy (under `BaseNode`)**: `PipeNode` and `ActionNode` only.
- **Implementation kinds (under `ActionNode`)**: `PythonActionNode`, `CliActionNode`, `ApiActionNode`.
- **Role** (`route_by_task_type`, `summarize_result`, `exec`, …) is a **class attribute**, not an inheritance axis.
- **Runner** stays minimal: it only runs `execute` in graph order; **no routing, provider selection, or role interpretation** inside the Runner.

Runtime primitives (`BaseNode.execute`, limits, revision stubs, `ExecutionContext`) live in **`nodeflow/core/`**. Dispatcher nodes live in **`nodeflow/nodes/`** (`base/`, `action/<role>/`, `pipe/`).

## Install

```bash
pip install -e .
```

Or with [uv](https://github.com/astral-sh/uv):

```bash
uv sync --extra dev
```

## Quick start

From the repository root (pipeline path may be workspace-relative or cwd-relative):

```bash
nodeflow examples/pipelines/hello.yaml -w examples -i task_type=review
```

## Exec and API failure semantics

- **External call returned a response** (HTTP error, error JSON, logical failure): represent it inside the normal output, e.g. port `execution_result` with `ok: false` and details in `stderr` / `raw_response` — same key shape as success (Part V §9).
- **Precondition not met before a meaningful request** (e.g. missing required API key env var): raise or let `BaseNode` surface **fatal** + `read_error()` — this is a node/configuration error, not a domain `execution_result`.

See `ApiActionNode` docstring in code for the same contract.

## SerialPipeNode (`compose`) and errors

The root graph built by the loader uses `SerialPipeNode`. Its `read_error()` returns the **first** child error only (not a list of all children). This is intentional and simpler than the old multi-error aggregation; for full child diagnostics, read each child node’s status and error.

## Workspace

The CLI `-w` / `--workspace` option sets the working directory (not necessarily a folder named `workspace`). Paths in YAML `params` are resolved relative to this directory when applicable.

## YAML (v1.5)

- `version` must be **`"1.5"`**.
- The loader always builds a root **`compose`** graph (`SerialPipeNode`) over `graph.nodes` and `graph.final`.
- **`type`** values are registry keys, for example: `compose` (internal root only), `python_route_by_task_type`, `python_summarize_result`, `codex_exec`, `claude_code_exec`, `kimi_exec`, `qwen_exec`, `review_dispatch`, `implement_dispatch`.

Example fragment:

```yaml
version: "1.5"
graph:
  nodes:
    - id: route
      type: python_route_by_task_type
      inputs:
        task_type: ${inputs.task_type}
      params: {}
  final: route
```

## API keys (optional)

- **Kimi (Moonshot)**: `MOONSHOT_API_KEY` for `kimi_exec`.
- **Qwen (DashScope compatible mode)**: `DASHSCOPE_API_KEY` for `qwen_exec`.

## Layout

```
nodeflow/
├── core/           # BaseNode.execute template, Runner, registry
├── nodes/          # PipeNode / ActionNode taxonomy and built-ins
│   ├── base/
│   ├── action/
│   └── pipe/
└── execution/      # YAML load + run entrypoints
```

## Custom nodes

Register classes on `nodeflow.core.registry.registry` with `register("your_type", YourClass)` and use `type: your_type` in YAML. Custom code typically subclasses `BaseNode` or `ActionNode` / `PythonActionNode` and lives in your workspace, not in this package.

## Upgrading from v1.4

- **CLI:** `nodeflow run …` is no longer used; pass the pipeline YAML as the first argument (see [CHANGELOG.md](CHANGELOG.md)).

## License

MIT
