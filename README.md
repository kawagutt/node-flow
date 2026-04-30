# NodeFlow

**NodeFlow v1.5** — task-oriented **dispatcher** over external CLIs and HTTP APIs (Part V of the [specification](doc/nodeflow_spec.md)).

## Overview

NodeFlow wires **routing**, **external execution**, and **summarization** into reusable **graphs**. The public model is strict:

- **Taxonomy (under `BaseNode`)**: `PipeNode` and `ActionNode` only.
- **Implementation kinds (under `ActionNode`)**: `PythonActionNode`, `CliActionNode`, `ApiActionNode`.
- **Role** (`route_by_task_type`, `summarize_result`, `exec`, …) is a **class attribute**, not an inheritance axis.
- **Runner** stays minimal: it only runs `execute` in graph order; **no routing, provider selection, or role interpretation** inside the Runner.

Runtime primitives (`BaseNode.execute`, limits, `_runtime` / `_usage`, `ExecutionContext`) and **taxonomy** (`PipeNode`, `ActionNode`, …) live in **`nodeflow/core/`**. Concrete dispatcher nodes live in **`nodeflow/nodes/`** under **role/purpose** folders (`routing/`, `summarize/`, `exec/`, `dispatch/`).

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

## Root graph and `PipeNode`

The YAML `graph` section (`graph.nodes`, `graph.final`) is loaded by **`nodeflow.core.loader.load_pipeline`**, which **assembles a root `PipeNode` in code** (this is **not** a public YAML `type` like `type: pipe`). The loader holds a frozen **`GraphSpec`** (child nodes, order, bindings, params, `final`) on that root; default **`run()`** uses **`RunnerFrame`** to step the child graph and returns the **`final` node’s domain output** (that node’s port name → dict payload, with child-level **`_runtime` / `_usage` stripped** via `domain_ports_from_observation`). The root’s own **`execute()`** then attaches the composite **`_runtime`** (port revisions) for the pipe—so the observable result is “final domain ports as the root’s domain ports,” plus the root’s **`_runtime`**, not a raw passthrough of the child’s full observation dict.

`read_error()` on that root `PipeNode` returns the **first** child error only when present; for full diagnostics, inspect child nodes.

Top-level execution (`load_and_kick_pipeline` / CLI) is **fail-fast**: if the root
status ends as anything other than `done` (`fatal` / `limit`), execution raises an error.

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

## YAML (v1.5)

- `version` must be **`"1.5"`**.
- **`graph.nodes`**: each entry has `id`, `type`, `inputs`, `params`. **`type`** values are **registry keys** for concrete nodes (for example: `python_route_by_task_type`, `python_summarize_result`, `codex_exec`, `claude_code_exec`, `kimi_exec`, `qwen_exec`, `review_with_claude`, `implement_with_codex`, `spec_plan_pipe`, `implement_pipe`, `review_pipe`, `development_flow_pipe`). There is **no** built-in YAML `type` for the root wrapper; the loader builds the root **`PipeNode`** internally. **CLI exec nodes** (`codex_exec`, `claude_code_exec`) require a **non-empty `params.argv`** list (subprocess precondition). For **`review_with_claude`** / **`implement_with_codex`**, put **nested exec params on that graph node’s `params`** (the same `params` object the loader passes into the composite), e.g. `claude_code_exec: { argv: [...] }` or `codex_exec: { argv: [...] }`—there are **no implicit default argv** in those pipes. These fixed-provider pipes expect both `task_prompt` and `task_type` inputs; `task_type` is forwarded to exec result context. See **`examples/pipelines/review_with_claude.yaml`** and **`examples/pipelines/implement_with_codex.yaml`**.
- **`graph.final`**: id of the terminal node whose output is exposed as the pipeline result.
- Nodes are executed in declaration order; `${node.port}` references are allowed only for nodes declared earlier (forward references are rejected).

Minimal example (routing only):

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

Fixed provider pipe examples with nested `argv` are under **`examples/pipelines/`** (`review_with_claude.yaml`, `implement_with_codex.yaml`).

## Development flow nodes and examples (P0/P2)

Development flow is intentionally split into two layers:

- **Implementation (`nodeflow/nodes/`)**: built-in reusable node types and registry keys, including top-level `development_flow_pipe` and stage pipes.
- **Usage examples (`examples/`)**: runnable samples/templates that instantiate those node types; no orchestration logic lives in example YAML files.

Naming and concrete file examples live in **[`nodeflow/nodes/development_flow/README.md`](nodeflow/nodes/development_flow/README.md)**.

For `development_flow_pipe`, `repo_root` means the target project repository (not the node-flow repository). The flow distinguishes:
- `source_repo_root`: target repository passed as `repo_root`
- `workspace_root`: execution root for implementation/review. Currently only `current_repo` is supported; future versions may add git worktree support.
- `artifact_root`: preferred per-run root under `.nodeflow/runs/<run_dir_name>/` for stage checkpoints (`spec_plan/`, `implement/`, `review/`) and `summary/` when the orchestrator passes `artifact_root` into stage pipes; top-level flow JSON may still use `flow_checkpoint.checkpoint_dir` (see `nodeflow/nodes/development_flow/README.md`).

`.nodeflow/` should usually be git-ignored. Fresh `prepare_workspace` / `start` clean checks skip paths under `.nodeflow/` by default so generated metadata does not block runs.

`nodeflow.core.loader.load_node_pipeline()` is a raw loader for version + top-level shape checks.
Use `nodeflow.core.loader.load_pipeline()` for executable graph validation.

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
│   ├── loader.py         # pipeline YAML parse + root PipeNode assembly
│   ├── config.py         # YAML IO helpers (load_yaml, deep merge)
│   └── run.py            # load_and_kick_pipeline entry
├── nodes/                # concrete nodes only (by role / purpose)
│   ├── routing/
│   ├── summarize/
│   ├── exec/
│   ├── dispatch/
│   └── development_flow/          # stage pipes; see development_flow/README.md
```

## Custom nodes

Register classes on `nodeflow.core.registry.registry` with `register("your_type", YourClass)` and use `type: your_type` in YAML. Custom code typically subclasses `BaseNode` or `ActionNode` / `PythonActionNode` and lives in your workspace, not in this package.

## Upgrading from v1.4

- **CLI:** `nodeflow run …` is no longer used; pass the pipeline YAML as the first argument (see [CHANGELOG.md](CHANGELOG.md)).

## License

MIT
