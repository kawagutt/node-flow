# Dev Process — P8 Named Pipe + Stage Input

P8 adds a unified NodeFlow entry point and stage-level interactive input collection. It does **not** grow the dev-process orchestration surface.

Related:

- [dev_process.md](./dev_process.md) — architecture
- [dev_process_p7_wrapper.md](./dev_process_p7_wrapper.md) — P7 `nodeflow-dev-process`

## P7 vs P8

| Phase | Entry | Scope |
|-------|-------|-------|
| P7 | `nodeflow-dev-process` | Thin wrapper around `run_flow` |
| P8 | `nodeflow --pipe dev-process` | Same wrapper via named pipe; stage inputs collected by stages |

## Design rule

> dev-process CLI must not grow stage-specific business arguments. Stage-specific inputs belong to the corresponding stage node.

dev-process controls flow only (start / resume / merge, checkpoint discovery, workspace / merge policy). Spec materials, revision comments, and rework feedback are collected by the corresponding stage.

**Do not add** to dev-process CLI: `--material`, `--comment`, `--reference-paths`, `--spec-note`, `--review-note`, etc.

## Entry points

```bash
# P8 (recommended)
nodeflow --pipe dev-process --repo-root /path/to/repo start
nodeflow --pipe dev-process status
nodeflow --pipe dev-process approve-spec
nodeflow --pipe dev-process revise-spec
nodeflow --pipe dev-process rework
nodeflow --pipe dev-process approve-final
nodeflow --pipe dev-process merge

# P7 (compatibility)
nodeflow-dev-process --repo-root /path/to/repo start

# Low-level JSON PipeSpec (unchanged)
nodeflow -w $NF examples/pipes/dev_process/dev_process.json -i action=start ...
```

`--repo-root` and `--repo_root` are both accepted. Default repo root is `.` (resolved to git toplevel).

## dev-process CLI arguments

### Global

| Flag | Default | Notes |
|------|---------|-------|
| `--repo-root` / `--repo_root` | `.` | Target git repository |
| `--json` | off | Raw JSON output |
| `--non-interactive` | off | Fail if stage input is missing (see below) |
| `--run-id` | — | Scope checkpoint discovery |
| `--checkpoint` | — | Explicit checkpoint path |

### `start` only

| Flag | Required | Notes |
|------|----------|-------|
| `--task-prompt` | no | Initial **provided** input for spec_plan stage (not a dev-process business arg) |
| `--workspace-strategy` | no | `current_repo` / `git_worktree` |
| `--merge-policy` | no | `record_only` / `git_merge_branch` |
| `--exec-worker-kind` | no | `codex` |
| `--exec-argv` | no | JSON array of strings |
| `--run-id` | no | Explicit run id |

### Resume commands

`approve-spec`, `revise-spec`, `rework`, `approve-final`, `merge`, `status` — action + global flags only.

## Interactive mode

```text
default (CLI):     interactive = True
--non-interactive: interactive = False
```

CI should always pass `--non-interactive`.

### Non-interactive required-input rules

| Stage | Missing required input | Behavior |
|-------|------------------------|----------|
| **spec_plan** | `task_prompt` | **Fail** — supply `--task-prompt` on `start` or pre-write `spec_plan/input.json` |
| **revise_spec** | `revision_comment` | **Fail** — pre-write `revision/input.json`, or pass legacy PipeSpec/node `task_prompt` (mapped to revision comment) |
| **rework** | explicit `rework_comment` | **Allowed fallback** — uses `"rework requested"`; prior **review findings** from artifacts are always included in `rework_context` |

Rework is the exception: review output is the primary human input. An explicit rework comment is still collected interactively or via `rework/input.json`; when neither is present in non-interactive mode (e.g. programmatic `rework_implementation` via PipeSpec/node), the fallback comment keeps backward compatibility with P7.

**revise_spec** also accepts legacy `task_prompt` on the flow port (mapped to `revision_comment`) for PipeSpec/node callers only — not as a dev-process CLI flag.

## `--task-prompt` semantics

`--task-prompt` on `start` is **not** a dev-process orchestration argument. It is an optional initial value passed to the **spec_plan** stage as `provided` input. If omitted, spec_plan prompts interactively (when interactive mode is on).

`checkpoint.task_prompt` remains for run identity / implement reuse. **`spec_plan/input.json`** is the canonical spec_plan stage input.

## Stage inputs

### spec_plan (`start` / after `revise-spec`)

| Field | Required |
|-------|----------|
| Task prompt | yes |
| Reference material paths | no |
| Additional constraints or notes | no |

### revise-spec

| Field | Required |
|-------|----------|
| Revision comment | yes |
| Additional reference paths | no |

### rework

| Field | Required |
|-------|----------|
| Implementation feedback | yes |

Collection order: `provided` → existing `{stage}/input.json` → interactive prompt → fail or stage-specific fallback (see table above).

## Input artifacts

```text
.nodeflow/runs/<run>/spec_plan/input.json
.nodeflow/runs/<run>/spec_plan/reference_materials.json
.nodeflow/runs/<run>/revision/input.json
.nodeflow/runs/<run>/rework/input.json
```

### `input.json` schema

```json
{
  "schema_version": "dev_process.stage_input.v1",
  "stage": "spec_plan",
  "collected_at": "2026-05-24T12:00:00+00:00",
  "inputs": {
    "task_prompt": "...",
    "reference_paths": [],
    "notes": ""
  }
}
```

Checkpoint `stages.<stage>` stores **paths only** (`input_artifact`, `reference_materials_artifact`), not inline input bodies.

### Reference materials

Text files (`.md`, `.txt`, `.json`, `.yaml`, `.yml`, `.py`, etc.) are read with a per-file excerpt cap (~20k chars). Binary files are skipped (path recorded only).

## Non-interactive CI example

```bash
nodeflow --pipe dev-process --repo-root "$REPO" --non-interactive start \
  --task-prompt 'Add status command' \
  --workspace-strategy current_repo
```

For `revise-spec`, place `revision/input.json` under the run artifact root before invoking, or run interactively.

For `rework`, optional `rework/input.json` with `rework_comment`; if omitted in non-interactive mode, the fallback `"rework requested"` applies and review findings are still attached.
