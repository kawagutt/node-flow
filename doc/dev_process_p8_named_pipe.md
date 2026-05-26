# Dev Process — CLI + Stage Input

Related: [dev_process.md](./dev_process.md) — architecture.

## Design rule

> dev-process CLI must not grow stage-specific business arguments. Stage-specific inputs belong to the corresponding stage node.

dev-process controls flow only (start / resume / merge, checkpoint discovery, workspace / merge policy). Spec materials, revision comments, and rework feedback are collected by the corresponding stage.

**Do not add** to dev-process CLI: `--material`, `--comment`, `--reference-paths`, `--spec-note`, `--review-note`, etc.

## Entry point

Run from the target repository root. Happy path:

```bash
nodeflow --pipe dev-process start
nodeflow --pipe dev-process approve-spec
nodeflow --pipe dev-process continue-implementation
nodeflow --pipe dev-process approve-final
nodeflow --pipe dev-process merge
```

All commands:

```bash
nodeflow --pipe dev-process start
nodeflow --pipe dev-process status
nodeflow --pipe dev-process approve-spec
nodeflow --pipe dev-process continue-implementation
nodeflow --pipe dev-process request-spec-revision
nodeflow --pipe dev-process revise-spec
nodeflow --pipe dev-process revise-plan
nodeflow --pipe dev-process rework
nodeflow --pipe dev-process approve-final
nodeflow --pipe dev-process merge
```

`--repo-root` is optional; when omitted it defaults to `.` (resolved to git toplevel). Mainly useful for automation or running from outside the repository.

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
| `--task-prompt` | no | Initial **provided** input for **spec** stage (not a dev-process business arg) |
| `--workspace-strategy` | no | `current_repo` / `git_worktree` |
| `--merge-policy` | no | `record_only` / `git_merge_branch` |
| `--exec-worker-kind` | no | `codex` |
| `--exec-argv` | see note | JSON array of strings |
| `--exec-policy-path` | see note | Path to `exec_policy.json` |
| `--run-id` | no | Explicit run id |

> **Execution argv resolution**: When a node execution is attempted, argv is resolved in priority order:
>
> 1. `exec_policy.nodes.<node>.argv` (per-node, highest priority)
> 2. `--exec-argv` / `exec_policy.default_argv` (snapshot-level default)
> 3. `WORKER_DEFAULT_ARGV` (empty — no implicit Codex invocation)
>
> **Interactive mode** (default): If neither `--exec-argv` nor `--exec-policy-path` is provided, `start` prompts the user to select an execution mode (full-auto / suggest / custom argv).
>
> **Non-interactive mode** (`--non-interactive`): Requires `--exec-argv` or `--exec-policy-path`. Without them, the first node execution fails with a clear error. No implicit Codex argv is used.
>
> `--exec-argv` and `--exec-policy-path` **can be used together**: the policy defines per-node settings and constraints, while `--exec-argv` sets `snapshot.default_argv`. Per-node argv in the policy always takes precedence.
>
> **Note**: `--exec-policy-path` alone is sufficient only if the policy provides `default_argv` or per-node `argv` for the nodes that will execute. A policy with only `constraints` and no argv will cause the first node execution to fail.
>
> Examples:
> ```bash
> # Interactive: prompts for execution mode
> nodeflow --pipe dev-process start
>
> # Explicit argv (interactive or non-interactive)
> nodeflow --pipe dev-process start --exec-argv '["codex","exec","--sandbox","workspace-write"]'
>
> # Policy file with constraints and per-node config
> nodeflow --pipe dev-process start --exec-policy-path ./exec_policy.json
>
> # Both: policy for structure, exec-argv as fallback default
> nodeflow --pipe dev-process start \
>   --exec-policy-path ./exec_policy.json \
>   --exec-argv '["codex","exec","--sandbox","workspace-write"]'
>
> # Non-interactive (CI): explicit argv required
> nodeflow --pipe dev-process start --non-interactive \
>   --exec-argv '["codex","exec","--sandbox","workspace-write"]'
> ```

### Resume commands

`approve-spec`, `continue-implementation`, `request-spec-revision`, `revise-spec`, `revise-plan`, `rework`, `approve-final`, `merge`, `status` — action + global flags only.

Resume commands that accept an optional **`--comment`** (for `--non-interactive` or to skip prompts): `request-spec-revision`, `revise-spec`, `revise-plan`.

## Interactive mode

```text
default (CLI):     interactive = True
--non-interactive: interactive = False
```

CI should always pass `--non-interactive`.

### Non-interactive required-input rules

| Stage | Missing required input | Behavior |
|-------|------------------------|----------|
| **spec** | `task_prompt` | **Fail** — supply `--task-prompt` on `start` or pre-write `spec/input.json` |
| **revise_spec** | *(none — uses spec_review findings)* | **Allowed** — optional `--comment` adds human context |
| **request_spec_revision** | `revision_comment` | **Fail** — use `--comment` or pre-write `revision/input.json` |
| **revise_plan** | *(none — uses plan_review findings)* | **Allowed** — optional `--comment` adds human context |
| **rework** | explicit `rework_comment` | **Allowed fallback** — uses `"rework requested"`; prior **review findings** from artifacts are always included in `rework_context` |

Rework is the exception: review output is the primary human input. An explicit rework comment is still collected interactively or via `rework/input.json`; when neither is present in non-interactive mode, the fallback `"rework requested"` applies.

## `--task-prompt` semantics

`--task-prompt` on `start` is **not** a dev-process orchestration argument. It is an optional initial value passed to the **spec** stage as `provided` input. If omitted, spec prompts interactively (when interactive mode is on).

`checkpoint.task_prompt` remains for run identity / implementation reuse. **`spec/input.json`** is the canonical spec stage input.

## Stage inputs

### spec (`start` / after `revise-spec`)

| Field | Required |
|-------|----------|
| Task prompt | yes |
| Reference material paths | no |
| Additional constraints or notes | no |

### revise-spec / request-spec-revision

| Action | Primary input | Optional |
|--------|---------------|----------|
| **revise_spec** (from `awaiting_spec_revision`) | spec_review findings | human `--comment`, additional reference paths |
| **request_spec_revision** (from human gate) | human revision comment | spec_review findings (if any) |

Both reuse `spec/input.json`, `spec/reference_materials.json`, and the previous `spec/spec.md` in the write prompt.

| Field | Required |
|-------|----------|
| Revision comment (`request_spec_revision` only) | yes |
| Revision comment (`revise_spec`) | no |
| Additional reference paths | no |

### revise-plan

| Action | Primary input | Optional |
|--------|---------------|----------|
| **revise_plan** (from `awaiting_plan_revision`) | plan_review findings | human `--comment` |

Reuses the previous `plan/plan.md` in the write prompt. No human gate on plan.

| Field | Required |
|-------|----------|
| Revision comment | no |

### rework

| Field | Required |
|-------|----------|
| Implementation feedback | yes |

Collection order: `provided` → existing `{stage}/input.json` → interactive prompt → fail or stage-specific fallback (see table above).

## Input artifacts

```text
.nodeflow/runs/<run>/spec/input.json
.nodeflow/runs/<run>/spec/reference_materials.json
.nodeflow/runs/<run>/revision/input.json
.nodeflow/runs/<run>/rework/input.json
```

### `input.json` schema

```json
{
  "schema_version": "dev_process.stage_input.v1",
  "stage": "spec",
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
