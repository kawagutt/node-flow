# Dev Process Flow

## Positioning

NodeFlow **dev-process** is independent from Hermes `skills/dev-process`.  
SOT: **checkpoint JSON**, **timeline.jsonl**, and **artifact files** under `.nodeflow/runs/<run_dir>/`.

`development_flow` ActionNodes are used only via **`reuse.py`** (single import boundary).  
Outbound registry keys are **`dev_process.*` only**.

## Goals

- Checkpoint/resume orchestration with explicit human gate states
- Codex exec worker (default); `params.exec_worker_kind` selects worker (`codex` in v1)
- Workspace strategies: `current_repo` (default) or `git_worktree` (`params.workspace_strategy`)
- Mechanical merge gate and review JSON contract
- Exec evidence under `artifact_root/evidence/`

## Non-goals

- No Hermes `jobs.yaml`
- No `.hermes/tasks`
- No `run-dp`
- No `reviewed.*` / `approved.*` state flags
- No Hermes profile names (`dp-strong`, etc.)
- No skill Markdown pipeline

## State machine (current)

```text
start (+ spec_plan) -> awaiting_spec_approval
  approve_spec -> implement + review -> awaiting_review_decision
    approve_final (merge_ready) -> awaiting_merge
      merge -> merged
    rework_implementation -> (re-run implement + review)
    reject_spec / reject_final -> failed (terminal)
```

`allowed_actions` on `awaiting_review_decision` omits `approve_final` when `merge_ready` is false.

### human_gates (checkpoint)

| Phase | Field | Values |
|-------|-------|--------|
| spec | `human_gates.spec` | `pending` → `approved` (on approve_spec) or `rejected` |
| final | `human_gates.final` | `not_reached` → `pending` (review merge_ok) → `approved` (approve_final) or `rejected` |

## Resume identity

On resume, when `repo_root` is supplied it is resolved to the git toplevel and compared to `run_context.repo_root` in the checkpoint.  
`run_id` in the request must match the checkpoint when provided.  
`artifact_root`, `workspace_root`, `workspace_strategy`, `planned_branch_name`, and `source_base_revision` are taken **only** from the checkpoint.

## implement stage

Codex runs first; **`collect_diff` runs after Codex** so review receives post-implementation changes.

## P2 — review presets

`dev_process.review_depth_preset`: `light` (diff + tests), `standard` (5 reviewers), `deep` (same set as standard in v1).  
Leaf registry: `dev_process.review_prompt.*`.

## P3 — exec evidence

Each exec writes `artifact_root/evidence/<stage>_<invoker>_<id>.json` with:

- `evidence_id`, `stdout_sha256`, `stderr_sha256`, `prompt_sha256`, `argv`, `cwd`
- `started_at`, `ended_at`, `exit_code`, `execution_fingerprint`

Validation rules:

- Duplicate `evidence_id` → fail
- Duplicate full `execution_fingerprint` (stdout + prompt + argv + cwd + timestamps) → fail
- Duplicate `stdout_sha256` only → warning in `evidence/warnings.jsonl`
- Invalid evidence JSON → fail
- `provider_meta.marker` in `{stub, manual, synthetic, fabricated}` → fail (on record and on store re-read)
- `exit_code != 0` → fail (on record and on store re-read)
- Required fields enforced on every evidence file
- Re-validated on `approve_final` and `merge`

Checkpoint / timeline `action` matches the flow action (`revise_spec`, `rework_implementation`, etc.).

`run_id` must match `[A-Za-z0-9_.-]+`. Flow checkpoints must live under `artifact_root/checkpoints/` (bare filename only).

Review prompt registry: five `dev_process.review_prompt.*` leaf types (wrappers via `reuse.py` only).

Rework with the same argv is allowed when timestamps (and usually prompt) differ.

## spec_plan output contract

Codex stdout must be a JSON object with non-empty string fields `spec` and `plan`. Silent fallback to raw stdout is **not** used.

## Input ports and resume contract

| Input | Required | Notes |
|-------|----------|-------|
| `action` | yes | `start`, `approve_spec`, `revise_spec`, `rework_implementation`, `approve_final`, `merge`, `reject_spec`, `reject_final` |
| `repo_root` | on `start` / resume check | Absolute path stored in checkpoint |
| `task_prompt` | on `start` | |
| `flow_checkpoint_path` | on resume | Under `artifact_root/checkpoints/` |
| `run_id` | no | Auto-generated on `start` |

## Failed state

Terminal. Not resumable; start a new run.

## Checkpoint file naming

- First `start`: `checkpoints/flow_start.json`
- Later actions: `checkpoints/{run_id}_{action}_flow.json`

## Timeline event schema

Every line: `ts`, `run_id`, `event` (required).

## Registry types

- `dev_process.flow` (orchestrator)
- `dev_process.review_prompt.*` (leaf prompt builders)
- `development_flow.*` composite pipes are **not** registered

## Merge gate

Requires `awaiting_merge` (after `approve_final`), `merge_ready=true`, all stages `completed`, `blocking_count==0`, non-stale review, and expected `stages.*.evidence_paths` on disk.

## P4 — exec workers

Stages call `resolve_exec_worker(kind)` then `run_exec`. Default kind is `codex` (`CodexExecWorker` → `codex_exec` node).  
Checkpoint stores `dev_process.exec_worker_kind`. Unknown kinds fail at stage start.

## P5 — git_worktree workspace

`PrepareWorkspaceNode` is a shared mechanical component; `dev_process` uses it only via `reuse.py`.  
No `development_flow.*` composite registry is restored.

On `approve_spec`, `prepare_workspace` with `strategy=git_worktree` creates  
`artifact_root/worktrees/<attempt>/` via `git worktree add -b <planned_branch> ...` (first attempt)  
or reattaches to the existing branch on later attempts after `revise_spec`.  
`planned_branch_name` is `feat/nodeflow/<run_id>` (dots/underscores normalized).  
Implement/review run in `workspace_context.workspace_root`; `run_context.workspace_root` stays frozen at source repo.  
`revise_spec` removes the prior worktree, increments `dev_process.workspace_attempt`, and clears `workspace_context`.  
`rework_implementation` passes `existing workspace_context` to reuse the same worktree.

## CLI

```bash
nodeflow -w /path/to/repo-root examples/pipes/dev_process/dev_process.json \
  -i action=start -i repo_root=/path/to/git/repo -i task_prompt='my task' \
  -i workspace_strategy=git_worktree
```

Input ports or node `default_config`: `workspace_strategy`, `exec_worker_kind` (input port first, then params, then start defaults).  
On resume, a supplied value must match the checkpoint or the flow fails.

`exec_argv` / `codex_argv` are **PipeSpec / programmatic-only** in P5: pass via node `params` or wired list inputs.  
The scalar `-i` CLI cannot pass list-valued inputs; do not use `-i exec_argv='[...]'` (string values fail validation).

Scalar `-i` values are wrapped for PipeSpec delivery; `dev_process.flow` accepts both flat and per-port dict payloads.

P5 assumes implementation workers do not commit. If workers may commit, `revise_spec` must reset the branch to `source_base_revision` or use an attempt-specific branch.
