# Dev Process — P7 Wrapper Spec

Thin CLI wrapper that removes manual checkpoint (`CP=...`) handling. It does **not** implement a new workflow engine.

Related:

- [dev_process.md](./dev_process.md) — architecture and merge contracts
- [dev_process_real_codex_dry_run.md](./dev_process_real_codex_dry_run.md) — manual `nodeflow` path (pre-P7)
- [dev_process_smoke_log.md](./dev_process_smoke_log.md) — real Codex smoke record

## Goal

Eliminate checkpoint hand-work; call `dev_process.flow` safely via `run_flow`.

## Non-goals

- No new state machine on the wrapper side
- No checkpoint JSON editing
- No additional git operations (push/fetch/merge) in the wrapper
- No duplicate merge gates — `run_flow` remains authoritative

## Entry point

```bash
nodeflow-dev-process --repo-root /path/to/target-repo <command> [options]
```

Installed via `pyproject.toml`:

```text
nodeflow-dev-process = nodeflow.workflows.dev_process.cli:main
```

Implementation: `nodeflow/workflows/dev_process/cli.py`  
Discovery: `nodeflow/workflows/dev_process/discovery.py`

## Commands

| CLI command | `run_flow` action | Notes |
|-------------|-------------------|--------|
| `start` | `start` | `--task-prompt` required |
| `status` | _(read checkpoint only)_ | latest or `--checkpoint` / `--run-id` |
| `approve-spec` | `approve_spec` | auto latest CP |
| `rework` | `rework_implementation` | auto latest CP |
| `revise-spec` | `revise_spec` | optional `--task-prompt` |
| `approve-final` | `approve_final` | auto latest CP |
| `merge` | `merge` | auto latest CP |

Global flags:

- `--repo-root` — target git repo (default: `.`, resolved to git toplevel)
- `--json` — emit raw `run_flow` / status JSON

Per-command flags:

- `--checkpoint` — explicit `flow_checkpoint_path`
- `--run-id` — scope discovery to one run

`start`-only flags mirror common pipe inputs: `--workspace-strategy`, `--merge-policy`, `--exec-worker-kind`, `--exec-argv` (JSON array).

## Run discovery

Latest checkpoint resolution:

1. Scan `repo_root/.nodeflow/runs/*/checkpoints/*.json`
2. Optionally filter by `--run-id` (`run_context.run_id` or run dir name)
3. Pick the checkpoint with the greatest `written_at` (fallback: file mtime)
4. Validate with `load_flow_checkpoint` (schema, self-reference, path under `artifact_root/checkpoints/`)

Explicit `--checkpoint` additionally requires the file path to be under `repo_root/.nodeflow/runs/`.

Self-reference: `flow_result.flow_checkpoint_path` must equal the file being loaded. A copied checkpoint under `checkpoints/` with a stale embedded path fails on `status` / resume (see `test_explicit_checkpoint_self_reference_mismatch_fails` and `test_checkpoint_self_reference_mismatch_on_resume`).

Resume actions use the discovered path as `flow_checkpoint_path` — no manual `CP=$(...)` extraction.

## Safety

Before resume actions, the wrapper reads the checkpoint and **fails** if the requested action is not in `flow_result.allowed_actions`. It does not re-implement transition rules; it trusts the checkpoint written by `run_flow`.

Explicit checkpoint and run scoping:

- **`--checkpoint`** — path must be under `repo_root/.nodeflow/runs/`; validates `run_context.repo_root` matches `--repo-root`
- **`--run-id`** — when set, must match `run_context.run_id` even with explicit `--checkpoint`
- **Discovery** — `run_id` uses exact match on `run_context.run_id` or run dir name (no substring match); repo-mismatch / out-of-scope checkpoints are **skipped** when picking latest
- Resume actions pass **`run_id`** through to `run_flow` for a second identity check

Terminal states (`merged`, `failed`) have empty `allowed_actions` → resume commands fail early with a clear message.

## Output (human mode)

After each mutating command:

```text
state: ...
ok: ...
allowed_actions: [...]
next_action: ...
merge_ready: ...
flow_checkpoint_path: /abs/path/checkpoints/....json
artifact_root: /abs/path/.nodeflow/runs/...
timeline: /abs/path/.nodeflow/runs/.../timeline.jsonl
summary: ...  (when present)
```

`status` shows the same fields without calling `run_flow`.

## Example (hermetic / CI)

```bash
cd /path/to/disposable-repo
git init -b main && echo '# test' > README.md && git add README.md && git commit -m init

nodeflow-dev-process start --task-prompt 'smoke' --workspace-strategy current_repo
nodeflow-dev-process status
nodeflow-dev-process approve-spec
nodeflow-dev-process approve-final
nodeflow-dev-process merge
```

No `--exec-argv` → hermetic Python stubs per stage (tests).

## Example (real Codex)

Pass Codex argv once on `start` (stored in checkpoint). Use `--repo-root` on every command (or `cd` into the repo first):

```bash
REPO=/path/to/disposable-repo

nodeflow-dev-process --repo-root "$REPO" start \
  --task-prompt 'Add CONTRIBUTING.md and commit' \
  --workspace-strategy git_worktree \
  --merge-policy record_only \
  --exec-argv '["codex","exec","--dangerously-bypass-approvals-and-sandbox"]'

nodeflow-dev-process --repo-root "$REPO" approve-spec
nodeflow-dev-process --repo-root "$REPO" approve-final
nodeflow-dev-process --repo-root "$REPO" merge
```

See [dev_process_smoke_log.md](./dev_process_smoke_log.md) for environment notes (bubblewrap, commit prompt, timeout).

### git_merge_branch (disposable repo)

Use `--merge-policy git_merge_branch` on `start`. Codex must **commit** on the attempt branch (implement prompt requires this). Then the same resume commands:

```bash
nodeflow-dev-process --repo-root "$REPO" start \
  --task-prompt 'Add CONTRIBUTING.md and commit on the attempt branch.' \
  --workspace-strategy git_worktree \
  --merge-policy git_merge_branch \
  --exec-argv '["codex","exec","--dangerously-bypass-approvals-and-sandbox"]'

nodeflow-dev-process --repo-root "$REPO" approve-spec
nodeflow-dev-process --repo-root "$REPO" approve-final
nodeflow-dev-process --repo-root "$REPO" merge
```

Verify `main` has the feature commit locally; still no push.

## P7 Done checklist

| Item | Status |
|------|--------|
| `start` → latest checkpoint displayed | ✅ |
| `status` → state / allowed_actions / next_action / artifact_root | ✅ |
| `approve-spec` → auto latest CP | ✅ |
| `approve-final` → auto latest CP | ✅ |
| `merge` → auto latest CP | ✅ |
| `--checkpoint` explicit override | ✅ |
| `--run-id` run scoping | ✅ |
| wrapper hermetic path to `merged` (tests) | ✅ |
| wrapper real Codex record_only smoke | ✅ (2026-05-24, ~87s) |
| wrapper real Codex git_merge_branch smoke | ✅ (2026-05-24, ~86s) |

## Tests

`tests/workflows/dev_process/test_cli.py` — discovery, status, full hermetic path, safety gate, `--checkpoint`, `--run-id`.
