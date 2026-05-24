# Dev Process — Real Codex Dry-Run Guide

This guide describes how to **manually** exercise NodeFlow dev-process with **real Codex**
on a **disposable git repository**, without a wrapper CLI.

**Preferred (P7):** use [dev_process_p7_wrapper.md](./dev_process_p7_wrapper.md) and `nodeflow-dev-process` — no manual checkpoint handling.

Read [dev_process.md](./dev_process.md) for architecture, state machine, and merge contracts.

## Goals

- Verify real Codex + `git_worktree` + `record_only` / `git_merge_branch` end-to-end
- Confirm checkpoint, timeline, evidence, and summary artifacts
- Confirm **no `git push`** and no remote operations
- Fix operational steps before building P7 wrapper UX

## Non-goals

- No `nodeflow-dev-process` wrapper (P7)
- No Hermes integration
- No production repository

---

## Safety rules (read first)

| Rule | Why |
|------|-----|
| Use a **disposable toy repo** | Real Codex may modify files; merge may change source git state |
| First runs: **`merge_policy=record_only`** | No local `git merge` until the flow is understood |
| **`git_merge_branch` when you expect committed changes on the attempt branch** | Merge applies **branch commits**, not uncommitted worktree diff |
| **Worktree must be clean before merge** | Uncommitted changes block `git_merge_branch` |
| **Branch HEAD must match review snapshot** | Commits after review block merge until review is re-run |
| **Merge is local only** | dev-process never runs `git push`, `fetch`, or `pull` |
| **Conflicts → `git merge --abort`** | Source repo may stay on target branch after failure |
| **Resume inputs must match checkpoint** | `workspace_strategy`, `exec_worker_kind`, `merge_policy` mismatches fail |

---

## Prerequisites

1. **NodeFlow** installed in a venv (from this repo):

   ```bash
   cd /path/to/node-flow
   python -m venv .venv
   .venv/bin/pip install -e .
   ```

2. **`codex` CLI** on `PATH` and authenticated for your environment.

   Install example: `npm install -g @openai/codex` (ensure the npm global `bin` directory is on `PATH`).

   On hosts **without bubblewrap / user namespaces**, prefer:

   ```bash
   -i exec_argv='["codex","exec","--dangerously-bypass-approvals-and-sandbox"]'
   ```

   on disposable repos only. `--full-auto` is deprecated (Codex 0.133+ warns to use `--sandbox workspace-write`).

   **`exec_argv` is stored in the checkpoint at `start`**; resume actions do not need to repeat it unless you intentionally override (mismatch fails).

   Recorded smoke run: [dev_process_smoke_log.md](./dev_process_smoke_log.md).

3. **Disposable git repo** (example):

   ```bash
   mkdir -p /tmp/dev-process-dry-run && cd /tmp/dev-process-dry-run
   git init -b main
   echo '# dry-run' > README.md
   git add README.md
   git commit -m 'init'
   ```

4. Set shell helpers (adjust paths):

   ```bash
   export NF=/path/to/node-flow
   export REPO=/tmp/dev-process-dry-run
   export PIPE=$NF/examples/pipes/dev_process/dev_process.json
   export NFCLI=$NF/.venv/bin/nodeflow

   run_nf() {
     $NFCLI -w "$NF" "$PIPE" "$@"
   }

   save_cp() {
     $NF/.venv/bin/python -c "import sys,json; t=sys.stdin.read(); i=t.find('{'); print(json.loads(t[i:])['flow_output']['flow_result']['flow_checkpoint_path'])"
   }
   ```

5. Optional reference params file: `examples/reference/dev_process/codex_params.example.json`  
   **Note:** that example uses `merge_policy=git_merge_branch` and `--full-auto`. For first dry-run, override on the CLI as shown below.

---

## Checkpoint handling

Every action writes a **new** flow checkpoint. For the next action, always use the latest
`flow_output.flow_result.flow_checkpoint_path` from the **previous** action's JSON output.

**Do not** keep using the `start` checkpoint after `approve_spec`, or the `approve_spec`
checkpoint after `approve_final`.

Pattern:

```bash
CP=$(run_nf ... start ... | tee /tmp/nf_start.json | save_cp)

CP=$(run_nf ... approve_spec ... -i flow_checkpoint_path="$CP" \
  | tee /tmp/nf_approve_spec.json | save_cp)

CP=$(run_nf ... approve_final ... -i flow_checkpoint_path="$CP" \
  | tee /tmp/nf_approve_final.json | save_cp)

run_nf ... merge ... -i flow_checkpoint_path="$CP" | tee /tmp/nf_merge.json
```

The `tee` lines are optional but useful for debugging. `artifact_root` is stable for a
run, but `flow_checkpoint_path` changes on every action.

---

## CLI conventions

- Workspace `-w` is the NodeFlow repo root (resolves pipe/node paths).
- `-i key=value` passes pipe inputs. Values starting with `[` or `{` are parsed as JSON.
- Each successful action prints JSON; checkpoint path is at:

  ```text
  .flow_output.flow_result.flow_checkpoint_path
  ```

  Example extraction (after any action):

  ```bash
  CP=$(run_nf ... | save_cp)
  ```

  Or copy the path from the printed JSON manually.

- Artifacts live under:

  ```text
  $REPO/.nodeflow/runs/<run_dir>/
  ```

---

## Recommended path: `git_worktree` + `record_only`

This is the **first real Codex dry-run** to run.

### 1. `start` (+ spec_plan)

Full sequence with checkpoint updates:

```bash
CP=$(run_nf \
  -i action=start \
  -i repo_root="$REPO" \
  -i task_prompt='small safe dry-run task' \
  -i workspace_strategy=git_worktree \
  -i merge_policy=record_only \
  -i exec_argv='["codex","exec","--dangerously-bypass-approvals-and-sandbox"]' \
  | tee /tmp/nf_start.json | save_cp)
```

Expected after `start`:

- `flow_result.state` = `awaiting_spec_approval`
- `spec_plan/spec.md` and `spec_plan/plan.md` under artifact root
- `timeline.jsonl` contains `stage_completed` for `spec_plan`

### 2. Human review spec/plan

```bash
ART=$(python -c "import json; print(json.load(open('$CP'))['run_context']['artifact_root'])")
ls "$ART/spec_plan/"
```

Edit files manually if needed, then continue.

### 3. `approve_spec` (implement + review)

```bash
CP=$(run_nf \
  -i action=approve_spec \
  -i repo_root="$REPO" \
  -i flow_checkpoint_path="$CP" \
  | tee /tmp/nf_approve_spec.json | save_cp)
```

Expected:

- `flow_result.state` = `awaiting_review_decision`
- `workspace_context.workspace_root` = `.../worktrees/001/`
- checkpoint `stages.review.reviewed_branch_name` and `reviewed_branch_head` recorded
- `implement/` and `review/` artifacts + `evidence/*.json`

### 4. Review decision

Check:

```bash
cat "$ART/review/aggregate.json"
```

If `merge_ready` is false, use `rework_implementation` or `revise_spec` (see Recovery).  
After either action, **update `CP`** from that action's output before continuing.

### 5. `approve_final`

```bash
CP=$(run_nf \
  -i action=approve_final \
  -i repo_root="$REPO" \
  -i flow_checkpoint_path="$CP" \
  | tee /tmp/nf_approve_final.json | save_cp)
```

Expected:

- `flow_result.state` = `awaiting_merge`
- **No git merge yet**

### 6. `merge` (`record_only`)

```bash
run_nf \
  -i action=merge \
  -i repo_root="$REPO" \
  -i flow_checkpoint_path="$CP" \
  | tee /tmp/nf_merge.json
```

Expected:

- `flow_result.state` = `merged`
- `merge_result.policy` = `record_only`
- `summary/merge_development_summary.json` exists
- **No merge is applied to the source branch** (`main` has no merge commit from this action).  
  The attempt branch and worktree may still exist for audit/debugging.

---

## Advanced path: `git_worktree` + `git_merge_branch`

Only after `record_only` dry-run succeeds.

Use `git_merge_branch` only when you expect **committed changes** on the attempt branch
(merge may be a no-op if the branch has no new commits, but that is usually not the goal).

### Additional requirements

1. Implementation worker should **commit** changes to the attempt branch before review completes.
   Real Codex may not commit by default — verify with:

   ```bash
   git -C "$ART/worktrees/001" status --porcelain
   git -C "$REPO" log --oneline refs/heads/feat/nodeflow/<run_id>/attempt-001
   ```

2. Start with explicit merge policy (update `CP` after each subsequent action):

   ```bash
   CP=$(run_nf \
     -i action=start \
     -i repo_root="$REPO" \
     -i task_prompt='merge dry-run' \
     -i workspace_strategy=git_worktree \
     -i merge_policy=git_merge_branch \
     -i exec_argv='["codex","exec","--dangerously-bypass-approvals-and-sandbox"]' \
     | tee /tmp/nf_start_merge.json | save_cp)
   ```

3. Run the same gate sequence, **refreshing `CP` after each action**:
   `approve_spec` → `approve_final` → `merge`.

4. After `merge`:

   - Feature commits appear on `main` (local only)
   - `merge_result.policy` = `git_merge_branch`
   - Still **no push**

### Merge gate reminders

Merge fails if:

- source repo dirty (except `.nodeflow/` under source)
- worktree dirty (uncommitted files)
- attempt branch HEAD ≠ `stages.review.reviewed_branch_head`
- attempt branch ≠ `feat/nodeflow/<run_id>/attempt-NNN`
- `source_base_revision` not ancestor of attempt/target branches
- merge conflict (aborted with `git merge --abort`)

---

## Minimal path: `current_repo` + `record_only`

Fastest smoke test without worktrees:

```bash
CP=$(run_nf \
  -i action=start \
  -i repo_root="$REPO" \
  -i task_prompt='current repo smoke' \
  -i workspace_strategy=current_repo \
  -i merge_policy=record_only \
  -i exec_argv='["codex","exec","--dangerously-bypass-approvals-and-sandbox"]' \
  | tee /tmp/nf_start_current.json | save_cp)
```

Then `approve_spec` → `approve_final` → `merge`, updating `CP` after each action (see [Checkpoint handling](#checkpoint-handling)).  
`git_merge_branch` is **not** available with `current_repo`.

---

## Resume contract

On every resume action, pass:

- `flow_checkpoint_path` from the **latest** checkpoint for this run
- `repo_root` (must match checkpoint toplevel)

Optional inputs (`workspace_strategy`, `exec_worker_kind`, `merge_policy`) must **match** checkpoint values if supplied.

Example resume mismatch failure:

```bash
# checkpoint stored merge_policy=record_only; CP must be latest checkpoint
CP=$(run_nf \
  -i action=approve_spec \
  -i repo_root="$REPO" \
  -i flow_checkpoint_path="$CP" \
  -i merge_policy=git_merge_branch \
  | save_cp)
# → merge_policy mismatch on resume
```

---

## Verification checklist

After a full run to `merged`:

```bash
ART=...  # artifact_root from checkpoint run_context

# Timeline
cat "$ART/timeline.jsonl"

# Checkpoints
ls "$ART/checkpoints/"

# Evidence
ls "$ART/evidence/"

# Stage outputs
test -f "$ART/spec_plan/spec.md"
test -f "$ART/spec_plan/plan.md"
test -f "$ART/review/aggregate.json"
test -f "$ART/summary/merge_development_summary.json"

# No push: verify remotes unchanged (optional)
git -C "$REPO" remote -v
git -C "$REPO" log --oneline -5
```

Timeline events you should see for a happy path:

```text
action_received → stage_started/completed (spec_plan, implement, review)
→ checkpoint_written → merge_attempted → checkpoint_written (merged)
```

If summary generation fails after a successful git merge, expect `summary_failed` in timeline and `development_summary.status=fallback` in checkpoint, with flow still `merged`.

---

## Failure recovery

| Situation | What to do |
|-----------|------------|
| `flow_failed` terminal | Do not resume; inspect `timeline.jsonl` and last checkpoint; start a new run |
| `approve_spec` / stage failure | Failed checkpoint + `flow_failed` in timeline; fix cause and start new run |
| Review blocking | `rework_implementation` (same worktree) or `revise_spec` (new attempt, old branch kept) |
| Merge blocked: dirty worktree | Commit or discard changes in worktree; re-run review if HEAD changed |
| Merge blocked: branch changed after review | `rework_implementation` or new review path; do not merge |
| Merge conflict | dev-process runs `git merge --abort`; resolve manually; may need new run |
| Post-merge repo on unexpected branch | Check out your branch manually; see P6 doc (no auto-restore) |

### `rework_implementation`

```bash
CP=$(run_nf \
  -i action=rework_implementation \
  -i repo_root="$REPO" \
  -i flow_checkpoint_path="$CP" \
  | tee /tmp/nf_rework.json | save_cp)
```

Re-runs implement + review in the same worktree. Use the new `CP` for subsequent actions.

### `revise_spec`

```bash
CP=$(run_nf \
  -i action=revise_spec \
  -i repo_root="$REPO" \
  -i flow_checkpoint_path="$CP" \
  -i task_prompt='revise: ...' \
  | tee /tmp/nf_revise.json | save_cp)
```

Removes prior worktree, increments attempt, creates new attempt branch.

---

## Cleanup (manual)

dev-process **does not** auto-delete worktrees or branches (audit retention).

Optional manual cleanup after experiments:

```bash
# List worktrees
git -C "$REPO" worktree list

# Remove dev-process worktree (only under artifact_root/worktrees/)
git -C "$REPO" worktree remove --force "$ART/worktrees/001"
git -C "$REPO" worktree prune

# Delete attempt branch (only if you are sure)
git -C "$REPO" branch -D feat/nodeflow/<run_id>/attempt-001
```

Keep `.nodeflow/runs/` if you want audit history.

---

## Push prohibition

dev-process **never** runs:

```text
git push
git fetch
git pull
git remote ...
```

Remote publication is outside this workflow. After dry-run, confirm with:

```bash
git -C "$REPO" status
git -C "$REPO" branch -vv
```

No new upstream tracking from dev-process is expected.

---

## Suggested dry-run sequence

```text
1. current_repo + record_only        (fast smoke, optional)
2. git_worktree + record_only        (recommended first real Codex run)
3. git_worktree + git_merge_branch   (when you expect committed attempt-branch changes)
4. Document any Codex argv / prompt tweaks needed for your repo
5. Proceed to P7 wrapper UX
```

---

## Related files

| Path | Purpose |
|------|---------|
| [dev_process.md](./dev_process.md) | Architecture and merge contracts |
| `examples/pipes/dev_process/dev_process.json` | PipeSpec entry point |
| `examples/nodes/dev_process_flow/node.json` | Flow node defaults |
| `examples/reference/dev_process/codex_params.example.json` | Example params (review before use) |
