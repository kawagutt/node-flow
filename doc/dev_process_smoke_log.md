# Dev Process — Real Codex Smoke Log

Recorded result of the **P7-pre** smoke test: real Codex + `git_worktree` + `record_only`.

Guide used: [dev_process_real_codex_dry_run.md](./dev_process_real_codex_dry_run.md)

## Run summary

| Field | Value |
|-------|-------|
| Date | 2026-05-24 |
| Result | **PASS** (full path to `merged`) |
| Duration | ~97s (start → merge) |
| Disposable repo | `/tmp/dev-process-smoke-v4` |
| Run ID | `20260524T024056107573Z` |
| Artifact root | `/tmp/dev-process-smoke-v4/.nodeflow/runs/001_20260524_add-one-line-contributing-md-and-commit-it_07573Z` |
| NodeFlow | `/data/github/node-flow` (editable install) |
| Codex CLI | `codex-cli 0.133.0` (`@openai/codex`, Node v20) |
| Model (from stderr) | `gpt-5.5` |

### Flow inputs

```text
workspace_strategy=git_worktree
merge_policy=record_only
task_prompt=Add one-line CONTRIBUTING.md and commit it.
exec_argv=["/path/to/codex","exec","--dangerously-bypass-approvals-and-sandbox"]
```

`exec_argv` was passed only on **`start`**. Resume actions used the checkpoint-stored argv (see fixes below).

### State transitions (happy path)

```text
start           → awaiting_spec_approval
approve_spec    → awaiting_review_decision (merge_ready=true)
approve_final   → awaiting_merge
merge           → merged (record_only, no git merge on main)
```

### What Codex did

| Stage | Contract | Observed |
|-------|----------|----------|
| spec_plan | stdout = `{"spec":"...","plan":"..."}` JSON | OK — valid JSON on stdout, `spec.md` / `plan.md` written |
| implement | modify worktree; **commit required** for clean review | OK — `CONTRIBUTING.md` added, commit `eb0e38e` on attempt branch |
| review (×5) | stdout JSON review contract | OK — `aggregate.json` → `decision=merge_ok`, `blocking_count=0` |
| merge | `record_only` | OK — `summary/merge_development_summary.json` written; `main` unchanged |

### Artifacts verified

```text
timeline.jsonl          — flow_started … merge_attempted … checkpoint_written (merged)
checkpoints/            — start, approve_spec, approve_final, merge
evidence/               — 7 files (spec_plan + implement + 5 reviewers)
spec_plan/spec.md       — present
review/aggregate.json   — present
summary/merge_development_summary.json — present
worktrees/001/          — committed feature branch; source main still at init only
```

### Timeline events (condensed)

```text
flow_started → checkpoint_written → stage_started/completed (spec_plan)
→ action_received (approve_spec) → implement → review
→ action_received (approve_final) → action_received (merge)
→ merge_attempted → checkpoint_written (merged)
```

---

## Operational friction found (and fixes)

### 1. CLI JSON array inputs (`exec_argv`)

**Symptom:** `-i exec_argv='["codex","exec"]'` failed with  
`Runner delivery … requires dict payload, got list`.

**Fix:** `nodeflow/core/run.py` — wrap **all** non-dict CLI inputs (including JSON arrays) as `{key: value}` for Runner delivery.

### 2. `exec_argv` lost on resume

**Symptom:** `start` used real Codex; `approve_spec` fell back to hermetic Python stubs (`implementation stub ok`), so review passed without real implementation.

**Fix:** Persist `exec_argv` in checkpoint `dev_process.exec_argv` at `start`; restore on resume when CLI omits it. Mismatch on resume raises `exec_argv mismatch on resume`.

### 3. Untracked files blocked review

**Symptom:** Codex created `CONTRIBUTING.md` but did not commit → `R_UNTRACKED_FILES` blocking finding → `merge_ready=false` → `approve_final` rejected.

**Fix:** Implement stage prompt now instructs Codex to `git add` + `git commit` and leave a clean worktree.

### 4. Sandbox / bubblewrap on this host

**Symptom:** `--full-auto` (deprecated) + default sandbox: bubblewrap/user-namespace errors in stderr; still returned spec JSON on simple runs, but unreliable for file writes.

**Mitigation for smoke:** use  
`codex exec --dangerously-bypass-approvals-and-sandbox`  
on disposable repos only. Prefer `--sandbox workspace-write` when bubblewrap works.

### 5. `save_cp` helper vs CLI banner

**Symptom:** piping nodeflow output to `json.load(sys.stdin)` fails because CLI prints `Pipeline execution completed.` before JSON.

**Mitigation:** extract from first `{` (documented in dry-run guide).

### 6. Codex timeout

**Change:** default stage timeout raised to **300s** (`EXEC_TIMEOUT_SECONDS`) for spec_plan / implement / review (real Codex often exceeds 120s with five reviewers).

### 7. Evidence duplicate stdout warnings

**Observed:** when review prompts are similar, Codex may return identical JSON for multiple reviewers → `dev_process evidence warning: stdout_sha256 … repeated`. Non-fatal; aggregation still worked.

---

## Tests after fixes

```text
263 passed (pytest, full suite)
```

(Current suite: **280+** after P7 CLI tests.)

New/updated coverage:

- `tests/test_cli_kick_pipeline.py::test_load_and_kick_dev_process_exec_argv_json_array`
- `tests/workflows/dev_process/test_p5_hardening.py::test_exec_argv_persisted_in_checkpoint`

---

## P7-pre Done checklist

| Item | Status |
|------|--------|
| 262+ tests pass | ✅ 263 |
| dry-run doc exists | ✅ |
| real Codex + git_worktree + record_only smoke pass | ✅ (this log) |
| smoke result log in doc | ✅ (this file) |
| smoke-driven small fixes reflected | ✅ (see above) |

**Next:** ~~P7 wrapper UX; later `git_merge_branch` dry-run on disposable repo.~~ Done — see below.

---

## git_merge_branch smoke (2026-05-24)

Real Codex + `git_worktree` + **`git_merge_branch`** via `nodeflow-dev-process`:

```bash
REPO=/tmp/dev-process-merge-smoke

nodeflow-dev-process --repo-root "$REPO" start \
  --task-prompt 'Add one-line CONTRIBUTING.md and commit it on the attempt branch.' \
  --workspace-strategy git_worktree \
  --merge-policy git_merge_branch \
  --exec-argv '["codex","exec","--dangerously-bypass-approvals-and-sandbox"]'

nodeflow-dev-process --repo-root "$REPO" approve-spec
nodeflow-dev-process --repo-root "$REPO" approve-final
nodeflow-dev-process --repo-root "$REPO" merge
```

| Field | Value |
|-------|-------|
| Result | **PASS** → `merged`, `merge_policy=git_merge_branch` |
| Duration | ~86s |
| Run ID | `20260524T034415922152Z` |
| Repo | `/tmp/dev-process-merge-smoke` |

Verified:

- Attempt branch commit `cb2c252` (CONTRIBUTING.md) on `feat/nodeflow/.../attempt-001`
- Worktree clean before merge
- **`main` advanced** to `cb2c252` (local merge; init was `8b45e0a`)
- No push / remote operations
- `summary/merge_development_summary.json` written

---

## Dev-process v1 Done checklist

| Item | Status |
|------|--------|
| P0–P6.5 core + tests | ✅ (280+ pytest) |
| real Codex record_only smoke | ✅ |
| P7 wrapper (`nodeflow-dev-process`) | ✅ |
| wrapper real Codex record_only smoke | ✅ (~87s) |
| real Codex **git_merge_branch** smoke | ✅ (~86s) |
| smoke logs in doc | ✅ (this file) |

**Later (post-v1):** original branch restore, `cleanup_worktrees` / `cleanup_branches` actions.

---

## P7 wrapper smoke — record_only (2026-05-24)

Real Codex + `git_worktree` + `record_only` via `nodeflow-dev-process` (no manual `CP=...`):

```bash
nodeflow-dev-process --repo-root /tmp/dev-process-wrapper-codex start \
  --task-prompt 'Add one-line CONTRIBUTING.md and commit it.' \
  --workspace-strategy git_worktree --merge-policy record_only \
  --exec-argv '["codex","exec","--dangerously-bypass-approvals-and-sandbox"]'

nodeflow-dev-process --repo-root /tmp/dev-process-wrapper-codex approve-spec
nodeflow-dev-process --repo-root /tmp/dev-process-wrapper-codex approve-final
nodeflow-dev-process --repo-root /tmp/dev-process-wrapper-codex merge
```

| Field | Value |
|-------|-------|
| Result | **PASS** → `merged` |
| Duration | ~87s |
| Run ID | `20260524T025642581075Z` |
| Repo | `/tmp/dev-process-wrapper-codex` |

Spec: [dev_process_p7_wrapper.md](./dev_process_p7_wrapper.md)
