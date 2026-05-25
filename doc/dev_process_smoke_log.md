# Dev Process — Smoke Log

Current P11 smoke results. Validates flow orchestration, checkpoint states, and merge gates.

## P11 smoke — record_only, hermetic (2026-05-25)

**Hermetic stubs** (no real Codex) + `git_worktree` + `record_only` via `nodeflow --pipe dev-process`.
Validates the P11 flow orchestration; does **not** verify real Codex commit behavior.

```bash
nodeflow --pipe dev-process --repo-root "$REPO" start \
  --task-prompt 'Add one-line CONTRIBUTING.md and commit it.' \
  --workspace-strategy git_worktree --merge-policy record_only
nodeflow --pipe dev-process --repo-root "$REPO" approve-spec
nodeflow --pipe dev-process --repo-root "$REPO" continue-implementation
nodeflow --pipe dev-process --repo-root "$REPO" approve-final
nodeflow --pipe dev-process --repo-root "$REPO" merge
```

| Item | Result |
|------|--------|
| Executor | hermetic stubs |
| Merge policy | `record_only` |
| Final state | `merged` |
| `main` branch | unchanged (init commit only) |
| Attempt branch | created; no commit (hermetic) |
| `node_runs[]` vs evidence | 9 = 9 |
| All 8 stages | `completed` |
| Stale | none |
| `summary/merge_development_summary.json` | written |

---

## P11 smoke — git_merge_branch, hermetic (2026-05-25)

**Hermetic stubs** (no real Codex) + `git_worktree` + `git_merge_branch` via `nodeflow --pipe dev-process`.
Validates merge gate logic; hermetic stubs do not commit, so the merge is a no-op fast-forward.

```bash
nodeflow --pipe dev-process --repo-root "$REPO" start \
  --task-prompt 'Add one-line CONTRIBUTING.md and commit it.' \
  --workspace-strategy git_worktree --merge-policy git_merge_branch
nodeflow --pipe dev-process --repo-root "$REPO" approve-spec
nodeflow --pipe dev-process --repo-root "$REPO" continue-implementation
nodeflow --pipe dev-process --repo-root "$REPO" approve-final
nodeflow --pipe dev-process --repo-root "$REPO" merge
```

| Item | Result |
|------|--------|
| Executor | hermetic stubs |
| Merge policy | `git_merge_branch` |
| Final state | `merged` |
| `main` branch | unchanged (no-op fast-forward; hermetic stubs do not commit) |
| `node_runs[]` vs evidence | 9 = 9 |
| All stages | `completed` |
| Stale | none |
| No remote / push operations | confirmed |

---

## Bug fixed during P11 development: committed diff

**Symptom:** real Codex `record_only` smoke (pre-P11) reached `awaiting_rework_decision` because `review_diff` reported "diff against base ref is empty" even though `CONTRIBUTING.md` was committed on the attempt branch.

**Root cause:** `CollectDiffNode` used `git diff <base_ref>` (working tree vs commit). After Codex committed, the worktree was clean, and the review prompt title said "working tree vs base ref" — Codex reviewer independently ran `git diff`, found nothing uncommitted, and reported the diff as empty, ignoring the prompt's diff content.

**Fixes applied:**

1. `CollectDiffNode`: default mode changed to `committed` (`git diff <base_ref> HEAD`); `diff_mode=working_tree` available for unstaged changes
2. Review prompt title: `"working tree vs base ref"` → `"committed changes since base ref"`
3. Empty diff guard in `run_implementation_stage()`: if `HEAD != base_revision` but collected diff is empty, raise `NodeExecutionFailure`
4. New tests: `test_committed_diff.py` (4 tests for collect_diff, implementation stage, and full flow)

---

## P11 Done checklist

| Item | Status |
|------|--------|
| P11 code: full implementation chain, owner routing, stale, final/merge | done |
| `collect_diff` default `committed` mode; `diff_mode` param added | done |
| Empty diff guard (fail if branch advanced but diff empty) | done |
| Hermetic smoke `record_only` | pass |
| Hermetic smoke `git_merge_branch` | pass |
| `node_runs[]` = evidence count | verified |
| **P11 real Codex smoke** | **pending** — v1 real Codex (record_only + git_merge_branch) は PASS 済み; P11 の spec/plan 分離 + continue-implementation 経路は hermetic のみ |
