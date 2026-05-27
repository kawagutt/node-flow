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

---

## PR5 — Codex argv order smoke (manual, pre-merge)

**目的:** `worker_adapter` が生成する argv が Codex CLI で解釈されるか確認する。hermetic テストは Python 側の argv 組み立てのみ検証する。

**注入順（Codex CLI 実機確認済み, 2026-05-27）:**

```text
codex exec --model <slug> <既存 exec flags...> resume <session_id> [-- passthrough...]
```

例: `codex exec --model gpt-5.5-medium --sandbox read-only resume <uuid>`

`codex exec --model … resume … --sandbox …` は **`unexpected argument '--sandbox'`** で拒否される。`worker_adapter` は exec フラグを `resume` より前に置く。

### 1. パースのみ（最短）

```bash
# 採用形（dev_process が生成する形）
codex exec --model gpt-5.5-medium --sandbox read-only resume 00000000-0000-0000-0000-000000000001 \
  --help 2>&1 | head -20

# 拒否される形（resume の後に exec フラグは不可）
codex exec --model gpt-5.5-medium resume 00000000-0000-0000-0000-000000000001 \
  --sandbox read-only --help 2>&1 | head -20
# → error: unexpected argument '--sandbox' found
```

実機結果 (2026-05-27): 採用形は `codex exec resume --help` が表示される。拒否形は上記エラー。

### 2. adapter 出力の確認（Python）

```bash
cd /path/to/node-flow
./.venv/bin/python -c "
from nodeflow.workflows.dev_process.worker_adapter import prepare_worker_argv
base = ['codex', 'exec', '--sandbox', 'workspace-write']
argv, m = prepare_worker_argv('codex', base, model='gpt-5.5-medium', provider_session_id='sess-test')
print('model:', m)
print('argv:', argv)
"
```

### 3. 実 exec（任意・認証必要）

```bash
codex exec --model gpt-5.5-medium --sandbox read-only 'Reply with OK only.'
# 成功したら、既知 SESSION_ID で resume（--last でも可）
codex exec resume --last 'Reply with OK only.'
```

| 結果 | 記録 |
|------|------|
| 採用形パース (`--model` … `--sandbox` … `resume`) | pass |
| 旧形 (`resume` 後に `--sandbox`) | fail — `unexpected argument '--sandbox'` |
| 実 exec（任意） | |
