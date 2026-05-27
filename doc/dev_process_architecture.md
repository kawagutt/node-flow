# Dev-process architecture (implementation contract)

Canonical specification for NodeFlow dev-process v3 (`dev_process.flow.v3`).  
Implementations and tests must match this document.

See also: [dev_process.md](./dev_process.md) (operator guide).

## 1. Purpose

Define the **process graph**, **checkpoint states**, **artifacts**, and **phase boundaries** (P9 / P10 / P11) so `DevProcessFlowNode` can orchestrate without ad-hoc state in code.

## 2. Three-layer model

### Target (PR2–PR4)

| Layer | Role |
|-------|------|
| **Coordinator** | `DevProcessFlowNode` — loops, human gates, `revision_context`, checkpoint write |
| **Subpipe** | Linear stage chains (`spec_cycle`, `plan_cycle`, `plan_review`, `phase_step`, `final_review`) via generic `PipeNode` |
| **Leaf ActionNode** | One execution attempt = one `node_runs[]` entry (PR2+) |

Principle: `1 PipeSpec leaf node execution attempt = 1 node_runs[] entry`.

### Current implementation

`DevProcessFlowNode` is an **orchestrator** (ActionNode), **not** a PipeNode.  
Linear segments are delegated to subpipes via `run_subpipe()`:

- `spec_cycle`: `write_spec` -> `review_spec`
- `plan_cycle`: `write_plan` (coordinator performs contract validation/phase init)
- `plan_review`: `review_plan`
- `phase_step`: `write_implementation` -> `write_tests` -> `lint_fix` -> `run_tests` -> `review_*` -> `review_aggregate`
- `final_review`: final-scope `review_*` -> `review_aggregate`

`plan_cycle` is intentionally write-only. The coordinator must validate phase
contracts and initialize phase metadata before any plan review runs, so
`review_plan` is separated into `plan_review` instead of being embedded in
`plan_cycle`.

Observability split (unchanged):

| Concern | Role |
|---------|------|
| **Process graph** | True execution flow (loops, steps) — expressed in code + timeline |
| **Checkpoint state** | Resumable stop points only (`flow_result.state`, `stages`, …) |
| **Timeline** | Execution history including transient steps |

Transient steps (`writing_spec`, `reviewing_spec`, …) are **timeline events only**, not checkpoint states.

## 3. Canonical process graph

```text
start → write_spec → review_spec
  ├─ fail → write_spec
  └─ pass → human_spec_gate
              ├─ request_spec_revision → write_spec
              └─ approve_spec → write_plan → review_plan
                                  ├─ fail → write_plan
                                  └─ pass → [P9 stop: awaiting_implementation]
                                            [P11: implementation chain …]
```

P11 continuation:

```text
write_implementation → write_tests → run_tests → review_changes → synthesis
  ├─ owner routing → upstream write steps
  └─ pass → human_final_gate → merge
```

## 4. Human gate policy (fixed)

| Gate | Present |
|------|---------|
| spec_human_gate | **yes** |
| plan_human_gate | **no** (no state, no config flag) |
| final_human_gate | **yes** (P11) |

## 5. Node categories

Node = processing unit; NodeRun = one execution record. `exec_policy.nodes[node_name]` configures worker, argv, and model. `worker_adapter` injects `--model` then preserves existing exec flags, then appends `resume <id>` at the end of the Codex option zone (between `exec` and `--`); passthrough after `--` is untouched; non-`codex exec` argv leaves policy model as audit-only (argv unchanged). If policy omits `model` but argv already has `--model`, `NodeRun.model` may be null and evidence `argv` is authoritative. `provider_session_id` fails fast at argv application when `codex exec` is missing; requested vs applied provider session ids may differ and are both recorded. PR5: per-node model injection and explicit Codex resume only (no automatic session reuse policies yet).

1. **write** — `write_spec`, `write_plan`, `write_implementation`, `write_tests`
2. **review** — `review_spec`, `review_plan`, plus v1 change reviewers: `review_requirements`, `review_architecture`, `review_test_quality`, `review_checklist_compliance`, `review_impact`, `review_diff_detail`, `review_naming_doc`
3. **local** — `run_tests` (local command, not an LLM node — excluded from exec policy)
4. **synthesis** — aggregate review outputs, assign owner (P11)
5. **gate / merge** — human_spec_gate, human_final_gate, merge

Registry type: `dev_process.<node_name>` (e.g. `dev_process.write_spec`).

P9: Python stage runners. P10: all LLM execs via `run_node_exec()` + `node_runs[]` on checkpoint. P11: full implementation chain.

## 6. Stage artifacts

```text
.nodeflow/runs/<run>/
  spec/input.json
  spec/spec.md
  spec_review/aggregate.json
  plan/input.json
  plan/plan.md
  plan_review/aggregate.json
  revision/input.json
  implementation/…
  test_implementation/…
  review/aggregate.json
```

## 7. Checkpoint states and allowed actions (contract)

### `initialized`

| Field | Value |
|-------|-------|
| allowed_actions | *(none — `start` is not resumable)* |
| runs | — |
| next | new run only via `start` without `flow_checkpoint_path` |

### `start` action (from `initialized` or new run)

| Field | Value |
|-------|-------|
| runs | `write_spec` → `review_spec` |
| timeline | `writing_spec`, `reviewing_spec` |
| artifacts | `spec/spec.md`, `spec_review/aggregate.json` |
| next (review fail) | `awaiting_spec_revision` |
| next (review pass) | `awaiting_spec_human_gate` |

### `awaiting_spec_revision`

| Field | Value |
|-------|-------|
| allowed_actions | `revise_spec` |
| runs | `write_spec` → `review_spec` |
| revision_context | reviewer findings from last `spec_review` |
| next | `awaiting_spec_revision` \| `awaiting_spec_human_gate` |

### `awaiting_spec_human_gate`

| Field | Value |
|-------|-------|
| allowed_actions | `approve_spec`, `request_spec_revision`, `reject_spec` |
| runs (approve) | `write_plan` → `review_plan` |
| runs (request_spec_revision) | collect human comment → `write_spec` → `review_spec` |
| next (approve, plan review fail) | `awaiting_plan_revision` |
| next (approve, plan review pass) | `awaiting_implementation` (P9 terminal) |
| stale (request_spec_revision) | plan, implementation, tests, review |

### `awaiting_plan_revision`

| Field | Value |
|-------|-------|
| allowed_actions | `revise_plan` |
| runs | `write_plan` → `review_plan` |
| revision_context | plan_review findings |
| next | `awaiting_plan_revision` \| `awaiting_implementation` |

### `awaiting_implementation` (P9 terminal)

| Field | Value |
|-------|-------|
| allowed_actions | `continue_implementation` (P11), `reject_spec` |
| runs | — in P9 |
| P11 runs | implementation → tests → review → synthesis |

### `awaiting_rework_decision` (P11)

| Field | Value |
|-------|-------|
| allowed_actions | `rework_implementation`, `revise_spec`, `revise_plan`, `reject_final` |
| runs | per owner routing table |

### `awaiting_final_approval` / `awaiting_merge` / `merged` / `failed`

Same semantics as P8 final/merge paths; wired in P11.

## 8. Loop rules

**Spec loop:** review fail or human request_revision → `write_spec` with `previous_spec`, stored spec inputs, and `revision_context` (review findings ± human comment).

**Plan loop:** plan_review fail → `write_plan` with `previous_plan` and plan_review findings (optional human comment). No human gate on plan.

## 9. Stale rules

| Modified | Stale downstream |
|----------|------------------|
| spec | plan, plan_review, implementation, test_implementation, review |
| plan | plan_review, implementation, test_implementation, review |
| implementation | tests, review |
| tests | review |

## 10. Owner routing (P11)

| owner | Return chain |
|-------|----------------|
| spec | write_spec → review_spec → human_spec_gate → write_plan → … |
| plan | write_plan → review_plan → implementation chain |
| implementation | write_implementation → write_tests → run_tests → review_changes |
| test | write_tests → run_tests → review_changes |

Plan path has **no** human gate.

## 11. Phase boundaries

| Phase | Scope |
|-------|--------|
| **P9** | spec/plan split, loops, stops at `awaiting_implementation`; CLI `exec_argv` → checkpoint `exec_policy_snapshot.default_argv` |
| **P10** | `run_node_exec()` on main path; `node_runs[]` on checkpoint records every LLM execution as a `NodeRun` (`node_name`, `node_type`, `stage`, `session_id`, `evidence_path`, `worker`, `model`, `argv`); `exec_policy_snapshot.nodes[node_name]` = argv + model resolution (not `jobs`); `worker_adapter` injects model into Codex argv; `exec_policy_path` input (start-only, CLI cwd-relative) with `policy_source` audit; resume rejects `exec_policy_path`; evidence JSON includes `node_name`/`session_id`/`model`/`worker`; `provider_meta.session_id` → `provider_session_id`; unknown node names in policy file are rejected at start; `run_tests` is a local command — excluded from `NODE_NAMES` |
| **P11** | Full implementation chain: `write_implementation` → `write_tests` → `run_tests` → `review_changes` → synthesis; owner routing (`spec` → spec cycle, `plan` → plan cycle, `implementation` → full chain, `test` → tests+review only, skipping `write_implementation`); stale markers propagation on upstream revision; `human_final_gate` → `awaiting_merge` → `merge`; rework from `awaiting_rework_decision` or `awaiting_final`; `node_runs[]` continuity across rework cycles; dead states removed (`awaiting_implementation_rework`, `awaiting_test_rework`) — single `awaiting_rework_decision` state |

## 12. Phase-based plan format (v1.6+)

Plans use markdown ``## Phase N: <title>`` sections. The workflow assigns stable ids
``phase_000``, ``phase_001``, … from **order**, not from the title string.

Only strict phase-formatted plans are supported. Non-phase / legacy plans are **not**
auto-converted: ``parse_new_plan()`` must succeed before ``plan_review`` runs. Checkpoints
with old ``plan.md`` / ``plan.json`` fail on resume with an explicit message (use
``revise-plan`` or restart the run).

Implementation always uses the phase loop (``total_phases`` > 0). There is no
non-phase ``continue_implementation`` fallback; checkpoints without phase state must
regenerate the plan before implementation.

### Phase contract and `contract_sha256`

Each phase has a `contract_sha256` computed from:

- Goal, Scope (include/exclude), Test plan
- Review plan (targets, agents), Review checklist, Acceptance criteria

**Not** included: phase title (`## Phase N: …`), heading text, or `phase_NNN` id
(the id is derived separately).

| Field | Role in rework |
|-------|----------------|
| Title | Display-only for contracts; completed phases keep **historical title** in `phase_results` (plan.md title may change without invalidating the phase) |
| Contract fields above | Immutable for `status=completed` phases; `validate_rework_contracts()` compares hashes |
| Pending / later phases | May be rewritten freely on plan rework |

`review_targets` and `review_agents` are **sorted** in `contract_sha256` so list order in
the plan markdown does not affect the hash. Reviewer **execution order** is not part of
the contract.

Continuation plans append new phases after all completed ones; completed contracts
are never rewritten (see `plan_prompt.py`).

Continuation merge retries pin ``dev_process.continuation_base_plan_version`` to the
accepted plan snapshot on first entry. Each retry loads
``plan/versions/<continuation_base_plan_version>.{md,json}`` as the merge base (not
polluted ``plan/plan.md`` from a failed attempt). On ``plan_review`` fail, latest
``plan/plan.*`` is restored from that version.

``load_plan_data()`` rejects ``plan.md`` / ``plan.json`` drift (``plan_sha256`` and
parsed phase ids / ``contract_sha256``).

### Review agents v1

Phase and final reviews use four **v1 agents** (one agent = one dedicated reviewer node):

```text
requirements, architecture, test_quality, checklist_compliance
```

Optional agents (`impact`, `diff_detail`, `naming_doc`) remain parseable but are omitted from
the plan LLM prompt. **Agents** in the phase plan are aliases only; at runtime they resolve to
``review_*`` node names for execution, ``review_inputs``, and aggregation. **Targets** are prompt
supplements only.

| Agent | Node | Role (in SKILL) | Diff limit (standard) |
|-------|------|-----------------|------------------------|
| `requirements` | `review_requirements` | spec / acceptance criteria | 6000 |
| `architecture` | `review_architecture` | structure / boundaries | 8000 |
| `test_quality` | `review_test_quality` | tests / failure cases | 6000 |
| `checklist_compliance` | `review_checklist_compliance` | checklist / criteria | 8000 |

Final review always runs: `requirements`, `test_quality`, `checklist_compliance`.

Routing is ``review_config.REVIEW_AGENT_TO_NODE`` only — no central role-instruction dict in
``run_review_stage()``. Per-node focus text lives in
``skills/dev-process/nodes/<node_name>/SKILL.md`` and is injected when building the prompt.
Configure **argv** per node in ``exec_policy.nodes.<review_node_name>`` (actual model
selection is encoded in argv). The ``model`` field on each node entry is **audit metadata
only** and is not injected into worker argv. ``node_runs[].node_name`` and ``review_inputs``
keys use ``review_*`` names, not bare plan agent keys.

### Phase review plan: `targets` vs `agents`

Per-phase **Review plan** has two sub-fields with different runtime roles:

- **targets** — *what to inspect* (allowed-value validation + text appended to the review prompt supplement). Does not select which reviewer nodes run.
- **agents** — *who inspects* (determines which reviewers `run_review_stage()` executes). If omitted, the global `review_depth_preset` reviewer set is used instead.

See `plan_prompt.py` for LLM-facing wording.

Implementation: `plan_phases._compute_contract_sha256()`, `contract_check.validate_rework_contracts()`.

Checkpoint: `_run_plan_cycle()` stores `run_plan_stage()` output at
`body["stages"]["plan"]` before reading `plan_json_path` / calling `init_phase_state()`.

### Plan rework: deferred version commit

During plan rework with completed phases, `write_plan_latest_only()` may update
`plan/plan.md` **before** contract validation. Until validation passes:

- `dev_process.draft_plan_pending_contract_validation` = `true`
- `dev_process.plan_version_status` = `draft_not_committed`
- `stages.plan.accepted_plan_version` = last committed `current_plan_version`

`commit_plan_version()` runs only after `validate_rework_contracts()` succeeds.
On rejection, the draft is archived and flags are cleared after restore.

### Task branch worktree cleanup (`git_worktree`)

When `workspace_strategy=git_worktree`, `create_task_branch()` places the task worktree
under ``<repo_parent>/.nodeflow-worktrees/`` (outside ``.nodeflow/runs/<run_id>``).
The checkpoint records cleanup metadata in ``dev_process.cleanup_targets``:

```json
{"kind": "git_worktree", "branch": "phase-base/…", "worktree_path": "…", "worktree_root": "…"}
```

Operators should remove these worktrees (``git worktree remove``) when abandoning a run.

### Final review and squash audit

`stages.final_review` stores ``reviewed_branch_head`` and ``reviewed_tree`` at review time.
``merge --squash`` adds ``squash_commit``, ``squash_tree``, and
``squash_tree_matches_reviewed_tree`` so merge can proceed when the commit hash changes
but the tree matches what final review inspected.

## 13. Breaking changes

### v3 (2026-05-27)

- `schema_version`: `dev_process.flow.v3` (was `dev_process.flow.v2`)
- **v2 checkpoints are not resumable**
- `review_depth_preset` keys are v1 **agent** names (`requirements`, …), not legacy `review_diff` node names
- `exec_policy.NODE_NAMES` and reference JSON list v1 review agent nodes only

### P8 / v1

- `schema_version`: `dev_process.flow.v2` (was `dev_process.flow.v1`)
- P8 / v1 checkpoints are **not** resumable
- `stages.spec_plan` removed; use `stages.spec`, `stages.plan`, …
