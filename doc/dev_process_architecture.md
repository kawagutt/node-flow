# Dev-process architecture (implementation contract)

Canonical specification for NodeFlow dev-process v2 (`dev_process.flow.v2`).  
Implementations and tests must match this document.

See also: [dev_process.md](./dev_process.md) (operator guide).

## 1. Purpose

Define the **process graph**, **checkpoint states**, **artifacts**, and **phase boundaries** (P9 / P10 / P11) so `DevProcessFlowNode` can orchestrate without ad-hoc state in code.

## 2. Three-layer model

| Layer | Role |
|-------|------|
| **process graph** | True execution flow (loops, steps) |
| **checkpoint state** | Resumable stop points only |
| **timeline** | Execution history including transient steps |

Transient steps (`writing_spec`, `reviewing_spec`, …) are **timeline events only**, not checkpoint states.

`DevProcessFlowNode` is an **orchestrator** (ActionNode). It is **not** a PipeNode.

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

Node = processing unit; NodeRun = one execution record. `exec_policy.nodes[node_name]` configures worker and argv (active), plus model (audit metadata only — not injected into worker argv; actual model selection is determined by argv).

1. **write** — `write_spec`, `write_plan`, `write_implementation`, `write_tests`
2. **review** — `review_spec`, `review_plan`, plus change reviewers: `review_diff`, `review_tests`, `review_spec_conformance`, `review_wide`, `review_spec_revision`
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
| **P10** | `run_node_exec()` on main path; `node_runs[]` on checkpoint records every LLM execution as a `NodeRun` (`node_name`, `node_type`, `stage`, `session_id`, `evidence_path`, `worker`, `model`, `argv`); `exec_policy_snapshot.nodes[node_name]` = argv resolution (not `jobs`); `exec_policy_path` input (start-only, CLI cwd-relative) with `policy_source` audit; resume rejects `exec_policy_path`; `model` is audit metadata only (not injected into worker argv; actual model determined by argv); evidence JSON includes `node_name`/`session_id`/`model`/`worker`; `provider_meta.session_id` → `provider_session_id`; unknown node names in policy file are rejected at start; `run_tests` is a local command — excluded from `NODE_NAMES` |
| **P11** | Full implementation chain: `write_implementation` → `write_tests` → `run_tests` → `review_changes` → synthesis; owner routing (`spec` → spec cycle, `plan` → plan cycle, `implementation` → full chain, `test` → tests+review only, skipping `write_implementation`); stale markers propagation on upstream revision; `human_final_gate` → `awaiting_merge` → `merge`; rework from `awaiting_rework_decision` or `awaiting_final`; `node_runs[]` continuity across rework cycles; dead states removed (`awaiting_implementation_rework`, `awaiting_test_rework`) — single `awaiting_rework_decision` state |

## 12. Breaking changes from P8

- `schema_version`: `dev_process.flow.v2` (was `dev_process.flow.v1`)
- P8 checkpoints are **not** resumable
- `stages.spec_plan` removed; use `stages.spec`, `stages.plan`, …
