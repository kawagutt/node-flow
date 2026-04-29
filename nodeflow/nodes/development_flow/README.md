# Development flow stage pipes

Built-in pipes for a development cycle. `development_flow_pipe` orchestrates actions via checkpoint/resume, while stage pipes (`spec_plan_pipe`, `implement_pipe`, `review_pipe`) execute single runs. NodeFlow does not pause in-process for human input.

## Ownership boundary

- `nodeflow/nodes/development_flow/`: implementation source of truth (registry types + concrete node classes).
- `examples/pipelines/`: usage examples only (runnable hermetic samples, templates, fixtures); these files instantiate node types and must not embed orchestration logic.

In short:

- `development_flow_pipe` = built-in node type implementation.
- `development_flow_*.yaml` = pipeline config that uses that node type.

YAML registry keys:

| `type`             | Purpose |
|--------------------|---------|
| `development_flow_pipe` | Top-level orchestration (`start` / `approve` / `rework_implementation` / `revise_spec` / `merge` / `force_merge`) with flow checkpoint output for resume. |
| `spec_plan_pipe`   | Collect repo context (git), run Codex (or other CLI) with that context on **stdin**, write checkpoint. |
| `implement_pipe`   | Load **one** approved JSON (`approved_checkpoint_path`), run implement CLI (stdin = full prompt), tests, `git diff <base_ref>`, write checkpoint. |
| `review_pipe`      | Load the same approved JSON, `git diff <base_ref>`, build 5 review prompts (diff / wide scan / tests / spec conformance / spec revision), run 5 review CLIs (**stdin** = prompt; **stdout** = JSON contract), aggregate, write checkpoint. |

Naming convention:

- `*_hermetic.yaml`: runnable, CI-safe sample.
- `*_codex_template.yaml`: Codex CLI template; edit argv and model names before use.
- `dev_cycle_*`: stage-level example.
- `development_flow_*`: top-level flow example.

Examples by role:

- Stage hermetic: `examples/pipelines/dev_cycle_spec_plan.yaml`, `examples/pipelines/dev_cycle_implement.yaml`, `examples/pipelines/dev_cycle_review.yaml`
- Stage Codex templates: `examples/pipelines/dev_cycle_spec_plan_codex_template.yaml`, `examples/pipelines/dev_cycle_implement_codex_template.yaml`, `examples/pipelines/dev_cycle_review_codex_template.yaml`
- Top-level flow: `examples/pipelines/development_flow_hermetic.yaml` (runnable), `examples/pipelines/development_flow_codex_template.yaml` (template)

Fixture for implement/review inputs:

- `examples/pipelines/fixtures/approved_development_flow_stub.json` — must contain top-level **`spec`** and **`plan`** (strings or JSON-serializable values).

## Directory layout (matches pipe hierarchy)

```text
nodeflow/nodes/development_flow/
  README.md                 # this file
  __init__.py
  common/
    collect_diff.py         # git diff <base_ref>, status --short, untracked + text excerpts
    load_checkpoint.py      # approved_checkpoint_path (single-file); paths vs repo_root
    write_checkpoint.py     # merges child ok into stage_result.ok; next_action_on_failure
    pipe_helpers.py         # shared fatal-child reporting
    check_source_workspace.py
    git_repo.py
    git_status.py
    prepare_workspace.py
    prepare_development_run_context.py
    write_development_summary.py
  development_flow_pipe/
    pipe.py                # top-level orchestration with checkpoint/resume actions
    profiles.py            # model/cost profile loading + merge helpers
    state_machine.py       # merge gate + allowed_actions helpers
  spec_plan_pipe/
    pipe.py
    collect_repo_context.py # git rev-parse + status + diff excerpt; builds Codex stdin body
  implement_pipe/
    pipe.py
    run_tests.py
  review_pipe/
    pipe.py
    review_parse.py         # JSON contract text + parse helpers (raw_decode); importable from other stages
    aggregate_reviews.py    # parses JSON from review stdout; merges with diff/exec signals
    build_diff_review_prompt.py
    build_wide_scan_review_prompt.py
    build_test_review_prompt.py
    build_spec_review_prompt.py
    build_spec_revision_review_prompt.py
```

`PipeNode` subclasses only wire children; semantics live in the `ActionNode` modules under each folder (or under `common/`).

## Approved checkpoint (P0)

**Preferred:** pipeline input `approved_checkpoint_path` → path to one JSON file:

```json
{
  "spec": "markdown or text …",
  "plan": "markdown or text …"
}
```

`implement_pipe` and `review_pipe` use this shape.

**Path resolution:** `approved_checkpoint_path` is resolved under pipeline input **`repo_root`** when the path is relative.

`LoadCheckpointNode` accepts only **`approved_checkpoint_path`** and expects top-level `spec`/`plan` in that single JSON.

## Codex stdin (`codex_exec` / nested argv)

`CodexExecNode` passes graph input port **`prompt`** to the subprocess as **`stdin`** when it is a non-empty string. argv is still required (e.g. `codex exec …` or a hermetic `python -c …` in examples).

## Review JSON contract (stdout)

Each review subprocess should print **one JSON object** on stdout (no markdown fences). Schema:

```json
{
  "ok": true,
  "blocking_findings": [
    {
      "id": "R001",
      "area": "diff",
      "summary": "…",
      "suggested_fix": "optional"
    }
  ],
  "non_blocking_findings": [],
  "spec_revision_needed": false
}
```

`AggregateReviewsNode` parses this from each review’s `execution_result` (stdout/stderr). If JSON is missing or invalid, a **blocking** parse finding is recorded. Subprocess `ok: false` still blocks as before.

The exact instruction text is in `review_pipe/review_parse.py` (`REVIEW_JSON_CONTRACT_TEXT`) and is prepended by the prompt builder nodes. Other stages (e.g. `implement_pipe`) may import from `review_pipe.review_parse` if they need the same contract.

## `stage_result` contract (P0)

Each stage pipe’s root output exposes a `stage_result` port (dict) with at least:

- `ok` (bool) — **`false` if any bound child signal failed** (`execution_result.ok`, `test_result.ok`, `diff_result.ok`, `review_result.ok` when passed to `write_checkpoint`), combined with optional `request.ok` from upstream checkpoint metadata.
- `stage` — one of `spec_plan`, `implement`, `review`
- `summary` (string)
- `artifacts` — list of `{ "path", "kind" }` (spec_plan may list **`spec_plan_candidate`** before the main **`checkpoint`**).
- `next_action` — e.g. `approve`, `review`, `rework_implementation`, `revise_spec`, `merge`, `stop`. When **`ok` is false**, `WriteCheckpointNode` **does not read** `request.next_action` (avoids a stale `"approve"`); it uses **`request.next_action_on_failure`** (set by `aggregate_reviews`) or **`params.next_action_on_failure`** (`implement_pipe` / `spec_plan_pipe` setdefaults), else **`stop`**.
- `human_decision_required` (bool)
- `raw_results` (dict)
- **`approved_candidate_path`** (optional, spec_plan only) — path to a slim JSON file `{ "spec", "plan" }` parsed from the draft executor stdout, suitable as **`approved_checkpoint_path`** for `implement_pipe` / `review_pipe` without hand-editing the full stage checkpoint.

Checkpoint files default under `.nodeflow/checkpoints/` (override via `write_checkpoint` params). If `checkpoint_dir` is relative, it is resolved under **`repo_root`** (pipeline input passed into `write_checkpoint` as `_repo_root_for_paths`) when set, else under CLI **`-w` / `_workspace_dir`**. JSON is written with **`ensure_ascii=False`** so non-ASCII prompts remain readable.

Checkpoint payload has top-level `schema_version` (default: `development_flow.v1`).

### `diff_result` extras

`CollectDiffNode` adds **`status_short`**, **`untracked_files`**, and **`untracked_file_excerpts`** (first N text files, truncated). Review prompts include these. **`AggregateReviewsNode`** treats non-empty **`untracked_files`** as a **blocking** finding (`R_UNTRACKED_FILES`) so merge is not suggested while important paths are still untracked. By default **`ignored_changed_file_prefixes`** is `[".nodeflow/"]` so local checkpoints under `.nodeflow/` are not counted as untracked noise (override with an explicit list, including `[]` to disable).

## Params (per graph node `params` on the stage pipe)

Keys are **child node ids** inside the composite pipe. Typical nesting:

- **`codex_exec`** (spec/implement) or **`review_diff_focused`** / **`review_wide_scan`** / **`review_test_focused`** / **`review_spec_conformance`** / **`review_spec_revision`** — `argv`, `timeout`, `cwd`, … (same as standalone `codex_exec`).
- **`write_checkpoint`** — `checkpoint_dir`, `run_id`, `stage`, `next_action_default`, `summary_default`, **`write_spec_plan_candidate`** (default **true** in `spec_plan_pipe`; writes `{run_id}_{spec_plan_candidate_suffix}.json`), **`spec_plan_candidate_suffix`** (default `approved_candidate`), etc. When `artifact_root` is provided by `development_flow_pipe`, stage pipes fail fast if `write_checkpoint.checkpoint_dir` is also explicitly set.
- **`collect_repo_context`** — `max_diff_chars`, `untracked_excerpt_max_files`, `untracked_excerpt_max_bytes`, `ignored_untracked_prefixes` (same defaults as `collect_diff`).
- **`collect_diff`** — `max_chars` (optional; default truncates long diffs), `ignored_changed_file_prefixes` (optional; default skips `.nodeflow/`).
- **`build_diff_review_prompt`** / **`build_wide_scan_review_prompt`** / **`build_test_review_prompt`** / **`build_spec_review_prompt`** / **`build_spec_revision_review_prompt`** — `max_diff_chars` clipping inside the prompt.
- **`aggregate_reviews`** — `spec_revision_needed_default` (bool).

### `git diff` behaviour

`CollectDiffNode` runs `git diff <base_ref>` (e.g. `base_ref=HEAD`) so **uncommitted** implementation edits show up. It does **not** use `base...HEAD` triple-dot or `--staged`-only modes that hide working-tree changes.

## Control flow (development_flow_pipe)

`development_flow_pipe` is designed for checkpoint/resume (no paused runtime):

- Inputs accepted by top-level `development_flow_pipe`: `action`, `task_prompt`, `repo_root`, `flow_checkpoint_path`, `human_comment_path`, `human_comment_text`, `planned_branch_name`, `development_name`, `run_id`.
- `development_flow_pipe` rejects `base_ref`, `branch_name`, and `approved_checkpoint_path` (those are stage-level concerns or flow-checkpoint-derived values).

- `action=start`: run `check_source_workspace` (git repo + clean working tree; detached HEAD is rejected by default), then `prepare_development_run_context`, then `spec_plan_pipe`, then `flow_result.state=awaiting_approval`.
  This step does not create/switch branches or workspaces, but it may create a run artifact directory under `.nodeflow/runs/`. Rejects `flow_checkpoint_path` (always a fresh spec-plan run).
- `action=revise_spec`: requires previous state `awaiting_review_decision`, `flow_checkpoint_path`, and a prior `review_checkpoint_path`; requires `run_context.source_base_revision` (from `start`). Clears `workspace_context`, then runs `spec_plan_pipe` with `revision_context` from the review. If `task_prompt` is empty, it uses `flow_result.task_prompt`; if missing, action fails fast.
- `action=approve`: requires previous state `awaiting_approval` and `flow_checkpoint_path`; uses `approved_candidate_path` / `approved_checkpoint_path` from that checkpoint, then prepares workspace (`prepare_workspace`, `strategy=current_repo`), runs `implement_pipe` and `review_pipe`, and sets `flow_result.state=awaiting_review_decision`.
- `action=rework_implementation`: requires previous state `awaiting_review_decision`, `flow_checkpoint_path`, a valid previous review checkpoint, and checkpoint `workspace_context`; it reuses that `workspace_context` and fails fast on inconsistency.
- **`current_repo` + `revise_spec`:** `revise_spec` requires the source repository to be clean, and `HEAD` must still match `run_context.source_base_revision`. Reset or stash previous implementation edits before revising the spec.
- **`current_repo` + `rework_implementation`:** `prepare_workspace` reuses `workspace_context` from the checkpoint and **does not** require a clean source tree, so you can iterate on the same dirty working tree while `base_revision` stays fixed.
- After `approve` / `rework_implementation`, `write_development_summary` writes a summary artifact and proposes a commit message based on implementation diff, review result, commit template, and recent commit style.
- `action=merge`: requires `flow_checkpoint_path` from `awaiting_review_decision`, `flow_result.ok == true`, `implement_stage_result.ok == true`, `review_stage_result.ok == true`, and `next_action == "merge"`. Otherwise use `action=force_merge` (still requires `flow_checkpoint_path`).
- `action=force_merge`: records merged state without the strict merge gate (human override) and writes audit fields (`forced`, `previous_flow_checkpoint_path`, optional `force_merge_reason` / human comment fields).

`flow_result.ok` after implement+review is **both** `implement_stage_result.ok` and `review_stage_result.ok`.
`flow_result.ok` means stages completed successfully; it does not by itself mean merge is allowed. Use `merge_ready` and `allowed_actions` / `next_action`.
`flow_result.merge_ready` is true only when merge is actually allowed by flow policy.

`flow_result` includes `allowed_actions` (narrowed when a stage fails) and `flow_checkpoint_path` (also embedded in the written JSON payload). The next invocation passes `action` and `flow_checkpoint_path` to continue.
For `action=start`, `flow_result` includes `run_context` (with `source_repo_root`, `source_base_revision`, `source_current_branch` frozen from that moment) and no concrete workspace yet.
For `action=approve` / `action=rework_implementation`, `flow_result` includes `run_context`, `workspace_context`, and `development_summary` (`commit_message_suggestion` + artifact path).
Resume actions (`approve`, `rework_implementation`, `revise_spec`, `merge`, `force_merge`) require checkpoint `flow_result.run_context.source_repo_root`; checkpoints without this field are not supported.

`approve` / `rework_implementation` use `run_context.source_base_revision` (not pipeline `base_ref` at resume time) for `prepare_workspace` and stage `base_ref`, so implementation/review stay aligned with the tree as of `start`. With `strategy=current_repo`, a fresh `approve` always requires `HEAD == source_base_revision`.
For `development_flow_pipe`, invocation-time `base_ref` does not change the frozen base during resume actions. `start` freezes `HEAD` into `run_context.source_base_revision`, and resume actions always use that value.

`prepare_development_run_context.branch_prefix` plus the generated slug produce names like `feat/nodeflow/001-add-config-validation` (slash after the prefix). This branch name is currently recorded as planned metadata in `run_context` / `workspace_context`; `development_flow` does not create or switch branches in current-repo mode.

`write_development_summary` drops paths under `ignored_changed_file_prefixes` (default `[".nodeflow/"]`) from `changed_files`, consistent with clean-check ignores.

Optional profile merge (fail-fast if profile names or files are wrong): set on `development_flow_pipe` params **`model_profiles_path`**, **`cost_profiles_path`**, **`model_profile`**, **`cost_profile`** (all four required together; partial config errors). Merge priority is `YAML direct params < model profile < cost profile`. Sample files live under `examples/reference/development_flow_profiles/` (reference only unless you wire these params).

`prepare_development_run_context.artifact_root_dir` controls where run artifacts are stored (`<artifact_root_dir>/<run_dir_name>` where `run_dir_name` is `<index>_<yyyymmdd>_<slug>`). `run_id` remains an internal ID in JSON/checkpoint references and is sourced from `development_flow_pipe` input `run_id` (or auto-generated when omitted).
`start` may create the per-run artifact directory before `spec_plan_pipe` completes; failed starts can leave an empty or partial run directory.

When `development_flow_pipe` passes `artifact_root` into stage pipes, checkpoints and related stage outputs are written under:

- `artifact_root/spec_plan/` — spec plan stage (`write_checkpoint`)
- `artifact_root/implement/` — implement stage
- `artifact_root/review/` — review stage
- `artifact_root/summary/` — `write_development_summary`

Top-level **flow** checkpoints (`flow_checkpoint.checkpoint_dir`, filenames like `{run_id}_{action}_flow.json`) may still live under `.nodeflow/checkpoints/` until migrated; they are separate from per-run stage artifacts under `artifact_root`.

**`workspace_context` and branch names:** with `strategy=current_repo`, the node does **not** switch branches. `workspace_context.current_branch` is the checked-out branch; `planned_branch_name` is planned naming metadata.

**`.nodeflow/` and clean checks:** keep `.nodeflow/` in `.gitignore` when possible. `check_source_workspace` / `prepare_workspace` (fresh prep only) treat paths under `.nodeflow/` as non-dirty by default (`ignored_dirty_prefixes`, default `[".nodeflow/"]`) so local flow metadata does not block runs.

## CLI examples

From the repository root:

```bash
nodeflow examples/pipelines/dev_cycle_spec_plan.yaml -w . \
  -i task_prompt="Describe the change" -i repo_root=. -i base_ref=HEAD
```

Implement (uses committed fixture; path is workspace-relative when using `-w .`):

```bash
nodeflow examples/pipelines/dev_cycle_implement.yaml -w . \
  -i approved_checkpoint_path=examples/pipelines/fixtures/approved_development_flow_stub.json \
  -i repo_root=. -i base_ref=HEAD -i task_type=implement
```

Review:

```bash
nodeflow examples/pipelines/dev_cycle_review.yaml -w . \
  -i approved_checkpoint_path=examples/pipelines/fixtures/approved_development_flow_stub.json \
  -i repo_root=. -i base_ref=HEAD -i task_type=review
```

For production, replace hermetic `argv` under `codex_exec` / review nodes with real **`codex exec`** (or equivalent) invocations; keep **stdin**-driven prompts so diff and spec text actually reach the model.

## Model/cost profiles (P2)

Reference samples (merge into YAML params manually, or point `development_flow_pipe` at them with the four params listed above):

- `examples/reference/development_flow_profiles/model_profiles.json`
- `examples/reference/development_flow_profiles/cost_profiles.json`
- `examples/reference/development_flow_profiles/README.md`

Verify model names available in your Codex CLI environment before use.

## P2 review lens semantics

- `wide_scan` means wider **change-set** review (diff/status/untracked context), not repository-wide file scanning.
- `test_focused` currently targets likely test gaps from change context; it does not inspect runtime test logs unless future wiring passes test outputs into review prompts.
