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
    load_checkpoint.py      # approved_checkpoint_path (+ legacy two-file); paths vs repo_root
    write_checkpoint.py     # merges child ok into stage_result.ok; next_action_on_failure
    pipe_helpers.py         # shared fatal-child reporting
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

`CollectDiffNode` adds **`status_short`**, **`untracked_files`**, and **`untracked_file_excerpts`** (first N text files, truncated). Review prompts include these. **`AggregateReviewsNode`** treats non-empty **`untracked_files`** as a **blocking** finding (`R_UNTRACKED_FILES`) so merge is not suggested while important paths are still untracked. By default **`ignored_untracked_prefixes`** is `[".nodeflow/"]` so local checkpoints under `.nodeflow/` are not counted as untracked noise (override with an explicit list, including `[]` to disable).

## Params (per graph node `params` on the stage pipe)

Keys are **child node ids** inside the composite pipe. Typical nesting:

- **`codex_exec`** (spec/implement) or **`review_diff_focused`** / **`review_wide_scan`** / **`review_test_focused`** / **`review_spec_conformance`** / **`review_spec_revision`** — `argv`, `timeout`, `cwd`, … (same as standalone `codex_exec`).
- **`write_checkpoint`** — `checkpoint_dir`, `run_id`, `stage`, `next_action_default`, `summary_default`, **`write_spec_plan_candidate`** (default **true** in `spec_plan_pipe`; writes `{run_id}_{spec_plan_candidate_suffix}.json`), **`spec_plan_candidate_suffix`** (default `approved_candidate`), etc.
- **`collect_repo_context`** — `max_diff_chars`, `untracked_excerpt_max_files`, `untracked_excerpt_max_bytes`, `ignored_untracked_prefixes` (same defaults as `collect_diff`).
- **`collect_diff`** — `max_chars` (optional; default truncates long diffs), `ignored_untracked_prefixes` (optional; default skips `.nodeflow/`).
- **`build_diff_review_prompt`** / **`build_wide_scan_review_prompt`** / **`build_test_review_prompt`** / **`build_spec_review_prompt`** / **`build_spec_revision_review_prompt`** — `max_diff_chars` clipping inside the prompt.
- **`aggregate_reviews`** — `spec_revision_needed_default` (bool).

### `git diff` behaviour

`CollectDiffNode` runs `git diff <base_ref>` (e.g. `base_ref=HEAD`) so **uncommitted** implementation edits show up. It does **not** use `base...HEAD` triple-dot or `--staged`-only modes that hide working-tree changes.

## Control flow (development_flow_pipe)

`development_flow_pipe` is designed for checkpoint/resume (no paused runtime):

- `action=start`: run `spec_plan_pipe`, then `flow_result.state=awaiting_approval`.
- `action=revise_spec`: requires previous state `awaiting_review_decision`, `flow_checkpoint_path`, and a prior `review_checkpoint_path` inside it; runs `spec_plan_pipe` with `revision_context` from that review. If `task_prompt` input is empty, it restores the previous `task_prompt` from top-level `flow_result.task_prompt` (fallback: previous `stage_result.raw_results.task_prompt`).
- `action=approve`: requires previous state `awaiting_approval`; runs `implement_pipe` then `review_pipe`, then `flow_result.state=awaiting_review_decision`. You may omit `approved_checkpoint_path` if you pass `flow_checkpoint_path` from the last `awaiting_approval` step (uses `approved_candidate_path` / `approved_checkpoint_path` stored there). If you omit `approved_checkpoint_path`, `flow_checkpoint_path` is **required**.
- `action=rework_implementation`: requires previous state `awaiting_review_decision`, `flow_checkpoint_path`, and a valid previous review checkpoint; passes **rework_context** into `implement_pipe`.
- `action=merge`: requires `flow_checkpoint_path` from `awaiting_review_decision`, `flow_result.ok == true`, `implement_stage_result.ok == true`, `review_stage_result.ok == true`, and `next_action == "merge"`. Otherwise use `action=force_merge` (still requires `flow_checkpoint_path`).
- `action=force_merge`: records merged state without the strict merge gate (human override) and writes audit fields (`forced`, `previous_flow_checkpoint_path`, optional `force_merge_reason` / human comment fields).
- `action=start`: rejects `flow_checkpoint_path` (fail-fast; start is always a fresh spec-plan run).

`flow_result.ok` after implement+review is **both** `implement_stage_result.ok` and `review_stage_result.ok`.
`flow_result.ok` means stages completed successfully; it does not by itself mean merge is allowed. Use `merge_ready` and `allowed_actions` / `next_action`.
`flow_result.merge_ready` is true only when merge is actually allowed by flow policy.

`flow_result` includes `allowed_actions` (narrowed when a stage fails) and `flow_checkpoint_path` (also embedded in the written JSON payload). The next invocation passes `action` and `flow_checkpoint_path` to continue.

Optional profile merge (fail-fast if profile names or files are wrong): set on `development_flow_pipe` params **`model_profiles_path`**, **`cost_profiles_path`**, **`model_profile`**, **`cost_profile`** (all four required together; partial config errors). Merge priority is `YAML direct params < model profile < cost profile`. Sample files live under `examples/reference/development_flow_profiles/` (reference only unless you wire these params).

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
