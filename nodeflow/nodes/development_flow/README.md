# Development flow stage pipes

Built-in **stage** `PipeNode` types for a development cycle: spec/plan, implementation, and review. Each stage is a **single run**; NodeFlow does not pause for human input inside the graph. After a run finishes, a **checkpoint JSON** is written and humans (or an outer driver script) decide what to run next.

YAML registry keys:

| `type`             | Purpose |
|--------------------|---------|
| `spec_plan_pipe`   | Collect repo context (git), run Codex (or other CLI) with that context on **stdin**, write checkpoint. |
| `implement_pipe`   | Load **one** approved JSON (`approved_checkpoint_path`), run implement CLI (stdin = full prompt), tests, `git diff <base_ref>`, write checkpoint. |
| `review_pipe`      | Load the same approved JSON, `git diff <base_ref>`, build two review prompts (diff + spec/plan+diff), run two review CLIs (**stdin** = prompt; **stdout** = JSON contract), aggregate, write checkpoint. |

Example pipelines (hermetic by default — no Codex binary required):

- `examples/pipelines/dev_cycle_spec_plan.yaml`
- `examples/pipelines/dev_cycle_implement.yaml`
- `examples/pipelines/dev_cycle_review.yaml`

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
    build_spec_review_prompt.py
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

**Path resolution:** `approved_checkpoint_path` (and legacy two-file paths) are resolved under pipeline input **`repo_root`** when the path is relative.

**Legacy (optional):** `LoadCheckpointNode` still accepts `approved_spec_path` + `approved_plan_path` (two JSON files) if you call it from a custom graph; the built-in `implement_pipe` / `review_pipe` YAML bindings only wire **`approved_checkpoint_path`** and **`repo_root`**.

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

### `diff_result` extras

`CollectDiffNode` adds **`status_short`**, **`untracked_files`**, and **`untracked_file_excerpts`** (first N text files, truncated). Review prompts include these. **`AggregateReviewsNode`** treats non-empty **`untracked_files`** as a **blocking** finding (`R_UNTRACKED_FILES`) so merge is not suggested while important paths are still untracked. By default **`ignored_untracked_prefixes`** is `[".nodeflow/"]` so local checkpoints under `.nodeflow/` are not counted as untracked noise (override with an explicit list, including `[]` to disable).

## Params (per graph node `params` on the stage pipe)

Keys are **child node ids** inside the composite pipe. Typical nesting:

- **`codex_exec`** (spec/implement) or **`review_diff_focused`** / **`review_spec_conformance`** — `argv`, `timeout`, `cwd`, … (same as standalone `codex_exec`).
- **`write_checkpoint`** — `checkpoint_dir`, `run_id`, `stage`, `next_action_default`, `summary_default`, **`write_spec_plan_candidate`** (default **true** in `spec_plan_pipe`; writes `{run_id}_{spec_plan_candidate_suffix}.json`), **`spec_plan_candidate_suffix`** (default `approved_candidate`), etc.
- **`collect_repo_context`** — `max_diff_chars`, `untracked_excerpt_max_files`, `untracked_excerpt_max_bytes`, `ignored_untracked_prefixes` (same defaults as `collect_diff`).
- **`collect_diff`** — `max_chars` (optional; default truncates long diffs), `ignored_untracked_prefixes` (optional; default skips `.nodeflow/`).
- **`build_diff_review_prompt`** / **`build_spec_review_prompt`** — `max_diff_chars` clipping inside the prompt.
- **`aggregate_reviews`** — `spec_revision_needed_default` (bool).

### `git diff` behaviour

`CollectDiffNode` runs `git diff <base_ref>` (e.g. `base_ref=HEAD`) so **uncommitted** implementation edits show up. It does **not** use `base...HEAD` triple-dot or `--staged`-only modes that hide working-tree changes.

## Control flow (outer driver)

1. Run `dev_cycle_spec_plan`; inspect `stage_result` and checkpoint JSON. When the draft stdout parses as `{ "spec", "plan" }`, **`approved_candidate_path`** points at a ready-made file for the next stage.
2. Human may rename or edit that file, or produce another **approved** JSON with top-level `spec` + `plan`.
3. Run `dev_cycle_implement` with `-i approved_checkpoint_path=…` (often the candidate path from step 1).
4. Run `dev_cycle_review` with the same `approved_checkpoint_path`; interpret `stage_result.next_action` outside NodeFlow.

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

## Operational YAML samples (Codex)

If you use a real Codex CLI, keep nested argv in a separate file (e.g. `dev_cycle_spec_plan.codex.yaml`) and pass non-interactive flags per Codex documentation. The hermetic `examples/pipelines/dev_cycle_*.yaml` files are intended to stay **CI-safe** without the Codex binary.
