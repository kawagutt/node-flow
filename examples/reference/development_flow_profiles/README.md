# Development flow profile samples (reference only)

These JSON files are **not loaded automatically** by NodeFlow unless you pass profile paths into `development_flow_pipe` params:

- `model_profiles_path` — path to a JSON object keyed by profile name, then by stage (`spec_plan`, `implement`, `review`), then by child node id (e.g. `codex_exec`, `review_diff_focused`).
- `cost_profiles_path` — same structure, for numeric / clipping params.
- `model_profile` and `cost_profile` — profile names to select from each file (both must be set to enable merging).

Alternatively, copy the nested dicts into your pipeline YAML under `params.spec_plan_pipe`, `params.implement_pipe`, and `params.review_pipe` by hand.

**Verify** model slugs and CLI flags against your local Codex installation; names in the samples are placeholders.
