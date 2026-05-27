"""review_spec node."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from nodeflow.workflows.dev_process.node_runner import review_argv_override_from_body, run_node_exec
from nodeflow.workflows.dev_process.paths import assert_path_under_run_dir
from nodeflow.workflows.dev_process.stages.review_aggregate import (
    aggregate_stage_review,
    append_review_json_contract,
)


def run_spec_review_stage(
    *,
    repo_root: Path,
    artifact_root: str,
    run_id: str,
    task_prompt: str,
    spec_text: str,
    body: Dict[str, Any],
) -> Dict[str, Any]:
    prompt_text = append_review_json_contract(
        "Review the specification for completeness and feasibility.\n\n"
        f"Task:\n{task_prompt}\n\n"
        f"## Spec\n{spec_text}\n"
    )
    cwd = str(repo_root)

    execution_output, evidence_path, _rec = run_node_exec(
        body,
        node_name="review_spec",
        stage="spec_review",
        prompt=prompt_text,
        cwd=cwd,
        run_id=run_id,
        artifact_root=artifact_root,
        argv_override=review_argv_override_from_body(body),
    )

    aggregate = aggregate_stage_review(execution_output, stage="spec_review")
    dp = body.get("dev_process") if isinstance(body.get("dev_process"), dict) else None
    if dp is not None:
        from nodeflow.workflows.dev_process.artifact_versions import (
            review_aggregate_metadata,
            spec_review_dir,
        )

        aggregate.update(review_aggregate_metadata(dp, review_scope="spec"))
        out_dir = spec_review_dir(artifact_root, dp)
    else:
        out_dir = Path(artifact_root) / "spec_review"
    out_dir.mkdir(parents=True, exist_ok=True)
    agg_path = out_dir / "aggregate.json"
    agg_path.write_text(json.dumps(aggregate, indent=2), encoding="utf-8")
    assert_path_under_run_dir(artifact_root, str(agg_path))
    return {
        "status": "completed",
        "aggregate": aggregate,
        "aggregate_path": str(agg_path),
        "decision": aggregate["decision"],
        "evidence_paths": [evidence_path],
    }
