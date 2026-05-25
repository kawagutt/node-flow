"""review_plan node."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

from nodeflow.workflows.dev_process.constants import EXEC_TIMEOUT_SECONDS
from nodeflow.workflows.dev_process.evidence import record_exec_evidence
from nodeflow.workflows.dev_process.hermetic_argv import plan_review_argv
from nodeflow.workflows.dev_process.paths import assert_path_under_run_dir
from nodeflow.workflows.dev_process.stages.review_aggregate import (
    aggregate_stage_review,
    append_review_json_contract,
)
from nodeflow.workflows.dev_process.workers import resolve_exec_worker, run_exec


def run_plan_review_stage(
    *,
    repo_root: Path,
    artifact_root: str,
    run_id: str,
    task_prompt: str,
    spec_text: str,
    plan_text: str,
    exec_argv: list[str] | None = None,
    exec_worker_kind: Optional[str] = None,
    force_blocking: bool = False,
    body: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    prompt_text = append_review_json_contract(
        "Review the plan for feasibility against the spec.\n\n"
        f"Task:\n{task_prompt}\n\n"
        f"## Spec\n{spec_text}\n\n"
        f"## Plan\n{plan_text}\n"
    )
    cwd = str(repo_root)

    if body is not None:
        from nodeflow.workflows.dev_process.node_runner import run_node_exec

        blocking_argv = plan_review_argv(blocking=True) if force_blocking else None
        execution_output, evidence_path, _rec = run_node_exec(
            body,
            node_name="review_plan",
            stage="plan_review",
            prompt=prompt_text,
            cwd=cwd,
            run_id=run_id,
            artifact_root=artifact_root,
            argv_override=blocking_argv,
        )
    else:
        worker = resolve_exec_worker(exec_worker_kind)
        argv = exec_argv if exec_argv is not None else plan_review_argv(blocking=force_blocking)
        execution_output = run_exec(
            worker, prompt=prompt_text, cwd=cwd, argv=argv, timeout=EXEC_TIMEOUT_SECONDS
        )
        evidence_path = record_exec_evidence(
            artifact_root=artifact_root,
            run_id=run_id,
            stage="plan_review",
            invoker=worker.invoker,
            execution_output=execution_output,
            argv=argv,
            prompt=prompt_text,
            cwd=cwd,
        )

    aggregate = aggregate_stage_review(execution_output, stage="plan_review")
    out_dir = Path(artifact_root) / "plan_review"
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
