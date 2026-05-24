"""review stage — reviewers + aggregate."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.constants import EXEC_TIMEOUT_SECONDS
from nodeflow.workflows.dev_process.evidence import record_exec_evidence
from nodeflow.workflows.dev_process.hermetic_argv import review_argv
from nodeflow.workflows.dev_process.paths import assert_path_under_run_dir
from nodeflow.workflows.dev_process.reuse import (
    aggregate_reviews,
    build_review_prompt,
    write_stage_checkpoint,
)
from nodeflow.workflows.dev_process.review_presets import normalize_preset, reviewer_keys_for_preset
from nodeflow.workflows.dev_process.workers import ExecWorker, resolve_exec_worker, run_exec


def _run_one_reviewer(
    *,
    repo_root: Path,
    base_revision: str,
    diff_result: Dict[str, Any],
    test_result: Dict[str, Any],
    approved_spec: str,
    approved_plan: str,
    reviewer_key: str,
    exec_argv: list[str],
    worker: ExecWorker,
) -> tuple[Dict[str, Any], str, str]:
    text = build_review_prompt(
        reviewer_key,
        repo_root=repo_root,
        base_revision=base_revision,
        diff_result=diff_result,
        test_result=test_result,
        approved_spec=approved_spec,
        approved_plan=approved_plan,
    )
    cwd = str(repo_root)
    er = run_exec(worker, prompt=text, cwd=cwd, argv=exec_argv, timeout=EXEC_TIMEOUT_SECONDS)
    return er, text, cwd


def run_review_stage(
    *,
    repo_root: Path,
    artifact_root: str,
    run_id: str,
    base_revision: str,
    approved_spec: str,
    approved_plan: str,
    diff_result: Dict[str, Any],
    test_result: Dict[str, Any],
    exec_argv: list[str] | None = None,
    codex_argv: list[str] | None = None,
    force_blocking: bool = False,
    review_depth_preset: str = "standard",
    exec_worker_kind: Optional[str] = None,
) -> Dict[str, Any]:
    worker = resolve_exec_worker(exec_worker_kind)
    argv = exec_argv if exec_argv is not None else codex_argv
    argv = argv if argv is not None else review_argv(blocking=force_blocking)
    preset = normalize_preset(review_depth_preset)
    active_keys = reviewer_keys_for_preset(preset)
    expected = set(active_keys)
    review_inputs: Dict[str, Any] = {}
    evidence_paths: List[str] = []
    for input_key in active_keys:
        er, prompt_text, cwd = _run_one_reviewer(
            repo_root=repo_root,
            base_revision=base_revision,
            diff_result=diff_result,
            test_result=test_result,
            approved_spec=approved_spec,
            approved_plan=approved_plan,
            reviewer_key=input_key,
            exec_argv=argv,
            worker=worker,
        )
        review_inputs[input_key] = er
        if not er.get("ok"):
            raise NodeExecutionFailure(f"review subprocess failed for {input_key}")
        ep = record_exec_evidence(
            artifact_root=artifact_root,
            run_id=run_id,
            stage="review",
            invoker=input_key,
            execution_output=er,
            argv=argv,
            prompt=prompt_text,
            cwd=cwd,
        )
        evidence_paths.append(ep)

    actual = set(review_inputs)
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        raise NodeExecutionFailure(
            f"review preset {preset!r} incomplete: missing={missing!r} extra={extra!r}"
        )

    review_result, checkpoint_request = aggregate_reviews(
        review_inputs=review_inputs,
        test_result=test_result,
        diff_result=diff_result,
    )

    stage_result = write_stage_checkpoint(
        request=checkpoint_request,
        checkpoint_dir=str(Path(artifact_root) / "review"),
        run_id=run_id,
        stage="review",
        repo_root=repo_root,
        extra_inputs={"review_result": review_result},
    )
    cp_path = None
    for art in stage_result.get("artifacts") or []:
        if isinstance(art, dict) and art.get("kind") == "checkpoint":
            cp_path = art.get("path")
            break
    if cp_path:
        assert_path_under_run_dir(artifact_root, cp_path)

    blocking: List[Any] = list(review_result.get("blocking_findings") or [])
    aggregate = {
        "blocking_count": len(blocking),
        "decision": review_result.get("decision"),
        "spec_revision_needed": review_result.get("spec_revision_needed"),
    }
    review_summary = Path(artifact_root) / "review" / "aggregate.json"
    review_summary.write_text(json.dumps(aggregate, indent=2), encoding="utf-8")

    merge_ready = not blocking and review_result.get("decision") == "merge_ok"

    return {
        "status": "completed",
        "stage_checkpoint_path": cp_path,
        "stage_result": stage_result,
        "review_result": review_result,
        "aggregate": aggregate,
        "merge_ready": merge_ready,
        "review_depth_preset": preset,
        "stale": False,
        "evidence_paths": evidence_paths,
    }
