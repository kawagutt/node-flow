"""Merge and final-approval handlers for dev-process."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process import timeline
from nodeflow.workflows.dev_process.constants import (
    ACTION_APPROVE_FINAL,
    ACTION_MERGE,
    MERGE_GATE_STAGES,
    STATE_AWAITING_FINAL,
    STATE_AWAITING_MERGE,
    STATE_MERGED,
)
from nodeflow.workflows.dev_process.evidence import assert_expected_stage_evidence
from nodeflow.workflows.dev_process.flow_context import _fail_checkpoint, _finalize
from nodeflow.workflows.dev_process.merge import execute_merge_policy, resolve_merge_policy
from nodeflow.workflows.dev_process.reuse import write_development_summary
from nodeflow.workflows.dev_process.stale import any_stale_remaining


def _merge_gate_ok(body: Dict[str, Any]) -> None:
    fr = body.get("flow_result") or {}
    stages = body.get("stages") or {}
    if fr.get("state") != STATE_AWAITING_MERGE:
        raise NodeExecutionFailure(f"merge requires state {STATE_AWAITING_MERGE!r}")
    if not fr.get("merge_ready"):
        raise NodeExecutionFailure("merge_ready is false")
    for name in MERGE_GATE_STAGES:
        st = stages.get(name) or {}
        if st.get("status") != "completed":
            raise NodeExecutionFailure(f"stages.{name}.status must be completed")
    if "spec_plan" in stages or "implement" in stages:
        raise NodeExecutionFailure("v2 checkpoint must not contain legacy stages spec_plan or implement")
    agg = (stages.get("review") or {}).get("aggregate") or {}
    if agg.get("blocking_count", 1) != 0:
        raise NodeExecutionFailure("review has blocking findings")
    review_st = stages.get("review") or {}
    if review_st.get("stale"):
        raise NodeExecutionFailure("stages.review is stale; rework or re-run review before merge")
    if any_stale_remaining(body):
        raise NodeExecutionFailure("checkpoint has stale stages; re-run affected stages before merge")


def _write_fallback_development_summary(
    body: Dict[str, Any],
    *,
    action: str,
    reason: str,
) -> Dict[str, Any]:
    run_context = body["run_context"]
    artifact_root = Path(run_context["artifact_root"])
    path = artifact_root / "summary" / f"{action}_development_summary.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    summary: Dict[str, Any] = {
        "status": "fallback",
        "reason": reason,
        "artifact_path": str(path.resolve()),
        "merge_result": body.get("merge_result"),
    }
    path.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return summary


def _handle_merge(body: Dict[str, Any], *, run_id: str) -> Dict[str, Any]:
    run_context = body["run_context"]
    assert_expected_stage_evidence(body, run_id=run_id)
    _merge_gate_ok(body)
    policy = resolve_merge_policy(body)
    artifact_root = run_context["artifact_root"]
    timeline.append_event(
        artifact_root,
        run_id,
        "merge_attempted",
        merge_ready=True,
        merge_policy=policy,
    )
    try:
        merge_result = execute_merge_policy(body)
        body["merge_result"] = merge_result
    except NodeExecutionFailure as e:
        _fail_checkpoint(body=body, run_id=run_id, action=ACTION_MERGE, reason=str(e))
        raise
    try:
        summary = write_development_summary(
            body=body,
            action=ACTION_MERGE,
            merge_ready=True,
        )
        body["development_summary"] = summary
    except NodeExecutionFailure as e:
        timeline.append_event(
            artifact_root,
            run_id,
            "summary_failed",
            reason=str(e),
            merge_policy=policy,
        )
        summary = _write_fallback_development_summary(
            body,
            action=ACTION_MERGE,
            reason=str(e),
        )
        body["development_summary"] = summary
    return _finalize(
        body=body,
        run_id=run_id,
        action=ACTION_MERGE,
        state=STATE_MERGED,
        merge_ready=True,
    )


def _handle_approve_final(body: Dict[str, Any], *, run_id: str) -> Dict[str, Any]:
    assert_expected_stage_evidence(body, run_id=run_id)
    fr = body.get("flow_result") or {}
    if fr.get("state") != STATE_AWAITING_FINAL:
        raise NodeExecutionFailure(
            f"approve_final requires state {STATE_AWAITING_FINAL!r}, got {fr.get('state')!r}"
        )
    if not fr.get("merge_ready"):
        raise NodeExecutionFailure("approve_final requires merge_ready=true")
    body.setdefault("dev_process", {}).setdefault("human_gates", {})["final"] = "approved"
    return _finalize(
        body=body,
        run_id=run_id,
        action=ACTION_APPROVE_FINAL,
        state=STATE_AWAITING_MERGE,
        merge_ready=True,
    )
