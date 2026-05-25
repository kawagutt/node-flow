"""Helpers for dev-process v2 flow tests."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from nodeflow.workflows.dev_process.constants import (
    ACTION_APPROVE_FINAL,
    ACTION_APPROVE_SPEC,
    ACTION_CONTINUE_IMPLEMENTATION,
    ACTION_MERGE,
    ACTION_START,
    STATE_AWAITING_FINAL,
    STATE_AWAITING_IMPLEMENTATION,
    STATE_AWAITING_MERGE,
    STATE_AWAITING_SPEC_HUMAN_GATE,
    STATE_MERGED,
)
from nodeflow.workflows.dev_process.dev_process_flow.node_dev_process_flow import (
    DevProcessFlowNode,
)


def run_action(repo: Path, payload: dict[str, Any], params: dict | None = None) -> dict[str, Any]:
    out = DevProcessFlowNode().execute(payload, params or {})
    return out["flow_output"]


def start_spec_human_gate(
    repo: Path,
    *,
    task_prompt: str = "feature",
    workspace_strategy: str | None = None,
    merge_policy: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "action": ACTION_START,
        "repo_root": str(repo),
        "task_prompt": task_prompt,
    }
    if workspace_strategy:
        payload["workspace_strategy"] = workspace_strategy
    if merge_policy:
        payload["merge_policy"] = merge_policy
    return run_action(repo, payload)


def approve_spec_to_implementation(repo: Path, cp: str) -> dict[str, Any]:
    return run_action(
        repo,
        {"action": ACTION_APPROVE_SPEC, "repo_root": str(repo), "flow_checkpoint_path": cp},
    )


def continue_to_review(repo: Path, cp: str, *, force_blocking: bool = False) -> dict[str, Any]:
    params = {"force_review_blocking": force_blocking} if force_blocking else {}
    return run_action(
        repo,
        {
            "action": ACTION_CONTINUE_IMPLEMENTATION,
            "repo_root": str(repo),
            "flow_checkpoint_path": cp,
        },
        params,
    )


def approve_and_continue(repo: Path, cp: str, *, force_blocking: bool = False) -> dict[str, Any]:
    """Human-gate checkpoint → plan approval → implementation + review."""
    after_approve = approve_spec_to_implementation(repo, cp)
    return continue_to_review(
        repo, after_approve["flow_result"]["flow_checkpoint_path"], force_blocking=force_blocking
    )


def continue_from_implementation(
    repo: Path, cp: str, *, force_blocking: bool = False
) -> dict[str, Any]:
    """awaiting_implementation checkpoint → implementation + review."""
    return continue_to_review(repo, cp, force_blocking=force_blocking)


def full_through_review(
    repo: Path,
    *,
    force_blocking: bool = False,
    workspace_strategy: str | None = None,
    merge_policy: str | None = None,
) -> dict[str, Any]:
    start = start_spec_human_gate(
        repo, workspace_strategy=workspace_strategy, merge_policy=merge_policy
    )
    assert start["flow_result"]["state"] == STATE_AWAITING_SPEC_HUMAN_GATE
    cp = start["flow_result"]["flow_checkpoint_path"]
    after_approve = approve_spec_to_implementation(repo, cp)
    assert after_approve["flow_result"]["state"] == STATE_AWAITING_IMPLEMENTATION
    cp2 = after_approve["flow_result"]["flow_checkpoint_path"]
    return continue_to_review(repo, cp2, force_blocking=force_blocking)


def through_approve_final(
    repo: Path,
    *,
    force_blocking: bool = False,
    workspace_strategy: str | None = None,
    merge_policy: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Returns (review_flow, approve_final_flow)."""
    review = full_through_review(
        repo,
        force_blocking=force_blocking,
        workspace_strategy=workspace_strategy,
        merge_policy=merge_policy,
    )
    cp = review["flow_result"]["flow_checkpoint_path"]
    final = run_action(
        repo,
        {
            "action": ACTION_APPROVE_FINAL,
            "repo_root": str(repo),
            "flow_checkpoint_path": cp,
        },
    )
    return review, final


def rework_from_blocking(repo: Path, cp: str, *, force_blocking: bool = False) -> dict[str, Any]:
    from nodeflow.workflows.dev_process.constants import ACTION_REWORK

    params: dict[str, Any] = {}
    if force_blocking:
        params["force_review_blocking"] = True
    return run_action(
        repo,
        {
            "action": ACTION_REWORK,
            "repo_root": str(repo),
            "flow_checkpoint_path": cp,
        },
        params,
    )


def merge_ready_flow(repo: Path) -> dict[str, Any]:
    review, final = through_approve_final(repo)
    flow = review
    assert flow["flow_result"]["state"] == STATE_AWAITING_FINAL
    assert flow["flow_result"]["merge_ready"] is True
    assert final["flow_result"]["state"] == STATE_AWAITING_MERGE
    cp2 = final["flow_result"]["flow_checkpoint_path"]
    merged = run_action(
        repo,
        {"action": ACTION_MERGE, "repo_root": str(repo), "flow_checkpoint_path": cp2},
    )
    assert merged["flow_result"]["state"] == STATE_MERGED
    return merged
