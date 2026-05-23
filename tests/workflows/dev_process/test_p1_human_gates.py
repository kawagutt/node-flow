"""P1: final human gate and reject actions."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.constants import (
    ACTION_APPROVE_FINAL,
    ACTION_MERGE,
    ACTION_REJECT_SPEC,
    STATE_AWAITING_FINAL,
    STATE_AWAITING_REVIEW,
    STATE_FAILED,
)
from nodeflow.workflows.dev_process.dev_process_flow.node_dev_process_flow import (
    DevProcessFlowNode,
)
from tests.workflows.dev_process.git_fixtures import git_repo_with_commit


def _through_review(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    start = DevProcessFlowNode().execute(
        {"action": "start", "repo_root": str(repo), "task_prompt": "gate"},
        {},
    )
    cp = start["flow_output"]["flow_result"]["flow_checkpoint_path"]
    appr = DevProcessFlowNode().execute(
        {
            "action": "approve_spec",
            "repo_root": str(repo),
            "flow_checkpoint_path": cp,
        },
        {},
    )
    return repo, appr


def test_merge_requires_approve_final(tmp_path) -> None:
    _, appr = _through_review(tmp_path)
    fr = appr["flow_output"]["flow_result"]
    assert fr["state"] == STATE_AWAITING_REVIEW
    cp2 = fr["flow_checkpoint_path"]
    with pytest.raises(NodeExecutionFailure, match="not allowed"):
        DevProcessFlowNode().execute(
            {
                "action": ACTION_MERGE,
                "repo_root": str(appr["flow_output"]["run_context"]["repo_root"]),
                "flow_checkpoint_path": cp2,
            },
            {},
        )


def test_reject_spec_terminal_failed(tmp_path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    start = DevProcessFlowNode().execute(
        {"action": "start", "repo_root": str(repo), "task_prompt": "x"},
        {},
    )
    cp = start["flow_output"]["flow_result"]["flow_checkpoint_path"]
    out = DevProcessFlowNode().execute(
        {
            "action": ACTION_REJECT_SPEC,
            "repo_root": str(repo),
            "flow_checkpoint_path": cp,
            "human_comment_text": "not aligned",
        },
        {},
    )
    assert out["flow_output"]["flow_result"]["state"] == STATE_FAILED


def test_human_gates_updated_after_approve_spec(tmp_path) -> None:
    _, appr = _through_review(tmp_path)
    cp = Path(appr["flow_output"]["flow_result"]["flow_checkpoint_path"])
    doc = json.loads(cp.read_text(encoding="utf-8"))
    gates = doc.get("dev_process", {}).get("human_gates", {})
    assert gates.get("spec") == "approved"
    assert gates.get("final") == "pending"


def test_blocking_review_next_action_prefers_rework(tmp_path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    start = DevProcessFlowNode().execute(
        {"action": "start", "repo_root": str(repo), "task_prompt": "x"},
        {},
    )
    cp = start["flow_output"]["flow_result"]["flow_checkpoint_path"]
    appr = DevProcessFlowNode().execute(
        {
            "action": "approve_spec",
            "repo_root": str(repo),
            "flow_checkpoint_path": cp,
        },
        {"force_review_blocking": True},
    )
    fr = appr["flow_output"]["flow_result"]
    assert fr["next_action"] == "rework_implementation"
    assert "approve_final" not in fr["allowed_actions"]


def test_allowed_actions_omit_approve_final_when_not_merge_ready(tmp_path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    start = DevProcessFlowNode().execute(
        {"action": "start", "repo_root": str(repo), "task_prompt": "x"},
        {},
    )
    cp = start["flow_output"]["flow_result"]["flow_checkpoint_path"]
    appr = DevProcessFlowNode().execute(
        {
            "action": "approve_spec",
            "repo_root": str(repo),
            "flow_checkpoint_path": cp,
        },
        {"force_review_blocking": True},
    )
    actions = appr["flow_output"]["flow_result"]["allowed_actions"]
    assert "approve_final" not in actions
    assert "rework_implementation" in actions


def test_approve_final_then_merge(tmp_path) -> None:
    repo, appr = _through_review(tmp_path)
    cp2 = appr["flow_output"]["flow_result"]["flow_checkpoint_path"]
    final = DevProcessFlowNode().execute(
        {
            "action": ACTION_APPROVE_FINAL,
            "repo_root": str(repo),
            "flow_checkpoint_path": cp2,
        },
        {},
    )
    assert final["flow_output"]["flow_result"]["state"] == STATE_AWAITING_FINAL
    cp3 = final["flow_output"]["flow_result"]["flow_checkpoint_path"]
    merged = DevProcessFlowNode().execute(
        {
            "action": ACTION_MERGE,
            "repo_root": str(repo),
            "flow_checkpoint_path": cp3,
        },
        {},
    )
    assert merged["flow_output"]["flow_result"]["state"] == "merged"
