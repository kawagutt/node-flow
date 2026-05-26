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
    STATE_AWAITING_MERGE,
    STATE_FAILED,
)
from nodeflow.workflows.dev_process.dev_process_flow.node_dev_process_flow import (
    DevProcessFlowNode,
)
from tests.workflows.dev_process.git_fixtures import git_repo_with_commit
from tests.workflows.dev_process.v2_flow_helpers import (
    continue_to_review,
    full_through_review,
    start_spec_human_gate,
)


def _through_review(tmp_path: Path):
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    flow = full_through_review(repo)
    return repo, flow


def test_merge_requires_approve_final(tmp_path: Path) -> None:
    _, flow = _through_review(tmp_path)
    fr = flow["flow_result"]
    assert fr["state"] == STATE_AWAITING_FINAL
    cp2 = fr["flow_checkpoint_path"]
    with pytest.raises(NodeExecutionFailure, match="not allowed"):
        DevProcessFlowNode().execute(
            {
                "action": ACTION_MERGE,
                "repo_root": str(flow["run_context"]["repo_root"]),
                "flow_checkpoint_path": cp2,
            },
            {},
        )


def test_reject_spec_terminal_failed(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    start = start_spec_human_gate(repo, task_prompt="x")
    cp = start["flow_result"]["flow_checkpoint_path"]
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


def test_human_gates_updated_after_approve_spec(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    start = start_spec_human_gate(repo)
    cp = start["flow_result"]["flow_checkpoint_path"]
    appr = DevProcessFlowNode().execute(
        {"action": "approve_spec", "repo_root": str(repo), "flow_checkpoint_path": cp},
        {},
    )
    doc = json.loads(Path(appr["flow_output"]["flow_result"]["flow_checkpoint_path"]).read_text())
    gates = doc.get("dev_process", {}).get("human_gates", {})
    assert gates.get("spec") == "approved"


def test_blocking_review_next_action_prefers_rework(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    start = start_spec_human_gate(repo)
    cp = start["flow_result"]["flow_checkpoint_path"]
    appr = DevProcessFlowNode().execute(
        {"action": "approve_spec", "repo_root": str(repo), "flow_checkpoint_path": cp},
        {"auto_continue": False},
    )
    cp2 = appr["flow_output"]["flow_result"]["flow_checkpoint_path"]
    cont = continue_to_review(repo, cp2, force_blocking=True)
    fr = cont["flow_result"]
    assert fr["next_action"] == "rework_implementation"
    assert "approve_final" not in fr["allowed_actions"]


def test_allowed_actions_omit_approve_final_when_not_merge_ready(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    flow = full_through_review(repo, force_blocking=True)
    assert "approve_final" not in flow["flow_result"]["allowed_actions"]


def test_approve_final_then_merge(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    flow = full_through_review(repo)
    assert flow["flow_result"]["merge_ready"] is True
    cp = flow["flow_result"]["flow_checkpoint_path"]
    final = DevProcessFlowNode().execute(
        {
            "action": ACTION_APPROVE_FINAL,
            "repo_root": str(repo),
            "flow_checkpoint_path": cp,
        },
        {},
    )
    assert final["flow_output"]["flow_result"]["state"] == STATE_AWAITING_MERGE
