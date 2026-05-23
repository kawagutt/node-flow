"""P0d: merge gate."""

from __future__ import annotations

import pytest

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.constants import (
    ACTION_APPROVE_FINAL,
    STATE_AWAITING_FINAL,
    STATE_MERGED,
)
from nodeflow.workflows.dev_process.dev_process_flow.node_dev_process_flow import (
    DevProcessFlowNode,
)
from tests.workflows.dev_process.git_fixtures import git_repo_with_commit


def test_merge_when_ready(tmp_path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    start = DevProcessFlowNode().execute(
        {"action": "start", "repo_root": str(repo), "task_prompt": "m"},
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
    cp2 = appr["flow_output"]["flow_result"]["flow_checkpoint_path"]
    assert appr["flow_output"]["flow_result"]["merge_ready"] is True
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
            "action": "merge",
            "repo_root": str(repo),
            "flow_checkpoint_path": cp3,
        },
        {},
    )
    assert merged["flow_output"]["flow_result"]["state"] == STATE_MERGED


def test_merge_fails_when_not_ready(tmp_path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    start = DevProcessFlowNode().execute(
        {"action": "start", "repo_root": str(repo), "task_prompt": "m"},
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
    cp2 = appr["flow_output"]["flow_result"]["flow_checkpoint_path"]
    with pytest.raises(NodeExecutionFailure, match="merge_ready"):
        DevProcessFlowNode().execute(
            {
                "action": ACTION_APPROVE_FINAL,
                "repo_root": str(repo),
                "flow_checkpoint_path": cp2,
            },
            {},
        )
