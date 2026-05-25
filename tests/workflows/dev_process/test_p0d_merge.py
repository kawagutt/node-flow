"""P0d: merge gate."""

from __future__ import annotations

import pytest

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.constants import (
    ACTION_APPROVE_FINAL,
    STATE_AWAITING_MERGE,
    STATE_MERGED,
)
from nodeflow.workflows.dev_process.dev_process_flow.node_dev_process_flow import (
    DevProcessFlowNode,
)
from tests.workflows.dev_process.git_fixtures import git_repo_with_commit
from tests.workflows.dev_process.v2_flow_helpers import (
    approve_and_continue,
    start_spec_human_gate,
    through_approve_final,
)


def test_merge_when_ready(tmp_path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    review, final = through_approve_final(repo)
    assert review["flow_result"]["merge_ready"] is True
    assert final["flow_result"]["state"] == STATE_AWAITING_MERGE
    cp3 = final["flow_result"]["flow_checkpoint_path"]
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
    start = start_spec_human_gate(repo)
    cp = start["flow_result"]["flow_checkpoint_path"]
    review = approve_and_continue(repo, cp, force_blocking=True)
    cp2 = review["flow_result"]["flow_checkpoint_path"]
    with pytest.raises(NodeExecutionFailure, match="not allowed"):
        DevProcessFlowNode().execute(
            {
                "action": ACTION_APPROVE_FINAL,
                "repo_root": str(repo),
                "flow_checkpoint_path": cp2,
            },
            {},
        )
