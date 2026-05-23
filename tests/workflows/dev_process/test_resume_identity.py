"""Resume identity checks on flow resume."""

from __future__ import annotations

import pytest

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.dev_process_flow.node_dev_process_flow import (
    DevProcessFlowNode,
)
from tests.workflows.dev_process.git_fixtures import git_repo_with_commit


def test_resume_rejects_repo_root_mismatch(tmp_path) -> None:
    repo_a = tmp_path / "repo_a"
    repo_b = tmp_path / "repo_b"
    repo_a.mkdir()
    repo_b.mkdir()
    git_repo_with_commit(repo_a)
    git_repo_with_commit(repo_b)

    start = DevProcessFlowNode().execute(
        {"action": "start", "repo_root": str(repo_a), "task_prompt": "x"},
        {},
    )
    cp = start["flow_output"]["flow_result"]["flow_checkpoint_path"]
    with pytest.raises(NodeExecutionFailure, match="resume identity mismatch"):
        DevProcessFlowNode().execute(
            {
                "action": "approve_spec",
                "repo_root": str(repo_b),
                "flow_checkpoint_path": cp,
            },
            {},
        )
