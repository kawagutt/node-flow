"""P0a: skeleton — initialized, timeline, checkpoint."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.constants import SCHEMA_VERSION, STATE_INITIALIZED
from nodeflow.workflows.dev_process.dev_process_flow.node_dev_process_flow import (
    DevProcessFlowNode,
)
from tests.workflows.dev_process.git_fixtures import git_repo_with_commit


def test_start_initialized_without_spec(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    node = DevProcessFlowNode()
    out = node.execute(
        {
            "action": "start",
            "repo_root": str(repo),
            "task_prompt": "demo task",
        },
        {"run_spec_on_start": False},
    )
    flow = out["flow_output"]
    fr = flow["flow_result"]
    assert fr["state"] == STATE_INITIALIZED
    assert fr["allowed_actions"] == []
    assert fr["next_action"] is None
    cp_path = Path(fr["flow_checkpoint_path"])
    assert cp_path.is_file()
    doc = json.loads(cp_path.read_text(encoding="utf-8"))
    assert doc["schema_version"] == SCHEMA_VERSION
    artifact_root = Path(doc["run_context"]["artifact_root"])
    assert artifact_root.is_dir()
    assert (artifact_root / "timeline.jsonl").is_file()
    lines = (artifact_root / "timeline.jsonl").read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) >= 2
    ev0 = json.loads(lines[0])
    assert ev0["event"] == "flow_started"
    assert "ts" in ev0 and "run_id" in ev0


def test_start_rejects_flow_checkpoint_path(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    node = DevProcessFlowNode()
    with pytest.raises(NodeExecutionFailure, match="start does not accept"):
        node.execute(
            {
                "action": "start",
                "repo_root": str(repo),
                "task_prompt": "x",
                "flow_checkpoint_path": "/tmp/fake.json",
            },
            {"run_spec_on_start": False},
        )


def test_checkpoint_self_reference_under_artifact_root(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    node = DevProcessFlowNode()
    out = node.execute(
        {"action": "start", "repo_root": str(repo), "task_prompt": "t"},
        {"run_spec_on_start": False},
    )
    cp = Path(out["flow_output"]["flow_result"]["flow_checkpoint_path"])
    artifact_root = cp.parent.parent
    cp.relative_to(artifact_root / "checkpoints")
