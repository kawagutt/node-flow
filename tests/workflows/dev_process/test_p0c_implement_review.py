"""P0c: approve_spec -> awaiting_review_decision."""

from __future__ import annotations

from pathlib import Path

from nodeflow.workflows.dev_process.constants import STATE_AWAITING_REVIEW
from nodeflow.workflows.dev_process.dev_process_flow.node_dev_process_flow import (
    DevProcessFlowNode,
)
from tests.workflows.dev_process.git_fixtures import git_repo_with_commit


def _start_and_approve(tmp_path: Path, *, force_blocking: bool = False) -> dict:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    start_out = DevProcessFlowNode().execute(
        {"action": "start", "repo_root": str(repo), "task_prompt": "feature"},
        {},
    )
    cp = start_out["flow_output"]["flow_result"]["flow_checkpoint_path"]
    params = {"force_review_blocking": force_blocking} if force_blocking else {}
    approve_out = DevProcessFlowNode().execute(
        {
            "action": "approve_spec",
            "repo_root": str(repo),
            "flow_checkpoint_path": cp,
        },
        params,
    )
    return approve_out["flow_output"]


def test_approve_spec_merge_ready(tmp_path: Path) -> None:
    flow = _start_and_approve(tmp_path)
    fr = flow["flow_result"]
    assert fr["state"] == STATE_AWAITING_REVIEW
    assert fr["merge_ready"] is True
    artifact_root = Path(flow["run_context"]["artifact_root"])
    assert (artifact_root / "implement" / "summary.txt").is_file()
    assert (artifact_root / "review" / "aggregate.json").is_file()


def test_approve_spec_blocking_review(tmp_path: Path) -> None:
    flow = _start_and_approve(tmp_path, force_blocking=True)
    assert flow["flow_result"]["merge_ready"] is False
