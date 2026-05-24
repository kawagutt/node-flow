"""P5: dev_process flow with git_worktree workspace."""

from __future__ import annotations

from pathlib import Path

from nodeflow.workflows.dev_process.constants import STATE_AWAITING_REVIEW
from nodeflow.workflows.dev_process.dev_process_flow.node_dev_process_flow import (
    DevProcessFlowNode,
)
from tests.workflows.dev_process.git_fixtures import git_repo_with_commit


def test_approve_spec_uses_git_worktree_workspace(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)

    start_out = DevProcessFlowNode().execute(
        {
            "action": "start",
            "repo_root": str(repo),
            "task_prompt": "wt feature",
            "workspace_strategy": "git_worktree",
        },
        {},
    )
    flow = start_out["flow_output"]
    assert flow["run_context"]["workspace_strategy"] == "git_worktree"

    cp = flow["flow_result"]["flow_checkpoint_path"]
    approve_out = DevProcessFlowNode().execute(
        {
            "action": "approve_spec",
            "repo_root": str(repo),
            "flow_checkpoint_path": cp,
        },
        {},
    )
    approved = approve_out["flow_output"]
    assert approved["flow_result"]["state"] == STATE_AWAITING_REVIEW

    wc = approved.get("workspace_context") or {}
    assert wc.get("strategy") == "git_worktree"
    wt = Path(wc["workspace_root"])
    assert wt.is_dir()
    assert wt != repo.resolve()
    assert (wt / "README.md").is_file()
    artifact_root = Path(approved["run_context"]["artifact_root"])
    assert wt == (artifact_root / "worktrees" / "001").resolve()
