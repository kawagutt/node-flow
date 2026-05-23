"""P0b: start with spec_plan -> awaiting_spec_approval."""

from __future__ import annotations

from pathlib import Path

from nodeflow.workflows.dev_process.constants import STATE_AWAITING_SPEC
from nodeflow.workflows.dev_process.dev_process_flow.node_dev_process_flow import (
    DevProcessFlowNode,
)
from tests.workflows.dev_process.git_fixtures import git_repo_with_commit


def test_start_runs_spec_plan_by_default(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    node = DevProcessFlowNode()
    out = node.execute(
        {"action": "start", "repo_root": str(repo), "task_prompt": "add feature"},
        {},
    )
    fr = out["flow_output"]["flow_result"]
    assert fr["state"] == STATE_AWAITING_SPEC
    assert "approve_spec" in fr["allowed_actions"]
    assert fr["next_action"] == "approve_spec"
    artifact_root = Path(out["flow_output"]["run_context"]["artifact_root"])
    assert (artifact_root / "spec_plan" / "spec.md").is_file()
    assert (artifact_root / "spec_plan" / "plan.md").is_file()
