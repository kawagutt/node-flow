"""P0b: start with spec + spec_review -> awaiting_spec_human_gate."""

from __future__ import annotations

from pathlib import Path

from nodeflow.workflows.dev_process.constants import STATE_AWAITING_SPEC_HUMAN_GATE
from tests.workflows.dev_process.git_fixtures import git_repo_with_commit
from tests.workflows.dev_process.v2_flow_helpers import start_spec_human_gate


def test_start_runs_spec_by_default(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    flow = start_spec_human_gate(repo, task_prompt="add feature")
    fr = flow["flow_result"]
    assert fr["state"] == STATE_AWAITING_SPEC_HUMAN_GATE
    assert "approve_spec" in fr["allowed_actions"]
    assert fr["next_action"] == "approve_spec"
    artifact_root = Path(flow["run_context"]["artifact_root"])
    assert (artifact_root / "spec" / "spec.md").is_file()
    assert not (artifact_root / "plan" / "plan.md").is_file()
