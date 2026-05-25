"""P0c: continue_implementation -> awaiting_rework_decision."""

from __future__ import annotations

from pathlib import Path

from nodeflow.workflows.dev_process.constants import (
    STATE_AWAITING_FINAL,
)
from tests.workflows.dev_process.git_fixtures import git_repo_with_commit
from tests.workflows.dev_process.v2_flow_helpers import full_through_review


def test_approve_spec_merge_ready(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    flow = full_through_review(repo)
    fr = flow["flow_result"]
    assert fr["state"] == STATE_AWAITING_FINAL
    assert fr["merge_ready"] is True
    artifact_root = Path(flow["run_context"]["artifact_root"])
    assert (artifact_root / "implementation" / "summary.txt").is_file()
    assert (artifact_root / "review" / "aggregate.json").is_file()


def test_approve_spec_blocking_review(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    flow = full_through_review(repo, force_blocking=True)
    assert flow["flow_result"]["merge_ready"] is False
