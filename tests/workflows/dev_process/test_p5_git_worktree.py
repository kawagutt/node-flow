"""P5: dev_process flow with git_worktree workspace."""

from __future__ import annotations

from pathlib import Path

from nodeflow.workflows.dev_process.constants import STATE_AWAITING_FINAL
from tests.workflows.dev_process.git_fixtures import git_repo_with_commit
from tests.workflows.dev_process.v2_flow_helpers import approve_and_continue, start_spec_human_gate


def test_approve_spec_uses_git_worktree_workspace(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)

    flow = start_spec_human_gate(repo, task_prompt="wt feature", workspace_strategy="git_worktree")
    assert flow["run_context"]["workspace_strategy"] == "git_worktree"

    cp = flow["flow_result"]["flow_checkpoint_path"]
    approved = approve_and_continue(repo, cp)
    assert approved["flow_result"]["state"] == STATE_AWAITING_FINAL

    import json as _json

    cp_path = approved["flow_result"]["flow_checkpoint_path"]
    cp_doc = _json.loads(Path(cp_path).read_text(encoding="utf-8"))
    dp = cp_doc.get("dev_process") or {}
    task_branch = dp.get("task_branch") or {}
    assert task_branch.get("created") is True
    wt_path = task_branch.get("worktree_path")
    assert wt_path, "git_worktree strategy should set worktree_path"
    wt = Path(wt_path)
    assert wt.is_dir()
    assert wt != repo.resolve()
    assert (wt / "README.md").is_file()
    assert not str(wt).startswith(str(repo.resolve()))
