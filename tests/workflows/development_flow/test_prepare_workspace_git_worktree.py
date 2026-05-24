"""git_worktree strategy for PrepareWorkspaceNode."""

from __future__ import annotations

from pathlib import Path

from nodeflow.workflows.development_flow.prepare_workspace import PrepareWorkspaceNode
from tests.workflows.development_flow.git_fixtures import (
    git_repo_with_commit,
    run_context_for_workspace,
)


def test_prepare_workspace_git_worktree_creates_branch_worktree(tmp_path: Path) -> None:
    repo = tmp_path / "repo_wt"
    repo.mkdir()
    git_repo_with_commit(repo)
    artifact_root = tmp_path / "run_artifacts"
    artifact_root.mkdir()
    rc = run_context_for_workspace(repo, "feat/nodeflow-wt")
    rc["artifact_root"] = str(artifact_root.resolve())
    rc["workspace_attempt"] = 1
    rc["worktree_subdirectory"] = "worktrees/001"

    node = PrepareWorkspaceNode()
    out = node.execute(
        {"source_repo_root": str(repo), "run_context": rc},
        {"strategy": "git_worktree"},
    )
    ctx = out["workspace_context"]
    assert ctx["strategy"] == "git_worktree"
    wt = Path(ctx["workspace_root"])
    assert wt == (artifact_root / "worktrees" / "001").resolve()
    assert wt.is_dir()
    assert ctx["current_branch"] == "feat/nodeflow-wt"
    assert ctx["base_revision"] == rc["source_base_revision"]
    assert wt != repo.resolve()


def test_prepare_workspace_git_worktree_reuse(tmp_path: Path) -> None:
    repo = tmp_path / "repo_wt_reuse"
    repo.mkdir()
    git_repo_with_commit(repo)
    artifact_root = tmp_path / "run_artifacts_reuse"
    artifact_root.mkdir()
    rc = run_context_for_workspace(repo, "feat/nodeflow-wt-reuse")
    rc["artifact_root"] = str(artifact_root.resolve())
    rc["workspace_attempt"] = 1
    rc["worktree_subdirectory"] = "worktrees/001"

    node = PrepareWorkspaceNode()
    first = node.execute(
        {"source_repo_root": str(repo), "run_context": rc},
        {"strategy": "git_worktree"},
    )["workspace_context"]
    node.reset_status()
    second = node.execute(
        {
            "source_repo_root": str(repo),
            "run_context": rc,
            "workspace_context": first,
        },
        {"strategy": "git_worktree"},
    )["workspace_context"]
    assert second["workspace_root"] == first["workspace_root"]
    assert second["current_branch"] == first["current_branch"]
