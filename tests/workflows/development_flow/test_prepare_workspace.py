"""PrepareWorkspaceNode."""

from __future__ import annotations

import subprocess
from pathlib import Path

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.development_flow.prepare_workspace import PrepareWorkspaceNode
from tests.workflows.development_flow.git_fixtures import (
    git_repo_with_commit,
    run_context_for_workspace,
)


def test_prepare_workspace_current_repo_uses_source_repo_as_workspace(tmp_path: Path) -> None:
    repo = tmp_path / "repo_current"
    repo.mkdir()
    git_repo_with_commit(repo)
    node = PrepareWorkspaceNode()
    out = node.execute(
        {
            "source_repo_root": str(repo),
            "run_context": run_context_for_workspace(repo, "feat/nodeflow-current"),
        },
        {"strategy": "current_repo"},
    )
    ctx = out["workspace_context"]
    assert ctx["strategy"] == "current_repo"
    assert Path(ctx["workspace_root"]).resolve() == repo.resolve()
    assert isinstance(ctx.get("current_branch"), str)
    assert ctx.get("planned_branch_name") == "feat/nodeflow-current"


def test_rework_current_repo_reuses_dirty_workspace(tmp_path: Path) -> None:
    repo = tmp_path / "repo_rework_dirty"
    repo.mkdir()
    git_repo_with_commit(repo)
    node = PrepareWorkspaceNode()
    first = node.execute(
        {
            "source_repo_root": str(repo),
            "run_context": run_context_for_workspace(repo, "feat/nodeflow-rework-dirty"),
        },
        {"strategy": "current_repo"},
    )["workspace_context"]
    (repo / "dirty_impl.py").write_text("x\n", encoding="utf-8")
    subprocess.run(["git", "add", "dirty_impl.py"], cwd=str(repo), check=True, capture_output=True)
    node.reset_status()
    second = node.execute(
        {
            "source_repo_root": str(repo),
            "run_context": run_context_for_workspace(repo, "feat/nodeflow-rework-dirty"),
            "workspace_context": first,
        },
        {"strategy": "current_repo"},
    )["workspace_context"]
    assert second["workspace_root"] == first["workspace_root"]
    assert second["base_revision"] == first["base_revision"]


def test_prepare_workspace_current_repo_rejects_changed_branch_on_reuse(tmp_path: Path) -> None:
    repo = tmp_path / "repo_reuse_branch"
    repo.mkdir()
    git_repo_with_commit(repo)
    node = PrepareWorkspaceNode()
    first = node.execute(
        {
            "source_repo_root": str(repo),
            "run_context": run_context_for_workspace(repo, "feat/nodeflow-reuse-branch"),
        },
        {"strategy": "current_repo"},
    )["workspace_context"]
    subprocess.run(
        ["git", "switch", "-c", "tmp-branch"],
        cwd=str(repo),
        check=True,
        capture_output=True,
    )
    node.reset_status()
    node.execute(
        {
            "source_repo_root": str(repo),
            "run_context": run_context_for_workspace(repo, "feat/nodeflow-reuse-branch"),
            "workspace_context": first,
        },
        {"strategy": "current_repo"},
    )
    assert node.read_status() == "fatal"
    assert "branch changed since previous checkpoint" in str(node.read_error())


def test_prepare_workspace_rejects_missing_source_repo_root(tmp_path: Path) -> None:
    missing = tmp_path / "does_not_exist"
    node = PrepareWorkspaceNode()
    node.execute(
        {
            "source_repo_root": str(missing),
            "run_context": {"planned_branch_name": "feat/nodeflow-missing"},
        },
        {"strategy": "current_repo"},
    )
    assert node.read_status() == "fatal"
    assert isinstance(node.read_error(), NodeExecutionFailure)
    assert "source_repo_root does not exist" in str(node.read_error())


def test_prepare_workspace_requires_source_repo_root_not_repo_root_fallback(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo_source_required"
    repo.mkdir()
    git_repo_with_commit(repo)
    node = PrepareWorkspaceNode()
    node.execute(
        {
            "repo_root": str(repo),
            "run_context": run_context_for_workspace(repo, "feat/nodeflow-source-required"),
        },
        {"strategy": "current_repo"},
    )
    assert node.read_status() == "fatal"
    assert "source_repo_root is required" in str(node.read_error())


def test_prepare_workspace_current_repo_rejects_source_branch_change_on_fresh(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo_source_branch_changed"
    repo.mkdir()
    git_repo_with_commit(repo)
    subprocess.run(
        ["git", "switch", "-c", "other-branch"],
        cwd=str(repo),
        check=True,
        capture_output=True,
    )
    node = PrepareWorkspaceNode()
    node.execute(
        {
            "source_repo_root": str(repo),
            "run_context": {
                "planned_branch_name": "feat/nodeflow-branch-check",
                "source_repo_root": str(repo.resolve()),
                "source_base_revision": subprocess.run(
                    ["git", "rev-parse", "HEAD"],
                    cwd=str(repo),
                    check=True,
                    capture_output=True,
                    text=True,
                ).stdout.strip(),
                "source_current_branch": "main",
            },
        },
        {"strategy": "current_repo"},
    )
    assert node.read_status() == "fatal"
    assert isinstance(node.read_error(), NodeExecutionFailure)
    assert "source branch changed since flow start" in str(node.read_error())


def test_prepare_workspace_rejects_unsupported_strategy(tmp_path: Path) -> None:
    repo = tmp_path / "repo_bad_strategy"
    repo.mkdir()
    git_repo_with_commit(repo)
    node = PrepareWorkspaceNode()
    node.execute(
        {
            "source_repo_root": str(repo),
            "run_context": run_context_for_workspace(repo, "feat/nodeflow-bad-strategy"),
        },
        {"strategy": "unsupported"},
    )
    assert node.read_status() == "fatal"
    assert "unsupported prepare_workspace.strategy" in str(node.read_error())


def test_prepare_workspace_current_repo_rejects_mismatched_existing_workspace(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo_bad_existing"
    repo.mkdir()
    git_repo_with_commit(repo)
    node = PrepareWorkspaceNode()
    first = node.execute(
        {
            "source_repo_root": str(repo),
            "run_context": run_context_for_workspace(repo, "feat/nodeflow-existing-a"),
        },
        {"strategy": "current_repo"},
    )["workspace_context"]
    first["planned_branch_name"] = "feat/nodeflow-existing-b"
    node.reset_status()
    node.execute(
        {
            "source_repo_root": str(repo),
            "run_context": run_context_for_workspace(repo, "feat/nodeflow-existing-a"),
            "workspace_context": first,
        },
        {"strategy": "current_repo"},
    )
    assert node.read_status() == "fatal"
    assert "planned_branch_name" in str(node.read_error())


def test_prepare_workspace_current_repo_rejects_changed_head_on_fresh_approve(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo_changed_head"
    repo.mkdir()
    git_repo_with_commit(repo)
    base_revision = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=str(repo),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    (repo / "next.txt").write_text("next\n", encoding="utf-8")
    subprocess.run(["git", "add", "next.txt"], cwd=str(repo), check=True, capture_output=True)
    subprocess.run(
        ["git", "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-m", "next"],
        cwd=str(repo),
        check=True,
        capture_output=True,
    )
    node = PrepareWorkspaceNode()
    node.execute(
        {
            "source_repo_root": str(repo),
            "run_context": {
                "planned_branch_name": "feat/nodeflow-changed-head",
                "source_repo_root": str(repo.resolve()),
                "source_base_revision": base_revision,
                "source_current_branch": "main",
            },
        },
        {"strategy": "current_repo"},
    )
    assert node.read_status() == "fatal"
    assert "HEAD changed since flow start" in str(node.read_error())


def test_prepare_workspace_reuse_requires_current_branch(tmp_path: Path) -> None:
    repo = tmp_path / "repo_reuse_current_branch_required"
    repo.mkdir()
    git_repo_with_commit(repo)
    node = PrepareWorkspaceNode()
    first = node.execute(
        {
            "source_repo_root": str(repo),
            "run_context": run_context_for_workspace(
                repo, "feat/nodeflow-reuse-current-branch-required"
            ),
        },
        {"strategy": "current_repo"},
    )["workspace_context"]
    first.pop("current_branch", None)
    node.reset_status()
    node.execute(
        {
            "source_repo_root": str(repo),
            "run_context": run_context_for_workspace(
                repo, "feat/nodeflow-reuse-current-branch-required"
            ),
            "workspace_context": first,
        },
        {"strategy": "current_repo"},
    )
    assert node.read_status() == "fatal"
    assert "existing workspace_context.current_branch is required" in str(node.read_error())
