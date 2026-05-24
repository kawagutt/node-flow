"""reuse.py wrappers must propagate child ActionNode fatal status."""

from __future__ import annotations

from pathlib import Path

import pytest

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process import reuse
from nodeflow.workflows.dev_process.action_node_utils import execute_or_raise
from tests.workflows.dev_process.git_fixtures import git_repo_with_commit


def test_action_node_utils_module_exists() -> None:
    assert callable(execute_or_raise)


def test_prepare_workspace_wrapper_raises_on_fatal(tmp_path: Path) -> None:
    missing = tmp_path / "missing_repo"
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    with pytest.raises(NodeExecutionFailure, match="source_repo_root does not exist"):
        reuse.prepare_workspace(
            source_repo_root=str(missing),
            run_context={
                "repo_root": str(missing),
                "source_base_revision": "HEAD",
                "artifact_root": str(artifact_root),
                "planned_branch_name": "feat/nodeflow/test",
                "source_current_branch": "main",
                "workspace_attempt": 1,
            },
            strategy="current_repo",
        )


def test_check_source_workspace_wrapper_raises_on_dirty_repo(tmp_path: Path) -> None:
    repo = tmp_path / "repo_dirty"
    repo.mkdir()
    git_repo_with_commit(repo)
    readme = repo / "README.md"
    readme.write_text("dirty\n", encoding="utf-8")
    with pytest.raises(NodeExecutionFailure, match="source repository is dirty"):
        reuse.check_source_workspace(repo)


def test_collect_diff_wrapper_raises_on_missing_repo(tmp_path: Path) -> None:
    with pytest.raises(NodeExecutionFailure):
        reuse.collect_diff(repo_root=tmp_path / "nope", base_revision="HEAD")
