"""PrepareDevelopmentRunContextNode."""

from __future__ import annotations

import re
from pathlib import Path

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.development_flow.prepare_development_run_context import (
    PrepareDevelopmentRunContextNode,
)
from tests.workflows.development_flow.git_fixtures import (
    git_repo_with_commit,
    source_workspace_check,
)


def test_prepare_development_run_context_creates_artifact_root_only(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    node = PrepareDevelopmentRunContextNode()
    out = node.execute(
        {
            "source_workspace_check": source_workspace_check(repo),
            "planned_branch_name": "feat/nodeflow-sample",
            "run_id": "r1",
        },
        {},
    )
    ctx = out["run_context"]
    assert ctx["run_id"] == "r1"
    assert ctx["run_index"] == 1
    assert ctx["run_slug"] == "development-flow"
    assert ctx["planned_branch_name"] == "feat/nodeflow-sample"
    assert "/.nodeflow/runs/001_" in ctx["artifact_root"]
    assert ctx["run_dir_name"] in ctx["artifact_root"]


def test_prepare_development_run_context_rejects_run_id_in_params(tmp_path: Path) -> None:
    repo = tmp_path / "repo_run_id_param"
    repo.mkdir()
    git_repo_with_commit(repo)
    node = PrepareDevelopmentRunContextNode()
    node.execute(
        {"source_workspace_check": source_workspace_check(repo)},
        {"run_id": "run-from-param"},
    )
    assert node.read_status() == "fatal"
    assert "does not accept params.run_id" in str(node.read_error())


def test_prepare_development_run_context_uses_human_readable_dir(tmp_path: Path) -> None:
    repo = tmp_path / "repo_human"
    repo.mkdir()
    git_repo_with_commit(repo)
    node = PrepareDevelopmentRunContextNode()
    out = node.execute(
        {
            "source_workspace_check": source_workspace_check(repo),
            "run_id": "internal-only",
            "task_prompt": "Add config validation\ndetails",
        },
        {},
    )
    ctx = out["run_context"]
    assert ctx["run_id"] == "internal-only"
    assert ctx["run_slug"] == "add-config-validation"
    assert ctx["run_dir_name"].startswith("001_")
    assert "internal-only" not in Path(ctx["artifact_root"]).name
    assert Path(ctx["artifact_root"]).is_dir()


def test_prepare_development_run_context_rejects_invalid_branch_name(tmp_path: Path) -> None:
    repo = tmp_path / "repo_invalid_branch"
    repo.mkdir()
    git_repo_with_commit(repo)
    node = PrepareDevelopmentRunContextNode()
    node.execute(
        {
            "source_workspace_check": source_workspace_check(repo),
            "planned_branch_name": "bad..branch",
            "run_id": "r-invalid",
        },
        {},
    )
    assert node.read_status() == "fatal"
    assert isinstance(node.read_error(), NodeExecutionFailure)


def test_prepare_development_run_context_normalizes_branch_prefix_trailing_slash(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo_branch_prefix_normalized"
    repo.mkdir()
    git_repo_with_commit(repo)
    node = PrepareDevelopmentRunContextNode()
    out = node.execute(
        {"source_workspace_check": source_workspace_check(repo), "task_prompt": "x"},
        {"branch_prefix": "feat/nodeflow/"},
    )
    assert "//" not in out["run_context"]["planned_branch_name"]
    assert out["run_context"]["planned_branch_name"].startswith("feat/nodeflow/")


def test_prepare_development_run_context_rejects_empty_branch_prefix(tmp_path: Path) -> None:
    repo = tmp_path / "repo_branch_prefix_empty"
    repo.mkdir()
    git_repo_with_commit(repo)
    node = PrepareDevelopmentRunContextNode()
    node.execute(
        {"source_workspace_check": source_workspace_check(repo), "task_prompt": "x"},
        {"branch_prefix": "/"},
    )
    assert node.read_status() == "fatal"
    assert "branch_prefix must not be empty" in str(node.read_error())


def test_prepare_development_run_context_invalid_branch_does_not_create_run_dir(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo_no_run_dir_on_invalid_branch"
    repo.mkdir()
    git_repo_with_commit(repo)
    runs = repo / ".nodeflow" / "runs"
    node = PrepareDevelopmentRunContextNode()
    node.execute(
        {
            "source_workspace_check": source_workspace_check(repo),
            "planned_branch_name": "bad..branch",
            "run_id": "r-inv2",
        },
        {},
    )
    assert node.read_status() == "fatal"
    if runs.exists():
        assert not any(re.match(r"^\d{3}_", p.name) for p in runs.iterdir())


def test_prepare_development_run_context_invalid_run_dir_format_raises_node_failure(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo_bad_run_dir_format"
    repo.mkdir()
    git_repo_with_commit(repo)
    node = PrepareDevelopmentRunContextNode()
    node.execute(
        {"source_workspace_check": source_workspace_check(repo)},
        {"run_dir_format": "{bad_placeholder}"},
    )
    assert node.read_status() == "fatal"
    assert isinstance(node.read_error(), NodeExecutionFailure)
    assert "invalid run_dir_format" in str(node.read_error())
