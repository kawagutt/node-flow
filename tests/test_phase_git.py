"""Tests for phase_git: task branch creation and phase commit."""

from __future__ import annotations

import subprocess
from pathlib import Path

from nodeflow.workflows.dev_process.phase_git import (
    create_task_branch,
    phase_commit,
    reset_to_ref,
    save_uncommitted_diff,
)


def _init_repo(path: Path) -> Path:
    """Initialize a git repo with one commit."""
    subprocess.run(["git", "init", str(path)], capture_output=True, check=True)
    subprocess.run(
        ["git", "-C", str(path), "config", "user.email", "test@test.com"],
        capture_output=True,
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(path), "config", "user.name", "Test"],
        capture_output=True,
        check=True,
    )
    (path / "README.md").write_text("init", encoding="utf-8")
    subprocess.run(["git", "-C", str(path), "add", "."], capture_output=True, check=True)
    subprocess.run(
        ["git", "-C", str(path), "commit", "-m", "initial"],
        capture_output=True,
        check=True,
    )
    return path


class TestCreateTaskBranch:
    def test_creates_branch_current_repo(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path / "repo")
        result = create_task_branch(repo, "test-run-id", workspace_strategy="current_repo")
        assert result["created"] is True
        assert result["name"] == "phase-base/test-run-id"
        assert result["base_ref"]

        cp = subprocess.run(
            ["git", "-C", str(repo), "branch", "--show-current"],
            capture_output=True,
            text=True,
            check=True,
        )
        assert cp.stdout.strip() == "phase-base/test-run-id"

    def test_creates_branch_git_worktree_strategy(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path / "repo")
        result = create_task_branch(repo, "wt-run-id", workspace_strategy="git_worktree")
        assert result.get("worktree_root")
        assert result["created"] is True
        assert "worktree_path" in result
        wt_path = Path(result["worktree_path"])
        assert wt_path.exists()
        assert not str(wt_path).startswith(str(repo))

        cp = subprocess.run(
            ["git", "-C", str(repo), "branch", "--list", "phase-base/wt-run-id"],
            capture_output=True,
            text=True,
            check=True,
        )
        assert "phase-base/wt-run-id" in cp.stdout

    def test_worktree_metadata_for_cleanup_registry(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path / "repo")
        result = create_task_branch(repo, "cleanup-run", workspace_strategy="git_worktree")
        dp: dict = {}
        if result.get("worktree_path"):
            dp.setdefault("cleanup_targets", []).append(
                {
                    "kind": "git_worktree",
                    "branch": result["name"],
                    "worktree_path": result["worktree_path"],
                    "worktree_root": result.get("worktree_root", ""),
                    "run_id": "cleanup-run",
                }
            )
        assert len(dp["cleanup_targets"]) == 1
        assert dp["cleanup_targets"][0]["kind"] == "git_worktree"

    def test_base_ref_matches_head(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path / "repo")
        cp = subprocess.run(
            ["git", "-C", str(repo), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
        head = cp.stdout.strip()
        result = create_task_branch(repo, "test-ref", workspace_strategy="current_repo")
        assert result["base_ref"] == head


class TestPhaseCommit:
    def test_commit_includes_source_file(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path / "repo")
        artifact_root = str(repo / ".nodeflow" / "runs" / "001")
        Path(artifact_root).mkdir(parents=True, exist_ok=True)

        (repo / "src.py").write_text("print('hello')", encoding="utf-8")
        result = phase_commit(
            repo,
            phase_id="phase_000",
            phase_title="Add source",
            artifact_roots=[artifact_root],
        )
        assert result["actual_commit_created"] is True
        assert result["phase_commit"]

        cp = subprocess.run(
            ["git", "-C", str(repo), "log", "--oneline", "-1"],
            capture_output=True,
            text=True,
            check=True,
        )
        assert "phase_000: Add source" in cp.stdout

    def test_excludes_nodeflow_artifacts(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path / "repo")
        artifact_root = str(repo / ".nodeflow" / "runs" / "001")
        Path(artifact_root).mkdir(parents=True, exist_ok=True)

        (Path(artifact_root) / "data.json").write_text("{}", encoding="utf-8")

        result = phase_commit(
            repo,
            phase_id="phase_000",
            phase_title="No changes",
            artifact_roots=[artifact_root],
        )
        assert result["actual_commit_created"] is False

    def test_empty_diff_returns_head(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path / "repo")
        cp = subprocess.run(
            ["git", "-C", str(repo), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
        head = cp.stdout.strip()

        result = phase_commit(
            repo,
            phase_id="phase_000",
            phase_title="Empty",
            artifact_roots=[],
        )
        assert result["phase_commit"] == head
        assert result["actual_commit_created"] is False

    def test_includes_untracked_new_files(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path / "repo")
        (repo / "new_module.py").write_text("x = 1", encoding="utf-8")

        result = phase_commit(
            repo,
            phase_id="phase_001",
            phase_title="New file",
            artifact_roots=[],
        )
        assert result["actual_commit_created"] is True


class TestSaveUncommittedDiff:
    def test_saves_tracked_diff(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path / "repo")
        artifact_root = str(tmp_path / "artifacts")
        (repo / "README.md").write_text("modified", encoding="utf-8")

        result = save_uncommitted_diff(
            repo,
            artifact_root=artifact_root,
            phase_id="phase_000",
        )
        assert Path(result["patch_path"]).exists()
        patch_content = Path(result["patch_path"]).read_text()
        assert "modified" in patch_content

    def test_saves_untracked_list(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path / "repo")
        artifact_root = str(tmp_path / "artifacts")
        (repo / "new_file.py").write_text("x = 1", encoding="utf-8")

        result = save_uncommitted_diff(
            repo,
            artifact_root=artifact_root,
            phase_id="phase_000",
        )
        untracked = Path(result["untracked_list_path"]).read_text()
        assert "new_file.py" in untracked


class TestResetToRef:
    def test_clean_untracked_removes_empty_parent_directories(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path / "repo")
        pkg = repo / "pkg" / "nested"
        pkg.mkdir(parents=True)
        (pkg / "mod.py").write_text("x = 1\n", encoding="utf-8")

        cp = subprocess.run(
            ["git", "-C", str(repo), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
        head = cp.stdout.strip()

        reset_to_ref(repo, head, clean_untracked=["pkg/nested/mod.py"])

        assert not (repo / "pkg").exists()
