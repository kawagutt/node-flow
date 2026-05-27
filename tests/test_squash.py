"""Tests for squash merge."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any, Dict

import pytest

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.squash import (
    build_squash_message,
    squash_phase_commits,
)


def _init_repo(path: Path) -> Path:
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


def _get_head(repo: Path) -> str:
    cp = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=True,
    )
    return cp.stdout.strip()


def _commit_file(repo: Path, name: str, content: str, msg: str) -> str:
    (repo / name).write_text(content, encoding="utf-8")
    subprocess.run(["git", "-C", str(repo), "add", name], capture_output=True, check=True)
    subprocess.run(
        ["git", "-C", str(repo), "commit", "-m", msg],
        capture_output=True,
        check=True,
    )
    return _get_head(repo)


class TestBuildSquashMessage:
    def test_with_spec_title(self) -> None:
        dp = {
            "total_phases": 2,
            "phase_results": {
                "phase_000": {"title": "Setup"},
                "phase_001": {"title": "Core"},
            },
        }
        msg = build_squash_message(dp, "Add auth feature")
        assert "Add auth feature" in msg
        assert "phase_000: Setup" in msg
        assert "phase_001: Core" in msg

    def test_without_spec_title(self) -> None:
        dp = {"total_phases": 1, "phase_results": {"phase_000": {"title": "Only"}}}
        msg = build_squash_message(dp)
        assert "Squashed" in msg


class TestSquashPhaseCommits:
    def test_squash_multiple_commits(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path / "repo")
        base_ref = _get_head(repo)
        _commit_file(repo, "a.py", "a=1", "phase_000: A")
        _commit_file(repo, "b.py", "b=1", "phase_001: B")

        dp: Dict[str, Any] = {
            "task_branch": {"base_ref": base_ref},
            "total_phases": 2,
            "phase_results": {
                "phase_000": {"title": "A"},
                "phase_001": {"title": "B"},
            },
        }
        result = squash_phase_commits(repo, dp)
        assert result["squashed"] is True
        assert result["squash_commit"]
        assert result["squash_tree"]
        assert result["squash_tree_matches_reviewed_tree"] is True
        assert result["reviewed_tree"] == result["squash_tree"]

        cp = subprocess.run(
            ["git", "-C", str(repo), "log", "--oneline"],
            capture_output=True,
            text=True,
            check=True,
        )
        lines = [line for line in cp.stdout.strip().splitlines() if line.strip()]
        assert len(lines) == 2

    def test_squash_no_base_ref_raises(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path / "repo")
        dp: Dict[str, Any] = {"task_branch": {}, "total_phases": 0, "phase_results": {}}
        with pytest.raises(NodeExecutionFailure, match="base_ref not found"):
            squash_phase_commits(repo, dp)

    def test_record_only_no_git_ops(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path / "repo")
        base_ref = _get_head(repo)
        _commit_file(repo, "a.py", "a=1", "phase_000: A")

        dp: Dict[str, Any] = {
            "task_branch": {"base_ref": base_ref},
            "total_phases": 1,
            "phase_results": {"phase_000": {"title": "A"}},
        }
        result = squash_phase_commits(repo, dp, record_only=True)
        assert result["squashed"] is False
        assert result["record_only"] is True

        cp = subprocess.run(
            ["git", "-C", str(repo), "log", "--oneline"],
            capture_output=True,
            text=True,
            check=True,
        )
        assert len(cp.stdout.strip().splitlines()) == 2

    def test_no_squash_keeps_history(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path / "repo")
        _commit_file(repo, "a.py", "a=1", "phase_000: A")
        _commit_file(repo, "b.py", "b=1", "phase_001: B")

        cp = subprocess.run(
            ["git", "-C", str(repo), "log", "--oneline"],
            capture_output=True,
            text=True,
            check=True,
        )
        assert len(cp.stdout.strip().splitlines()) == 3
