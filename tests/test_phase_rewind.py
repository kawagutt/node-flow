"""Tests for phase rewind after final review failure."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any, Dict

import pytest

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.phase_rewind import rewind_to_phase


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


def _commit_file(repo: Path, name: str, content: str, msg: str) -> str:
    (repo / name).write_text(content, encoding="utf-8")
    subprocess.run(["git", "-C", str(repo), "add", name], capture_output=True, check=True)
    subprocess.run(
        ["git", "-C", str(repo), "commit", "-m", msg],
        capture_output=True,
        check=True,
    )
    cp = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=True,
    )
    return cp.stdout.strip()


class TestRewindToPhase:
    def test_rewind_impl_to_phase_001(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path / "repo")
        base = subprocess.run(
            ["git", "-C", str(repo), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()

        phase_000_start = _commit_file(repo, "p0.py", "x=0", "phase_000 start")
        phase_001_start = _commit_file(repo, "p1.py", "x=1", "phase_001 start")
        _commit_file(repo, "p2.py", "x=2", "phase_002 commit")

        dp: Dict[str, Any] = {
            "total_phases": 3,
            "phase_index": 3,
            "current_phase_id": "",
            "phase_results": {
                "phase_000": {
                    "status": "completed",
                    "phase_start_git_ref": base,
                    "phase_commit": phase_000_start,
                },
                "phase_001": {
                    "status": "completed",
                    "phase_start_git_ref": phase_000_start,
                    "phase_commit": phase_001_start,
                },
                "phase_002": {
                    "status": "completed",
                    "phase_start_git_ref": phase_001_start,
                },
            },
            "recovery_refs": [],
        }

        result = rewind_to_phase(dp, repo, target_phase="phase_001", owner="implementation")

        assert result["recovery_ref"]
        assert result["target_phase"] == "phase_001"
        assert result["skip_implementation"] is False
        assert dp["phase_results"]["phase_000"]["status"] == "completed"
        assert dp["phase_results"]["phase_001"]["status"] == "pending"
        assert dp["phase_results"]["phase_002"]["status"] == "pending"
        assert dp["phase_index"] == 1
        assert dp["current_phase_id"] == "phase_001"
        assert len(dp["recovery_refs"]) == 1
        rr = dp["recovery_refs"][0]
        assert isinstance(rr, dict)
        assert rr["reason"] == "final_review_rewind"
        assert rr["target_phase"] == "phase_001"
        assert rr["owner"] == "implementation"
        assert rr["ref"]
        assert rr["reset_to_ref"] == phase_000_start

    def test_rewind_test_owner_does_not_skip_impl_in_v1(self, tmp_path: Path) -> None:
        """In v1, rewind always re-runs implementation because reset loses the impl diff."""
        repo = _init_repo(tmp_path / "repo")
        ref = subprocess.run(
            ["git", "-C", str(repo), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()

        dp: Dict[str, Any] = {
            "total_phases": 2,
            "phase_index": 2,
            "current_phase_id": "",
            "phase_results": {
                "phase_000": {
                    "status": "completed",
                    "phase_start_git_ref": ref,
                    "phase_commit": ref,
                },
                "phase_001": {
                    "status": "completed",
                    "phase_start_git_ref": ref,
                },
            },
            "recovery_refs": [],
        }

        result = rewind_to_phase(dp, repo, target_phase="phase_001", owner="test")
        assert result["skip_implementation"] is False

    def test_invalid_target_phase_raises(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path / "repo")
        dp: Dict[str, Any] = {
            "total_phases": 2,
            "phase_index": 2,
            "phase_results": {},
            "recovery_refs": [],
        }
        with pytest.raises(NodeExecutionFailure, match="Invalid target_phase"):
            rewind_to_phase(dp, repo, target_phase="phase_999", owner="implementation")

    def test_missing_start_ref_raises(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path / "repo")
        dp: Dict[str, Any] = {
            "total_phases": 2,
            "phase_index": 2,
            "phase_results": {
                "phase_000": {"status": "completed"},
            },
            "recovery_refs": [],
        }
        with pytest.raises(NodeExecutionFailure, match="no phase_start_git_ref"):
            rewind_to_phase(dp, repo, target_phase="phase_000", owner="implementation")
