"""Tests for rework save/reset before spec/plan upstream rework."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict
from unittest.mock import patch

from nodeflow.workflows.dev_process.flow_actions import _rework_save_and_reset


def _body(dp: Dict[str, Any], *, repo: Path) -> Dict[str, Any]:
    return {
        "dev_process": dp,
        "run_context": {"artifact_root": str(repo / "artifacts"), "repo_root": str(repo)},
    }


class TestReworkSaveAndReset:
    def test_spec_rework_uses_task_branch_base_when_no_current_phase(self, tmp_path: Path) -> None:
        repo = tmp_path / "repo"
        repo.mkdir()
        dp: Dict[str, Any] = {
            "total_phases": 2,
            "current_phase_id": "",
            "task_branch": {"name": "task/run", "base_ref": "base123"},
            "phase_results": {
                "phase_000": {"status": "completed", "phase_start_git_ref": "p0start"},
            },
        }
        body = _body(dp, repo=repo)

        with patch(
            "nodeflow.workflows.dev_process.flow_actions._phase_repo_root",
            return_value=repo,
        ), patch(
            "nodeflow.workflows.dev_process.phase_git.save_uncommitted_diff",
            return_value={"patch_path": "/p", "untracked_list_path": ""},
        ) as mock_save, patch(
            "nodeflow.workflows.dev_process.phase_git.reset_to_ref",
        ) as mock_reset:
            _rework_save_and_reset(body, prefer_task_branch_base=True)

        mock_save.assert_called_once()
        assert mock_save.call_args.kwargs["phase_id"] == "phase_001"
        mock_reset.assert_called_once()
        assert mock_reset.call_args.args[1] == "base123"

    def test_plan_rework_uses_current_phase_start_ref(self, tmp_path: Path) -> None:
        repo = tmp_path / "repo"
        repo.mkdir()
        dp = {
            "total_phases": 2,
            "current_phase_id": "phase_001",
            "task_branch": {"name": "task/run", "base_ref": "base123"},
            "phase_results": {
                "phase_001": {"phase_start_git_ref": "phase1start"},
            },
        }
        body = _body(dp, repo=repo)

        with patch(
            "nodeflow.workflows.dev_process.flow_actions._phase_repo_root",
            return_value=repo,
        ), patch(
            "nodeflow.workflows.dev_process.phase_git.save_uncommitted_diff",
            return_value={"patch_path": "/p", "untracked_list_path": ""},
        ), patch(
            "nodeflow.workflows.dev_process.phase_git.reset_to_ref",
        ) as mock_reset:
            _rework_save_and_reset(body, prefer_task_branch_base=False)

        mock_reset.assert_called_once()
        assert mock_reset.call_args.args[1] == "phase1start"
