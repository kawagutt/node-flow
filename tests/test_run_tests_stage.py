"""run_run_tests_stage contract: always exposes test_result.ok."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.stages.run_tests import run_run_tests_stage


def test_run_tests_stage_sets_ok_true_on_success(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    art = tmp_path / "art"
    art.mkdir()
    out = run_run_tests_stage(
        repo_root=repo,
        artifact_root=str(art),
        run_id="r1",
    )
    assert out["test_result"]["ok"] is True
    assert out["status"] == "completed"


@patch("nodeflow.workflows.dev_process.stages.run_tests.run_tests")
def test_run_tests_stage_sets_ok_false_on_failure(mock_run, tmp_path: Path) -> None:
    mock_run.return_value = {
        "ok": False,
        "returncode": 1,
        "stdout": "",
        "stderr": "fail",
    }
    repo = tmp_path / "repo"
    repo.mkdir()
    art = tmp_path / "art"
    art.mkdir()
    out = run_run_tests_stage(
        repo_root=repo,
        artifact_root=str(art),
        run_id="r1",
    )
    assert out["test_result"]["ok"] is False
    assert out["status"] == "failed"


@patch("nodeflow.workflows.dev_process.stages.run_tests.run_tests")
def test_run_tests_stage_rejects_missing_ok(mock_run, tmp_path: Path) -> None:
    mock_run.return_value = {"returncode": 0}
    repo = tmp_path / "repo"
    repo.mkdir()
    with pytest.raises(NodeExecutionFailure, match="must include boolean field 'ok'"):
        run_run_tests_stage(
            repo_root=repo,
            artifact_root=str(tmp_path / "art"),
            run_id="r1",
        )
