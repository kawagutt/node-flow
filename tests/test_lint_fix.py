"""Tests for lint_fix stage."""

from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import patch

from nodeflow.workflows.dev_process.stages.lint_fix import run_lint_fix_stage


def _make_ruff_result(returncode: int = 0, stdout: str = "", stderr: str = ""):
    """Build a CompletedProcess simulating ruff output."""
    return subprocess.CompletedProcess(
        args=["ruff", "check", "--fix"],
        returncode=returncode,
        stdout=stdout,
        stderr=stderr,
    )


class TestLintFixStage:
    def test_dependency_name_alone_does_not_enable_ruff(self, tmp_path: Path) -> None:
        repo = tmp_path / "repo"
        repo.mkdir()
        (repo / "pyproject.toml").write_text(
            '[project]\ndependencies = ["ruff>=0.4"]\n',
            encoding="utf-8",
        )
        result = run_lint_fix_stage(
            repo_root=repo,
            changed_paths=["src.py"],
            artifact_root=str(tmp_path / "artifacts"),
        )
        assert result["lint_fix"] == "skipped"

    def test_skipped_when_no_ruff_config(self, tmp_path: Path) -> None:
        repo = tmp_path / "repo"
        repo.mkdir()
        result = run_lint_fix_stage(
            repo_root=repo,
            changed_paths=["src.py"],
            artifact_root=str(tmp_path / "artifacts"),
        )
        assert result["lint_fix"] == "skipped"
        assert "not configured" in result["reason"]

    def test_skipped_when_no_python_files(self, tmp_path: Path) -> None:
        repo = tmp_path / "repo"
        repo.mkdir()
        (repo / "pyproject.toml").write_text("[tool.ruff]\n", encoding="utf-8")
        result = run_lint_fix_stage(
            repo_root=repo,
            changed_paths=["readme.md", "data.json"],
            artifact_root=str(tmp_path / "artifacts"),
        )
        assert result["lint_fix"] == "skipped"
        assert "no Python" in result["reason"]

    def test_ruff_runs_on_python_files(self, tmp_path: Path) -> None:
        repo = tmp_path / "repo"
        repo.mkdir()
        (repo / "pyproject.toml").write_text("[tool.ruff]\n", encoding="utf-8")
        src = repo / "src.py"
        src.write_text("import os\nx = 1\n", encoding="utf-8")

        with patch(
            "nodeflow.workflows.dev_process.stages.lint_fix._is_ruff_installed",
            return_value=True,
        ), patch(
            "nodeflow.workflows.dev_process.stages.lint_fix.subprocess.run",
            return_value=_make_ruff_result(0, stdout="All checks passed!\n"),
        ) as mock_run:
            result = run_lint_fix_stage(
                repo_root=repo,
                changed_paths=["src.py"],
                artifact_root=str(tmp_path / "artifacts"),
            )
        assert result["lint_fix"] == "passed"
        assert "src.py" in result["fixed_files"]
        mock_run.assert_called_once()

    def test_ruff_failed_returncode(self, tmp_path: Path) -> None:
        repo = tmp_path / "repo"
        repo.mkdir()
        (repo / "pyproject.toml").write_text("[tool.ruff]\n", encoding="utf-8")
        (repo / "src.py").write_text("x=1\n", encoding="utf-8")

        with patch(
            "nodeflow.workflows.dev_process.stages.lint_fix._is_ruff_installed",
            return_value=True,
        ), patch(
            "nodeflow.workflows.dev_process.stages.lint_fix.subprocess.run",
            return_value=_make_ruff_result(1, stderr="Found 3 errors\n"),
        ):
            result = run_lint_fix_stage(
                repo_root=repo,
                changed_paths=["src.py"],
                artifact_root=str(tmp_path / "artifacts"),
            )
        assert result["lint_fix"] == "ruff_failed"
        assert result["ruff_exit_code"] == 1

    def test_ruff_fix_does_not_stage(self, tmp_path: Path) -> None:
        """Verify lint_fix does not call git add."""
        repo = tmp_path / "repo"
        repo.mkdir()
        (repo / "pyproject.toml").write_text("[tool.ruff]\nline-length = 100\n", encoding="utf-8")
        (repo / "clean.py").write_text("x = 1\n", encoding="utf-8")

        with patch(
            "nodeflow.workflows.dev_process.stages.lint_fix._is_ruff_installed",
            return_value=True,
        ), patch(
            "nodeflow.workflows.dev_process.stages.lint_fix.subprocess.run",
            return_value=_make_ruff_result(0),
        ) as mock_run:
            result = run_lint_fix_stage(
                repo_root=repo,
                changed_paths=["clean.py"],
                artifact_root=str(tmp_path / "artifacts"),
            )
        assert result["lint_fix"] == "passed"
        call_args = mock_run.call_args[0][0]
        assert "git" not in call_args

    def test_ruff_not_installed(self, tmp_path: Path) -> None:
        repo = tmp_path / "repo"
        repo.mkdir()
        (repo / "pyproject.toml").write_text("[tool.ruff]\n", encoding="utf-8")
        (repo / "src.py").write_text("x=1\n", encoding="utf-8")

        with patch(
            "nodeflow.workflows.dev_process.stages.lint_fix._is_ruff_installed",
            return_value=False,
        ):
            result = run_lint_fix_stage(
                repo_root=repo,
                changed_paths=["src.py"],
                artifact_root=str(tmp_path / "artifacts"),
            )
        assert result["lint_fix"] == "skipped"
        assert "not installed" in result["reason"]

    def test_evidence_files_saved(self, tmp_path: Path) -> None:
        repo = tmp_path / "repo"
        repo.mkdir()
        (repo / "pyproject.toml").write_text("[tool.ruff]\n", encoding="utf-8")
        (repo / "src.py").write_text("x = 1\n", encoding="utf-8")
        art = str(tmp_path / "artifacts")

        with patch(
            "nodeflow.workflows.dev_process.stages.lint_fix._is_ruff_installed",
            return_value=True,
        ), patch(
            "nodeflow.workflows.dev_process.stages.lint_fix.subprocess.run",
            return_value=_make_ruff_result(0, stdout="OK\n"),
        ):
            run_lint_fix_stage(
                repo_root=repo,
                changed_paths=["src.py"],
                artifact_root=art,
                phase_id="phase_000",
            )
        log_dir = Path(art) / "phases" / "phase_000" / "lint_fix"
        assert log_dir.exists()
        assert (log_dir / "returncode.txt").exists()
        assert (log_dir / "stdout.txt").exists()
        assert (log_dir / "stderr.txt").exists()
        ev_dir = Path(art) / "phases" / "phase_000" / "evidence"
        assert ev_dir.exists()
        assert (ev_dir / "lint_fix.json").exists()

    def test_evidence_paths_only_json(self, tmp_path: Path) -> None:
        """evidence_paths should contain only JSON files, not logs."""
        repo = tmp_path / "repo"
        repo.mkdir()
        (repo / "pyproject.toml").write_text("[tool.ruff]\n", encoding="utf-8")
        (repo / "src.py").write_text("x = 1\n", encoding="utf-8")
        art = str(tmp_path / "artifacts")

        with patch(
            "nodeflow.workflows.dev_process.stages.lint_fix._is_ruff_installed",
            return_value=True,
        ), patch(
            "nodeflow.workflows.dev_process.stages.lint_fix.subprocess.run",
            return_value=_make_ruff_result(0),
        ):
            result = run_lint_fix_stage(
                repo_root=repo,
                changed_paths=["src.py"],
                artifact_root=art,
                phase_id="phase_000",
            )
        for p in result.get("evidence_paths", []):
            assert p.endswith(".json"), f"evidence_paths should be JSON-only, got {p}"
        for p in result.get("log_paths", []):
            assert p.endswith(".txt"), f"log_paths should be txt, got {p}"
