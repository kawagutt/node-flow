"""P8 named pipe CLI dispatch tests."""

from __future__ import annotations

import json
from io import StringIO
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from nodeflow.cli import main, pipeline_main
from nodeflow.workflows.dev_process.hermetic_argv import spec_argv
from tests.workflows.dev_process.git_fixtures import git_repo_with_commit


@pytest.fixture
def cli_runner() -> CliRunner:
    return CliRunner()


def _hermetic_start_argv_json() -> str:
    return json.dumps(spec_argv())


def _invoke_main(argv: list[str]) -> tuple[int, str]:
    out_buf = StringIO()
    err_buf = StringIO()
    with patch("sys.stdout", out_buf), patch("sys.stderr", err_buf):
        code = main(argv)
    return code, out_buf.getvalue() + err_buf.getvalue()


def test_main_dispatches_named_pipe_start(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    code, output = _invoke_main(
        [
            "--pipe",
            "dev-process",
            "--repo-root",
            str(repo),
            "--non-interactive",
            "start",
            "--task-prompt",
            "named pipe smoke",
            "--workspace-strategy",
            "current_repo",
        ]
    )
    assert code == 0, output
    assert "awaiting_spec_human_gate" in output


def test_main_named_pipe_status(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    code, _ = _invoke_main(
        [
            "--pipe",
            "dev-process",
            "--repo-root",
            str(repo),
            "--non-interactive",
            "start",
            "--task-prompt",
            "status via pipe",
            "--workspace-strategy",
            "current_repo",
        ]
    )
    assert code == 0
    code, output = _invoke_main(["--pipe", "dev-process", "--repo-root", str(repo), "status"])
    assert code == 0, output
    assert "awaiting_spec_human_gate" in output


def test_main_unknown_pipe_fails() -> None:
    code, output = _invoke_main(["--pipe", "unknown-pipe", "start"])
    assert code != 0
    assert "unknown pipe" in output.lower()


def test_pipeline_main_unchanged(cli_runner: CliRunner) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    result = cli_runner.invoke(
        pipeline_main,
        [
            str(repo_root / "examples/pipes/hello.json"),
            "-w",
            str(repo_root),
            "-i",
            "incoming={}",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Hello from NodeFlow!" in result.output


def test_main_json_pipe_backward_compat(cli_runner: CliRunner) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    code, output = _invoke_main(
        [
            str(repo_root / "examples/pipes/hello.json"),
            "-w",
            str(repo_root),
            "-i",
            "incoming={}",
        ]
    )
    assert code == 0, output
    assert "Hello from NodeFlow!" in output


def test_named_pipe_full_hermetic_path(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    argv_base = ["--pipe", "dev-process", "--repo_root", str(repo)]
    start_argv = argv_base + [
        "--non-interactive",
        "start",
        "--task-prompt",
        "pipe path",
        "--workspace-strategy",
        "current_repo",
        "--merge-policy",
        "record_only",
    ]
    assert _invoke_main(start_argv)[0] == 0
    ni_base = argv_base + ["--non-interactive"]
    code, output = _invoke_main(ni_base + ["approve-spec"])
    assert code == 0, output
    assert _invoke_main(ni_base + ["approve-final"])[0] == 0
    code, output = _invoke_main(ni_base + ["merge"])
    assert code == 0, output
    assert "merged" in output
