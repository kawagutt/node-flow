"""CLI kick entry — JSON PipeSpec execution."""

from __future__ import annotations

from pathlib import Path

from nodeflow.core.run import load_and_kick_pipeline


def test_load_and_kick_hello_json() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    out = load_and_kick_pipeline(
        str(repo_root),
        "examples/pipes/hello.json",
        initial_inputs={"incoming": {}},
    )
    assert out["greeting"]["data"] == "Hello from NodeFlow!"


def test_load_and_kick_dev_process_flat_string_inputs(tmp_path) -> None:
    from tests.workflows.dev_process.git_fixtures import git_repo_with_commit

    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    repo_root = Path(__file__).resolve().parents[1]
    out = load_and_kick_pipeline(
        str(repo_root),
        "examples/pipes/dev_process/dev_process.json",
        initial_inputs={
            "action": "start",
            "repo_root": str(repo),
            "task_prompt": "flat cli",
        },
    )
    fr = out["flow_output"]["flow_result"]
    assert fr["state"] in ("initialized", "awaiting_spec_approval")


def test_load_and_kick_dev_process_start_initialized(tmp_path) -> None:
    from tests.workflows.dev_process.git_fixtures import git_repo_with_commit

    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    repo_root = Path(__file__).resolve().parents[1]
    out = load_and_kick_pipeline(
        str(repo_root),
        "examples/pipes/dev_process/dev_process.json",
        initial_inputs={
            "action": {"action": "start"},
            "repo_root": {"repo_root": str(repo)},
            "task_prompt": {"task_prompt": "cli smoke"},
        },
    )
    fr = out["flow_output"]["flow_result"]
    assert fr["state"] in ("initialized", "awaiting_spec_approval")
