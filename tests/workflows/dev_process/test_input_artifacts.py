"""P8 stage input artifact integration tests."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from nodeflow.workflows.dev_process.checkpoint import load_flow_checkpoint
from nodeflow.workflows.dev_process.cli import main
from nodeflow.workflows.dev_process.constants import STATE_AWAITING_SPEC_HUMAN_GATE
from nodeflow.workflows.dev_process.discovery import resolve_checkpoint_path
from tests.workflows.dev_process.git_fixtures import git_repo_with_commit


def test_start_writes_spec_input_artifact(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "--repo-root",
            str(repo),
            "--non-interactive",
            "start",
            "--task-prompt",
            "artifact test",
            "--workspace-strategy",
            "current_repo",
        ],
    )
    assert result.exit_code == 0, result.output
    assert STATE_AWAITING_SPEC_HUMAN_GATE in result.output

    runs = list((repo / ".nodeflow/runs").iterdir())
    assert len(runs) == 1
    input_path = runs[0] / "spec" / "input.json"
    assert input_path.is_file()
    doc = json.loads(input_path.read_text(encoding="utf-8"))
    assert doc["inputs"]["task_prompt"] == "artifact test"
    assert (runs[0] / "spec" / "spec.md").is_file()

    cp_path = resolve_checkpoint_path(repo, checkpoint=None, run_id=None)
    checkpoint = load_flow_checkpoint(cp_path)
    sp = checkpoint.get("stages", {}).get("spec", {})
    assert sp.get("status") == "completed"
    assert sp.get("spec_artifact")


def test_start_interactive_collects_task_prompt(tmp_path: Path, monkeypatch) -> None:
    from nodeflow.workflows.dev_process import stage_inputs as si

    prompts: list[str] = []

    def fake_prompt(q: si.InputQuestion, default: str | None = None) -> str:
        if q.key == "task_prompt":
            prompts.append(q.key)
            return "interactive task"
        return default or ""

    monkeypatch.setattr(si, "default_prompt_fn", fake_prompt)

    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "--repo-root",
            str(repo),
            "start",
            "--workspace-strategy",
            "current_repo",
        ],
    )
    assert result.exit_code == 0, result.output
    assert prompts == ["task_prompt"]
    runs = list((repo / ".nodeflow/runs").iterdir())
    input_path = runs[0] / "spec" / "input.json"
    doc = json.loads(input_path.read_text(encoding="utf-8"))
    assert doc["inputs"]["task_prompt"] == "interactive task"


def test_non_interactive_start_without_task_prompt_fails(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "--repo-root",
            str(repo),
            "--non-interactive",
            "start",
            "--workspace-strategy",
            "current_repo",
        ],
    )
    assert result.exit_code != 0
    assert "task_prompt" in result.output
