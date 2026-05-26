"""P8 stage input collection tests."""

from __future__ import annotations

import json
from pathlib import Path

import click
import pytest

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.stage_inputs import (
    REVISION_QUESTIONS,
    REWORK_QUESTIONS,
    SPEC_INPUT_QUESTIONS,
    InputQuestion,
    collect_revision_inputs,
    collect_spec_inputs,
    collect_stage_inputs,
    default_prompt_fn,
    write_stage_input_artifact,
)


def test_collect_stage_inputs_prefers_provided() -> None:
    out = collect_stage_inputs(
        stage="spec",
        questions=SPEC_INPUT_QUESTIONS,
        provided={"task_prompt": "from cli"},
        interactive=False,
    )
    assert out["task_prompt"] == "from cli"


def test_collect_stage_inputs_reuses_input_json(tmp_path: Path) -> None:
    art = tmp_path / "spec"
    write_stage_input_artifact(
        artifact_dir=art,
        stage="spec",
        inputs={"task_prompt": "from artifact", "reference_paths": [], "notes": ""},
    )
    out = collect_stage_inputs(
        stage="spec",
        questions=SPEC_INPUT_QUESTIONS,
        provided={},
        interactive=False,
        input_artifact_path=art / "input.json",
    )
    assert out["task_prompt"] == "from artifact"


def test_non_interactive_missing_required_fails() -> None:
    with pytest.raises(NodeExecutionFailure, match="requires 'task_prompt'"):
        collect_stage_inputs(
            stage="spec",
            questions=SPEC_INPUT_QUESTIONS,
            provided={},
            interactive=False,
        )


def test_interactive_prompt_fn() -> None:
    def fake_prompt(q: InputQuestion, default: str | None = None) -> str:
        if q.key == "task_prompt":
            return "typed task"
        return default or ""

    out = collect_stage_inputs(
        stage="spec",
        questions=SPEC_INPUT_QUESTIONS,
        provided={},
        interactive=True,
        prompt_fn=fake_prompt,
    )
    assert out["task_prompt"] == "typed task"


def test_collect_spec_writes_artifacts(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    ref = repo / "docs.md"
    ref.parent.mkdir(parents=True, exist_ok=True)
    ref.write_text("# Reference\n", encoding="utf-8")
    artifact_root = str(tmp_path / "run")
    inputs, materials, input_path, ref_path = collect_spec_inputs(
        artifact_root=artifact_root,
        repo_root=repo,
        provided={
            "task_prompt": "build feature",
            "reference_paths": [str(ref)],
            "notes": "keep scope small",
        },
        interactive=False,
    )
    assert inputs["task_prompt"] == "build feature"
    assert input_path.is_file()
    doc = json.loads(input_path.read_text(encoding="utf-8"))
    assert doc["schema_version"] == "dev_process.stage_input.v1"
    assert doc["inputs"]["notes"] == "keep scope small"
    assert materials and materials[0]["path"] == str(ref.resolve())
    assert ref_path is not None and ref_path.is_file()


def test_collect_revision_inputs(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    artifact_root = str(tmp_path / "run")
    inputs, _materials, input_path, _ref = collect_revision_inputs(
        artifact_root=artifact_root,
        repo_root=repo,
        provided={"revision_comment": "narrow scope"},
        interactive=False,
    )
    assert inputs["revision_comment"] == "narrow scope"
    assert input_path.name == "input.json"
    assert "revision" in str(input_path)


# --- multiline / click.edit tests ---


def test_default_prompt_fn_uses_editor_for_multiline(monkeypatch: pytest.MonkeyPatch) -> None:
    question = InputQuestion(
        key="notes",
        label="Notes",
        required=False,
        multiline=True,
    )
    called: dict = {}

    def fake_edit(text: str = "", require_save: bool = True) -> str:
        called["text"] = text
        called["require_save"] = require_save
        return "line1\nline2\n"

    monkeypatch.setattr(click, "edit", fake_edit)
    result = default_prompt_fn(question, default="initial")
    assert result == "line1\nline2"
    assert called["text"] == "initial"
    assert called["require_save"] is True


def test_default_prompt_fn_multiline_returns_default_when_editor_cancelled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    question = InputQuestion(
        key="notes",
        label="Notes",
        required=False,
        multiline=True,
    )
    monkeypatch.setattr(click, "edit", lambda **kwargs: None)
    assert default_prompt_fn(question, default="existing") == "existing"


def test_default_prompt_fn_multiline_returns_empty_when_cancelled_without_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    question = InputQuestion(
        key="notes",
        label="Notes",
        required=False,
        multiline=True,
    )
    monkeypatch.setattr(click, "edit", lambda **kwargs: None)
    assert default_prompt_fn(question) == ""


def test_expected_questions_are_multiline() -> None:
    spec_questions = {q.key: q for q in SPEC_INPUT_QUESTIONS}
    assert spec_questions["notes"].multiline is False
    assert spec_questions["task_prompt"].multiline is False

    revision_questions = {q.key: q for q in REVISION_QUESTIONS}
    assert revision_questions["revision_comment"].multiline is True

    rework_questions = {q.key: q for q in REWORK_QUESTIONS}
    assert rework_questions["rework_comment"].multiline is True


def test_required_multiline_cancelled_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    """Editor cancel on a required multiline field → collect_stage_inputs raises."""
    monkeypatch.setattr(click, "edit", lambda **kwargs: None)
    monkeypatch.setattr(click, "echo", lambda *a, **kw: None)

    questions = [
        InputQuestion("comment", "Comment", required=True, multiline=True),
    ]
    with pytest.raises(NodeExecutionFailure, match="requires 'comment'"):
        collect_stage_inputs(
            stage="test",
            questions=questions,
            provided={},
            interactive=True,
        )


def test_status_writes_to_stderr(capsys: pytest.CaptureFixture[str]) -> None:
    from nodeflow.workflows.dev_process.flow_actions import _status

    _status("Writing spec...")
    captured = capsys.readouterr()
    assert captured.out == ""
    assert ">> Writing spec..." in captured.err


def test_default_prompt_fn_multiline_prints_editor_guide(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    question = InputQuestion("notes", "Notes", required=False, multiline=True)
    monkeypatch.setattr(click, "edit", lambda **kwargs: "ok")

    default_prompt_fn(question)

    captured = capsys.readouterr()
    assert captured.out == ""
    assert "Notes (opening" in captured.err
    assert "save and close to submit" in captured.err
