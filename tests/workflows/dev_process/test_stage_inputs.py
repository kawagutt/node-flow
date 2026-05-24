"""P8 stage input collection tests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.stage_inputs import (
    SPEC_PLAN_QUESTIONS,
    InputQuestion,
    collect_revision_inputs,
    collect_spec_plan_inputs,
    collect_stage_inputs,
    write_stage_input_artifact,
)


def test_collect_stage_inputs_prefers_provided() -> None:
    out = collect_stage_inputs(
        stage="spec_plan",
        questions=SPEC_PLAN_QUESTIONS,
        provided={"task_prompt": "from cli"},
        interactive=False,
    )
    assert out["task_prompt"] == "from cli"


def test_collect_stage_inputs_reuses_input_json(tmp_path: Path) -> None:
    art = tmp_path / "spec_plan"
    write_stage_input_artifact(
        artifact_dir=art,
        stage="spec_plan",
        inputs={"task_prompt": "from artifact", "reference_paths": [], "notes": ""},
    )
    out = collect_stage_inputs(
        stage="spec_plan",
        questions=SPEC_PLAN_QUESTIONS,
        provided={},
        interactive=False,
        input_artifact_path=art / "input.json",
    )
    assert out["task_prompt"] == "from artifact"


def test_non_interactive_missing_required_fails() -> None:
    with pytest.raises(NodeExecutionFailure, match="requires 'task_prompt'"):
        collect_stage_inputs(
            stage="spec_plan",
            questions=SPEC_PLAN_QUESTIONS,
            provided={},
            interactive=False,
        )


def test_interactive_prompt_fn() -> None:
    def fake_prompt(q: InputQuestion, default: str | None = None) -> str:
        if q.key == "task_prompt":
            return "typed task"
        return default or ""

    out = collect_stage_inputs(
        stage="spec_plan",
        questions=SPEC_PLAN_QUESTIONS,
        provided={},
        interactive=True,
        prompt_fn=fake_prompt,
    )
    assert out["task_prompt"] == "typed task"


def test_collect_spec_plan_writes_artifacts(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    ref = repo / "docs.md"
    ref.parent.mkdir(parents=True, exist_ok=True)
    ref.write_text("# Reference\n", encoding="utf-8")
    artifact_root = str(tmp_path / "run")
    inputs, materials, input_path, ref_path = collect_spec_plan_inputs(
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
