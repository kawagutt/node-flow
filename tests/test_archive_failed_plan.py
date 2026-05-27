"""Tests for rejected plan archival and previous-plan restore."""

from __future__ import annotations

import pytest

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.flow_actions import _archive_failed_plan_attempt
from tests.test_plan_phases import _make_phase_md


def test_archive_restores_plan_json_from_previous_plan(tmp_path) -> None:
    artifact_root = str(tmp_path / "run")
    plan_dir = tmp_path / "run" / "plan"
    plan_dir.mkdir(parents=True)
    previous = _make_phase_md()
    rejected = _make_phase_md(goal="Rejected goal")
    (plan_dir / "plan.md").write_text(rejected, encoding="utf-8")
    (plan_dir / "plan.json").write_text('{"rejected": true}', encoding="utf-8")

    _archive_failed_plan_attempt(artifact_root, previous)

    assert (plan_dir / "plan.md").read_text(encoding="utf-8") == previous
    restored = (plan_dir / "plan.json").read_text(encoding="utf-8")
    assert "rejected" not in restored
    assert (plan_dir / "rework_attempts" / "attempt_001" / "plan.md").exists()


def test_archive_raises_when_previous_plan_unparseable(tmp_path) -> None:
    artifact_root = str(tmp_path / "run")
    plan_dir = tmp_path / "run" / "plan"
    plan_dir.mkdir(parents=True)
    (plan_dir / "plan.md").write_text("draft", encoding="utf-8")
    (plan_dir / "plan.json").write_text('{"draft": true}', encoding="utf-8")
    plan_stage: dict = {}

    with pytest.raises(NodeExecutionFailure, match="restoring the previous plan.json"):
        _archive_failed_plan_attempt(
            artifact_root,
            "not valid phase plan",
            plan_stage=plan_stage,
        )

    assert plan_stage.get("plan_restore_failed") is True
    assert plan_stage.get("plan_restore_error")
    assert (plan_dir / "plan.md").read_text(encoding="utf-8") == "not valid phase plan"
    assert "draft" in (plan_dir / "plan.json").read_text(encoding="utf-8")
