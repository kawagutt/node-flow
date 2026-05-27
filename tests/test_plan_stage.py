"""Tests for plan stage with parse retry logic."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.plan_prompt import build_plan_prompt
from nodeflow.workflows.dev_process.stages.plan import (
    MAX_PLAN_PARSE_RETRIES,
    run_plan_stage,
)


def _valid_phase_plan() -> str:
    return """## Phase 1: Add feature

**Goal:**
Implement the feature.

**Scope:**
- Add module.

**Excluded:**
- Nothing.

**Test plan:**
- Unit tests.

**Review plan:**
- targets: implementation_phase
- agents: architecture

**Review checklist:**
- Code is clean.

**Acceptance criteria:**
- Feature works."""


def _invalid_plan() -> str:
    """A plan with Phase heading but missing required sections."""
    return "## Phase 1: Bad phase\n\n**Goal:**\nDo something\n"


class TestRunPlanStageParseRetry:
    def _mock_generation(self, plan_text: str):
        """Return a patcher for _run_plan_generation that yields (plan_text, evidence_path)."""
        return patch(
            "nodeflow.workflows.dev_process.stages.plan._run_plan_generation",
            return_value=(plan_text, None),
        )

    def test_valid_plan_succeeds_first_attempt(self, tmp_path: Path) -> None:
        artifact_root = str(tmp_path / "artifacts")
        Path(artifact_root).mkdir(parents=True)
        with self._mock_generation(_valid_phase_plan()):
            result = run_plan_stage(
                repo_root=tmp_path,
                artifact_root=artifact_root,
                run_id="test_001",
                task_prompt="Test task",
                approved_spec="Test spec",
                body={},
            )
        assert result["status"] == "completed"
        assert result["parse_attempts"] == 1
        assert result["phase_count"] == 1
        assert "plan_json_path" in result
        assert "plan_sha256" in result

    def test_invalid_plan_retries_and_fails(self, tmp_path: Path) -> None:
        artifact_root = str(tmp_path / "artifacts")
        Path(artifact_root).mkdir(parents=True)
        with self._mock_generation(_invalid_plan()):
            with pytest.raises(NodeExecutionFailure, match="Plan parse failed after"):
                run_plan_stage(
                    repo_root=tmp_path,
                    artifact_root=artifact_root,
                    run_id="test_001",
                    task_prompt="Test task",
                    approved_spec="Test spec",
                    body={},
                )
        inv_dir = Path(artifact_root) / "plan" / "invalid_attempts"
        assert inv_dir.exists()
        for i in range(1, MAX_PLAN_PARSE_RETRIES + 1):
            assert (inv_dir / f"attempt_{i:03d}.md").exists()
            assert (inv_dir / f"attempt_{i:03d}_error.txt").exists()

    def test_retry_succeeds_on_second_attempt(self, tmp_path: Path) -> None:
        artifact_root = str(tmp_path / "artifacts")
        Path(artifact_root).mkdir(parents=True)
        call_count = {"n": 0}

        def gen_side_effect(**kwargs):
            call_count["n"] += 1
            text = _invalid_plan() if call_count["n"] == 1 else _valid_phase_plan()
            return (text, None)

        with patch(
            "nodeflow.workflows.dev_process.stages.plan._run_plan_generation",
            side_effect=gen_side_effect,
        ):
            result = run_plan_stage(
                repo_root=tmp_path,
                artifact_root=artifact_root,
                run_id="test_001",
                task_prompt="Test task",
                approved_spec="Test spec",
                body={},
            )
        assert result["status"] == "completed"
        assert result["parse_attempts"] == 2

    def test_parse_error_included_in_retry_prompt(self, tmp_path: Path) -> None:
        artifact_root = str(tmp_path / "artifacts")
        Path(artifact_root).mkdir(parents=True)
        kwargs_seen: list[dict] = []

        def gen_side_effect(**kwargs):
            kwargs_seen.append(kwargs)
            text = _invalid_plan() if len(kwargs_seen) < 3 else _valid_phase_plan()
            return (text, None)

        with patch(
            "nodeflow.workflows.dev_process.stages.plan._run_plan_generation",
            side_effect=gen_side_effect,
        ):
            run_plan_stage(
                repo_root=tmp_path,
                artifact_root=artifact_root,
                run_id="test_001",
                task_prompt="Test task",
                approved_spec="Test spec",
                body={},
            )
        assert kwargs_seen[1].get("parse_error_feedback") is not None
        assert "Phase" in kwargs_seen[1]["parse_error_feedback"]


class TestBuildPlanPrompt:
    def test_includes_format_rules(self) -> None:
        prompt = build_plan_prompt(task_prompt="task", approved_spec="spec")
        assert "## Phase N:" in prompt
        assert "**Goal:**" in prompt
        assert "implementation_phase" in prompt

    def test_completed_phases_injected(self) -> None:
        completed = [
            {"id": "phase_000", "title": "Done phase", "contract_sha256": "abc123"},
        ]
        prompt = build_plan_prompt(
            task_prompt="task",
            approved_spec="spec",
            completed_phases=completed,
        )
        assert "phase_000" in prompt
        assert "abc123" in prompt
        assert "IMMUTABLE" in prompt

    def test_parse_error_feedback_injected(self) -> None:
        prompt = build_plan_prompt(
            task_prompt="task",
            approved_spec="spec",
            parse_error_feedback="Missing required section **Goal:**",
        )
        assert "Parse error from previous attempt" in prompt
        assert "Missing required section" in prompt

    def test_backward_compat_without_new_params(self) -> None:
        prompt = build_plan_prompt(
            task_prompt="task",
            approved_spec="spec",
            revision_context="fix something",
            previous_plan="old plan",
        )
        assert "fix something" in prompt
        assert "old plan" in prompt
