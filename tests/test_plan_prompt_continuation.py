"""Continuation planning mode metadata in prompts."""

from __future__ import annotations

from nodeflow.workflows.dev_process.plan_prompt import format_planning_mode_context


def test_format_planning_mode_context_includes_metadata() -> None:
    block = format_planning_mode_context(
        {
            "planning_mode": "continuation_from_head",
            "continuation_count": 2,
            "continuation_start_phase": "phase_003",
            "current_plan_version": "plan_v01_02",
            "current_spec_version": "spec_v01_00",
        }
    )
    assert "continuation_from_head" in block
    assert "phase_003" in block
    assert "plan_v01_02" in block


def test_format_planning_mode_context_empty_for_normal_rework() -> None:
    assert format_planning_mode_context({"planning_mode": "rework"}) == ""
