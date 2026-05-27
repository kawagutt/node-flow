"""Agent prompt limits respect review_depth_preset scaling."""

from __future__ import annotations

from nodeflow.workflows.dev_process.review_presets import PRESET_DEEP, PRESET_LIGHT, PRESET_STANDARD
from nodeflow.workflows.dev_process.review_prompt_limits import prompt_params_for_reviewer


def test_agent_limits_scale_with_preset() -> None:
    light = prompt_params_for_reviewer(PRESET_LIGHT, "architecture")["max_diff_chars"]
    standard = prompt_params_for_reviewer(PRESET_STANDARD, "architecture")["max_diff_chars"]
    deep = prompt_params_for_reviewer(PRESET_DEEP, "architecture")["max_diff_chars"]
    assert light < standard < deep


def test_requirements_sees_diff() -> None:
    for preset in (PRESET_LIGHT, PRESET_STANDARD, PRESET_DEEP):
        assert prompt_params_for_reviewer(preset, "requirements")["max_diff_chars"] > 0


def test_checklist_compliance_sees_diff() -> None:
    for preset in (PRESET_LIGHT, PRESET_STANDARD, PRESET_DEEP):
        assert prompt_params_for_reviewer(preset, "checklist_compliance")["max_diff_chars"] > 0
