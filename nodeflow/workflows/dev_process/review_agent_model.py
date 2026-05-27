"""Model profile metadata for review nodes (audit + exec_policy default entries).

Actual model selection is via ``exec_policy.nodes.<review_node_name>.argv``. The ``model``
field written by ``default_node_entries()`` is audit metadata only — not applied to argv.
"""

from __future__ import annotations

from nodeflow.workflows.dev_process.review_presets import PRESET_DEEP, normalize_preset

PROFILE_STRONG_REASONING = "strong_reasoning"
PROFILE_CODE_MAIN = "code_main"
PROFILE_CHEAP_AUX = "cheap_aux"

AGENT_MODEL_PROFILE: dict[str, str] = {
    "requirements": PROFILE_STRONG_REASONING,
    "architecture": PROFILE_CODE_MAIN,
    "test_quality": PROFILE_CODE_MAIN,
    "checklist_compliance": PROFILE_CODE_MAIN,
    "impact": PROFILE_STRONG_REASONING,
    "diff_detail": PROFILE_CODE_MAIN,
    "naming_doc": PROFILE_CHEAP_AUX,
}

DEEP_PRESET_MODEL_OVERRIDES: dict[str, str] = {
    "architecture": PROFILE_STRONG_REASONING,
    "checklist_compliance": PROFILE_STRONG_REASONING,
}


def effective_model_profile(agent: str, preset: str | None = None) -> str:
    """Return audit model profile for *agent* under *preset*."""
    preset_norm = normalize_preset(preset)
    if preset_norm == PRESET_DEEP and agent in DEEP_PRESET_MODEL_OVERRIDES:
        return DEEP_PRESET_MODEL_OVERRIDES[agent]
    return AGENT_MODEL_PROFILE.get(agent, PROFILE_CODE_MAIN)
