"""Model profile metadata for review nodes (exec_policy default entries).

The ``model`` field on review nodes is an audit profile key (``strong_reasoning``, etc.)
resolved to a Codex slug by ``worker_adapter.resolve_worker_model``. For ``codex exec``
argv, exec_policy ``model`` overrides any existing ``--model`` flag. For non-``codex exec``
argv overrides, argv is left unchanged and the resolved model is audit metadata only.
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
