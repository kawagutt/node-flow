"""P2: review_depth_preset mapping."""

from __future__ import annotations

import pytest

from nodeflow.workflows.dev_process.review_presets import (
    expected_exec_evidence_count_for_full_flow,
    expected_review_evidence_count,
    reviewer_keys_for_preset,
)
from nodeflow.workflows.dev_process.review_prompt_limits import assert_preset_limits_cover_reviewers


def test_light_preset_fewer_reviewers() -> None:
    light = reviewer_keys_for_preset("light")
    standard = reviewer_keys_for_preset("standard")
    deep = reviewer_keys_for_preset("deep")
    assert len(light) == 2
    assert len(standard) == 3
    assert len(deep) == 5
    assert len(light) < len(standard) < len(deep)
    assert "review_diff" in light
    assert "review_tests" in light


def test_unknown_preset_raises() -> None:
    with pytest.raises(ValueError, match="unknown review_depth_preset"):
        reviewer_keys_for_preset("ultra")


def test_expected_evidence_counts_from_preset() -> None:
    assert expected_review_evidence_count("standard") == 3
    assert expected_exec_evidence_count_for_full_flow("standard") == 9


def test_preset_limits_cover_all_reviewers() -> None:
    assert_preset_limits_cover_reviewers()


def test_review_prompt_leaf_wide_scan_with_standard_preset_uses_deep_limits() -> None:
    from nodeflow.core.registry import registry

    node_cls = registry.get("dev_process.review_prompt.wide_scan")
    assert node_cls is not None
    out = node_cls().execute(
        {
            "repo_root": ".",
            "base_ref": "HEAD",
            "review_depth_preset": "standard",
            "diff_result": {"diff": "x" * 100, "status_short": "", "untracked_files": []},
            "test_result": {},
            "approved_spec_plan": {"spec": "s", "plan": "p"},
        },
        {},
    )
    assert out["codex_task_prompt"]["text"]


def test_review_prompt_leaf_wide_scan_without_preset() -> None:
    from nodeflow.core.registry import registry

    node_cls = registry.get("dev_process.review_prompt.wide_scan")
    assert node_cls is not None
    out = node_cls().execute(
        {
            "repo_root": ".",
            "base_ref": "HEAD",
            "diff_result": {"diff": "x", "status_short": "", "untracked_files": []},
            "test_result": {},
            "approved_spec_plan": {"spec": "s", "plan": "p"},
        },
        {},
    )
    text = out["codex_task_prompt"]["text"]
    assert "Review" in text or "review" in text.lower()
