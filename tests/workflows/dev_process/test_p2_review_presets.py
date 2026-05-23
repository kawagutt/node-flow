"""P2: review_depth_preset mapping."""

from __future__ import annotations

import pytest

from nodeflow.workflows.dev_process.review_presets import (
    reviewer_keys_for_preset,
)


def test_light_preset_fewer_reviewers() -> None:
    light = reviewer_keys_for_preset("light")
    standard = reviewer_keys_for_preset("standard")
    assert len(light) < len(standard)
    assert "review_diff" in light
    assert "review_tests" in light


def test_unknown_preset_raises() -> None:
    with pytest.raises(ValueError, match="unknown review_depth_preset"):
        reviewer_keys_for_preset("ultra")
