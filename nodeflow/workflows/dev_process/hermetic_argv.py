"""Hermetic argv presets for dev_process tests (no real Codex binary)."""

from __future__ import annotations

import sys


def _review_payload(*, blocking: bool) -> dict:
    return {
        "ok": not blocking,
        "blocking_findings": []
        if not blocking
        else [
            {
                "id": "R001",
                "area": "review",
                "summary": "hermetic blocking",
                "suggested_fix": "fix",
            }
        ],
        "non_blocking_findings": [],
        "spec_revision_needed": False,
    }


def spec_argv() -> list[str]:
    script = 'import json; print(json.dumps({"spec": "# Spec\\n\\nTask spec."}))'
    return [sys.executable, "-c", script]


def plan_argv() -> list[str]:
    script = 'import json; print(json.dumps({"plan": "# Plan\\n\\nTask plan."}))'
    return [sys.executable, "-c", script]


def spec_review_argv(*, blocking: bool = False) -> list[str]:
    payload = _review_payload(blocking=blocking)
    script = f"import json; print(json.dumps({payload!r}))"
    return [sys.executable, "-c", script]


def plan_review_argv(*, blocking: bool = False) -> list[str]:
    payload = _review_payload(blocking=blocking)
    script = f"import json; print(json.dumps({payload!r}))"
    return [sys.executable, "-c", script]


def implement_argv() -> list[str]:
    return [sys.executable, "-c", "print('implementation stub ok')"]


def review_argv(*, blocking: bool = False) -> list[str]:
    payload = _review_payload(blocking=blocking)
    script = f"import json; print(json.dumps({payload!r}))"
    return [sys.executable, "-c", script]
