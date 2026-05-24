"""Hermetic argv presets for dev_process tests (no real Codex binary)."""

from __future__ import annotations

import sys


def spec_plan_argv() -> list[str]:
    script = (
        "import json; "
        'print(json.dumps({"spec": "# Spec\\n\\nTask spec.", "plan": "# Plan\\n\\nTask plan."}))'
    )
    return [sys.executable, "-c", script]


def implement_argv() -> list[str]:
    return [sys.executable, "-c", "print('implementation stub ok')"]


def review_argv(*, blocking: bool = False) -> list[str]:
    payload = {
        "ok": not blocking,
        "blocking_findings": []
        if not blocking
        else [
            {
                "id": "R001",
                "area": "diff",
                "summary": "hermetic blocking",
                "suggested_fix": "fix",
            }
        ],
        "non_blocking_findings": [],
        "spec_revision_needed": False,
    }
    script = f"import json; print(json.dumps({payload!r}))"
    return [sys.executable, "-c", script]
