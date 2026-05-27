"""Hermetic argv presets for dev_process tests ONLY (no real Codex binary).

These functions return Python one-liner commands that produce stub JSON output.
They are used exclusively in tests via explicit --exec-argv or argv_override.
Production code MUST NOT import from this module; use exec_policy.WORKER_DEFAULT_ARGV
or resolve_node_exec() instead.
"""

from __future__ import annotations

import json
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


PHASE_PLAN_TEXT = (
    "## Phase 1: Implement task\n\n"
    "**Goal:**\nImplement the task.\n\n"
    "**Scope:**\n- Implement feature.\n\n"
    "**Excluded:**\n- Nothing.\n\n"
    "**Test plan:**\n- Unit tests.\n\n"
    "**Review plan:**\n- targets: implementation_phase\n- agents: architecture, checklist_compliance\n\n"
    "**Review checklist:**\n- Code is clean.\n\n"
    "**Acceptance criteria:**\n- Feature works."
)


THREE_PHASE_PLAN_TEXT = (
    "## Phase 1: Setup\n\n"
    "**Goal:**\nSetup.\n\n"
    "**Scope:**\n- Init.\n\n"
    "**Excluded:**\n- Nothing.\n\n"
    "**Test plan:**\n- UNIQUE_TEST_PLAN_PHASE_000\n\n"
    "**Review plan:**\n- targets: implementation_phase\n- agents: architecture, checklist_compliance\n\n"
    "**Review checklist:**\n- Clean.\n\n"
    "**Acceptance criteria:**\n- Done.\n\n"
    "## Phase 2: Core\n\n"
    "**Goal:**\nCore.\n\n"
    "**Scope:**\n- Core work.\n\n"
    "**Excluded:**\n- Nothing.\n\n"
    "**Test plan:**\n- UNIQUE_TEST_PLAN_PHASE_001\n\n"
    "**Review plan:**\n- targets: implementation_phase\n- agents: architecture, checklist_compliance\n\n"
    "**Review checklist:**\n- Clean.\n\n"
    "**Acceptance criteria:**\n- Done.\n\n"
    "## Phase 3: Polish\n\n"
    "**Goal:**\nPolish.\n\n"
    "**Scope:**\n- Polish.\n\n"
    "**Excluded:**\n- Nothing.\n\n"
    "**Test plan:**\n- UNIQUE_TEST_PLAN_PHASE_002\n\n"
    "**Review plan:**\n- targets: implementation_phase\n- agents: architecture, checklist_compliance\n\n"
    "**Review checklist:**\n- Clean.\n\n"
    "**Acceptance criteria:**\n- Done."
)


def spec_argv() -> list[str]:
    payload = json.dumps({"spec": "# Spec\n\nTask spec."})
    script = f"import json; print({payload!r})"
    return [sys.executable, "-c", script]


def plan_argv() -> list[str]:
    payload = json.dumps({"plan": PHASE_PLAN_TEXT})
    script = f"import json; print({payload!r})"
    return [sys.executable, "-c", script]


def spec_review_argv(*, blocking: bool = False) -> list[str]:
    payload = json.dumps(_review_payload(blocking=blocking))
    script = f"import json; print({payload!r})"
    return [sys.executable, "-c", script]


def plan_review_argv(*, blocking: bool = False) -> list[str]:
    payload = json.dumps(_review_payload(blocking=blocking))
    script = f"import json; print({payload!r})"
    return [sys.executable, "-c", script]


def implement_argv() -> list[str]:
    return [sys.executable, "-c", "print('implementation stub ok')"]


def review_argv(*, blocking: bool = False) -> list[str]:
    payload = json.dumps(_review_payload(blocking=blocking))
    script = f"import json; print({payload!r})"
    return [sys.executable, "-c", script]


def blocking_review_argv() -> list[str]:
    """Alias for ``review_argv(blocking=True)`` (force_review_blocking integration tests)."""
    return review_argv(blocking=True)


def _model_probe_script() -> str:
    return (
        "import json, sys\n"
        "def pick_model(args):\n"
        "    i = 0\n"
        "    while i < len(args):\n"
        "        if args[i] in ('--model', '-m') and i + 1 < len(args):\n"
        "            return args[i + 1]\n"
        "        i += 1\n"
        "    return None\n"
        "print(json.dumps({'model': pick_model(sys.argv[1:])}))\n"
    )


def model_probe_argv() -> list[str]:
    """Hermetic codex-like argv; stdout JSON includes resolved ``--model`` flag."""
    return [sys.executable, "-c", _model_probe_script(), "codex", "exec", "--", "probe"]
