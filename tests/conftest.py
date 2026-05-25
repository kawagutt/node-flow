"""Top-level test fixtures.

The _hermetic_worker_default_global fixture prevents any test from accidentally
invoking a real Codex binary. Production behavior (WORKER_DEFAULT_ARGV = {})
is explicitly tested in test_constraints.py::TestNoImplicitWorkerExec.
"""

from __future__ import annotations

import sys

import pytest

from nodeflow.workflows.dev_process import exec_policy

_MULTI_STUB_SCRIPT = (
    "import json,sys; print(json.dumps("
    '{"spec":"# Spec\\n\\nTask spec.",'
    '"plan":"# Plan\\n\\nTask plan.",'
    '"ok":True,'
    '"blocking_findings":[],'
    '"non_blocking_findings":[],'
    '"spec_revision_needed":False}'
    "))"
)


@pytest.fixture(autouse=True)
def _hermetic_worker_default_global(monkeypatch: pytest.MonkeyPatch) -> None:
    """Patch WORKER_DEFAULT_ARGV globally so no test invokes real codex.

    Individual tests that need to verify production empty-default behavior
    should re-patch to {} via monkeypatch.setattr().
    """
    monkeypatch.setattr(
        exec_policy,
        "WORKER_DEFAULT_ARGV",
        {"codex": [sys.executable, "-c", _MULTI_STUB_SCRIPT]},
    )
