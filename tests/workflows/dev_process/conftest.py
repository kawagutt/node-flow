"""Fixtures for dev_process workflow tests."""

from __future__ import annotations

import pytest

from nodeflow.builtins import register_builtin_nodes


@pytest.fixture(autouse=True)
def _ensure_dev_process_registered() -> None:
    register_builtin_nodes()
    yield
    # builtins re-register on import; no teardown needed
