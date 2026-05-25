"""P4: exec worker abstraction."""

from __future__ import annotations

import pytest

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.hermetic_argv import spec_argv
from nodeflow.workflows.dev_process.workers import (
    EXEC_WORKER_CODEX,
    CodexExecWorker,
    resolve_exec_worker,
    run_exec,
)


def test_resolve_exec_worker_codex() -> None:
    worker = resolve_exec_worker(EXEC_WORKER_CODEX)
    assert isinstance(worker, CodexExecWorker)
    assert worker.kind == "codex"
    assert worker.invoker == "codex_exec"


def test_resolve_exec_worker_unknown() -> None:
    with pytest.raises(NodeExecutionFailure, match="unsupported exec_worker kind"):
        resolve_exec_worker("claude")


def test_codex_worker_hermetic_run(tmp_path) -> None:
    worker = resolve_exec_worker(None)
    out = run_exec(
        worker,
        prompt="ignored",
        cwd=str(tmp_path),
        argv=spec_argv(),
        timeout=30,
    )
    assert out.get("ok") is True
    assert '"spec"' in str(out.get("stdout") or "")


def test_codex_worker_propagates_codex_node_fatal(monkeypatch) -> None:
    err = NodeExecutionFailure("child failed")

    class FakeCodexExecNode:
        def execute(self, inputs, params):
            return {}

        def read_status(self):
            return "fatal"

        def read_error(self):
            return err

    monkeypatch.setattr(
        "nodeflow.workflows.dev_process.workers.CodexExecNode",
        FakeCodexExecNode,
    )

    with pytest.raises(NodeExecutionFailure, match="child failed"):
        CodexExecWorker().run(prompt="p", cwd=".", argv=["x"])
