"""Guard: PipeNode default path must not depend on legacy RunnerFrame."""

from __future__ import annotations

import inspect

from nodeflow.core.node_kinds.pipe_node import PipeNode


def test_pipe_node_default_run_has_no_runner_frame() -> None:
    src = inspect.getsource(PipeNode.run)
    assert "RunnerFrame" not in src
    assert "runner_frame" not in src
