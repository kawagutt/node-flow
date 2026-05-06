"""E2E-style checks — PipeSpec pipes + YAML loader removal."""

from __future__ import annotations

from types import MappingProxyType

import pytest

from nodeflow.core.base_node import BaseNode, ExecutionContext
from nodeflow.core.run import load_and_kick_pipeline


def test_e2e_yaml_load_pipeline_removed(tmp_path) -> None:
    from nodeflow.core.loader import load_pipeline

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    yaml_path = workspace / "pipeline.yaml"
    yaml_path.write_text('version: "1.5"\ngraph:\n  nodes: []\n  final: x\n')
    with pytest.raises(NotImplementedError, match="YAML 1.5"):
        load_pipeline(str(workspace), str(yaml_path))


def test_run_returning_runtime_is_fatal():
    class BadPipe(BaseNode):
        def run(self, inputs, params: MappingProxyType, context: ExecutionContext):
            return {"out": {"x": 1}, "_runtime": {"ports": {}}}

    n = BadPipe()
    out = n.execute({}, {})
    assert out["_state"]["value"] == "fatal"
    assert out["_usage"] == {}
    assert out["_runtime"]["ports"] == {}
    assert n.read_status() == "fatal"


def test_load_and_kick_pipeline_removed(tmp_path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    yaml_path = workspace / "fatal.yaml"
    yaml_path.write_text("x: 1")
    with pytest.raises(NotImplementedError, match="load_and_kick_pipeline"):
        load_and_kick_pipeline(
            str(workspace), str(yaml_path), initial_inputs={"task_prompt": "hello"}
        )
