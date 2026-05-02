"""E2E — route → codex_exec → summarize (linear PipeNode graph)."""

from __future__ import annotations

from types import MappingProxyType

import pytest

from nodeflow.core.base_node import BaseNode, ExecutionContext, NodeExecutionFailure
from nodeflow.core.graph_spec import GraphSpec
from nodeflow.core.loader import load_pipeline
from nodeflow.core.node_kinds import PipeNode
from nodeflow.core.run import load_and_kick_pipeline
from nodeflow.nodes.exec.codex_exec import CodexExecNode
from nodeflow.nodes.routing.python_route_by_task_type import PythonRouteByTaskTypeNode
from nodeflow.nodes.summarize.python_summarize_result import PythonSummarizeResultNode


class _TestPipeNode(PipeNode):
    def __init__(self, spec: GraphSpec) -> None:
        super().__init__()
        self._spec = spec

    def graph(self) -> GraphSpec:
        return self._spec


def test_route_exec_summarize_in_memory():
    nodes = {
        "route": PythonRouteByTaskTypeNode(),
        "exec": CodexExecNode(),
        "summarize": PythonSummarizeResultNode(),
    }
    node_input_bindings = {
        "route": {"task_type": ("inputs", "task_type")},
        "exec": {"prompt": ("inputs", "task_prompt")},
        "summarize": {"execution_result": ("node", "exec", "execution_result")},
    }
    node_param_definitions = {
        "route": {},
        "exec": {"argv": ["sh", "-c", "echo e2e-out"]},
        "summarize": {},
    }
    pipe = _TestPipeNode(
        GraphSpec(
            nodes=nodes,
            order=["route", "exec", "summarize"],
            bindings=node_input_bindings,
            params=node_param_definitions,
            final="summarize",
        )
    )
    out = pipe.execute({"task_type": "implement", "task_prompt": "x"}, {})
    assert pipe.read_status() == "done"
    assert nodes["route"].read_status() == "done"
    assert nodes["exec"].read_status() == "done"
    assert nodes["summarize"].read_status() == "done"
    assert "summary" in out
    assert "short" in out["summary"]


def test_e2e_via_load_pipeline(tmp_path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    yaml_path = workspace / "pipeline.yaml"
    yaml_path.write_text(
        """
version: "1.5"
name: e2e

graph:
  nodes:
    - id: route
      type: python_route_by_task_type
      inputs:
        task_type: ${inputs.task_type}
      params: {}
    - id: exec
      type: codex_exec
      inputs:
        prompt: ${inputs.task_prompt}
      params:
        argv: ["sh", "-c", "echo yaml-e2e"]
    - id: summarize
      type: python_summarize_result
      inputs:
        execution_result: ${exec.execution_result}
      params: {}
  final: summarize
"""
    )

    root = load_pipeline(str(workspace), str(yaml_path))
    out = root.execute({"task_type": "implement", "task_prompt": "hello"}, {})
    assert root.read_status() == "done"
    assert "summary" in out
    assert "revision" in out["_runtime"]["ports"]["summary"]


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


def test_load_and_kick_pipeline_raises_on_root_fatal(tmp_path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    yaml_path = workspace / "fatal.yaml"
    yaml_path.write_text(
        """
version: "1.5"
graph:
  nodes:
    - id: exec
      type: codex_exec
      inputs:
        prompt: ${inputs.task_prompt}
      params: {}
  final: exec
"""
    )
    with pytest.raises(NodeExecutionFailure, match=r"status=fatal: .*params\.argv"):
        load_and_kick_pipeline(
            str(workspace), str(yaml_path), initial_inputs={"task_prompt": "hello"}
        )


def test_load_and_kick_pipeline_raises_on_root_limit(tmp_path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    yaml_path = workspace / "limit.yaml"
    yaml_path.write_text(
        """
version: "1.5"
graph:
  nodes:
    - id: route
      type: python_route_by_task_type
      inputs:
        task_type: ${inputs.task_type}
      params: {}
  final: route
"""
    )
    with pytest.raises(NodeExecutionFailure, match="status=limit"):
        load_and_kick_pipeline(
            str(workspace),
            str(yaml_path),
            initial_inputs={"task_type": "review"},
            params={"limit": {"max_calls": 0}},
        )


def test_load_and_kick_pipeline_injects_workspace_for_cli_cwd(tmp_path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    yaml_path = workspace / "cwd.yaml"
    yaml_path.write_text(
        """
version: "1.5"
graph:
  nodes:
    - id: exec
      type: codex_exec
      inputs:
        prompt: ${inputs.task_prompt}
      params:
        argv: ["sh", "-c", "pwd"]
  final: exec
"""
    )
    out = load_and_kick_pipeline(
        str(workspace),
        str(yaml_path),
        initial_inputs={"task_prompt": "hello"},
    )
    assert "execution_result" in out
    stdout = (out["execution_result"]["stdout"] or "").strip()
    assert stdout == str(workspace.resolve())
