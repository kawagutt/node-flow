"""E2E — compose: route → codex_exec → summarize."""

from __future__ import annotations

from nodeflow.execution.loader import load_pipeline
from nodeflow.nodes.action.exec.codex_exec import CodexExecNode
from nodeflow.nodes.action.routing.python_route_by_task_type import (
    PythonRouteByTaskTypeNode,
)
from nodeflow.nodes.action.transform.python_summarize_result import (
    PythonSummarizeResultNode,
)
from nodeflow.nodes.pipe.serial_pipe import SerialPipeNode


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
    pipe = SerialPipeNode(
        graph_node_order=["route", "exec", "summarize"],
        nodes=nodes,
        node_input_bindings=node_input_bindings,
        node_param_definitions=node_param_definitions,
        final_id="summarize",
    )
    out = pipe.execute({"task_type": "implement", "task_prompt": "x"}, {})
    assert pipe.read_status() == "done"
    for n in nodes.values():
        assert n.read_status() == "done"
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
    assert "_meta" in out["summary"]
    assert "revision" in out["summary"]["_meta"]
