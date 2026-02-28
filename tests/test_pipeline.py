"""
Tier 3: PipelineNode テスト。Script → Script の最小パイプライン。
"""

import tempfile
from pathlib import Path

import pytest

from nodeflow.node import BaseNode, NodeExecutionFailure, NodeExecutionLimit
from nodeflow.nodes import PythonScriptNode
from nodeflow.pipeline_node import PipelineNode
from nodeflow.runner import Runner


def test_script_to_script(tmp_path):
    """最小 Pipeline: Script A → Script B。最終出力と各 status を確認。"""
    a_py = tmp_path / "a.py"
    a_py.write_text('def main(inputs): return {"value": inputs.get("x", "") + ":a"}')
    b_py = tmp_path / "b.py"
    b_py.write_text(
        'def main(inputs): data = inputs.get("data", {}); return {"out": data.get("value", "") + ":b"}'
    )

    nodes = {
        "a": PythonScriptNode(),
        "b": PythonScriptNode(),
    }
    graph_node_order = ["a", "b"]
    node_input_bindings = {
        "a": {"x": ("inputs", "x")},
        "b": {"data": ("node", "a", "result")},
    }
    node_param_definitions = {
        "a": {"script": str(a_py)},
        "b": {"script": str(b_py)},
    }
    pipeline = PipelineNode(
        graph_node_order=graph_node_order,
        nodes=nodes,
        node_input_bindings=node_input_bindings,
        node_param_definitions=node_param_definitions,
        final_id="b",
    )
    out = pipeline.execute({"x": "in"}, {})
    assert pipeline.read_status() == "done"
    assert nodes["a"].read_status() == "done"
    assert nodes["b"].read_status() == "done"
    assert "result" in out
    assert out["result"]["out"] == "in:a:b"


def test_pipeline_fatal_propagates(tmp_path):
    """中間ノードが fatal なら Pipeline も fatal、{} を返す。"""
    a_py = tmp_path / "a.py"
    a_py.write_text('def main(inputs): raise RuntimeError("a failed")')
    b_py = tmp_path / "b.py"
    b_py.write_text('def main(inputs): return {"out": 1}')

    nodes = {"a": PythonScriptNode(), "b": PythonScriptNode()}
    pipeline = PipelineNode(
        graph_node_order=["a", "b"],
        nodes=nodes,
        node_input_bindings={"a": {"x": ("inputs", "x")}, "b": {"data": ("node", "a", "result")}},
        node_param_definitions={"a": {"script": str(a_py)}, "b": {"script": str(b_py)}},
        final_id="b",
    )
    out = pipeline.execute({"x": 1}, {})
    assert pipeline.read_status() == "fatal"
    assert out == {}
    assert nodes["a"].read_status() == "fatal"


def test_pipeline_limit_propagates(tmp_path):
    """中間ノードが limit なら Pipeline も limit、{} を返す。"""
    from nodeflow.node import NodeExecutionLimit

    class LimitOnceNode(BaseNode):
        def run(self, inputs, params, context):
            raise NodeExecutionLimit()

    b_py = tmp_path / "b.py"
    b_py.write_text('def main(inputs): return {"v": 2}')
    nodes = {"a": LimitOnceNode(), "b": PythonScriptNode()}

    pipeline = PipelineNode(
        graph_node_order=["a", "b"],
        nodes=nodes,
        node_input_bindings={"a": {"x": ("inputs", "x")}, "b": {"data": ("node", "a", "result")}},
        node_param_definitions={"a": {}, "b": {"script": str(b_py)}},
        final_id="b",
    )
    out = pipeline.execute({"x": 1}, {})
    assert nodes["a"].read_status() == "limit"
    assert pipeline.read_status() == "limit"
    assert out == {}


def test_pipeline_max_calls_limit(tmp_path):
    """PipelineNode 自身に max_calls=1 を渡すと 2 回目の execute で limit になる。"""
    a_py = tmp_path / "a.py"
    a_py.write_text('def main(inputs): return {"value": inputs.get("x", "")}')
    b_py = tmp_path / "b.py"
    b_py.write_text('def main(inputs): return {"out": inputs.get("data", "")}')
    nodes = {"a": PythonScriptNode(), "b": PythonScriptNode()}
    pipeline = PipelineNode(
        graph_node_order=["a", "b"],
        nodes=nodes,
        node_input_bindings={"a": {"x": ("inputs", "x")}, "b": {"data": ("node", "a", "result")}},
        node_param_definitions={"a": {"script": str(a_py)}, "b": {"script": str(b_py)}},
        final_id="b",
    )
    params = {"limit": {"max_calls": 1}}
    out1 = pipeline.execute({"x": "1"}, params)
    assert pipeline.read_status() == "done"
    assert "result" in out1
    pipeline.reset_status()
    out2 = pipeline.execute({"x": "2"}, params)
    assert pipeline.read_status() == "limit"
    assert out2 == {}
