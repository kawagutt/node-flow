"""
Tier 2: PythonScriptNode / LLMNode (mock) テスト。
"""

import os
import tempfile
from pathlib import Path

import pytest

from nodeflow.nodes import LLMNode, PythonScriptNode


def test_python_script_returns_dict(tmp_path):
    script = tmp_path / "main.py"
    script.write_text(
        """
def main(inputs):
    return {"value": inputs.get("x", 0)}
"""
    )
    node = PythonScriptNode()
    out = node.execute({"x": 42}, {"script": str(script)})
    assert node.read_status() == "done"
    assert "result" in out
    assert out["result"]["value"] == 42


def test_python_script_raises_fatal(tmp_path):
    script = tmp_path / "fail.py"
    script.write_text(
        """
def main(inputs):
    raise RuntimeError("script error")
"""
    )
    node = PythonScriptNode()
    out = node.execute({}, {"script": str(script)})
    assert node.read_status() == "fatal"
    assert out == {}
    assert "script error" in str(node.read_error())


def test_python_script_non_dict_return_fatal(tmp_path):
    script = tmp_path / "bad.py"
    script.write_text(
        """
def main(inputs):
    return "not a dict"
"""
    )
    node = PythonScriptNode()
    out = node.execute({}, {"script": str(script)})
    assert node.read_status() == "fatal"
    assert out == {}
    assert isinstance(node.read_error(), TypeError)


def test_llm_node_mock_returns_response():
    node = LLMNode()
    out = node.execute({"prompt": "hello"}, {})
    assert node.read_status() == "done"
    assert "response" in out
    assert out["response"] == "mock:hello"


def test_llm_node_mock_empty_prompt():
    node = LLMNode()
    out = node.execute({}, {})
    assert node.read_status() == "done"
    assert out["response"] == "mock:"
