"""Built-in ActionNode behaviour."""

from __future__ import annotations

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.nodes.exec.codex_exec import CodexExecNode
from nodeflow.nodes.routing.python_route_by_task_type import PythonRouteByTaskTypeNode
from nodeflow.nodes.summarize.python_summarize_result import PythonSummarizeResultNode


def test_python_route_review_default():
    node = PythonRouteByTaskTypeNode()
    out = node.execute({"task_type": "review"}, {})
    assert node.read_status() == "done"
    assert out["route"]["executor"] == "claude_code"
    assert out["route"]["next_node"] == "review_dispatch"


def test_python_route_implement_path():
    node = PythonRouteByTaskTypeNode()
    out = node.execute({"task_type": "implement", "needs_repo_write": True}, {})
    assert out["route"]["executor"] == "codex"
    assert out["route"]["next_node"] == "implement_dispatch"


def test_python_summarize_reads_execution_result():
    node = PythonSummarizeResultNode()
    er = {
        "ok": True,
        "executor": "codex",
        "provider": "codex",
        "model": None,
        "task_type": "implement",
        "summary": None,
        "stdout": "patch applied\n",
        "stderr": "",
        "raw_response": {"rc": 0},
        "artifacts": [],
        "provider_meta": {},
        "next_hint": None,
    }
    out = node.execute({"execution_result": er}, {})
    assert node.read_status() == "done"
    assert "summary" in out
    assert "short" in out["summary"]
    assert out["summary"]["key_findings"]


def test_codex_exec_missing_argv_is_fatal():
    node = CodexExecNode()
    out = node.execute({}, {})
    assert node.read_status() == "fatal"
    assert out == {}
    assert isinstance(node.read_error(), NodeExecutionFailure)


def test_codex_exec_runs_with_valid_argv():
    node = CodexExecNode()
    out = node.execute({}, {"argv": ["echo", "codex-ok"]})
    assert node.read_status() == "done"
    assert "execution_result" in out
    pl = out["execution_result"]
    assert pl["ok"] is True
    assert pl["executor"] == "codex"
    assert pl["raw_response"] == {"returncode": 0, "args": ["echo", "codex-ok"]}
    assert "revision" in out["_runtime"]["ports"]["execution_result"]


def test_codex_exec_custom_argv():
    node = CodexExecNode()
    out = node.execute({}, {"argv": ["sh", "-c", "echo hi"]})
    assert out["execution_result"]["stdout"] is not None
    assert "hi" in (out["execution_result"]["stdout"] or "")
