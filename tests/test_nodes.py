"""Built-in ActionNode behaviour."""

from __future__ import annotations

from pathlib import Path

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.nodes.exec.codex_exec import CodexExecNode
from nodeflow.nodes.routing.python_route_by_task_type import PythonRouteByTaskTypeNode
from nodeflow.nodes.summarize.python_summarize_result import PythonSummarizeResultNode


def test_python_route_review_default():
    node = PythonRouteByTaskTypeNode()
    out = node.execute({"task_type": "review"}, {})
    assert node.read_status() == "done"
    assert out["route"]["executor"] == "claude_code"
    assert "recommended_pipe_type" not in out["route"]


def test_python_route_implement_path():
    node = PythonRouteByTaskTypeNode()
    out = node.execute({"task_type": "implement", "needs_repo_write": True}, {})
    assert out["route"]["executor"] == "codex"
    assert "recommended_pipe_type" not in out["route"]


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
    assert pl["raw_response"] == {
        "returncode": 0,
        "args": ["echo", "codex-ok"],
        "stdin_used": False,
    }
    assert "revision" in out["_runtime"]["ports"]["execution_result"]


def test_codex_exec_custom_argv():
    node = CodexExecNode()
    out = node.execute({}, {"argv": ["sh", "-c", "echo hi"]})
    assert out["execution_result"]["stdout"] is not None
    assert "hi" in (out["execution_result"]["stdout"] or "")


def test_codex_exec_resolves_relative_cwd_against_workspace(tmp_path):
    node = CodexExecNode()
    workspace = tmp_path / "ws"
    subdir = workspace / "sub"
    subdir.mkdir(parents=True)
    out = node.execute(
        {},
        {
            "argv": ["sh", "-c", "pwd"],
            "_workspace_dir": str(workspace),
            "cwd": "sub",
        },
    )
    assert node.read_status() == "done"
    stdout = (out["execution_result"]["stdout"] or "").strip()
    assert stdout == str(subdir.resolve())
    assert out["execution_result"]["provider_meta"]["cwd"] == str(subdir.resolve())


def test_codex_exec_defaults_cwd_to_workspace(tmp_path):
    node = CodexExecNode()
    workspace = tmp_path / "ws2"
    workspace.mkdir(parents=True)
    out = node.execute(
        {},
        {
            "argv": ["sh", "-c", "pwd"],
            "_workspace_dir": str(workspace),
        },
    )
    assert node.read_status() == "done"
    stdout = (out["execution_result"]["stdout"] or "").strip()
    assert Path(stdout).resolve() == workspace.resolve()
