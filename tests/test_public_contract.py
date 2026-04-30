"""Public contract: loader root PipeNode, runtime revision, Runner surface.

Stable entrypoints for callers and tests: ``nodeflow.core.loader.load_pipeline``,
``nodeflow.core.loader.load_node_pipeline``, ``nodeflow.core.run.load_and_kick_pipeline``.
Prefer these over deep imports of private loader helpers unless necessary.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from nodeflow.core.loader import load_pipeline
from nodeflow.core.node_kinds import PipeNode
from nodeflow.core.runner import Runner


def test_loader_root_is_internal_pipe_node(tmp_path):
    yaml_path = tmp_path / "pipeline.yaml"
    yaml_path.write_text(
        """
version: "1.5"
graph:
  nodes:
    - id: r
      type: python_route_by_task_type
      inputs:
        task_type: ${inputs.task_type}
      params: {}
  final: r
"""
    )
    root = load_pipeline(str(tmp_path), str(yaml_path))
    assert isinstance(root, PipeNode)


def test_execute_attaches_runtime_ports_revision():
    from types import MappingProxyType

    from nodeflow.core.base_node import BaseNode, ExecutionContext

    class N(BaseNode):
        def run(self, inputs, params: MappingProxyType, context: ExecutionContext):
            return {"p": {"x": 1}}

    n = N()
    out = n.execute({}, {})
    assert out["p"]["x"] == 1
    assert "revision" in out["_runtime"]["ports"]["p"]


def test_runner_has_no_resolve_role_method():
    assert not hasattr(Runner, "resolve_role")


def test_fixed_provider_pipe_bundles_child_domain_only():
    """Custom PipeNode.run() must not leak child _runtime into bundled domain ports."""
    from nodeflow.workflows.implement_with_codex.node_implement_with_codex import (
        ImplementWithCodexPipeNode,
    )
    from nodeflow.workflows.review_with_claude.node_review_with_claude import (
        ReviewWithClaudePipeNode,
    )

    impl = ImplementWithCodexPipeNode()
    out_impl = impl.execute(
        {"task_type": "implement", "task_prompt": "hi"},
        {"codex_exec": {"argv": ["sh", "-c", "echo dispatch-contract"]}},
    )
    assert impl.read_status() == "done"
    for port in ("summary", "execution_result"):
        assert port in out_impl
        assert isinstance(out_impl[port], dict)
        assert "_runtime" not in out_impl[port]
    assert "_runtime" in out_impl
    assert "revision" in out_impl["_runtime"]["ports"]["execution_result"]

    rev = ReviewWithClaudePipeNode()
    out_rev = rev.execute(
        {"task_type": "review", "task_prompt": "x"},
        {"claude_code_exec": {"argv": ["sh", "-c", "echo review-pipe"]}},
    )
    assert rev.read_status() == "done"
    for port in ("summary", "execution_result"):
        assert port in out_rev
        assert "_runtime" not in out_rev[port]
    assert "_runtime" in out_rev
    assert "revision" in out_rev["_runtime"]["ports"]["execution_result"]


def test_fixed_provider_pipe_resolves_relative_cwd_against_workspace(tmp_path):
    from nodeflow.workflows.implement_with_codex.node_implement_with_codex import (
        ImplementWithCodexPipeNode,
    )
    from nodeflow.workflows.review_with_claude.node_review_with_claude import (
        ReviewWithClaudePipeNode,
    )

    workspace = tmp_path / "workspace"
    subdir = workspace / "sub"
    subdir.mkdir(parents=True)

    impl = ImplementWithCodexPipeNode()
    out_impl = impl.execute(
        {"task_type": "implement", "task_prompt": "hello"},
        {
            "_workspace_dir": str(workspace),
            "codex_exec": {"argv": ["sh", "-c", "pwd"], "cwd": "sub"},
        },
    )
    assert impl.read_status() == "done"
    assert (out_impl["execution_result"]["stdout"] or "").strip() == str(subdir.resolve())

    rev = ReviewWithClaudePipeNode()
    out_rev = rev.execute(
        {"task_type": "review", "task_prompt": "hello"},
        {
            "_workspace_dir": str(workspace),
            "claude_code_exec": {"argv": ["sh", "-c", "pwd"], "cwd": "sub"},
        },
    )
    assert rev.read_status() == "done"
    assert (out_rev["execution_result"]["stdout"] or "").strip() == str(subdir.resolve())


def test_fixed_provider_pipe_requires_task_type_input():
    from nodeflow.workflows.implement_with_codex.node_implement_with_codex import (
        ImplementWithCodexPipeNode,
    )
    from nodeflow.workflows.review_with_claude.node_review_with_claude import (
        ReviewWithClaudePipeNode,
    )

    impl = ImplementWithCodexPipeNode()
    out_impl = impl.execute(
        {"task_prompt": "x"},
        {"codex_exec": {"argv": ["sh", "-c", "echo should-not-run"]}},
    )
    assert out_impl == {}
    assert impl.read_status() == "fatal"
    assert "inputs.task_type is required" in str(impl.read_error())

    rev = ReviewWithClaudePipeNode()
    out_rev = rev.execute(
        {"task_prompt": "x"},
        {"claude_code_exec": {"argv": ["sh", "-c", "echo should-not-run"]}},
    )
    assert out_rev == {}
    assert rev.read_status() == "fatal"
    assert "inputs.task_type is required" in str(rev.read_error())


def test_examples_fixed_provider_yaml_nested_argv_matches_readme():
    """Sample pipelines under examples/pipelines must include nested exec argv."""
    repo = Path(__file__).resolve().parents[1]
    for filename, inputs in (
        ("review_with_claude.yaml", {"task_type": "review", "task_prompt": "x"}),
        ("implement_with_codex.yaml", {"task_type": "implement", "task_prompt": "x"}),
    ):
        path = repo / "examples/pipelines" / filename
        assert path.is_file(), f"missing {path}"
        root = load_pipeline(str(repo / "examples"), str(path))
        assert isinstance(root, PipeNode)
        out = root.execute(inputs, {})
        assert root.read_status() == "done"
        assert "execution_result" in out
        assert "_runtime" not in out["execution_result"]


def test_loader_rejects_missing_id_or_type(tmp_path):
    yaml_path = tmp_path / "bad.yaml"
    yaml_path.write_text(
        """
version: "1.5"
graph:
  nodes:
    - type: python_route_by_task_type
      inputs:
        task_type: ${inputs.task_type}
      params: {}
  final: route
"""
    )
    with pytest.raises(ValueError, match="id is required"):
        load_pipeline(str(tmp_path), str(yaml_path))


def test_loader_rejects_unknown_final_node(tmp_path):
    yaml_path = tmp_path / "bad_final.yaml"
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
  final: missing
"""
    )
    with pytest.raises(ValueError, match="unknown node id"):
        load_pipeline(str(tmp_path), str(yaml_path))


def test_loader_rejects_invalid_reference_syntax(tmp_path):
    yaml_path = tmp_path / "bad_ref.yaml"
    yaml_path.write_text(
        """
version: "1.5"
graph:
  nodes:
    - id: route
      type: python_route_by_task_type
      inputs:
        task_type: ${inputs.task_type.extra}
      params: {}
  final: route
"""
    )
    with pytest.raises(ValueError, match="invalid reference syntax"):
        load_pipeline(str(tmp_path), str(yaml_path))


def test_loader_rejects_forward_reference(tmp_path):
    yaml_path = tmp_path / "bad_forward_ref.yaml"
    yaml_path.write_text(
        """
version: "1.5"
graph:
  nodes:
    - id: a
      type: python_summarize_result
      inputs:
        execution_result: ${b.execution_result}
      params: {}
    - id: b
      type: codex_exec
      inputs:
        prompt: ${inputs.task_prompt}
      params:
        argv: ["sh", "-c", "echo x"]
  final: b
"""
    )
    with pytest.raises(ValueError, match="before it is available"):
        load_pipeline(str(tmp_path), str(yaml_path))
