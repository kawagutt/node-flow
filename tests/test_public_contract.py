"""Public contract: Runner surface, PipeNode.execute, fixed-provider pipes."""

from __future__ import annotations

from pathlib import Path

import pytest

from nodeflow.core.loader import load_pipeline
from nodeflow.core.runner import Runner


def test_load_pipeline_removed(tmp_path) -> None:
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
    with pytest.raises(NotImplementedError, match="YAML 1.5"):
        load_pipeline(str(tmp_path), str(yaml_path))


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
    assert out_impl["_state"]["value"] == "fatal"
    assert impl.read_status() == "fatal"
    assert "inputs.task_type is required" in str(impl.read_error())

    rev = ReviewWithClaudePipeNode()
    out_rev = rev.execute(
        {"task_prompt": "x"},
        {"claude_code_exec": {"argv": ["sh", "-c", "echo should-not-run"]}},
    )
    assert out_rev["_state"]["value"] == "fatal"
    assert rev.read_status() == "fatal"
    assert "inputs.task_type is required" in str(rev.read_error())


def test_examples_pipelines_has_no_public_yaml_examples() -> None:
    """v1.6 public samples are JSON; YAML under ``examples/pipelines`` is obsolete noise."""
    repo = Path(__file__).resolve().parents[1]
    root = repo / "examples" / "pipelines"
    yamls = list(root.rglob("*.yaml")) + list(root.rglob("*.yml"))
    assert yamls == [], f"remove YAML examples: {yamls}"
