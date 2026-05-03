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
    """Fixed-provider pipes must not leak child _runtime into bundled domain ports."""
    from nodeflow.workflows.implement_with_codex.node_implement_with_codex import (
        ImplementWithCodexPipeNode,
    )
    from nodeflow.workflows.review_with_claude.node_review_with_claude import (
        ReviewWithClaudePipeNode,
    )

    impl = ImplementWithCodexPipeNode()
    out_impl = impl.execute(
        {
            "task_type": {"value": "implement"},
            "task_prompt": {"text": "hi"},
        },
        {"codex_exec": {"argv": ["sh", "-c", "echo dispatch-contract"]}},
    )
    assert impl.read_status() == "done"
    for port in ("summary", "execution_output"):
        assert port in out_impl
        assert isinstance(out_impl[port], dict)
        assert "_runtime" not in out_impl[port]
    assert "_runtime" in out_impl
    assert "revision" in out_impl["_runtime"]["ports"]["execution_output"]

    rev = ReviewWithClaudePipeNode()
    out_rev = rev.execute(
        {
            "task_type": {"value": "review"},
            "task_prompt": {"text": "x"},
        },
        {"claude_code_exec": {"argv": ["sh", "-c", "echo review-pipe"]}},
    )
    assert rev.read_status() == "done"
    for port in ("summary", "execution_output"):
        assert port in out_rev
        assert isinstance(out_rev[port], dict)
        assert "_runtime" not in out_rev[port]
    assert "_runtime" in out_rev
    assert "revision" in out_rev["_runtime"]["ports"]["execution_output"]


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
        {
            "task_type": {"value": "implement"},
            "task_prompt": {"text": "hello"},
        },
        {
            "_workspace_dir": str(workspace),
            "codex_exec": {"argv": ["sh", "-c", "pwd"], "cwd": "sub"},
        },
    )
    assert impl.read_status() == "done"
    assert (out_impl["execution_output"]["stdout"] or "").strip() == str(subdir.resolve())

    rev = ReviewWithClaudePipeNode()
    out_rev = rev.execute(
        {
            "task_type": {"value": "review"},
            "task_prompt": {"text": "hello"},
        },
        {
            "_workspace_dir": str(workspace),
            "claude_code_exec": {"argv": ["sh", "-c", "pwd"], "cwd": "sub"},
        },
    )
    assert rev.read_status() == "done"
    assert (out_rev["execution_output"]["stdout"] or "").strip() == str(subdir.resolve())


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
    assert 'inputs.task_type must be a dict with string key "value"' in str(impl.read_error())

    rev = ReviewWithClaudePipeNode()
    out_rev = rev.execute(
        {"task_prompt": {"text": "x"}},
        {"claude_code_exec": {"argv": ["sh", "-c", "echo should-not-run"]}},
    )
    assert out_rev["_state"]["value"] == "fatal"
    assert rev.read_status() == "fatal"
    assert 'inputs.task_type must be a dict with string key "value"' in str(rev.read_error())


def test_fixed_provider_pipe_spec_stable_after_execute():
    """pipe_spec() must not depend on runtime param holder or last execute()."""
    from nodeflow.workflows.implement_with_codex.node_implement_with_codex import (
        ImplementWithCodexPipeNode,
    )
    from nodeflow.workflows.review_with_claude.node_review_with_claude import (
        ReviewWithClaudePipeNode,
    )

    impl = ImplementWithCodexPipeNode()
    s1 = impl.pipe_spec()
    impl.execute(
        {
            "task_prompt": {"text": "x"},
            "task_type": {"value": "implement"},
        },
        {"codex_exec": {"argv": ["sh", "-c", "echo ok"]}},
    )
    impl.reset_status()
    s2 = impl.pipe_spec()
    assert tuple(s1.graph_node_order) == tuple(s2.graph_node_order)
    assert s1.nodes["exec"].params == {}
    assert s2.nodes["exec"].params == {}
    assert s1.nodes["summarize"].params == {}
    assert s2.nodes["summarize"].params == {}

    rev = ReviewWithClaudePipeNode()
    r1 = rev.pipe_spec()
    rev.execute(
        {
            "task_prompt": {"text": "x"},
            "task_type": {"value": "review"},
        },
        {"claude_code_exec": {"argv": ["sh", "-c", "echo ok"]}},
    )
    rev.reset_status()
    r2 = rev.pipe_spec()
    assert tuple(r1.graph_node_order) == tuple(r2.graph_node_order)
    assert r1.nodes["exec"].params == {}
    assert r2.nodes["exec"].params == {}
    assert r1.nodes["summarize"].params == {}
    assert r2.nodes["summarize"].params == {}


def test_fixed_provider_pipe_rejects_non_dict_child_param_merge_keys():
    from nodeflow.workflows.implement_with_codex.node_implement_with_codex import (
        ImplementWithCodexPipeNode,
    )
    from nodeflow.workflows.review_with_claude.node_review_with_claude import (
        ReviewWithClaudePipeNode,
    )

    valid_in = {
        "task_prompt": {"text": "x"},
        "task_type": {"value": "implement"},
    }
    impl = ImplementWithCodexPipeNode()
    out = impl.execute(valid_in, {"codex_exec": "not-a-dict"})
    assert out["_state"]["value"] == "fatal"
    assert impl.read_status() == "fatal"
    assert "params.codex_exec must be a dict" in str(impl.read_error())

    impl2 = ImplementWithCodexPipeNode()
    out2 = impl2.execute(valid_in, {"python_summarize_result": []})
    assert out2["_state"]["value"] == "fatal"
    assert "params.python_summarize_result must be a dict" in str(impl2.read_error())

    rev_in = {
        "task_prompt": {"text": "x"},
        "task_type": {"value": "review"},
    }
    rev = ReviewWithClaudePipeNode()
    out_rev = rev.execute(rev_in, {"claude_code_exec": 123})
    assert out_rev["_state"]["value"] == "fatal"
    assert "params.claude_code_exec must be a dict" in str(rev.read_error())


def test_examples_pipelines_has_no_public_yaml_examples() -> None:
    """v1.6 public samples are JSON; YAML under ``examples/pipelines`` is obsolete noise."""
    repo = Path(__file__).resolve().parents[1]
    root = repo / "examples" / "pipelines"
    yamls = list(root.rglob("*.yaml")) + list(root.rglob("*.yml"))
    assert yamls == [], f"remove YAML examples: {yamls}"
