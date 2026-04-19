"""Public contract: loader root PipeNode, runtime revision, Runner surface."""

from __future__ import annotations

from pathlib import Path

from nodeflow.core.node_kinds import PipeNode
from nodeflow.core.runner import Runner
from nodeflow.execution.loader import load_pipeline


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


def test_reusable_dispatch_pipe_bundles_child_domain_only():
    """Custom PipeNode.run() must not leak child _runtime into bundled domain ports."""
    from nodeflow.nodes.dispatch.implement_dispatch_pipe import ImplementDispatchPipeNode
    from nodeflow.nodes.dispatch.review_dispatch_pipe import ReviewDispatchPipeNode

    impl = ImplementDispatchPipeNode()
    out_impl = impl.execute(
        {"task_type": "implement", "task_prompt": "hi"},
        {"codex_exec": {"argv": ["sh", "-c", "echo dispatch-contract"]}},
    )
    assert impl.read_status() == "done"
    for port in ("route", "summary", "execution_result"):
        assert port in out_impl
        assert isinstance(out_impl[port], dict)
        assert "_runtime" not in out_impl[port]
    assert "_runtime" in out_impl
    assert "revision" in out_impl["_runtime"]["ports"]["execution_result"]

    rev = ReviewDispatchPipeNode()
    out_rev = rev.execute(
        {"task_type": "review", "task_prompt": "x"},
        {"claude_code_exec": {"argv": ["sh", "-c", "echo review-pipe"]}},
    )
    assert rev.read_status() == "done"
    for port in ("route", "summary", "execution_result"):
        assert port in out_rev
        assert "_runtime" not in out_rev[port]
    assert "_runtime" in out_rev
    assert "revision" in out_rev["_runtime"]["ports"]["execution_result"]


def test_examples_dispatch_yaml_nested_argv_matches_readme():
    """Sample pipelines under examples/pipelines must include nested exec argv."""
    repo = Path(__file__).resolve().parents[1]
    for filename, inputs in (
        ("review_dispatch.yaml", {"task_type": "review", "task_prompt": "x"}),
        ("implement_dispatch.yaml", {"task_type": "implement", "task_prompt": "x"}),
    ):
        path = repo / "examples/pipelines" / filename
        assert path.is_file(), f"missing {path}"
        root = load_pipeline(str(repo / "examples"), str(path))
        assert isinstance(root, PipeNode)
        out = root.execute(inputs, {})
        assert root.read_status() == "done"
        assert "execution_result" in out
        assert "_runtime" not in out["execution_result"]
