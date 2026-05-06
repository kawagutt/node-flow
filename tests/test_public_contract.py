"""Public contract: Runner surface, PipeSpec loader, and removed legacy entrypoints."""

from __future__ import annotations

import ast
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


def test_no_pipenode_subclass_under_workflows() -> None:
    root = Path(__file__).resolve().parents[1] / "nodeflow" / "workflows"

    def _base_name(base: ast.expr) -> str | None:
        if isinstance(base, ast.Name):
            return base.id
        if isinstance(base, ast.Attribute):
            return base.attr
        return None

    violations: list[str] = []
    for path in root.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                if any(_base_name(base) == "PipeNode" for base in node.bases):
                    rel = path.relative_to(root).as_posix()
                    violations.append(f"{rel}:{node.name}")
    assert violations == [], f"remove workflow-specific PipeNode subclasses: {violations}"


def test_examples_pipelines_has_no_public_yaml_examples() -> None:
    repo = Path(__file__).resolve().parents[1]
    root = repo / "examples" / "pipelines"
    yamls = list(root.rglob("*.yaml")) + list(root.rglob("*.yml"))
    assert yamls == [], f"remove YAML examples: {yamls}"
