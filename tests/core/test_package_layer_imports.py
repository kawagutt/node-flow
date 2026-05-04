"""Static import boundaries between core, nodes, and workflows packages."""

from __future__ import annotations

import ast
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def _gather_offenders(pkg_root: Path, *, ban_workflows: bool, ban_nodes: bool) -> list[str]:
    offenders: list[str] = []
    for path in sorted(pkg_root.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                mod = node.module
                if ban_workflows and (
                    mod == "nodeflow.workflows"
                    or (isinstance(mod, str) and mod.startswith("nodeflow.workflows."))
                ):
                    offenders.append(f"{path.relative_to(REPO_ROOT)}:{node.lineno}: from {mod!r}")
                if ban_nodes and (
                    mod == "nodeflow.nodes"
                    or (isinstance(mod, str) and mod.startswith("nodeflow.nodes."))
                ):
                    offenders.append(f"{path.relative_to(REPO_ROOT)}:{node.lineno}: from {mod!r}")
                if mod == "nodeflow":
                    for alias in node.names:
                        if ban_workflows and alias.name == "workflows":
                            offenders.append(
                                f"{path.relative_to(REPO_ROOT)}:{node.lineno}: "
                                "from nodeflow import workflows"
                            )
                        if ban_nodes and alias.name == "nodes":
                            offenders.append(
                                f"{path.relative_to(REPO_ROOT)}:{node.lineno}: "
                                "from nodeflow import nodes"
                            )
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    name = alias.name
                    if ban_workflows and (
                        name == "nodeflow.workflows" or name.startswith("nodeflow.workflows.")
                    ):
                        offenders.append(f"{path.relative_to(REPO_ROOT)}:{node.lineno}: import {name!r}")
                    if ban_nodes and (name == "nodeflow.nodes" or name.startswith("nodeflow.nodes.")):
                        offenders.append(f"{path.relative_to(REPO_ROOT)}:{node.lineno}: import {name!r}")
    return offenders


def test_core_python_files_do_not_import_nodeflow_workflows() -> None:
    core = REPO_ROOT / "nodeflow" / "core"
    offenders = _gather_offenders(core, ban_workflows=True, ban_nodes=False)
    assert not offenders, "core must not import nodeflow.workflows:\n" + "\n".join(offenders)


def test_core_python_files_do_not_import_nodeflow_nodes() -> None:
    core = REPO_ROOT / "nodeflow" / "core"
    offenders = _gather_offenders(core, ban_workflows=False, ban_nodes=True)
    assert not offenders, "core must not import nodeflow.nodes:\n" + "\n".join(offenders)


def test_nodes_python_files_do_not_import_nodeflow_workflows() -> None:
    nodes = REPO_ROOT / "nodeflow" / "nodes"
    offenders = _gather_offenders(nodes, ban_workflows=True, ban_nodes=False)
    assert not offenders, "nodes must not import nodeflow.workflows:\n" + "\n".join(offenders)


def test_nodes_package_init_does_not_import_global_builtins() -> None:
    """``nodeflow.nodes`` must stay a pure building-blocks package at import time.

    Built-in registry side effects belong in ``nodeflow`` / ``nodeflow.builtins`` only.
    """
    init_py = REPO_ROOT / "nodeflow" / "nodes" / "__init__.py"
    text = init_py.read_text(encoding="utf-8")
    assert "nodeflow.builtins" not in text
