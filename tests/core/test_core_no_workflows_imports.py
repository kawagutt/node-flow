"""Core must not import workflow packages (layer boundary)."""

from __future__ import annotations

import ast
from pathlib import Path


def test_core_python_files_do_not_import_nodeflow_workflows() -> None:
    repo = Path(__file__).resolve().parents[2]
    core = repo / "nodeflow" / "core"
    offenders: list[str] = []
    for path in sorted(core.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                mod = node.module
                if mod == "nodeflow.workflows" or (
                    isinstance(mod, str) and mod.startswith("nodeflow.workflows.")
                ):
                    offenders.append(f"{path.relative_to(repo)}:{node.lineno}: from {mod!r}")
                if mod == "nodeflow":
                    for alias in node.names:
                        if alias.name == "workflows":
                            offenders.append(
                                f"{path.relative_to(repo)}:{node.lineno}: "
                                "from nodeflow import workflows"
                            )
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    name = alias.name
                    if name == "nodeflow.workflows" or name.startswith("nodeflow.workflows."):
                        offenders.append(f"{path.relative_to(repo)}:{node.lineno}: import {name!r}")
    assert not offenders, "core must not import nodeflow.workflows:\n" + "\n".join(offenders)
