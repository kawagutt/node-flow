"""Guardrails: no resurrected execution package or removed PipeNode constructor API."""

from __future__ import annotations

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
_GUARD_PATH = Path(__file__).resolve()

# README.md, doc/, nodeflow/, tests/, examples/ must not advertise removed paths.
_LEGACY_SUBSTRINGS = (
    "nodeflow.execution",
    "execution.loader",
    "execution.run",
    "nodeflow/execution",
)

# Removed PipeNode(graph_node_order=..., ...) style (use PipeNode(spec: PipeSpec) instead).
_PIPE_NODE_KW_SUBSTRINGS = (
    "PipeNode(graph_node_order",
    "PipeNode(node_input_bindings",
    "PipeNode(node_param_definitions",
    "PipeNode(final_id",
)


def _iter_text_files_under(relative: str) -> list[Path]:
    base = ROOT / relative
    if not base.exists():
        return []
    out: list[Path] = []
    for path in base.rglob("*"):
        if path.is_dir():
            continue
        if "__pycache__" in path.parts:
            continue
        if path.suffix not in {".md", ".py", ".yaml", ".yml", ".rst"}:
            continue
        out.append(path)
    return out


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


@pytest.mark.parametrize("root", ("README.md", "doc", "nodeflow", "tests", "examples"))
def test_no_legacy_execution_references_in_docs_and_code(root: str) -> None:
    paths: list[Path] = []
    p = ROOT / root
    if p.is_file():
        paths = [p]
    elif p.is_dir():
        paths = _iter_text_files_under(root)

    violations: list[str] = []
    for path in paths:
        if path.resolve() == _GUARD_PATH:
            continue
        text = _read_text(path)
        for sub in _LEGACY_SUBSTRINGS:
            if sub in text:
                violations.append(f"{path.relative_to(ROOT)}: contains {sub!r}")

    assert violations == [], (
        "Remove legacy execution imports/paths from docs and tree:\n" + "\n".join(violations)
    )


@pytest.mark.parametrize("root", ("nodeflow", "tests"))
def test_no_removed_pipenode_constructor_kwargs(root: str) -> None:
    violations: list[str] = []
    for path in _iter_text_files_under(root):
        if path.resolve() == _GUARD_PATH:
            continue
        if path.suffix != ".py":
            continue
        text = _read_text(path)
        for sub in _PIPE_NODE_KW_SUBSTRINGS:
            if sub in text:
                violations.append(f"{path.relative_to(ROOT)}: contains {sub!r}")

    assert violations == [], (
        "Do not call PipeNode with removed graph kwargs; construct PipeNode(spec):\n"
        + "\n".join(violations)
    )


def test_nodeflow_execution_package_dir_is_absent() -> None:
    assert not (ROOT / "nodeflow" / "execution").exists(), (
        "nodeflow/execution/ must not exist (no shim package)"
    )
