"""Layout checks for nodeflow/nodes and nodeflow/workflows (``node_<dirname>.py`` gate).

Only **descendant** directories of each check root are inspected; the package roots
``nodeflow/nodes/`` and ``nodeflow/workflows/`` themselves are **not** required to
contain ``node_nodes.py`` / ``node_workflows.py`` (they are container roots only).
"""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

CHECK_ROOTS = [
    ROOT / "nodeflow" / "nodes",
    ROOT / "nodeflow" / "workflows",
]

FORBIDDEN_DIR_NAMES = {
    "common",
    "utils",
    "helpers",
    "shared",
    "lib",
    "scripts",
}

ALLOWED_EXISTING_FORBIDDEN_DIRS: set[str] = set()

ALLOWED_EXISTING_MISSING_NODE_FILE: set[str] = {
    "nodeflow/workflows/dev_process/stages",
}


def _rel(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def _iter_descendant_dirs(container: Path):
    """Yield directories under ``container``, excluding ``container`` itself and ``__pycache__`` trees."""
    if not container.exists():
        return
    for path in container.rglob("*"):
        if not path.is_dir():
            continue
        if path == container:
            continue
        if "__pycache__" in path.parts:
            continue
        yield path


def test_forbidden_node_folder_names_are_not_added():
    violations: list[str] = []

    for root in CHECK_ROOTS:
        for path in _iter_descendant_dirs(root):
            rel = _rel(path)
            if path.name in FORBIDDEN_DIR_NAMES and rel not in ALLOWED_EXISTING_FORBIDDEN_DIRS:
                violations.append(rel)

    assert violations == []


def test_new_node_folders_have_matching_node_file():
    violations: list[str] = []

    for root in CHECK_ROOTS:
        for path in _iter_descendant_dirs(root):
            rel = _rel(path)
            if rel in ALLOWED_EXISTING_MISSING_NODE_FILE:
                continue

            expected = path / f"node_{path.name}.py"
            if not expected.exists():
                violations.append(f"{rel} missing {expected.name}")

    assert violations == []


def test_no_development_flow_legacy_pipe_directories_remain() -> None:
    base = ROOT / "nodeflow" / "workflows" / "development_flow"
    violations = [p for p in base.rglob("*_pipe") if p.is_dir()]
    assert violations == []


def test_no_pipe_py_under_development_flow_workflows() -> None:
    base = ROOT / "nodeflow" / "workflows" / "development_flow"
    violations = [p for p in base.rglob("pipe.py") if p.is_file()]
    assert violations == []


def test_no_root_level_legacy_nodes_directory() -> None:
    """Repo root ``nodes/`` was the old v1.x YAML layout; NodeFlow concrete nodes live under ``nodeflow/nodes/``."""
    assert not (ROOT / "nodes").is_dir(), "remove stray repository-root nodes/ directory"
