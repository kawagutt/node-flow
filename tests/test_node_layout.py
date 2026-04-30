"""Forward-looking layout checks for nodeflow/nodes and nodeflow/workflows."""

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

# Existing violations allowed in this PR. Remove entries in later phases.
ALLOWED_EXISTING_FORBIDDEN_DIRS: set[str] = set()

# Existing directories that do not yet follow node_<name>.py.
# This test is forward-looking in Phase 0-2.
ALLOWED_EXISTING_MISSING_NODE_FILE = {
    "nodeflow/nodes",
    "nodeflow/nodes/base",
    "nodeflow/nodes/exec",
    "nodeflow/nodes/git",
    "nodeflow/nodes/routing",
    "nodeflow/nodes/summarize",
    "nodeflow/workflows",
}


def _rel(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def _iter_dirs(root: Path):
    if not root.exists():
        return
    for path in root.rglob("*"):
        if path.is_dir() and "__pycache__" not in path.parts:
            yield path


def test_forbidden_node_folder_names_are_not_added():
    violations: list[str] = []

    for root in CHECK_ROOTS:
        for path in _iter_dirs(root):
            rel = _rel(path)
            if path.name in FORBIDDEN_DIR_NAMES and rel not in ALLOWED_EXISTING_FORBIDDEN_DIRS:
                violations.append(rel)

    assert violations == []


def test_new_node_folders_have_matching_node_file():
    violations: list[str] = []

    for root in CHECK_ROOTS:
        for path in _iter_dirs(root):
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
