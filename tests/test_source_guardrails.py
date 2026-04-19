"""Guardrails: block legacy vocabulary and forbidden patterns in sources and samples."""

from __future__ import annotations

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
FORBIDDEN_SUBSTRINGS = [
    "StructuralNode",
    "PipelineNode",
    "python_script",
    "nodeflow.extensions",
    "LLMNode",
    "OpenRouterNode",
    'version: "1.4"',
]

# Package sources must not reintroduce these (narrow list; avoid bare `_meta`).
FORBIDDEN_IN_NODEFLOW_PY = [
    "SerialPipeNode",
    "nodeflow.nodes.base",
    "nodeflow.nodes.action",
    "nodeflow.nodes.pipe",
    "_meta.revision",
    'register("compose"',
]


@pytest.mark.parametrize("path", [REPO_ROOT / "README.md"])
def test_readme_has_no_legacy_vocabulary(path: Path):
    text = path.read_text(encoding="utf-8")
    for s in FORBIDDEN_SUBSTRINGS:
        assert s not in text, f"{path}: forbidden substring {s!r}"


def _iter_public_sample_yaml_files() -> list[Path]:
    """YAML under examples/ and nodes/ (repo sample trees), excluding nothing."""
    out: list[Path] = []
    for root_name in ("examples", "nodes"):
        root = REPO_ROOT / root_name
        if not root.is_dir():
            continue
        for pattern in ("*.yaml", "*.yml"):
            out.extend(root.rglob(pattern))
    return sorted(set(out))


def test_public_sample_yaml_files_have_no_legacy_vocabulary():
    paths = _iter_public_sample_yaml_files()
    assert paths, "expected at least one sample *.yaml under examples/ or nodes/"
    for path in paths:
        text = path.read_text(encoding="utf-8")
        for s in FORBIDDEN_SUBSTRINGS:
            assert s not in text, f"{path}: forbidden substring {s!r}"


def test_nodeflow_package_has_no_extensions_import():
    for path in (REPO_ROOT / "nodeflow").rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        assert "nodeflow.extensions" not in text, f"{path} still imports extensions"


def test_nodeflow_sources_avoid_banned_legacy_regressions():
    for path in (REPO_ROOT / "nodeflow").rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        for s in FORBIDDEN_IN_NODEFLOW_PY:
            assert s not in text, f"{path}: forbidden substring {s!r}"


def test_readme_avoids_banned_compose_serial_terms():
    text = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    for s in FORBIDDEN_IN_NODEFLOW_PY:
        assert s not in text, f"README.md: forbidden substring {s!r}"
