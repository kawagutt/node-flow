"""Exec policy snapshot for dev-process.

P10: every exec resolves worker/model/argv from the frozen
``exec_policy_snapshot`` on the checkpoint.  An external policy file
(``--exec-policy`` / ``exec_policy_path``) can be loaded at ``start``
and is merged into the snapshot; on resume only the snapshot is used.

``exec_policy_path`` is resolved relative to the process cwd (CLI-natural).

Terminology
-----------
*Node* = a processing unit in the dev-process graph (e.g. ``write_spec``).
``exec_policy.nodes[node_name]`` configures the worker/model/argv for that node.
"""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List, Optional

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.hermetic_argv import (
    implement_argv,
    plan_argv,
    plan_review_argv,
    review_argv,
    spec_argv,
    spec_review_argv,
)

POLICY_SCHEMA = "dev_process.exec_policy.v1"

NODE_NAMES = (
    "write_spec",
    "review_spec",
    "write_plan",
    "review_plan",
    "write_implementation",
    "write_tests",
    "review_diff",
    "review_tests",
    "review_spec_conformance",
    "review_wide",
    "review_spec_revision",
)

SUPPORTED_WORKERS = frozenset({"codex"})


def default_argv_for_node(node_name: str) -> List[str]:
    """Hermetic argv used when neither node entry nor default_argv supplies argv."""
    if node_name == "write_spec":
        return spec_argv()
    if node_name == "review_spec":
        return spec_review_argv()
    if node_name == "write_plan":
        return plan_argv()
    if node_name == "review_plan":
        return plan_review_argv()
    if node_name in ("write_implementation", "write_tests"):
        return implement_argv()
    if node_name.startswith("review_"):
        return review_argv()
    return implement_argv()


def default_node_entries() -> Dict[str, Dict[str, Any]]:
    """Worker defaults only — argv resolved via default_argv then default_argv_for_node."""
    return {name: {"worker": "codex"} for name in NODE_NAMES}


def _validate_policy_overrides(overrides: Dict[str, Any]) -> None:
    """Validate exec policy file contents at start time.

    Raises ``NodeExecutionFailure`` on unknown node names, invalid argv types, or
    unsupported workers so that configuration errors surface immediately rather than
    silently falling back to defaults.
    """
    if "jobs" in overrides:
        raise NodeExecutionFailure(
            "exec_policy 'jobs' key is no longer supported; use 'nodes' instead"
        )
    nodes = overrides.get("nodes")
    if nodes is not None:
        if not isinstance(nodes, dict):
            raise NodeExecutionFailure("exec_policy.nodes must be a JSON object")
        for name, entry in nodes.items():
            if name not in NODE_NAMES:
                raise NodeExecutionFailure(
                    f"unknown node name {name!r} in exec_policy.nodes; "
                    f"valid names: {sorted(NODE_NAMES)}"
                )
            if not isinstance(entry, dict):
                raise NodeExecutionFailure(f"exec_policy.nodes.{name} must be a JSON object")
            argv = entry.get("argv")
            if argv is not None:
                if not isinstance(argv, list) or not all(isinstance(x, str) for x in argv):
                    raise NodeExecutionFailure(
                        f"exec_policy.nodes.{name}.argv must be a list of strings"
                    )
                if not argv:
                    raise NodeExecutionFailure(f"exec_policy.nodes.{name}.argv must not be empty")
            worker = entry.get("worker")
            if worker is not None and worker not in SUPPORTED_WORKERS:
                raise NodeExecutionFailure(
                    f"unsupported worker {worker!r} in exec_policy.nodes.{name}; "
                    f"supported: {sorted(SUPPORTED_WORKERS)}"
                )

    default_argv = overrides.get("default_argv")
    if default_argv is not None:
        if not isinstance(default_argv, list) or not all(isinstance(x, str) for x in default_argv):
            raise NodeExecutionFailure("exec_policy.default_argv must be a list of strings")
        if not default_argv:
            raise NodeExecutionFailure("exec_policy.default_argv must not be empty")

    default_worker = overrides.get("default_worker")
    if default_worker is not None and default_worker not in SUPPORTED_WORKERS:
        raise NodeExecutionFailure(
            f"unsupported default_worker {default_worker!r}; supported: {sorted(SUPPORTED_WORKERS)}"
        )


def load_exec_policy_file(path: str | Path) -> Dict[str, Any]:
    """Load an external exec policy JSON and validate its shape.

    The returned dict includes ``_policy_source`` with the resolved path
    and content sha256 for audit traceability.

    Path is resolved relative to the process cwd (CLI-natural).

    Expected schema::

        {
          "default_worker": "codex",          # optional
          "default_model": "gpt-4.1",         # optional
          "default_argv": ["codex", "exec"],   # optional
          "nodes": {                           # optional
            "write_spec": {"worker": "codex", "model": "...", "argv": [...]},
            ...
          }
        }
    """
    p = Path(path).resolve()
    if not p.is_file():
        raise NodeExecutionFailure(f"exec_policy_path not found: {p}")
    try:
        raw = p.read_text(encoding="utf-8")
        doc = json.loads(raw)
    except (OSError, json.JSONDecodeError) as e:
        raise NodeExecutionFailure(f"cannot read exec policy file {p}: {e}") from e
    if not isinstance(doc, dict):
        raise NodeExecutionFailure(f"exec policy must be a JSON object: {p}")
    _validate_policy_overrides(doc)
    doc["_policy_source"] = {
        "path": str(p),
        "sha256": hashlib.sha256(raw.encode("utf-8")).hexdigest(),
    }
    return doc


def build_exec_policy_snapshot(
    *,
    exec_worker_kind: str = "codex",
    exec_argv: Optional[list[str]] = None,
    exec_model: Optional[str] = None,
    exec_policy_overrides: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build a frozen snapshot from CLI args + optional policy file overrides.

    Resolution order (last wins per field):

    1. built-in defaults (``default_node_entries``, ``exec_worker_kind``)
    2. ``exec_policy_overrides`` (from ``--exec-policy`` file)
    3. ``exec_argv`` / ``exec_model`` (CLI scalars)

    Notes on precedence:

    - CLI ``--exec-argv`` sets ``snapshot.default_argv``.
    - Per-node ``nodes.<name>.argv`` always overrides ``default_argv``.
    - CLI ``--exec-argv`` does **not** overwrite per-node argv entries from
      ``exec_policy_path``.
    """
    nodes = default_node_entries()
    snapshot: Dict[str, Any] = {
        "schema": POLICY_SCHEMA,
        "default_worker": exec_worker_kind,
        "nodes": deepcopy(nodes),
    }
    if exec_policy_overrides and isinstance(exec_policy_overrides, dict):
        if "default_worker" in exec_policy_overrides:
            snapshot["default_worker"] = str(exec_policy_overrides["default_worker"])
        if "default_model" in exec_policy_overrides:
            snapshot["default_model"] = str(exec_policy_overrides["default_model"])
        if isinstance(exec_policy_overrides.get("default_argv"), list):
            snapshot["default_argv"] = list(exec_policy_overrides["default_argv"])
        override_nodes = exec_policy_overrides.get("nodes")
        if isinstance(override_nodes, dict):
            for nn, entry in override_nodes.items():
                if isinstance(entry, dict):
                    snapshot["nodes"].setdefault(nn, {}).update(deepcopy(entry))
        ps = exec_policy_overrides.get("_policy_source")
        if isinstance(ps, dict):
            snapshot["policy_source"] = ps
    if exec_model:
        snapshot["default_model"] = exec_model
    if exec_argv is not None:
        snapshot["default_argv"] = list(exec_argv)
    return snapshot


def apply_snapshot_to_body(body: Dict[str, Any], snapshot: Dict[str, Any]) -> None:
    dp = body.setdefault("dev_process", {})
    dp["exec_policy_snapshot"] = snapshot
