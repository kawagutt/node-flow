"""
NodeFlow — load + execute entry (core).

YAML 1.5 ``load_and_kick_pipeline`` was removed. Prefer :class:`~nodeflow.core.node_kinds.PipeNode`
with :meth:`~nodeflow.core.node_kinds.PipeNode.pipe_spec` and ``execute`` directly.

**Next (Phase 7):** reimplement this entrypoint on top of a v1.6 JSON → :class:`~nodeflow.core.pipe_spec.PipeSpec`
loader so the public name stays useful; until then this stub should not linger unmentioned in release notes.
"""

from __future__ import annotations

from typing import Any, Dict


def load_and_kick_pipeline(
    workspace_dir: str,
    pipeline_path: str,
    initial_inputs: Dict[str, Any] | None = None,
    params: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Removed: YAML root pipeline loading. Construct a v1.6 ``PipeNode`` and call ``execute``."""
    raise NotImplementedError(
        "load_and_kick_pipeline (YAML 1.5) was removed. Use nodeflow.core.loader.load_pipeline(..., "
        "'path/to/pipe.json') for v1.6 JSON PipeSpec, or construct a PipeNode and call execute()."
    )
