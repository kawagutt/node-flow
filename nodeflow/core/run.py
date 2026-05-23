"""
NodeFlow — load + execute entry (core).

Prefer :class:`~nodeflow.core.node_kinds.PipeNode` with a v1.7 JSON PipeSpec, or
:func:`load_and_kick_pipeline` for CLI-style one-shot runs.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.core.loader import PipeSpecLoadError, load_pipeline
from nodeflow.core.runner import Runner


def _normalize_pipe_inputs(initial_inputs: Dict[str, Any]) -> Dict[str, Any]:
    """Wrap scalar CLI values so Runner can deliver dict payloads per port."""
    out: Dict[str, Any] = {}
    for key, value in initial_inputs.items():
        if isinstance(value, dict):
            out[key] = value
        elif isinstance(value, str):
            out[key] = {key: value}
        else:
            out[key] = value
    return out


def load_and_kick_pipeline(
    workspace_dir: str,
    pipeline_path: str,
    initial_inputs: Dict[str, Any] | None = None,
    params: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Load a v1.7 ``*.json`` PipeSpec and run until pipe outputs are filled.

    ``initial_inputs`` maps pipe input port names to payloads.
    ``params`` is reserved for future per-node overlays; ignored in v1.7 kick.
    """
    del params  # node-level overlays belong on PipeSpec ``config`` entries
    ws = str(Path(workspace_dir).resolve())
    try:
        spec = load_pipeline(ws, pipeline_path)
    except NotImplementedError:
        raise
    except PipeSpecLoadError as e:
        raise NodeExecutionFailure(str(e)) from e

    runner = Runner.from_pipe_spec(
        spec, pipe_inputs=_normalize_pipe_inputs(dict(initial_inputs or {}))
    )
    for _ in range(10_000):
        if runner.all_pipe_outputs_filled():
            return dict(runner.filled_pipe_outputs())
        statuses = [spec.nodes[nid].node.read_status() for nid in spec.graph_node_order]
        if "fatal" in statuses:
            raise NodeExecutionFailure("pipeline child entered fatal state")
        if not runner.step():
            break
    raise NodeExecutionFailure("pipeline did not complete: outputs not filled")
