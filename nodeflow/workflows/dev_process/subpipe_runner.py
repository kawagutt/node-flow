"""Run dev_process linear subpipes via generic PipeNode."""

from __future__ import annotations

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.core.loader import load_pipeline
from nodeflow.core.node_kinds.pipe_node import PipeNode
from nodeflow.workflows.dev_process.action_node_utils import execute_or_raise


def run_subpipe(spec_path: str, ctx: dict, *, workspace: str) -> dict:
    spec = load_pipeline(workspace, spec_path)
    node = PipeNode(spec)

    out = execute_or_raise(
        node,
        {"ctx": ctx},
        {
            "_workspace_dir": workspace,
            # Runner may call execute() before upstream delivery (doc §14.1).
            # Subpipes rely on leaf nodes being allowed to no-op until inputs arrive.
            "_allow_pending_inputs_noop": True,
        },
    )

    result = out.get("cycle_result")
    if not isinstance(result, dict):
        raise NodeExecutionFailure(
            f"subpipe {spec_path!r} did not produce dict output 'cycle_result'"
        )
    return result
