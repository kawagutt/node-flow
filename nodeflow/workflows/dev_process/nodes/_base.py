"""Base class for dev-process leaf ActionNodes."""

from __future__ import annotations

from typing import Any

from nodeflow.core.base_node import ExecutionContext, NodeExecutionFailure
from nodeflow.core.node_kinds import PythonActionNode
from nodeflow.workflows.dev_process.nodes._ctx import copy_flow_ctx, flow_params


class DevProcessLeafNode(PythonActionNode):
    """Leaf node: deep-copy input ctx, mutate body, return updated ctx.

    Direct execution requires input port ``ctx``. When invoked by Runner before
    upstream delivery, missing ``ctx`` is only allowed when pipe params set
    ``_allow_pending_inputs_noop=True``; otherwise execution fails fast.
    Subpipe output validation (``cycle_result``) catches unresolved pipelines.
    """

    node_name: str = ""
    stage_key: str = ""

    def run(
        self,
        inputs: dict[str, Any],
        params: dict[str, Any],
        context: ExecutionContext,
    ) -> dict[str, Any]:
        raw = inputs.get("ctx")
        if raw is None:
            # Runner may call execute() before upstream delivery (doc §14.1).
            # Only allow this no-op path when explicitly enabled by the parent pipe.
            allow_noop = params.get("_allow_pending_inputs_noop") is True
            if not allow_noop:
                allow_noop = bool(getattr(context, "_allow_pending_inputs_noop", False))
            if allow_noop:
                return {}
            raise NodeExecutionFailure(f"{self.node_name}: missing input port 'ctx'")
        ctx, body = copy_flow_ctx(raw)
        self._execute(ctx, body, flow_params(ctx), params, context)
        ctx["body"] = body
        return {"ctx": ctx}

    def _execute(
        self,
        ctx: dict[str, Any],
        body: dict[str, Any],
        node_params: dict[str, Any],
        pipe_params: dict[str, Any],
        context: ExecutionContext,
    ) -> None:
        raise NotImplementedError
