"""ApiActionNode — HTTP API calls (Part V §5.2, §11.3)."""

from __future__ import annotations

from nodeflow.nodes.base.action import ActionNode


class ApiActionNode(ActionNode):
    """Implementation kind: api.

    **Failure semantics (external call vs node error)**

    - When the remote API returns an HTTP response (including error status or a body
      the client maps to failure), prefer returning a normal output dict with
      ``execution_result.ok = False`` and details in ``stderr`` / ``raw_response`` /
      ``provider_meta`` — same shape as success (Part V §9).
    - Raise only for **preconditions the node cannot satisfy** (e.g. missing API key
      env var before any request), **programming errors**, or truly unrecoverable
      client failures; those propagate as fatal ``execute`` / ``read_error`` per
      ``BaseNode``.
    """
