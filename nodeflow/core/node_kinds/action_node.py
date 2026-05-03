"""ActionNode taxonomy — implementation kinds."""

from __future__ import annotations

from nodeflow.core.base_node import BaseNode


class ActionNode(BaseNode):
    """Meaning is role (class attribute); inheritance axis is implementation kind only."""

    role: str = ""


class PythonActionNode(ActionNode):
    """Implementation kind: python."""

    pass


class CliActionNode(ActionNode):
    """Implementation kind: cli."""

    pass


class ApiActionNode(ActionNode):
    """Implementation kind: api.

    **Failure semantics (external call vs node error)**

    - When the remote API returns an HTTP response (including error status or a body
      the client maps to failure), prefer returning a normal output dict with
      ``execution_output.ok = False`` and details in ``stderr`` / ``raw_output`` /
      ``provider_meta`` — same shape as success.
    - Raise only for **preconditions the node cannot satisfy** (e.g. missing API key
      env var before any request), **programming errors**, or truly unrecoverable
      client failures; those propagate as fatal ``execute`` / ``read_error`` per
      ``BaseNode``.
    """

    pass
