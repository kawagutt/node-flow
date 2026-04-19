"""CliActionNode — external CLI via subprocess (Part V §5.2, §11.2)."""

from __future__ import annotations

from nodeflow.nodes.base.action import ActionNode


class CliActionNode(ActionNode):
    """Implementation kind: cli."""
