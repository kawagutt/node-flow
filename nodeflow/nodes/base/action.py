"""ActionNode — single-responsibility dispatcher unit (Part V §5)."""

from __future__ import annotations

from nodeflow.core.base_node import BaseNode


class ActionNode(BaseNode):
    """Meaning is role (class attribute); inheritance axis is implementation kind only."""

    role: str = ""
