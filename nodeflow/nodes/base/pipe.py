"""PipeNode — composite with internal subgraph (Part V §5, §8)."""

from __future__ import annotations

from nodeflow.core.base_node import BaseNode


class PipeNode(BaseNode):
    """Subgraph execution and output shaping belong in run() only; execute stays BaseNode."""

    # Composite PipeNodes may appear as children of a serial compose graph (Part V §8.3).
    ALLOW_AS_CHILD = True
