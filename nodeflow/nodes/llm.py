"""
NodeFlow v1.41 — LLMNode (§13.2). 本版では mock 実装（実 API は Phase 8）。
"""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Dict

from ..node import BaseNode, ExecutionContext


class LLMNode(BaseNode):
    """LLM API を呼び出す Node。本版は mock: response = f'mock:{prompt}'。"""

    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType,
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        prompt = inputs.get("prompt", "")
        return {"response": f"mock:{prompt}"}
