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
        completion = f"mock:{prompt}"
        # mock token count = character length（実 tokenizer とは一致しない）
        prompt_tokens = len(prompt)
        completion_tokens = len(completion)
        total_tokens = prompt_tokens + completion_tokens
        return {
            "response": {"value": completion},
            "_usage": {
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "total_tokens": total_tokens,
            },
        }
