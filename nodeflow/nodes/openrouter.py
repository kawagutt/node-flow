"""
NodeFlow v1.41 — OpenRouterNode. OpenRouter API を呼び出す Node（実 API）。
"""

from __future__ import annotations

import os
from types import MappingProxyType
from typing import Any, Dict

from openai import OpenAI

from ..node import BaseNode, ExecutionContext


class OpenRouterNode(BaseNode):
    """OpenRouter API を呼び出す Node。OPENROUTER_API_KEY が必要。"""

    def __init__(self) -> None:
        super().__init__()
        self._client: OpenAI | None = None

    def _get_client(self) -> OpenAI:
        """OpenRouter 用 OpenAI クライアントを返す（lazy init）。run() 経由でのみ呼ぶこと。
        run() 外から呼ぶと OPENROUTER_API_KEY 未設定時の RuntimeError が BaseNode に捕捉されず漏れる。
        """
        if self._client is None:
            api_key = os.environ.get("OPENROUTER_API_KEY")
            if not api_key:
                raise RuntimeError("OPENROUTER_API_KEY is not set")
            self._client = OpenAI(
                api_key=api_key,
                base_url="https://openrouter.ai/api/v1",
            )
        return self._client

    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType,
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        client = self._get_client()
        prompt = inputs.get("prompt", "")
        model = params.get("model", "openai/gpt-4o-mini")
        system_prompt = params.get("system_prompt", "")
        temperature = params.get("temperature", 0.7)

        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        resp = client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=temperature,
        )

        content = ""
        if resp.choices:
            content = resp.choices[0].message.content or ""

        usage = resp.usage
        if usage is None:
            prompt_tokens = completion_tokens = total_tokens = 0
        else:
            prompt_tokens = getattr(usage, "prompt_tokens", None) or 0
            completion_tokens = getattr(usage, "completion_tokens", None) or 0
            total_tokens = getattr(usage, "total_tokens", None) or 0

        return {
            "response": {"value": content},
            "_usage": {
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "total_tokens": total_tokens,
            },
        }
