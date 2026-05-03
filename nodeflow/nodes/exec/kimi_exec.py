"""KimiExecNode — Moonshot OpenAI-compatible API, single request per execute."""

from __future__ import annotations

import os
from types import MappingProxyType
from typing import Any, Dict, List, Optional

from openai import OpenAI

from nodeflow.core.base_node import ExecutionContext
from nodeflow.core.node_kinds import ApiActionNode


def _execution_output_payload(
    *,
    ok: bool,
    external_executor: str,
    provider: str,
    model: Optional[str],
    task_type: Optional[str],
    summary: Optional[str],
    stdout: Optional[str],
    stderr: Optional[str],
    raw_output: Any,
    artifacts: List[Any],
    provider_meta: Dict[str, Any],
    next_hint: Optional[str],
) -> Dict[str, Any]:
    return {
        "ok": ok,
        "external_executor": external_executor,
        "provider": provider,
        "model": model,
        "task_type": task_type,
        "summary": summary,
        "stdout": stdout,
        "stderr": stderr,
        "raw_output": raw_output,
        "artifacts": artifacts,
        "provider_meta": provider_meta,
        "next_hint": next_hint,
    }


class KimiExecNode(ApiActionNode):
    role = "exec"

    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType,
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        p = dict(params)
        api_key = os.environ.get("MOONSHOT_API_KEY")
        if not api_key:
            raise RuntimeError("MOONSHOT_API_KEY is not set")

        base_url = str(p.get("base_url", "https://api.moonshot.cn/v1"))
        model = str(p.get("model", "moonshot-v1-8k"))
        temperature = p.get("temperature", 0.7)
        if not isinstance(temperature, (int, float)):
            temperature = 0.7

        prompt = str(inputs.get("prompt", ""))
        system_prompt = p.get("system_prompt", "") or ""

        client = OpenAI(api_key=api_key, base_url=base_url)
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        task_type = inputs.get("task_type")
        if task_type is not None:
            task_type = str(task_type)

        try:
            resp = client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=float(temperature),
            )
        except Exception as exc:
            return {
                "execution_output": _execution_output_payload(
                    ok=False,
                    external_executor="kimi",
                    provider="moonshot",
                    model=model,
                    task_type=task_type,
                    summary=None,
                    stdout=None,
                    stderr=str(exc),
                    raw_output={"error": str(exc)},
                    artifacts=[],
                    provider_meta={"base_url": base_url},
                    next_hint=None,
                ),
                "_usage": {
                    "prompt_tokens": 0,
                    "completion_tokens": 0,
                    "total_tokens": 0,
                },
            }

        content = ""
        if resp.choices:
            content = resp.choices[0].message.content or ""

        raw_out = resp.model_dump() if hasattr(resp, "model_dump") else resp  # type: ignore[assignment]

        usage_obj = resp.usage
        prompt_tokens = completion_tokens = total_tokens = 0
        if usage_obj is not None:
            prompt_tokens = int(getattr(usage_obj, "prompt_tokens", None) or 0)
            completion_tokens = int(getattr(usage_obj, "completion_tokens", None) or 0)
            total_tokens = int(getattr(usage_obj, "total_tokens", None) or 0)

        summary = (content[:500] + "…") if len(content) > 500 else (content or None)

        return {
            "execution_output": _execution_output_payload(
                ok=True,
                external_executor="kimi",
                provider="moonshot",
                model=model,
                task_type=task_type,
                summary=summary,
                stdout=None,
                stderr=None,
                raw_output=raw_out,
                artifacts=[],
                provider_meta={"base_url": base_url},
                next_hint=None,
            ),
            "_usage": {
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "total_tokens": total_tokens,
            },
        }
