"""ClaudeCodeExecNode — Part V §7.2 (single subprocess)."""

from __future__ import annotations

import subprocess
from types import MappingProxyType
from typing import Any, Dict, List, Optional

from nodeflow.core.base_node import ExecutionContext
from nodeflow.nodes.base.cli_action import CliActionNode


def _execution_result_payload(
    *,
    ok: bool,
    executor: str,
    provider: str,
    model: Optional[str],
    task_type: Optional[str],
    summary: Optional[str],
    stdout: Optional[str],
    stderr: Optional[str],
    raw_response: Any,
    artifacts: List[Any],
    provider_meta: Dict[str, Any],
    next_hint: Optional[str],
) -> Dict[str, Any]:
    return {
        "ok": ok,
        "executor": executor,
        "provider": provider,
        "model": model,
        "task_type": task_type,
        "summary": summary,
        "stdout": stdout,
        "stderr": stderr,
        "raw_response": raw_response,
        "artifacts": artifacts,
        "provider_meta": provider_meta,
        "next_hint": next_hint,
    }


class ClaudeCodeExecNode(CliActionNode):
    role = "exec"

    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType,
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        p = dict(params)
        argv = p.get("argv")
        if not isinstance(argv, list) or not all(isinstance(x, str) for x in argv):
            argv = ["echo", "claude-code-exec-placeholder"]
        timeout = p.get("timeout", 120)
        if not isinstance(timeout, (int, float)) or timeout <= 0:
            timeout = 120.0

        model = p.get("model")
        if model is not None:
            model = str(model)

        task_type = inputs.get("task_type")
        if task_type is not None:
            task_type = str(task_type)

        try:
            proc = subprocess.run(
                argv,
                capture_output=True,
                text=True,
                timeout=float(timeout),
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            return {
                "execution_result": _execution_result_payload(
                    ok=False,
                    executor="claude_code",
                    provider="anthropic",
                    model=model,
                    task_type=task_type,
                    summary="subprocess timeout",
                    stdout=getattr(exc, "stdout", None) or None,
                    stderr=getattr(exc, "stderr", None) or str(exc),
                    raw_response={"error": "timeout", "cmd": argv},
                    artifacts=[],
                    provider_meta={"argv": argv},
                    next_hint=None,
                )
            }

        ok = proc.returncode == 0
        raw_response = {
            "returncode": proc.returncode,
            "args": argv,
        }
        return {
            "execution_result": _execution_result_payload(
                ok=ok,
                executor="claude_code",
                provider="anthropic",
                model=model,
                task_type=task_type,
                summary=None,
                stdout=proc.stdout or None,
                stderr=proc.stderr or None,
                raw_response=raw_response,
                artifacts=[],
                provider_meta={"argv": argv},
                next_hint=None,
            )
        }
