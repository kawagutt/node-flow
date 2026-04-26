"""CodexExecNode — single subprocess, no session; Common Result on execution_result port."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from types import MappingProxyType
from typing import Any, Dict, List, Optional

from nodeflow.core.base_node import ExecutionContext, NodeExecutionFailure
from nodeflow.core.node_kinds import CliActionNode


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


class CodexExecNode(CliActionNode):
    role = "exec"

    def _resolve_cwd(self, params: Dict[str, Any]) -> Optional[str]:
        cwd = params.get("cwd")
        workspace_dir = params.get("_workspace_dir")
        if cwd is None:
            if isinstance(workspace_dir, str) and workspace_dir:
                return workspace_dir
            return None
        cwd_s = str(cwd)
        if os.path.isabs(cwd_s):
            return cwd_s
        if isinstance(workspace_dir, str) and workspace_dir:
            return str((Path(workspace_dir) / cwd_s).resolve())
        return str(Path(cwd_s).resolve())

    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType,
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        p = dict(params)
        argv = p.get("argv")
        if not isinstance(argv, list) or not argv:
            raise NodeExecutionFailure(
                "params.argv must be a non-empty list of strings (CLI precondition)"
            )
        if not all(isinstance(x, str) for x in argv):
            raise NodeExecutionFailure("params.argv must contain only strings (CLI precondition)")
        timeout = p.get("timeout", 120)
        if not isinstance(timeout, (int, float)) or timeout <= 0:
            timeout = 120.0

        task_type = inputs.get("task_type")
        if task_type is not None:
            task_type = str(task_type)
        resolved_cwd = self._resolve_cwd(p)

        prompt = inputs.get("prompt")
        stdin: str | None = None
        if isinstance(prompt, str) and prompt.strip():
            stdin = prompt

        try:
            proc = subprocess.run(
                argv,
                capture_output=True,
                text=True,
                timeout=float(timeout),
                check=False,
                cwd=resolved_cwd,
                input=stdin,
            )
        except subprocess.TimeoutExpired as exc:
            return {
                "execution_result": _execution_result_payload(
                    ok=False,
                    executor="codex",
                    provider="codex",
                    model=None,
                    task_type=task_type,
                    summary="subprocess timeout",
                    stdout=exc.stdout or None if hasattr(exc, "stdout") else None,
                    stderr=getattr(exc, "stderr", None) or str(exc),
                    raw_response={"error": "timeout", "cmd": argv, "stdin_used": bool(stdin)},
                    artifacts=[],
                    provider_meta={"argv": argv, "cwd": resolved_cwd},
                    next_hint=None,
                )
            }

        ok = proc.returncode == 0
        raw_response = {
            "returncode": proc.returncode,
            "args": argv,
            "stdin_used": bool(stdin),
        }
        return {
            "execution_result": _execution_result_payload(
                ok=ok,
                executor="codex",
                provider="codex",
                model=None,
                task_type=task_type,
                summary=None,
                stdout=proc.stdout or None,
                stderr=proc.stderr or None,
                raw_response=raw_response,
                artifacts=[],
                provider_meta={"argv": argv, "cwd": resolved_cwd},
                next_hint=None,
            )
        }
