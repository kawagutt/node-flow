"""ClaudeCodeExecNode — single subprocess; Common Output on execution_output port."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from types import MappingProxyType
from typing import Any, Dict, List, Optional

from nodeflow.core.base_node import ExecutionContext, NodeExecutionFailure
from nodeflow.core.node_kinds import CliActionNode


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


class ClaudeCodeExecNode(CliActionNode):
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

        model = p.get("model")
        if model is not None:
            model = str(model)

        task_type_raw = inputs.get("task_type")
        if isinstance(task_type_raw, dict):
            task_type = task_type_raw.get("task_type") or task_type_raw.get("value")
        else:
            task_type = task_type_raw
        if task_type is not None:
            task_type = str(task_type)
        resolved_cwd = self._resolve_cwd(p)

        try:
            proc = subprocess.run(
                argv,
                capture_output=True,
                text=True,
                timeout=float(timeout),
                check=False,
                cwd=resolved_cwd,
            )
        except subprocess.TimeoutExpired as exc:
            return {
                "execution_output": _execution_output_payload(
                    ok=False,
                    external_executor="claude_code",
                    provider="anthropic",
                    model=model,
                    task_type=task_type,
                    summary="subprocess timeout",
                    stdout=getattr(exc, "stdout", None) or None,
                    stderr=getattr(exc, "stderr", None) or str(exc),
                    raw_output={"error": "timeout", "cmd": argv},
                    artifacts=[],
                    provider_meta={"argv": argv, "cwd": resolved_cwd},
                    next_hint=None,
                )
            }

        ok = proc.returncode == 0
        raw_out = {
            "returncode": proc.returncode,
            "args": argv,
        }
        return {
            "execution_output": _execution_output_payload(
                ok=ok,
                external_executor="claude_code",
                provider="anthropic",
                model=model,
                task_type=task_type,
                summary=None,
                stdout=proc.stdout or None,
                stderr=proc.stderr or None,
                raw_output=raw_out,
                artifacts=[],
                provider_meta={"argv": argv, "cwd": resolved_cwd},
                next_hint=None,
            )
        }
