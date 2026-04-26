"""Run tests command once and expose result."""

from __future__ import annotations

import subprocess
from pathlib import Path
from types import MappingProxyType
from typing import Any, Dict

from nodeflow.core.base_node import ExecutionContext, NodeExecutionFailure
from nodeflow.core.node_kinds import PythonActionNode


class RunTestsNode(PythonActionNode):
    role = "run_tests"

    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType,
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        repo_root = Path(str(inputs.get("repo_root") or ".")).resolve()
        argv = params.get("argv")
        if not isinstance(argv, list) or not argv or not all(isinstance(x, str) for x in argv):
            raise NodeExecutionFailure("run_tests.argv must be a non-empty list[str]")

        timeout = float(params.get("timeout", 300))
        try:
            proc = subprocess.run(
                argv,
                cwd=str(repo_root),
                capture_output=True,
                text=True,
                check=False,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired as exc:
            return {
                "test_result": {
                    "ok": False,
                    "argv": argv,
                    "returncode": None,
                    "stdout": (exc.stdout or "")[:8000] if isinstance(exc.stdout, str) else "",
                    "stderr": (exc.stderr or str(exc))[:4000]
                    if isinstance(exc.stderr, str)
                    else str(exc),
                    "timeout": True,
                    "timeout_seconds": timeout,
                }
            }

        return {
            "test_result": {
                "ok": proc.returncode == 0,
                "argv": argv,
                "returncode": proc.returncode,
                "stdout": (proc.stdout or "")[:8000],
                "stderr": (proc.stderr or "")[:4000],
                "timeout": False,
            }
        }
