"""run_tests stage — execute test command in workspace."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, Optional

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.paths import assert_path_under_run_dir
from nodeflow.workflows.dev_process.reuse import run_tests, write_stage_checkpoint


def run_run_tests_stage(
    *,
    repo_root: Path,
    artifact_root: str,
    run_id: str,
    test_argv: list[str] | None = None,
    diff_result: Optional[Dict[str, Any]] = None,
    execution_output: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    test_argv_use = (
        test_argv
        if test_argv is not None
        else [
            sys.executable,
            "-c",
            "import sys; sys.exit(0)",
        ]
    )
    test_result = run_tests(repo_root=repo_root, argv=test_argv_use, timeout=60)
    if not isinstance(test_result, dict):
        raise NodeExecutionFailure("run_tests must return a test_result dict")
    if "ok" not in test_result:
        raise NodeExecutionFailure("run_tests test_result must include boolean field 'ok'")
    test_result = {**test_result, "ok": test_result["ok"] is True}
    stage_cp_dir = str(Path(artifact_root) / "run_tests")
    stage_result = write_stage_checkpoint(
        request={
            "stage": "run_tests",
            "ok": bool(test_result.get("ok", True)),
            "summary": "run_tests completed",
            "next_action": "review",
        },
        checkpoint_dir=stage_cp_dir,
        run_id=run_id,
        stage="run_tests",
        repo_root=repo_root,
        extra_inputs={
            "execution_output": execution_output or {},
            "test_result": test_result,
            "diff_result": diff_result or {},
        },
        params={"next_action_default": "review"},
    )
    cp_path = None
    for art in stage_result.get("artifacts") or []:
        if isinstance(art, dict) and art.get("kind") == "checkpoint":
            cp_path = art.get("path")
            break
    if cp_path:
        assert_path_under_run_dir(artifact_root, cp_path)
    return {
        "status": "completed" if test_result.get("ok") else "failed",
        "test_result": test_result,
        "stage_checkpoint_path": cp_path,
        "stage_result": stage_result,
    }
