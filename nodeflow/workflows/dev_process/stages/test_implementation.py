"""write_tests stage — optional test scaffolding after implementation."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from nodeflow.workflows.dev_process.node_runner import run_node_exec


def run_test_implementation_stage(
    *,
    repo_root: Path,
    artifact_root: str,
    run_id: str,
    approved_spec: str,
    approved_plan: str,
    body: Dict[str, Any],
    rework_context: str | None = None,
) -> Dict[str, Any]:
    prompt = (
        "Add or update automated tests for the implementation.\n\n"
        f"## Spec\n{approved_spec}\n\n## Plan\n{approved_plan}\n"
    )
    if rework_context:
        prompt += f"\n## Rework Context\n{rework_context}\n"
    cwd = str(repo_root)

    execution_output, evidence_path, _rec = run_node_exec(
        body,
        node_name="write_tests",
        stage="test_implementation",
        prompt=prompt,
        cwd=cwd,
        run_id=run_id,
        artifact_root=artifact_root,
    )

    tests_note = Path(artifact_root) / "test_implementation" / "tests_written.txt"
    tests_note.parent.mkdir(parents=True, exist_ok=True)
    tests_note.write_text(str(execution_output.get("stdout") or "tests stage ok"), encoding="utf-8")
    return {
        "status": "completed",
        "evidence_paths": [evidence_path],
        "tests_artifact": str(tests_note),
    }
