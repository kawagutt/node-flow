"""lint_fix stage: run ruff --fix on changed files."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path
from typing import Any, Dict, List

_RUFF_PYPROJECT_SECTION = re.compile(r"^\[tool\.ruff(?:\.[^\]]+)?\]", re.MULTILINE)


def _detect_ruff_configured(repo_root: Path) -> bool:
    """Check if ruff is configured via pyproject.toml or ruff.toml."""
    if (repo_root / "ruff.toml").exists():
        return True
    if (repo_root / ".ruff.toml").exists():
        return True
    pyproject = repo_root / "pyproject.toml"
    if pyproject.exists():
        content = pyproject.read_text(encoding="utf-8")
        if _RUFF_PYPROJECT_SECTION.search(content):
            return True
    return False


def _is_ruff_installed() -> bool:
    """Check if ruff is available in PATH."""
    cp = subprocess.run(
        ["ruff", "--version"],
        capture_output=True,
        text=True,
        check=False,
    )
    return cp.returncode == 0


def _collect_python_files(repo_root: Path, changed_paths: List[str]) -> List[str]:
    """Filter changed paths to only Python files."""
    py_files = []
    for p in changed_paths:
        if p.endswith(".py"):
            full = repo_root / p
            if full.exists():
                py_files.append(p)
    return py_files


def run_lint_fix_stage(
    *,
    repo_root: Path,
    changed_paths: List[str],
    artifact_root: str,
    phase_id: str = "",
) -> Dict[str, Any]:
    """Run ruff check --fix on changed Python files.

    Returns evidence dict with lint_fix status.
    Does NOT touch git staging — phase commit handles that.
    """
    if not _detect_ruff_configured(repo_root):
        return {
            "lint_fix": "skipped",
            "reason": "ruff not configured",
            "fixed_files": [],
        }

    if not _is_ruff_installed():
        return {
            "lint_fix": "skipped",
            "reason": "ruff not installed",
            "fixed_files": [],
            "warning": "ruff is not installed; install with pip install ruff",
        }

    py_files = _collect_python_files(repo_root, changed_paths)
    if not py_files:
        return {
            "lint_fix": "skipped",
            "reason": "no Python files changed",
            "fixed_files": [],
        }

    abs_files = [str(repo_root / f) for f in py_files]
    cp = subprocess.run(
        ["ruff", "check", "--fix"] + abs_files,
        capture_output=True,
        text=True,
        cwd=str(repo_root),
        check=False,
    )

    log_dir = (
        Path(artifact_root) / "phases" / phase_id / "lint_fix"
        if phase_id
        else Path(artifact_root) / "lint_fix"
    )
    log_dir.mkdir(parents=True, exist_ok=True)
    (log_dir / "stdout.txt").write_text(cp.stdout or "", encoding="utf-8")
    (log_dir / "stderr.txt").write_text(cp.stderr or "", encoding="utf-8")
    (log_dir / "returncode.txt").write_text(str(cp.returncode), encoding="utf-8")

    log_files = [
        str(log_dir / "stdout.txt"),
        str(log_dir / "stderr.txt"),
    ]

    evidence_base = (
        Path(artifact_root) / "phases" / phase_id / "evidence"
        if phase_id
        else Path(artifact_root) / "evidence"
    )
    evidence_base.mkdir(parents=True, exist_ok=True)
    evidence_json_path = str(evidence_base / "lint_fix.json")
    import json

    evidence_data = {
        "ruff_exit_code": cp.returncode,
        "target_files": py_files,
        "stdout_path": str(log_dir / "stdout.txt"),
        "stderr_path": str(log_dir / "stderr.txt"),
    }
    Path(evidence_json_path).write_text(
        json.dumps(evidence_data, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    evidence_paths = [evidence_json_path]

    if cp.returncode == 0:
        return {
            "lint_fix": "passed",
            "fixed_files": py_files,
            "ruff_exit_code": 0,
            "log_paths": log_files,
            "evidence_paths": evidence_paths,
        }

    return {
        "lint_fix": "ruff_failed",
        "fixed_files": py_files,
        "ruff_exit_code": cp.returncode,
        "ruff_stdout": (cp.stdout or "")[:2000],
        "ruff_stderr": (cp.stderr or "")[:2000],
        "log_paths": log_files,
        "evidence_paths": evidence_paths,
    }
