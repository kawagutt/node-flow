"""Discover dev-process runs and checkpoints under ``repo_root/.nodeflow/runs``."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.checkpoint import load_flow_checkpoint


def runs_dir(repo_root: Path) -> Path:
    return (repo_root / ".nodeflow" / "runs").resolve()


def list_run_dirs(repo_root: Path) -> List[Path]:
    root = runs_dir(repo_root)
    if not root.is_dir():
        return []
    return sorted(
        (p for p in root.iterdir() if p.is_dir()),
        key=lambda p: p.name,
    )


def _run_dir_matches_run_id(run_dir: Path, run_id: str) -> bool:
    cp_dir = run_dir / "checkpoints"
    if cp_dir.is_dir():
        for cp_file in cp_dir.glob("*.json"):
            try:
                doc = load_flow_checkpoint(cp_file)
            except NodeExecutionFailure:
                continue
            rc = doc.get("run_context") or {}
            if rc.get("run_id") == run_id:
                return True
    return run_dir.name == run_id


def _assert_checkpoint_under_repo_runs(repo_root: Path, cp: Path) -> None:
    runs = runs_dir(repo_root)
    try:
        cp.resolve().relative_to(runs)
    except ValueError as e:
        raise NodeExecutionFailure(f"checkpoint must be under {runs}: {cp}") from e


def _validate_checkpoint_scope(
    doc: Dict[str, Any],
    *,
    repo_root: Path,
    run_id: Optional[str] = None,
) -> None:
    rc = doc.get("run_context") if isinstance(doc.get("run_context"), dict) else {}
    cp_repo = rc.get("repo_root")
    if not isinstance(cp_repo, str) or not cp_repo.strip():
        raise NodeExecutionFailure("checkpoint missing run_context.repo_root")
    if Path(cp_repo).resolve() != repo_root.resolve():
        raise NodeExecutionFailure(
            f"checkpoint repo_root does not match --repo-root: "
            f"checkpoint {cp_repo!r} != request {str(repo_root)!r}"
        )
    if run_id is not None and rc.get("run_id") != run_id:
        raise NodeExecutionFailure(
            f"checkpoint run_id mismatch: checkpoint {rc.get('run_id')!r} != request {run_id!r}"
        )


def iter_checkpoint_files(
    repo_root: Path,
    *,
    run_id: Optional[str] = None,
) -> Iterator[Path]:
    for run_dir in list_run_dirs(repo_root):
        if run_id is not None and not _run_dir_matches_run_id(run_dir, run_id):
            continue
        cp_dir = run_dir / "checkpoints"
        if not cp_dir.is_dir():
            continue
        for cp_file in sorted(cp_dir.glob("*.json")):
            yield cp_file


def _checkpoint_sort_key(cp_file: Path, doc: Dict[str, Any]) -> Tuple[float, str]:
    written = doc.get("written_at")
    if isinstance(written, str) and written.strip():
        try:
            ts = datetime.fromisoformat(written.replace("Z", "+00:00")).timestamp()
            return (ts, cp_file.name)
        except ValueError:
            pass
    return (cp_file.stat().st_mtime, cp_file.name)


def find_latest_checkpoint(
    repo_root: Path,
    *,
    run_id: Optional[str] = None,
) -> Tuple[Path, Dict[str, Any]]:
    best_path: Optional[Path] = None
    best_doc: Optional[Dict[str, Any]] = None
    best_key: Tuple[float, str] = (-1.0, "")

    for cp_file in iter_checkpoint_files(repo_root, run_id=run_id):
        try:
            doc = load_flow_checkpoint(cp_file)
        except NodeExecutionFailure:
            continue
        if run_id is not None:
            rc = doc.get("run_context") if isinstance(doc.get("run_context"), dict) else {}
            if rc.get("run_id") != run_id:
                continue
        try:
            _validate_checkpoint_scope(doc, repo_root=repo_root, run_id=run_id)
        except NodeExecutionFailure:
            continue
        key = _checkpoint_sort_key(cp_file, doc)
        if key > best_key:
            best_path = cp_file
            best_doc = doc
            best_key = key

    if best_path is None or best_doc is None:
        hint = f" for run_id={run_id!r}" if run_id else ""
        raise NodeExecutionFailure(
            f"no dev-process checkpoint found under {runs_dir(repo_root)}{hint}"
        )

    return best_path, best_doc


def resolve_checkpoint_path(
    repo_root: Path,
    *,
    checkpoint: Optional[str] = None,
    run_id: Optional[str] = None,
) -> str:
    if checkpoint:
        cp = Path(checkpoint).resolve()
        _assert_checkpoint_under_repo_runs(repo_root, cp)
        doc = load_flow_checkpoint(cp)
        _validate_checkpoint_scope(doc, repo_root=repo_root, run_id=run_id)
        return str(cp)
    path, doc = find_latest_checkpoint(repo_root, run_id=run_id)
    _validate_checkpoint_scope(doc, repo_root=repo_root, run_id=run_id)
    return str(path.resolve())


def checkpoint_status(doc: Dict[str, Any], *, checkpoint_path: str) -> Dict[str, Any]:
    fr = doc.get("flow_result") if isinstance(doc.get("flow_result"), dict) else {}
    rc = doc.get("run_context") if isinstance(doc.get("run_context"), dict) else {}
    artifact_root = str(rc.get("artifact_root") or "")
    art = Path(artifact_root) if artifact_root else None
    summary_path = None
    if art and art.is_dir():
        summary_dir = art / "summary"
        if summary_dir.is_dir():
            summaries = sorted(summary_dir.glob("*_development_summary.json"))
            if summaries:
                summary_path = str(summaries[-1].resolve())

    dp = doc.get("dev_process") if isinstance(doc.get("dev_process"), dict) else {}
    phase_info = _extract_phase_status(dp)

    return {
        "flow_checkpoint_path": checkpoint_path,
        "state": fr.get("state"),
        "ok": fr.get("ok"),
        "allowed_actions": list(fr.get("allowed_actions") or []),
        "next_action": fr.get("next_action"),
        "merge_ready": fr.get("merge_ready"),
        "run_id": rc.get("run_id"),
        "repo_root": rc.get("repo_root"),
        "artifact_root": artifact_root,
        "timeline_path": str((art / "timeline.jsonl").resolve()) if art else None,
        "summary_path": summary_path,
        "workspace_strategy": rc.get("workspace_strategy"),
        **phase_info,
    }


def _extract_phase_status(dp: Dict[str, Any]) -> Dict[str, Any]:
    """Extract phase tracking info from dev_process state."""
    total = dp.get("total_phases")
    if not isinstance(total, int) or total < 1:
        return {}
    phase_index = dp.get("phase_index", 0)
    current_id = dp.get("current_phase_id", "")
    results = dp.get("phase_results") or {}
    plan_json_path = str(dp.get("plan_json_path") or "")

    phase_list: List[Dict[str, Any]] = []
    for i in range(total):
        pid = f"phase_{i:03d}"
        pr = results.get(pid) or {}
        status = str(pr.get("status") or "pending")
        title = str(pr.get("title") or "")
        if i == phase_index and status != "completed":
            display_status = "current"
        elif status == "completed":
            display_status = "completed"
        else:
            display_status = "pending"
        phase_list.append({"id": pid, "title": title, "status": display_status})

    return {
        "phase_index": phase_index,
        "current_phase_id": current_id,
        "total_phases": total,
        "phases": phase_list,
        "plan_json_path": plan_json_path if plan_json_path else None,
    }
