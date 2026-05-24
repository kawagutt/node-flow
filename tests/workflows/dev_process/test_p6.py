"""P6: merge summary, merge policy, attempt branches, CLI JSON inputs."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from nodeflow.cli import _parse_cli_value, _parse_kv_pairs
from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.checkpoint import load_flow_checkpoint
from nodeflow.workflows.dev_process.constants import (
    ACTION_APPROVE_FINAL,
    MERGE_POLICY_GIT_MERGE_BRANCH,
    MERGE_POLICY_RECORD_ONLY,
    STATE_FAILED,
    STATE_MERGED,
)
from nodeflow.workflows.dev_process.dev_process_flow.node_dev_process_flow import (
    DevProcessFlowNode,
)
from nodeflow.workflows.dev_process.merge import execute_merge_policy
from nodeflow.workflows.dev_process.paths import planned_branch_name_for_attempt
from tests.workflows.dev_process.git_fixtures import git_repo_with_commit


def _timeline_events(artifact_root: Path) -> list[str]:
    path = artifact_root / "timeline.jsonl"
    if not path.is_file():
        return []
    lines = path.read_text(encoding="utf-8").strip().splitlines()
    return [json.loads(line).get("event") for line in lines if line]


def test_planned_branch_name_for_attempt_unique_per_attempt() -> None:
    a = planned_branch_name_for_attempt("20260524T120000000001Z", 1)
    b = planned_branch_name_for_attempt("20260524T120000000001Z", 2)
    assert a.endswith("/attempt-001")
    assert b.endswith("/attempt-002")
    assert a != b


def test_cli_parse_json_array_value() -> None:
    parsed = _parse_kv_pairs(('exec_argv=["codex","exec"]',))
    assert parsed["exec_argv"] == ["codex", "exec"]


def test_cli_parse_invalid_json_raises() -> None:
    with pytest.raises(Exception, match="invalid JSON"):
        _parse_cli_value("[not json")


def test_merge_writes_development_summary(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    start = DevProcessFlowNode().execute(
        {"action": "start", "repo_root": str(repo), "task_prompt": "summary merge"},
        {},
    )
    cp = start["flow_output"]["flow_result"]["flow_checkpoint_path"]
    appr = DevProcessFlowNode().execute(
        {"action": "approve_spec", "repo_root": str(repo), "flow_checkpoint_path": cp},
        {},
    )
    cp2 = appr["flow_output"]["flow_result"]["flow_checkpoint_path"]
    final = DevProcessFlowNode().execute(
        {"action": ACTION_APPROVE_FINAL, "repo_root": str(repo), "flow_checkpoint_path": cp2},
        {},
    )
    cp3 = final["flow_output"]["flow_result"]["flow_checkpoint_path"]
    merged = DevProcessFlowNode().execute(
        {"action": "merge", "repo_root": str(repo), "flow_checkpoint_path": cp3},
        {},
    )["flow_output"]
    assert merged["flow_result"]["state"] == STATE_MERGED
    assert "development_summary" in merged
    artifact_root = Path(merged["run_context"]["artifact_root"])
    assert (artifact_root / "summary" / "merge_development_summary.json").is_file()


def test_git_merge_policy_rejected_for_current_repo(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    body = {
        "run_context": {
            "repo_root": str(repo),
            "workspace_strategy": "current_repo",
            "source_current_branch": "main",
        },
        "dev_process": {"merge_policy": MERGE_POLICY_GIT_MERGE_BRANCH},
        "workspace_context": {
            "strategy": "current_repo",
            "source_repo_root": str(repo),
            "workspace_root": str(repo),
            "base_revision": "HEAD",
            "planned_branch_name": "feat/x",
            "current_branch": "main",
        },
    }
    with pytest.raises(NodeExecutionFailure, match="requires workspace_strategy git_worktree"):
        execute_merge_policy(body)


def test_git_merge_branch_into_main(tmp_path: Path) -> None:
    repo = tmp_path / "repo_merge"
    repo.mkdir()
    git_repo_with_commit(repo)
    run_id = "20260524T120000000001Z"
    body, wt_dir, _branch = _setup_git_merge_worktree_with_review(tmp_path, repo, run_id)
    (wt_dir / "feature.txt").write_text("x\n", encoding="utf-8")
    subprocess.run(["git", "add", "feature.txt"], cwd=str(wt_dir), check=True, capture_output=True)
    subprocess.run(
        ["git", "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-m", "feat"],
        cwd=str(wt_dir),
        check=True,
        capture_output=True,
    )
    _sync_review_snapshot(body, repo)
    result = execute_merge_policy(body)
    assert result["policy"] == MERGE_POLICY_GIT_MERGE_BRANCH
    assert result["ok"] is True
    assert (repo / "feature.txt").is_file()


def test_merge_policy_record_only_default_on_start(tmp_path: Path) -> None:
    repo = tmp_path / "repo_mp"
    repo.mkdir()
    git_repo_with_commit(repo)
    out = DevProcessFlowNode().execute(
        {"action": "start", "repo_root": str(repo), "task_prompt": "mp"},
        {},
    )["flow_output"]
    from nodeflow.workflows.dev_process.checkpoint import load_flow_checkpoint

    doc = load_flow_checkpoint(out["flow_result"]["flow_checkpoint_path"])
    assert doc["dev_process"]["merge_policy"] == MERGE_POLICY_RECORD_ONLY


def test_start_rejects_invalid_merge_policy(tmp_path: Path) -> None:
    repo = tmp_path / "repo_bad_mp"
    repo.mkdir()
    git_repo_with_commit(repo)
    with pytest.raises(NodeExecutionFailure, match="unsupported merge_policy"):
        DevProcessFlowNode().execute(
            {
                "action": "start",
                "repo_root": str(repo),
                "task_prompt": "bad mp",
                "merge_policy": "git_merge_branhc",
            },
            {},
        )


def test_start_rejects_git_merge_with_current_repo(tmp_path: Path) -> None:
    repo = tmp_path / "repo_mp_combo"
    repo.mkdir()
    git_repo_with_commit(repo)
    with pytest.raises(NodeExecutionFailure, match="requires workspace_strategy=git_worktree"):
        DevProcessFlowNode().execute(
            {
                "action": "start",
                "repo_root": str(repo),
                "task_prompt": "combo",
                "workspace_strategy": "current_repo",
                "merge_policy": "git_merge_branch",
            },
            {},
        )


def _setup_git_merge_worktree(
    tmp_path: Path,
    repo: Path,
    run_id: str,
    *,
    attempt: int = 1,
) -> tuple[dict, Path, str]:
    branch = planned_branch_name_for_attempt(run_id, attempt)
    artifact_root = tmp_path / "artifacts" / run_id
    wt_dir = artifact_root / "worktrees" / f"{attempt:03d}"
    wt_dir.parent.mkdir(parents=True, exist_ok=True)
    base_revision = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=str(repo),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    subprocess.run(
        ["git", "worktree", "add", "-b", branch, str(wt_dir), "HEAD"],
        cwd=str(repo),
        check=True,
        capture_output=True,
    )
    body = {
        "run_context": {
            "run_id": run_id,
            "repo_root": str(repo),
            "artifact_root": str(artifact_root),
            "workspace_strategy": "git_worktree",
            "source_current_branch": "main",
            "source_base_revision": base_revision,
        },
        "dev_process": {
            "merge_policy": MERGE_POLICY_GIT_MERGE_BRANCH,
            "workspace_attempt": attempt,
        },
        "workspace_context": {
            "strategy": "git_worktree",
            "source_repo_root": str(repo),
            "workspace_root": str(wt_dir),
            "base_revision": base_revision,
            "planned_branch_name": branch,
            "current_branch": branch,
        },
    }
    return body, wt_dir, branch


def _sync_review_snapshot(body: dict, repo: Path) -> None:
    branch = body["workspace_context"]["planned_branch_name"]
    head = subprocess.run(
        ["git", "rev-parse", f"refs/heads/{branch}"],
        cwd=str(repo),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    body.setdefault("stages", {})["review"] = {
        "status": "completed",
        "reviewed_branch_name": branch,
        "reviewed_branch_head": head,
    }


def _setup_git_merge_worktree_with_review(
    tmp_path: Path,
    repo: Path,
    run_id: str,
    *,
    attempt: int = 1,
) -> tuple[dict, Path, str]:
    body, wt_dir, branch = _setup_git_merge_worktree(tmp_path, repo, run_id, attempt=attempt)
    _sync_review_snapshot(body, repo)
    return body, wt_dir, branch


def _commit_during_implement(monkeypatch: pytest.MonkeyPatch) -> None:
    from nodeflow.workflows.dev_process.stages import implement as implement_mod

    real = implement_mod.run_implement_stage

    def _wrapped(**kwargs: object) -> dict:
        out = real(**kwargs)  # type: ignore[arg-type]
        repo_root = kwargs.get("repo_root")
        if repo_root is not None:
            _git_commit_file(Path(str(repo_root)), "feature.txt", "x\n", "feat")
        return out

    monkeypatch.setattr(
        "nodeflow.workflows.dev_process.flow_actions.run_implement_stage",
        _wrapped,
    )


def test_git_merge_fails_on_dirty_source_repo(tmp_path: Path) -> None:
    repo = tmp_path / "repo_dirty_merge"
    repo.mkdir()
    git_repo_with_commit(repo)
    run_id = "20260524T120000000001Z"
    body, wt_dir, _branch = _setup_git_merge_worktree_with_review(tmp_path, repo, run_id)
    (wt_dir / "feature.txt").write_text("x\n", encoding="utf-8")
    subprocess.run(["git", "add", "feature.txt"], cwd=str(wt_dir), check=True, capture_output=True)
    subprocess.run(
        ["git", "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-m", "feat"],
        cwd=str(wt_dir),
        check=True,
        capture_output=True,
    )
    _sync_review_snapshot(body, repo)
    (repo / "dirty.txt").write_text("dirty\n", encoding="utf-8")
    with pytest.raises(NodeExecutionFailure, match="uncommitted changes before merge"):
        execute_merge_policy(body)


def test_git_merge_fails_on_invalid_target_branch(tmp_path: Path) -> None:
    repo = tmp_path / "repo_bad_target"
    repo.mkdir()
    git_repo_with_commit(repo)
    run_id = "20260524T120000000002Z"
    body, _wt_dir, _branch = _setup_git_merge_worktree_with_review(tmp_path, repo, run_id)
    body["run_context"]["source_current_branch"] = "no-such-branch"
    with pytest.raises(NodeExecutionFailure, match="merge target branch not found"):
        execute_merge_policy(body)


def test_git_merge_fails_on_head_target_branch(tmp_path: Path) -> None:
    repo = tmp_path / "repo_head_target"
    repo.mkdir()
    git_repo_with_commit(repo)
    run_id = "20260524T120000000006Z"
    body, _wt_dir, _branch = _setup_git_merge_worktree_with_review(tmp_path, repo, run_id)
    body["run_context"]["source_current_branch"] = "HEAD"
    with pytest.raises(NodeExecutionFailure, match="invalid merge target branch"):
        execute_merge_policy(body)


def test_git_merge_ignores_nodeflow_dirty_paths(tmp_path: Path) -> None:
    repo = tmp_path / "repo_nodeflow_dirty"
    repo.mkdir()
    git_repo_with_commit(repo)
    run_id = "20260524T120000000003Z"
    body, wt_dir, _branch = _setup_git_merge_worktree_with_review(tmp_path, repo, run_id)
    (wt_dir / "feature.txt").write_text("x\n", encoding="utf-8")
    subprocess.run(["git", "add", "feature.txt"], cwd=str(wt_dir), check=True, capture_output=True)
    subprocess.run(
        ["git", "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-m", "feat"],
        cwd=str(wt_dir),
        check=True,
        capture_output=True,
    )
    _sync_review_snapshot(body, repo)
    nodeflow_dir = repo / ".nodeflow" / "runs" / run_id
    nodeflow_dir.mkdir(parents=True)
    (nodeflow_dir / "timeline.jsonl").write_text("{}\n", encoding="utf-8")
    result = execute_merge_policy(body)
    assert result["ok"] is True
    assert (repo / "feature.txt").is_file()


def test_git_merge_aborts_on_conflict(tmp_path: Path) -> None:
    repo = tmp_path / "repo_merge_conflict"
    repo.mkdir()
    git_repo_with_commit(repo)
    run_id = "20260524T120000000004Z"
    body, wt_dir, _branch = _setup_git_merge_worktree_with_review(tmp_path, repo, run_id)
    (wt_dir / "README.md").write_text("branch change\n", encoding="utf-8")
    subprocess.run(["git", "add", "README.md"], cwd=str(wt_dir), check=True, capture_output=True)
    subprocess.run(
        ["git", "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-m", "branch"],
        cwd=str(wt_dir),
        check=True,
        capture_output=True,
    )
    _sync_review_snapshot(body, repo)
    (repo / "README.md").write_text("main change\n", encoding="utf-8")
    subprocess.run(["git", "add", "README.md"], cwd=str(repo), check=True, capture_output=True)
    subprocess.run(
        ["git", "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-m", "main"],
        cwd=str(repo),
        check=True,
        capture_output=True,
    )
    with pytest.raises(NodeExecutionFailure, match="failed \\(aborted\\)"):
        execute_merge_policy(body)
    status = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=str(repo),
        check=True,
        capture_output=True,
        text=True,
    )
    assert status.stdout.strip() == ""
    merge_head = repo / ".git" / "MERGE_HEAD"
    assert not merge_head.exists()


def _flow_to_awaiting_merge(
    repo: Path,
    *,
    workspace_strategy: str = "current_repo",
    merge_policy: str = MERGE_POLICY_GIT_MERGE_BRANCH,
) -> tuple[str, Path]:
    start = DevProcessFlowNode().execute(
        {
            "action": "start",
            "repo_root": str(repo),
            "task_prompt": "merge fail flow",
            "workspace_strategy": workspace_strategy,
            "merge_policy": merge_policy,
        },
        {},
    )["flow_output"]
    cp = start["flow_result"]["flow_checkpoint_path"]
    artifact_root = Path(start["run_context"]["artifact_root"])
    appr = DevProcessFlowNode().execute(
        {"action": "approve_spec", "repo_root": str(repo), "flow_checkpoint_path": cp},
        {},
    )["flow_output"]
    cp2 = appr["flow_result"]["flow_checkpoint_path"]
    final = DevProcessFlowNode().execute(
        {"action": ACTION_APPROVE_FINAL, "repo_root": str(repo), "flow_checkpoint_path": cp2},
        {},
    )["flow_output"]
    return final["flow_result"]["flow_checkpoint_path"], artifact_root


def test_merge_failure_records_timeline_and_checkpoint(tmp_path: Path) -> None:
    repo = tmp_path / "repo_merge_fail_audit"
    repo.mkdir()
    git_repo_with_commit(repo)
    cp3, artifact_root = _flow_to_awaiting_merge(
        repo,
        workspace_strategy="git_worktree",
        merge_policy=MERGE_POLICY_GIT_MERGE_BRANCH,
    )
    (repo / "dirty.txt").write_text("dirty\n", encoding="utf-8")
    with pytest.raises(NodeExecutionFailure, match="uncommitted changes before merge"):
        DevProcessFlowNode().execute(
            {"action": "merge", "repo_root": str(repo), "flow_checkpoint_path": cp3},
            {},
        )
    events = _timeline_events(artifact_root)
    assert "merge_attempted" in events
    assert "flow_failed" in events
    assert events.index("merge_attempted") < events.index("flow_failed")
    merge_checkpoints = list((artifact_root / "checkpoints").glob("*_merge_flow.json"))
    assert len(merge_checkpoints) == 1
    doc = load_flow_checkpoint(str(merge_checkpoints[0]))
    assert doc["flow_result"]["state"] == STATE_FAILED
    assert doc["flow_result"]["ok"] is False


def _git_commit_file(repo_or_wt: Path, rel_path: str, content: str, msg: str) -> None:
    path = repo_or_wt / rel_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    subprocess.run(["git", "add", rel_path], cwd=str(repo_or_wt), check=True, capture_output=True)
    subprocess.run(
        ["git", "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-m", msg],
        cwd=str(repo_or_wt),
        check=True,
        capture_output=True,
    )


def test_git_merge_branch_full_flow_with_worktree_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = tmp_path / "repo_full_merge"
    repo.mkdir()
    git_repo_with_commit(repo)
    _commit_during_implement(monkeypatch)
    start = DevProcessFlowNode().execute(
        {
            "action": "start",
            "repo_root": str(repo),
            "task_prompt": "full merge",
            "workspace_strategy": "git_worktree",
            "merge_policy": MERGE_POLICY_GIT_MERGE_BRANCH,
        },
        {},
    )["flow_output"]
    cp = start["flow_result"]["flow_checkpoint_path"]
    appr = DevProcessFlowNode().execute(
        {"action": "approve_spec", "repo_root": str(repo), "flow_checkpoint_path": cp},
        {},
    )["flow_output"]
    cp2 = appr["flow_result"]["flow_checkpoint_path"]
    review_doc = load_flow_checkpoint(cp2)
    assert review_doc["stages"]["review"]["reviewed_branch_head"]
    final = DevProcessFlowNode().execute(
        {"action": ACTION_APPROVE_FINAL, "repo_root": str(repo), "flow_checkpoint_path": cp2},
        {},
    )["flow_output"]
    cp3 = final["flow_result"]["flow_checkpoint_path"]
    merged = DevProcessFlowNode().execute(
        {"action": "merge", "repo_root": str(repo), "flow_checkpoint_path": cp3},
        {},
    )["flow_output"]
    assert merged["flow_result"]["state"] == STATE_MERGED
    assert merged["merge_result"]["policy"] == MERGE_POLICY_GIT_MERGE_BRANCH
    assert (repo / "feature.txt").is_file()
    artifact_root = Path(merged["run_context"]["artifact_root"])
    assert (artifact_root / "summary" / "merge_development_summary.json").is_file()


def test_git_merge_rejects_tampered_planned_branch_name(tmp_path: Path) -> None:
    repo = tmp_path / "repo_tamper"
    repo.mkdir()
    git_repo_with_commit(repo)
    run_id = "20260524T120000000005Z"
    body, _wt_dir, _branch = _setup_git_merge_worktree_with_review(tmp_path, repo, run_id)
    body["workspace_context"]["planned_branch_name"] = "feat/evil-branch"
    body["workspace_context"]["current_branch"] = "feat/evil-branch"
    with pytest.raises(NodeExecutionFailure, match="merge branch mismatch"):
        execute_merge_policy(body)


def test_merge_summary_failure_after_git_merge_uses_fallback_and_merged_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = tmp_path / "repo_summary_fail"
    repo.mkdir()
    git_repo_with_commit(repo)
    _commit_during_implement(monkeypatch)
    start = DevProcessFlowNode().execute(
        {
            "action": "start",
            "repo_root": str(repo),
            "task_prompt": "summary fail",
            "workspace_strategy": "git_worktree",
            "merge_policy": MERGE_POLICY_GIT_MERGE_BRANCH,
        },
        {},
    )["flow_output"]
    cp = start["flow_result"]["flow_checkpoint_path"]
    artifact_root = Path(start["run_context"]["artifact_root"])
    appr = DevProcessFlowNode().execute(
        {"action": "approve_spec", "repo_root": str(repo), "flow_checkpoint_path": cp},
        {},
    )["flow_output"]
    cp2 = appr["flow_result"]["flow_checkpoint_path"]
    final = DevProcessFlowNode().execute(
        {"action": ACTION_APPROVE_FINAL, "repo_root": str(repo), "flow_checkpoint_path": cp2},
        {},
    )["flow_output"]
    cp3 = final["flow_result"]["flow_checkpoint_path"]

    def _boom_summary(**kwargs: object) -> dict:
        raise NodeExecutionFailure("summary boom")

    monkeypatch.setattr(
        "nodeflow.workflows.dev_process.flow_merge.write_development_summary",
        _boom_summary,
    )
    merged = DevProcessFlowNode().execute(
        {"action": "merge", "repo_root": str(repo), "flow_checkpoint_path": cp3},
        {},
    )["flow_output"]
    assert (repo / "feature.txt").is_file()
    assert merged["flow_result"]["state"] == STATE_MERGED
    assert merged["development_summary"]["status"] == "fallback"
    events = _timeline_events(artifact_root)
    assert "merge_attempted" in events
    assert "summary_failed" in events
    assert "flow_failed" not in events
    summary_path = artifact_root / "summary" / "merge_development_summary.json"
    assert summary_path.is_file()


def test_git_merge_rejects_branch_changed_after_review(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = tmp_path / "repo_late_commit"
    repo.mkdir()
    git_repo_with_commit(repo)
    _commit_during_implement(monkeypatch)
    start = DevProcessFlowNode().execute(
        {
            "action": "start",
            "repo_root": str(repo),
            "task_prompt": "late commit",
            "workspace_strategy": "git_worktree",
            "merge_policy": MERGE_POLICY_GIT_MERGE_BRANCH,
        },
        {},
    )["flow_output"]
    cp = start["flow_result"]["flow_checkpoint_path"]
    appr = DevProcessFlowNode().execute(
        {"action": "approve_spec", "repo_root": str(repo), "flow_checkpoint_path": cp},
        {},
    )["flow_output"]
    wt_dir = Path(appr["workspace_context"]["workspace_root"])
    _git_commit_file(wt_dir, "late.txt", "late\n", "late change")
    cp2 = appr["flow_result"]["flow_checkpoint_path"]
    final = DevProcessFlowNode().execute(
        {"action": ACTION_APPROVE_FINAL, "repo_root": str(repo), "flow_checkpoint_path": cp2},
        {},
    )["flow_output"]
    cp3 = final["flow_result"]["flow_checkpoint_path"]
    with pytest.raises(NodeExecutionFailure, match="changed after review"):
        DevProcessFlowNode().execute(
            {"action": "merge", "repo_root": str(repo), "flow_checkpoint_path": cp3},
            {},
        )


def test_git_merge_rejects_dirty_worktree(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    repo = tmp_path / "repo_dirty_wt"
    repo.mkdir()
    git_repo_with_commit(repo)
    _commit_during_implement(monkeypatch)
    start = DevProcessFlowNode().execute(
        {
            "action": "start",
            "repo_root": str(repo),
            "task_prompt": "dirty wt",
            "workspace_strategy": "git_worktree",
            "merge_policy": MERGE_POLICY_GIT_MERGE_BRANCH,
        },
        {},
    )["flow_output"]
    cp = start["flow_result"]["flow_checkpoint_path"]
    appr = DevProcessFlowNode().execute(
        {"action": "approve_spec", "repo_root": str(repo), "flow_checkpoint_path": cp},
        {},
    )["flow_output"]
    wt_dir = Path(appr["workspace_context"]["workspace_root"])
    (wt_dir / "dirty.txt").write_text("dirty\n", encoding="utf-8")
    cp2 = appr["flow_result"]["flow_checkpoint_path"]
    final = DevProcessFlowNode().execute(
        {"action": ACTION_APPROVE_FINAL, "repo_root": str(repo), "flow_checkpoint_path": cp2},
        {},
    )["flow_output"]
    cp3 = final["flow_result"]["flow_checkpoint_path"]
    with pytest.raises(NodeExecutionFailure, match="uncommitted changes"):
        DevProcessFlowNode().execute(
            {"action": "merge", "repo_root": str(repo), "flow_checkpoint_path": cp3},
            {},
        )


def test_git_merge_rejects_workspace_root_outside_worktrees(tmp_path: Path) -> None:
    repo = tmp_path / "repo_bad_wt_root"
    repo.mkdir()
    git_repo_with_commit(repo)
    run_id = "20260524T120000000007Z"
    body, _wt_dir, _branch = _setup_git_merge_worktree_with_review(tmp_path, repo, run_id)
    body["workspace_context"]["workspace_root"] = str(tmp_path / "outside")
    with pytest.raises(NodeExecutionFailure, match="must be under"):
        execute_merge_policy(body)


def test_dev_process_never_runs_git_push() -> None:
    root = Path("nodeflow/workflows/dev_process")
    text = "\n".join(p.read_text(encoding="utf-8") for p in root.rglob("*.py"))
    assert '"push"' not in text
    assert "'push'" not in text


def test_git_merge_rejects_attempt_branch_unrelated_to_source_base(tmp_path: Path) -> None:
    repo = tmp_path / "repo_unrelated_attempt"
    repo.mkdir()
    git_repo_with_commit(repo)
    first = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=str(repo),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    (repo / "README.md").write_text("second\n", encoding="utf-8")
    subprocess.run(["git", "add", "README.md"], cwd=str(repo), check=True, capture_output=True)
    subprocess.run(
        ["git", "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-m", "second"],
        cwd=str(repo),
        check=True,
        capture_output=True,
    )
    run_id = "20260524T120000000008Z"
    branch = planned_branch_name_for_attempt(run_id, 1)
    artifact_root = tmp_path / "artifacts" / run_id
    wt_dir = artifact_root / "worktrees" / "001"
    wt_dir.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        ["git", "worktree", "add", "-b", branch, str(wt_dir), first],
        cwd=str(repo),
        check=True,
        capture_output=True,
    )
    (wt_dir / "feature.txt").write_text("x\n", encoding="utf-8")
    subprocess.run(["git", "add", "feature.txt"], cwd=str(wt_dir), check=True, capture_output=True)
    subprocess.run(
        ["git", "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-m", "feat"],
        cwd=str(wt_dir),
        check=True,
        capture_output=True,
    )
    artifact_root = tmp_path / "artifacts" / run_id
    body = {
        "run_context": {
            "run_id": run_id,
            "repo_root": str(repo),
            "artifact_root": str(artifact_root),
            "workspace_strategy": "git_worktree",
            "source_current_branch": "main",
            "source_base_revision": subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=str(repo),
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip(),
        },
        "dev_process": {
            "merge_policy": MERGE_POLICY_GIT_MERGE_BRANCH,
            "workspace_attempt": 1,
        },
        "workspace_context": {
            "strategy": "git_worktree",
            "source_repo_root": str(repo),
            "workspace_root": str(wt_dir),
            "base_revision": first,
            "planned_branch_name": branch,
            "current_branch": branch,
        },
    }
    _sync_review_snapshot(body, repo)
    with pytest.raises(
        NodeExecutionFailure, match="attempt branch .* unrelated to flow start base"
    ):
        execute_merge_policy(body)


def test_git_merge_rejects_target_branch_unrelated_to_source_base(tmp_path: Path) -> None:
    repo = tmp_path / "repo_unrelated_target"
    repo.mkdir()
    git_repo_with_commit(repo)
    run_id = "20260524T120000000009Z"
    body, wt_dir, branch = _setup_git_merge_worktree_with_review(tmp_path, repo, run_id)
    _git_commit_file(wt_dir, "feature.txt", "x\n", "feat")
    _sync_review_snapshot(body, repo)
    subprocess.run(
        ["git", "checkout", "--orphan", "side"],
        cwd=str(repo),
        check=True,
        capture_output=True,
    )
    (repo / "README.md").write_text("side\n", encoding="utf-8")
    subprocess.run(["git", "add", "README.md"], cwd=str(repo), check=True, capture_output=True)
    subprocess.run(
        ["git", "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-m", "side"],
        cwd=str(repo),
        check=True,
        capture_output=True,
    )
    body["run_context"]["source_current_branch"] = "side"
    with pytest.raises(
        NodeExecutionFailure, match="merge target branch .* unrelated to flow start base"
    ):
        execute_merge_policy(body)
