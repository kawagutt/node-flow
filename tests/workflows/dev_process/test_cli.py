"""Dev-process CLI tests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.checkpoint import load_flow_checkpoint
from nodeflow.workflows.dev_process.cli import main
from nodeflow.workflows.dev_process.constants import STATE_AWAITING_SPEC_HUMAN_GATE, STATE_MERGED
from nodeflow.workflows.dev_process.discovery import find_latest_checkpoint, resolve_checkpoint_path
from nodeflow.workflows.dev_process.hermetic_argv import spec_argv
from tests.workflows.dev_process.git_fixtures import git_repo_with_commit


@pytest.fixture
def cli_runner() -> CliRunner:
    return CliRunner()


def _hermetic_start_argv_json() -> str:
    return json.dumps(spec_argv())


def test_discovery_latest_checkpoint(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    runner = CliRunner()
    r1 = runner.invoke(
        main,
        [
            "--repo-root",
            str(repo),
            "start",
            "--task-prompt",
            "run one",
            "--workspace-strategy",
            "current_repo",
        ],
    )
    assert r1.exit_code == 0, r1.output
    cp1 = resolve_checkpoint_path(repo, checkpoint=None, run_id=None)

    r2 = runner.invoke(
        main,
        [
            "--repo-root",
            str(repo),
            "start",
            "--task-prompt",
            "run two",
            "--workspace-strategy",
            "current_repo",
        ],
    )
    assert r2.exit_code == 0, r2.output
    cp2 = resolve_checkpoint_path(repo, checkpoint=None, run_id=None)
    assert cp2 != cp1
    latest_path, latest_doc = find_latest_checkpoint(repo)
    assert str(latest_path) == cp2
    assert latest_doc["task_prompt"] == "run two"


def test_status_shows_state_and_paths(tmp_path: Path, cli_runner: CliRunner) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    start = cli_runner.invoke(
        main,
        [
            "--repo-root",
            str(repo),
            "start",
            "--task-prompt",
            "status test",
            "--workspace-strategy",
            "current_repo",
        ],
    )
    assert start.exit_code == 0, start.output
    status = cli_runner.invoke(main, ["--repo-root", str(repo), "status"])
    assert status.exit_code == 0, status.output
    assert f"state: {STATE_AWAITING_SPEC_HUMAN_GATE}" in status.output
    assert "artifact_root:" in status.output
    assert "timeline:" in status.output
    assert "flow_checkpoint_path:" in status.output
    assert "allowed_actions:" in status.output
    assert "summary: <not written yet>" in status.output


def test_wrapper_hermetic_full_path_to_merged(tmp_path: Path, cli_runner: CliRunner) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    common = ["--repo-root", str(repo)]

    start = cli_runner.invoke(
        main,
        common
        + [
            "start",
            "--task-prompt",
            "wrapper smoke",
            "--workspace-strategy",
            "current_repo",
            "--merge-policy",
            "record_only",
        ],
    )
    assert start.exit_code == 0, start.output
    assert "flow_checkpoint_path:" in start.output

    appr = cli_runner.invoke(main, common + ["approve-spec"])
    assert appr.exit_code == 0, appr.output
    assert "awaiting_implementation" in appr.output

    cont = cli_runner.invoke(main, common + ["continue-implementation"])
    assert cont.exit_code == 0, cont.output
    assert "awaiting_final_approval" in cont.output

    final = cli_runner.invoke(main, common + ["approve-final"])
    assert final.exit_code == 0, final.output
    assert "awaiting_merge" in final.output

    merged = cli_runner.invoke(main, common + ["merge"])
    assert merged.exit_code == 0, merged.output
    assert f"state: {STATE_MERGED}" in merged.output
    assert "summary:" in merged.output

    cp = resolve_checkpoint_path(repo, checkpoint=None, run_id=None)
    doc = load_flow_checkpoint(cp)
    assert doc["flow_result"]["state"] == STATE_MERGED


def test_resume_uses_latest_checkpoint_without_manual_cp(
    tmp_path: Path, cli_runner: CliRunner
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    common = ["--repo-root", str(repo)]
    cli_runner.invoke(
        main,
        common
        + [
            "start",
            "--task-prompt",
            "auto cp",
            "--workspace-strategy",
            "current_repo",
        ],
    )
    cp_after_start = resolve_checkpoint_path(repo, checkpoint=None, run_id=None)
    cli_runner.invoke(main, common + ["approve-spec"])
    cp_after_approve = resolve_checkpoint_path(repo, checkpoint=None, run_id=None)
    assert cp_after_approve != cp_after_start


def test_explicit_checkpoint_flag(tmp_path: Path, cli_runner: CliRunner) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    common = ["--repo-root", str(repo)]
    cli_runner.invoke(
        main,
        common
        + [
            "start",
            "--task-prompt",
            "explicit cp",
            "--workspace-strategy",
            "current_repo",
        ],
    )
    cp = resolve_checkpoint_path(repo, checkpoint=None, run_id=None)
    status = cli_runner.invoke(main, common + ["status", "--checkpoint", cp])
    assert status.exit_code == 0, status.output
    assert cp in status.output


def test_run_id_scopes_latest_checkpoint(tmp_path: Path, cli_runner: CliRunner) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    common = ["--repo-root", str(repo)]
    r1 = cli_runner.invoke(
        main,
        common
        + [
            "start",
            "--task-prompt",
            "scoped one",
            "--workspace-strategy",
            "current_repo",
        ],
    )
    assert r1.exit_code == 0
    _cp1, doc1 = find_latest_checkpoint(repo)
    run_id_1 = doc1["run_context"]["run_id"]

    cli_runner.invoke(
        main,
        common
        + [
            "start",
            "--task-prompt",
            "scoped two",
            "--workspace-strategy",
            "current_repo",
        ],
    )
    _cp2, doc2 = find_latest_checkpoint(repo)
    assert doc2["run_context"]["run_id"] != run_id_1

    scoped_path, scoped_doc = find_latest_checkpoint(repo, run_id=run_id_1)
    assert scoped_doc["run_context"]["run_id"] == run_id_1
    assert scoped_doc["task_prompt"] == "scoped one"


def test_disallowed_action_fails_before_run_flow(tmp_path: Path, cli_runner: CliRunner) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    common = ["--repo-root", str(repo)]
    cli_runner.invoke(
        main,
        common
        + [
            "start",
            "--task-prompt",
            "gate test",
            "--workspace-strategy",
            "current_repo",
        ],
    )
    merge = cli_runner.invoke(main, common + ["merge"])
    assert merge.exit_code != 0
    assert "not allowed" in merge.output.lower() or "Error:" in merge.output


def test_exec_argv_json_array_on_start(tmp_path: Path, cli_runner: CliRunner) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    result = cli_runner.invoke(
        main,
        [
            "--repo-root",
            str(repo),
            "start",
            "--task-prompt",
            "argv cli",
            "--workspace-strategy",
            "current_repo",
            "--exec-argv",
            _hermetic_start_argv_json(),
        ],
    )
    assert result.exit_code == 0, result.output
    cp = resolve_checkpoint_path(repo, checkpoint=None, run_id=None)
    doc = load_flow_checkpoint(cp)
    assert doc["dev_process"]["exec_policy_snapshot"]["default_argv"] == spec_argv()


def test_no_checkpoint_status_fails(tmp_path: Path, cli_runner: CliRunner) -> None:
    repo = tmp_path / "empty"
    repo.mkdir()
    git_repo_with_commit(repo)
    with pytest.raises(NodeExecutionFailure, match="no dev-process checkpoint"):
        find_latest_checkpoint(repo)
    status = cli_runner.invoke(main, ["--repo-root", str(repo), "status"])
    assert status.exit_code != 0
    assert "no dev-process checkpoint" in status.output.lower() or "Error:" in status.output


def test_explicit_checkpoint_wrong_run_id_fails(tmp_path: Path, cli_runner: CliRunner) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    common = ["--repo-root", str(repo)]
    cli_runner.invoke(
        main,
        common
        + [
            "start",
            "--task-prompt",
            "run id gate",
            "--workspace-strategy",
            "current_repo",
        ],
    )
    cp = resolve_checkpoint_path(repo, checkpoint=None, run_id=None)
    result = cli_runner.invoke(
        main,
        common + ["status", "--checkpoint", cp, "--run-id", "wrong-run-id"],
    )
    assert result.exit_code != 0
    assert "run_id mismatch" in result.output.lower() or "Error:" in result.output


def test_explicit_checkpoint_other_repo_fails(tmp_path: Path, cli_runner: CliRunner) -> None:
    repo_a = tmp_path / "repo_a"
    repo_b = tmp_path / "repo_b"
    repo_a.mkdir()
    repo_b.mkdir()
    git_repo_with_commit(repo_a)
    git_repo_with_commit(repo_b)
    cli_runner.invoke(
        main,
        [
            "--repo-root",
            str(repo_a),
            "start",
            "--task-prompt",
            "repo a",
            "--workspace-strategy",
            "current_repo",
        ],
    )
    cp_a = resolve_checkpoint_path(repo_a, checkpoint=None, run_id=None)
    result = cli_runner.invoke(
        main,
        ["--repo-root", str(repo_b), "status", "--checkpoint", cp_a],
    )
    assert result.exit_code != 0
    assert "repo_root does not match" in result.output.lower() or "Error:" in result.output


def test_run_id_substring_does_not_match_unrelated_run(
    tmp_path: Path, cli_runner: CliRunner
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    common = ["--repo-root", str(repo)]
    cli_runner.invoke(
        main,
        common
        + [
            "start",
            "--task-prompt",
            "prefix run",
            "--workspace-strategy",
            "current_repo",
        ],
    )
    with pytest.raises(NodeExecutionFailure, match="no dev-process checkpoint"):
        find_latest_checkpoint(repo, run_id="001")


def test_exec_argv_non_string_elements_fail(tmp_path: Path, cli_runner: CliRunner) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    result = cli_runner.invoke(
        main,
        [
            "--repo-root",
            str(repo),
            "start",
            "--task-prompt",
            "bad argv",
            "--workspace-strategy",
            "current_repo",
            "--exec-argv",
            "[1, 2, 3]",
        ],
    )
    assert result.exit_code != 0
    assert "array of strings" in result.output.lower() or "Error:" in result.output


def test_explicit_checkpoint_outside_runs_dir_fails(tmp_path: Path, cli_runner: CliRunner) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    cli_runner.invoke(
        main,
        [
            "--repo-root",
            str(repo),
            "start",
            "--task-prompt",
            "outside runs",
            "--workspace-strategy",
            "current_repo",
        ],
    )
    cp = resolve_checkpoint_path(repo, checkpoint=None, run_id=None)
    outside = tmp_path / "copied_checkpoint.json"
    outside.write_text(Path(cp).read_text(encoding="utf-8"), encoding="utf-8")
    result = cli_runner.invoke(
        main,
        ["--repo-root", str(repo), "status", "--checkpoint", str(outside)],
    )
    assert result.exit_code != 0
    assert "must be under" in result.output.lower() or "Error:" in result.output


def test_find_latest_skips_repo_mismatch_checkpoint(tmp_path: Path, cli_runner: CliRunner) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    cli_runner.invoke(
        main,
        [
            "--repo-root",
            str(repo),
            "start",
            "--task-prompt",
            "valid run",
            "--workspace-strategy",
            "current_repo",
        ],
    )
    good_path, good_doc = find_latest_checkpoint(repo)
    runs = repo / ".nodeflow" / "runs"
    bad_run = runs / "999_stale_mismatch"
    bad_cp_dir = bad_run / "checkpoints"
    bad_cp_dir.mkdir(parents=True)
    bad_cp = bad_cp_dir / "stale_flow.json"
    bad_doc = json.loads(json.dumps(good_doc))
    bad_doc["run_context"]["repo_root"] = str(tmp_path / "other_repo")
    bad_doc["flow_result"]["flow_checkpoint_path"] = str(bad_cp.resolve())
    bad_cp.write_text(json.dumps(bad_doc, indent=2), encoding="utf-8")

    latest_path, latest_doc = find_latest_checkpoint(repo)
    assert latest_path == good_path
    assert latest_doc["task_prompt"] == "valid run"


def test_explicit_checkpoint_self_reference_mismatch_fails(
    tmp_path: Path, cli_runner: CliRunner
) -> None:
    """Wrapper --checkpoint must pass load_flow_checkpoint self-reference guard."""
    repo = tmp_path / "repo"
    repo.mkdir()
    git_repo_with_commit(repo)
    cli_runner.invoke(
        main,
        [
            "--repo-root",
            str(repo),
            "start",
            "--task-prompt",
            "selfref wrapper",
            "--workspace-strategy",
            "current_repo",
        ],
    )
    cp = Path(resolve_checkpoint_path(repo, checkpoint=None, run_id=None))
    copied = cp.parent / "copied_flow.json"
    copied.write_text(cp.read_text(encoding="utf-8"), encoding="utf-8")
    result = cli_runner.invoke(
        main,
        ["--repo-root", str(repo), "status", "--checkpoint", str(copied)],
    )
    assert result.exit_code != 0
    assert "self-reference mismatch" in result.output.lower() or "Error:" in result.output
