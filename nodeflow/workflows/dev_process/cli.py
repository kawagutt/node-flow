"""P7 thin CLI wrapper for dev-process — no duplicated state machine or git ops."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, Optional

import click

from nodeflow.cli import _parse_cli_value
from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.checkpoint import load_flow_checkpoint
from nodeflow.workflows.dev_process.constants import (
    ACTION_APPROVE_FINAL,
    ACTION_APPROVE_SPEC,
    ACTION_MERGE,
    ACTION_REVISE_SPEC,
    ACTION_REWORK,
    ACTION_START,
    EXEC_WORKER_CODEX,
    MERGE_POLICY_RECORD_ONLY,
    WORKSPACE_STRATEGY_GIT_WORKTREE,
)
from nodeflow.workflows.dev_process.discovery import (
    checkpoint_status,
    resolve_checkpoint_path,
)
from nodeflow.workflows.dev_process.flow_runner import run_flow
from nodeflow.workflows.dev_process.paths import resolve_git_toplevel


def _resolve_repo_root(repo_root: str) -> Path:
    return resolve_git_toplevel(Path(repo_root).resolve())


def _parse_exec_argv(raw: Optional[str]) -> Optional[list[str]]:
    if raw is None:
        return None
    argv = _parse_cli_value(raw)
    if not isinstance(argv, list) or not all(isinstance(x, str) for x in argv):
        raise click.ClickException("--exec-argv must be a JSON array of strings")
    return argv


def _assert_action_allowed(doc: Dict[str, Any], action: str) -> None:
    fr = doc.get("flow_result") if isinstance(doc.get("flow_result"), dict) else {}
    allowed = fr.get("allowed_actions")
    if not isinstance(allowed, list):
        raise NodeExecutionFailure("checkpoint missing flow_result.allowed_actions")
    if action not in allowed:
        state = fr.get("state")
        raise NodeExecutionFailure(
            f"action {action!r} is not allowed in state {state!r}; "
            f"allowed_actions={allowed!r}. "
            f"Use `nodeflow-dev-process status` to inspect the latest checkpoint."
        )


def _artifact_paths(out: Dict[str, Any]) -> Dict[str, Optional[str]]:
    rc = out.get("run_context") if isinstance(out.get("run_context"), dict) else {}
    artifact_root = str(rc.get("artifact_root") or "")
    if not artifact_root:
        return {"artifact_root": None, "timeline_path": None, "summary_path": None}
    art = Path(artifact_root)
    summary_path = None
    summary_dir = art / "summary"
    if summary_dir.is_dir():
        summaries = sorted(summary_dir.glob("*_development_summary.json"))
        if summaries:
            summary_path = str(summaries[-1].resolve())
    return {
        "artifact_root": str(art.resolve()),
        "timeline_path": str((art / "timeline.jsonl").resolve()),
        "summary_path": summary_path,
    }


def _format_result(out: Dict[str, Any]) -> str:
    fr = out.get("flow_result") if isinstance(out.get("flow_result"), dict) else {}
    paths = _artifact_paths(out)
    cp = fr.get("flow_checkpoint_path") or out.get("flow_checkpoint_path")
    lines = [
        f"state: {fr.get('state')}",
        f"ok: {fr.get('ok')}",
        f"allowed_actions: {json.dumps(fr.get('allowed_actions') or [], ensure_ascii=False)}",
        f"next_action: {fr.get('next_action')}",
        f"merge_ready: {fr.get('merge_ready')}",
        f"flow_checkpoint_path: {cp}",
        f"artifact_root: {paths['artifact_root']}",
        f"timeline: {paths['timeline_path']}",
    ]
    if paths.get("summary_path"):
        lines.append(f"summary: {paths['summary_path']}")
    if isinstance(out.get("merge_result"), dict) and out["merge_result"]:
        lines.append(f"merge_policy: {out['merge_result'].get('policy')}")
    return "\n".join(lines)


def _emit_result(out: Dict[str, Any], *, as_json: bool) -> None:
    if as_json:
        click.echo(json.dumps(out, indent=2, ensure_ascii=False))
    else:
        click.echo(_format_result(out))


def _run_resume_action(
    *,
    repo_root: Path,
    action: str,
    checkpoint: Optional[str],
    run_id: Optional[str],
    as_json: bool,
    task_prompt: str = "",
    human_comment_text: str = "",
    **run_flow_kwargs: Any,
) -> None:
    try:
        cp_path = resolve_checkpoint_path(repo_root, checkpoint=checkpoint, run_id=run_id)
        doc = load_flow_checkpoint(cp_path)
        _assert_action_allowed(doc, action)
        out = run_flow(
            action=action,
            repo_root=str(repo_root),
            flow_checkpoint_path=cp_path,
            run_id=run_id,
            task_prompt=task_prompt,
            human_comment_text=human_comment_text,
            **run_flow_kwargs,
        )
    except NodeExecutionFailure as e:
        raise click.ClickException(str(e)) from e
    _emit_result(out, as_json=as_json)


@click.group()
@click.option(
    "--repo-root",
    type=click.Path(exists=True, file_okay=False),
    default=".",
    help="Target git repository (dev-process artifact host).",
)
@click.option("--json", "as_json", is_flag=True, help="Print raw flow output JSON.")
@click.pass_context
def main(ctx: click.Context, repo_root: str, as_json: bool) -> None:
    """Thin wrapper around dev_process.flow — discovers checkpoints, calls run_flow."""
    ctx.ensure_object(dict)
    try:
        ctx.obj["repo_root"] = _resolve_repo_root(repo_root)
    except NodeExecutionFailure as e:
        raise click.ClickException(str(e)) from e
    ctx.obj["as_json"] = as_json


@main.command("start")
@click.option("--task-prompt", required=True, help="Task description for spec_plan.")
@click.option(
    "--workspace-strategy",
    default=WORKSPACE_STRATEGY_GIT_WORKTREE,
    show_default=True,
    type=click.Choice(["current_repo", "git_worktree"]),
)
@click.option(
    "--merge-policy",
    default=MERGE_POLICY_RECORD_ONLY,
    show_default=True,
    type=click.Choice(["record_only", "git_merge_branch"]),
)
@click.option("--exec-worker-kind", default=EXEC_WORKER_CODEX, show_default=True)
@click.option(
    "--exec-argv",
    default=None,
    help='JSON array, e.g. \'["codex","exec","--sandbox","workspace-write"]\'',
)
@click.option("--run-id", default=None, help="Optional explicit run_id for a new run.")
@click.pass_context
def cmd_start(
    ctx: click.Context,
    task_prompt: str,
    workspace_strategy: str,
    merge_policy: str,
    exec_worker_kind: str,
    exec_argv: Optional[str],
    run_id: Optional[str],
) -> None:
    """Start a new dev-process run (spec_plan on start)."""
    repo_root: Path = ctx.obj["repo_root"]
    argv = _parse_exec_argv(exec_argv)
    try:
        out = run_flow(
            action=ACTION_START,
            repo_root=str(repo_root),
            task_prompt=task_prompt,
            run_id=run_id,
            workspace_strategy=workspace_strategy,
            merge_policy=merge_policy,
            exec_worker_kind=exec_worker_kind,
            exec_argv=argv,
        )
    except NodeExecutionFailure as e:
        raise click.ClickException(str(e)) from e
    _emit_result(out, as_json=ctx.obj["as_json"])


@main.command("status")
@click.option("--checkpoint", default=None, help="Explicit flow checkpoint path.")
@click.option("--run-id", default=None, help="Limit discovery to a run_id.")
@click.pass_context
def cmd_status(ctx: click.Context, checkpoint: Optional[str], run_id: Optional[str]) -> None:
    """Show current state and artifact paths from the latest (or selected) checkpoint."""
    repo_root: Path = ctx.obj["repo_root"]
    try:
        cp_path = resolve_checkpoint_path(repo_root, checkpoint=checkpoint, run_id=run_id)
        doc = load_flow_checkpoint(cp_path)
        status = checkpoint_status(doc, checkpoint_path=cp_path)
    except NodeExecutionFailure as e:
        raise click.ClickException(str(e)) from e

    if ctx.obj["as_json"]:
        click.echo(json.dumps(status, indent=2, ensure_ascii=False))
    else:
        lines = [
            f"state: {status['state']}",
            f"ok: {status['ok']}",
            f"allowed_actions: {json.dumps(status['allowed_actions'], ensure_ascii=False)}",
            f"next_action: {status['next_action']}",
            f"merge_ready: {status['merge_ready']}",
            f"run_id: {status['run_id']}",
            f"flow_checkpoint_path: {status['flow_checkpoint_path']}",
            f"artifact_root: {status['artifact_root']}",
            f"timeline: {status['timeline_path']}",
        ]
        if status.get("summary_path"):
            lines.append(f"summary: {status['summary_path']}")
        else:
            lines.append("summary: <not written yet>")
        click.echo("\n".join(lines))


@main.command("approve-spec")
@click.option("--checkpoint", default=None)
@click.option("--run-id", default=None)
@click.pass_context
def cmd_approve_spec(ctx: click.Context, checkpoint: Optional[str], run_id: Optional[str]) -> None:
    """Run implement + review after human spec approval."""
    _run_resume_action(
        repo_root=ctx.obj["repo_root"],
        action=ACTION_APPROVE_SPEC,
        checkpoint=checkpoint,
        run_id=run_id,
        as_json=ctx.obj["as_json"],
    )


@main.command("rework")
@click.option("--checkpoint", default=None)
@click.option("--run-id", default=None)
@click.pass_context
def cmd_rework(ctx: click.Context, checkpoint: Optional[str], run_id: Optional[str]) -> None:
    """Re-run implement + review in the same worktree."""
    _run_resume_action(
        repo_root=ctx.obj["repo_root"],
        action=ACTION_REWORK,
        checkpoint=checkpoint,
        run_id=run_id,
        as_json=ctx.obj["as_json"],
    )


@main.command("revise-spec")
@click.option("--task-prompt", default="", help="Revision comment / updated task prompt.")
@click.option("--checkpoint", default=None)
@click.option("--run-id", default=None)
@click.pass_context
def cmd_revise_spec(
    ctx: click.Context,
    task_prompt: str,
    checkpoint: Optional[str],
    run_id: Optional[str],
) -> None:
    """Revise spec/plan (new attempt when using git_worktree)."""
    _run_resume_action(
        repo_root=ctx.obj["repo_root"],
        action=ACTION_REVISE_SPEC,
        checkpoint=checkpoint,
        run_id=run_id,
        as_json=ctx.obj["as_json"],
        task_prompt=task_prompt,
    )


@main.command("approve-final")
@click.option("--checkpoint", default=None)
@click.option("--run-id", default=None)
@click.pass_context
def cmd_approve_final(ctx: click.Context, checkpoint: Optional[str], run_id: Optional[str]) -> None:
    """Human final approval before merge."""
    _run_resume_action(
        repo_root=ctx.obj["repo_root"],
        action=ACTION_APPROVE_FINAL,
        checkpoint=checkpoint,
        run_id=run_id,
        as_json=ctx.obj["as_json"],
    )


@main.command("merge")
@click.option("--checkpoint", default=None)
@click.option("--run-id", default=None)
@click.pass_context
def cmd_merge(ctx: click.Context, checkpoint: Optional[str], run_id: Optional[str]) -> None:
    """Execute merge policy (record_only or git_merge_branch)."""
    _run_resume_action(
        repo_root=ctx.obj["repo_root"],
        action=ACTION_MERGE,
        checkpoint=checkpoint,
        run_id=run_id,
        as_json=ctx.obj["as_json"],
    )


if __name__ == "__main__":
    try:
        main(prog_name="nodeflow-dev-process")
    except click.ClickException as e:
        click.echo(f"Error: {e.message}", err=True)
        sys.exit(1)
