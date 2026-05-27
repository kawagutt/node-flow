"""PR5: exec_policy model/session applied via worker_adapter before subprocess exec."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.argv_builder import resolve_node_exec
from nodeflow.workflows.dev_process.exec_policy import (
    WORKER_DEFAULT_ARGV,
    apply_snapshot_to_body,
    build_exec_policy_snapshot,
)
from nodeflow.workflows.dev_process.node_runner import run_node_exec
from nodeflow.workflows.dev_process.review_agent_model import PROFILE_CODE_MAIN
from nodeflow.workflows.dev_process.worker_adapter import (
    prepare_worker_argv,
    resolve_worker_model,
)
from tests.workflows.dev_process.hermetic_argv import model_probe_argv


def _argv_model(argv: list[str]) -> str | None:
    i = 0
    while i < len(argv):
        if argv[i] in ("--model", "-m") and i + 1 < len(argv):
            return argv[i + 1]
        i += 1
    return None


def test_resolve_worker_model_maps_profiles() -> None:
    assert resolve_worker_model(PROFILE_CODE_MAIN) == "gpt-5.3-codex-high-fast"
    assert resolve_worker_model("gpt-4.1-custom") == "gpt-4.1-custom"


def test_prepare_worker_argv_injects_codex_model() -> None:
    base = ["codex", "exec", "--sandbox", "workspace-write"]
    argv, model = prepare_worker_argv("codex", base, model="gpt-5.5-medium")
    assert model == "gpt-5.5-medium"
    assert _argv_model(argv) == "gpt-5.5-medium"


def test_prepare_worker_argv_replaces_existing_model_flag() -> None:
    base = ["codex", "exec", "--model", "old-model", "--", "cat"]
    argv, model = prepare_worker_argv("codex", base, model="new-model")
    assert model == "new-model"
    assert _argv_model(argv) == "new-model"
    assert "old-model" not in argv


def test_prepare_worker_argv_codex_resume() -> None:
    base = ["codex", "exec", "--sandbox", "workspace-write"]
    argv, _ = prepare_worker_argv(
        "codex",
        base,
        model=None,
        provider_session_id="sess-abc-123",
    )
    assert argv == [
        "codex",
        "exec",
        "--sandbox",
        "workspace-write",
        "resume",
        "sess-abc-123",
    ]


def test_prepare_worker_argv_replaces_existing_model_equals_flag() -> None:
    base = ["codex", "exec", "--model=old-model", "--", "cat"]
    argv, model = prepare_worker_argv("codex", base, model="new-model")
    assert model == "new-model"
    assert "--model=old-model" not in argv
    assert _argv_model(argv) == "new-model"


def test_prepare_worker_argv_codex_resume_and_model_order() -> None:
    """Exec flags before ``resume`` subcommand (Codex CLI verified)."""
    base = ["codex", "exec", "--sandbox", "workspace-write"]
    argv, model = prepare_worker_argv(
        "codex",
        base,
        model="model-x",
        provider_session_id="sess-1",
    )
    assert model == "model-x"
    assert argv == [
        "codex",
        "exec",
        "--model",
        "model-x",
        "--sandbox",
        "workspace-write",
        "resume",
        "sess-1",
    ]


def test_prepare_worker_argv_matches_codex_cli_resume_help_shape() -> None:
    """Shape accepted by ``codex exec resume --help`` (manual smoke 2026-05-27)."""
    argv, model = prepare_worker_argv(
        "codex",
        ["codex", "exec", "--sandbox", "read-only"],
        model="gpt-5.5-medium",
        provider_session_id="00000000-0000-0000-0000-000000000001",
    )
    assert model == "gpt-5.5-medium"
    assert argv == [
        "codex",
        "exec",
        "--model",
        "gpt-5.5-medium",
        "--sandbox",
        "read-only",
        "resume",
        "00000000-0000-0000-0000-000000000001",
    ]


def test_record_exec_evidence_provider_session_fields(tmp_path: Path) -> None:
    from nodeflow.workflows.dev_process.evidence import record_exec_evidence

    art = tmp_path / "artifacts"
    art.mkdir()
    execution_output = {
        "ok": True,
        "stdout": "ok",
        "stderr": "",
        "provider": "codex",
        "external_executor": "codex",
        "provider_meta": {"session_id": "applied-sess-88"},
        "raw_output": {"returncode": 0},
    }
    ep = record_exec_evidence(
        execution_output=execution_output,
        stage="spec",
        invoker="codex_exec",
        prompt="p",
        cwd=str(tmp_path),
        run_id="run-pr5-sess",
        artifact_root=str(art),
        session_id="logical-1",
        node_name="write_spec",
        model="model-z",
        worker="codex",
        argv=[
            "codex",
            "exec",
            "--model",
            "model-z",
            "resume",
            "requested-sess-99",
        ],
        provider_session_id_requested="requested-sess-99",
        provider_session_mode="resume",
    )
    doc = json.loads(Path(ep).read_text(encoding="utf-8"))
    assert doc["provider_session_id_requested"] == "requested-sess-99"
    assert doc["provider_session_mode"] == "resume"
    assert doc["provider_session_id_applied"] == "applied-sess-88"
    assert doc["provider_session_id"] == "applied-sess-88"


def test_run_node_exec_passes_provider_session_to_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: dict = {}

    def _fake_run_exec(worker, **kwargs):
        captured["argv"] = list(kwargs["argv"])
        return {
            "ok": True,
            "stdout": "ok",
            "stderr": "",
            "provider": "codex",
            "external_executor": "codex",
            "provider_meta": {"session_id": "applied-from-worker"},
            "raw_output": {"returncode": 0},
        }

    monkeypatch.setattr(
        "nodeflow.workflows.dev_process.node_runner.run_exec",
        _fake_run_exec,
    )
    monkeypatch.setitem(WORKER_DEFAULT_ARGV, "codex", model_probe_argv())

    body: dict = {
        "schema_version": "dev_process.flow.v3",
        "dev_process": {},
        "run_context": {"artifact_root": str(tmp_path / "artifacts")},
    }
    snapshot = build_exec_policy_snapshot(
        exec_argv=model_probe_argv(),
        exec_policy_overrides={
            "nodes": {
                "write_spec": {
                    "model": "model-z",
                    "provider_session_id": "requested-sess-99",
                }
            }
        },
    )
    apply_snapshot_to_body(body, snapshot)

    _, _, record = run_node_exec(
        body,
        node_name="write_spec",
        stage="spec",
        prompt="p",
        cwd=str(tmp_path),
        run_id="run-pr5-sess",
        artifact_root=str(tmp_path / "artifacts"),
    )
    assert "resume" in captured["argv"]
    assert "requested-sess-99" in captured["argv"]
    doc = json.loads(Path(record.evidence_path).read_text(encoding="utf-8"))
    assert doc["provider_session_id_requested"] == "requested-sess-99"
    assert doc["provider_session_mode"] == "resume"
    assert doc["provider_session_id_applied"] == "applied-from-worker"


def test_per_node_model_changes_subprocess_argv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Changing exec_policy.nodes.<name>.model must change argv passed to the worker."""
    monkeypatch.setitem(WORKER_DEFAULT_ARGV, "codex", model_probe_argv())
    body: dict = {
        "schema_version": "dev_process.flow.v3",
        "dev_process": {},
        "run_context": {"artifact_root": str(tmp_path / "artifacts")},
    }
    snapshot = build_exec_policy_snapshot(
        exec_argv=model_probe_argv(),
        exec_policy_overrides={
            "nodes": {
                "write_spec": {"model": "model-alpha"},
                "review_spec": {"model": "model-beta"},
            }
        },
    )
    apply_snapshot_to_body(body, snapshot)

    _, _, record_a = run_node_exec(
        body,
        node_name="write_spec",
        stage="spec",
        prompt="p",
        cwd=str(tmp_path),
        run_id="run-pr5",
        artifact_root=str(tmp_path / "artifacts"),
    )
    _, _, record_b = run_node_exec(
        body,
        node_name="review_spec",
        stage="spec_review",
        prompt="p",
        cwd=str(tmp_path),
        run_id="run-pr5",
        artifact_root=str(tmp_path / "artifacts"),
    )

    assert _argv_model(record_a.argv) == "model-alpha"
    assert _argv_model(record_b.argv) == "model-beta"
    assert record_a.model == "model-alpha"
    assert record_b.model == "model-beta"

    out_a = json.loads(Path(record_a.evidence_path).read_text(encoding="utf-8"))
    assert out_a.get("model") == "model-alpha"
    assert out_a.get("argv") == record_a.argv


def test_resolve_node_exec_then_adapter_end_to_end(tmp_path: Path) -> None:
    body: dict = {
        "dev_process": {
            "exec_policy_snapshot": {
                "default_worker": "codex",
                "default_argv": model_probe_argv(),
                "nodes": {
                    "write_plan": {"model": "plan-model-x"},
                },
            }
        }
    }
    worker, model, argv = resolve_node_exec(body, "write_plan")
    argv2, model2 = prepare_worker_argv(worker, argv, model=model)
    assert model2 == "plan-model-x"
    assert _argv_model(argv2) == "plan-model-x"


def test_unsupported_worker_rejects_model_injection() -> None:
    with pytest.raises(NodeExecutionFailure, match="does not support"):
        prepare_worker_argv("unknown-worker", ["echo"], model="m")


def test_is_codex_exec_argv_ignores_exec_after_double_dash() -> None:
    from nodeflow.workflows.dev_process.worker_adapter import _is_codex_exec_argv

    assert not _is_codex_exec_argv(["codex", "--", "exec", "--model", "user"])
    assert _is_codex_exec_argv(["python", "-c", "x", "codex", "exec", "--", "probe"])
    assert not _is_codex_exec_argv(["wrapper", "--", "codex", "exec", "--model", "user"])


def test_codex_exec_after_top_level_double_dash_is_not_injection_target() -> None:
    base = ["wrapper", "--", "codex", "exec", "--model", "user_arg"]
    argv, model = prepare_worker_argv("codex", base, model="policy-model")
    assert argv == base
    assert model == "policy-model"


def test_provider_session_id_rejects_codex_exec_after_top_level_double_dash() -> None:
    with pytest.raises(NodeExecutionFailure, match="provider_session_id requires"):
        prepare_worker_argv(
            "codex",
            ["wrapper", "--", "codex", "exec"],
            model=None,
            provider_session_id="sess-1",
        )


def test_prepare_worker_argv_passthrough_exec_not_codex_exec() -> None:
    """`exec` after `--` must not trigger codex exec injection."""
    base = ["codex", "--", "exec", "--model", "user_arg"]
    argv, model = prepare_worker_argv("codex", base, model="policy-model")
    assert model == "policy-model"
    assert argv == base
    assert _argv_model(argv) == "user_arg"


def test_provider_session_id_requires_codex_exec_argv() -> None:
    with pytest.raises(NodeExecutionFailure, match="provider_session_id requires"):
        prepare_worker_argv(
            "codex",
            ["echo", "hello"],
            model=None,
            provider_session_id="sess-1",
        )


def test_prepare_worker_argv_preserves_passthrough_model_flag() -> None:
    base = ["codex", "exec", "--", "python", "app.py", "--model", "user_arg"]
    argv, model = prepare_worker_argv("codex", base, model="codex-model")
    assert model == "codex-model"
    assert _argv_model(argv) == "codex-model"
    assert argv[-2:] == ["--model", "user_arg"]
    assert "user_arg" in argv


def test_prepare_worker_argv_replaces_resume_anywhere_in_option_zone() -> None:
    base = [
        "codex",
        "exec",
        "--model",
        "old",
        "resume",
        "old-session",
        "--sandbox",
        "workspace-write",
    ]
    argv, model = prepare_worker_argv(
        "codex",
        base,
        model="new",
        provider_session_id="new-session",
    )
    assert model == "new"
    assert argv.count("resume") == 1
    assert "new-session" in argv
    assert "old-session" not in argv
    assert argv == [
        "codex",
        "exec",
        "--model",
        "new",
        "--sandbox",
        "workspace-write",
        "resume",
        "new-session",
    ]


def test_prepare_worker_argv_option_zone_and_passthrough_together() -> None:
    base = [
        "codex",
        "exec",
        "--model=old",
        "resume",
        "old-session",
        "--",
        "cmd",
        "--model",
        "user_arg",
    ]
    argv, model = prepare_worker_argv(
        "codex",
        base,
        model="new-model",
        provider_session_id="new-session",
    )
    assert model == "new-model"
    assert _argv_model(argv) == "new-model"
    assert "--model=old" not in argv
    assert "old-session" not in argv
    assert argv.count("resume") == 1
    sep = argv.index("--")
    assert argv[sep:] == ["--", "cmd", "--model", "user_arg"]
