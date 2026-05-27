"""Tests for dev-process constraints module."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

import pytest

from nodeflow.workflows.dev_process.constraints import (
    format_constraints_for_prompt,
    generate_agents_md,
    resolve_constraint_defs,
    resolve_constraints,
)
from nodeflow.workflows.dev_process.exec_policy import (
    build_exec_policy_snapshot,
    default_argv_for_worker,
    load_exec_policy_file,
)


class TestResolveConstraints:
    def test_empty_snapshot_returns_empty(self) -> None:
        assert resolve_constraints({}) == []

    def test_global_constraints_returned(self) -> None:
        snap = {"constraints": ["NO_GIT_PUSH", "NO_TOUCH_EXISTING_UNTRACKED"]}
        assert resolve_constraints(snap) == ["NO_GIT_PUSH", "NO_TOUCH_EXISTING_UNTRACKED"]

    def test_node_constraints_appended(self) -> None:
        snap = {
            "constraints": ["NO_GIT_PUSH"],
            "nodes": {"write_implementation": {"constraints": ["EDIT_TARGET_ONLY"]}},
        }
        result = resolve_constraints(snap, "write_implementation")
        assert result == ["NO_GIT_PUSH", "EDIT_TARGET_ONLY"]

    def test_deduplication(self) -> None:
        snap = {
            "constraints": ["NO_GIT_PUSH", "EDIT_TARGET_ONLY"],
            "nodes": {"write_spec": {"constraints": ["NO_GIT_PUSH"]}},
        }
        result = resolve_constraints(snap, "write_spec")
        assert result == ["NO_GIT_PUSH", "EDIT_TARGET_ONLY"]

    def test_node_without_constraints(self) -> None:
        snap = {
            "constraints": ["NO_GIT_PUSH"],
            "nodes": {"write_spec": {"worker": "codex"}},
        }
        result = resolve_constraints(snap, "write_spec")
        assert result == ["NO_GIT_PUSH"]

    def test_no_node_name_returns_global_only(self) -> None:
        snap = {
            "constraints": ["NO_GIT_PUSH"],
            "nodes": {"write_spec": {"constraints": ["EDIT_TARGET_ONLY"]}},
        }
        result = resolve_constraints(snap)
        assert result == ["NO_GIT_PUSH"]


class TestResolveConstraintDefs:
    def test_builtin_def_resolved(self) -> None:
        defs = resolve_constraint_defs(["NO_GIT_PUSH"], {})
        assert "NO_GIT_PUSH" in defs
        assert "push" in defs["NO_GIT_PUSH"].lower()

    def test_project_def_overrides_builtin(self) -> None:
        snap = {"constraint_defs": {"NO_GIT_PUSH": "Custom: no push ever."}}
        defs = resolve_constraint_defs(["NO_GIT_PUSH"], snap)
        assert defs["NO_GIT_PUSH"] == "Custom: no push ever."

    def test_unknown_id_gets_placeholder(self) -> None:
        defs = resolve_constraint_defs(["CUSTOM_RULE"], {})
        assert "unknown" in defs["CUSTOM_RULE"].lower()

    def test_project_custom_id(self) -> None:
        snap = {"constraint_defs": {"MY_RULE": "Do something specific."}}
        defs = resolve_constraint_defs(["MY_RULE"], snap)
        assert defs["MY_RULE"] == "Do something specific."


class TestFormatConstraintsForPrompt:
    def test_empty_returns_empty(self) -> None:
        assert format_constraints_for_prompt([], {}) == ""

    def test_contains_constraint_ids(self) -> None:
        snap = {"constraints": ["NO_GIT_PUSH", "NO_TOUCH_EXISTING_UNTRACKED"]}
        text = format_constraints_for_prompt(["NO_GIT_PUSH", "NO_TOUCH_EXISTING_UNTRACKED"], snap)
        assert "NO_GIT_PUSH" in text
        assert "NO_TOUCH_EXISTING_UNTRACKED" in text
        assert "MUST" in text

    def test_should_severity_shown(self) -> None:
        text = format_constraints_for_prompt(["BRANCH_BEFORE_WORK"], {})
        assert "SHOULD" in text

    def test_full_text_included_not_truncated(self) -> None:
        """Ensure multi-sentence constraint text is NOT truncated to first sentence."""
        snap = {"constraints": ["NO_GIT_PUSH"]}
        text = format_constraints_for_prompt(["NO_GIT_PUSH"], snap)
        assert "prompt-enforced only" in text


class TestGenerateAgentsMd:
    def test_empty_ids(self) -> None:
        md = generate_agents_md([], {})
        assert "No constraints configured" in md

    def test_must_section(self) -> None:
        md = generate_agents_md(["NO_GIT_PUSH"], {})
        assert "### MUST" in md
        assert "#### NO_GIT_PUSH" in md
        assert "push" in md.lower()

    def test_should_section(self) -> None:
        md = generate_agents_md(["BRANCH_BEFORE_WORK"], {})
        assert "### SHOULD" in md
        assert "#### BRANCH_BEFORE_WORK" in md

    def test_mixed_sections(self) -> None:
        md = generate_agents_md(["NO_GIT_PUSH", "BRANCH_BEFORE_WORK"], {})
        assert "### MUST" in md
        assert "### SHOULD" in md

    def test_project_defs_used(self) -> None:
        snap = {"constraint_defs": {"CUSTOM": "Do not use deprecated APIs."}}
        md = generate_agents_md(["CUSTOM"], snap)
        assert "deprecated APIs" in md

    def test_per_node_section_for_review(self) -> None:
        snap: Dict[str, Any] = {"constraints": ["NO_GIT_PUSH"]}
        md = generate_agents_md(["NO_GIT_PUSH"], snap)
        assert "## Per-node constraints" in md
        assert "review_spec" in md
        assert "READ_ONLY_NODE" in md

    def test_per_node_from_policy(self) -> None:
        snap: Dict[str, Any] = {
            "constraints": ["NO_GIT_PUSH"],
            "nodes": {"write_implementation": {"constraints": ["EDIT_TARGET_ONLY"]}},
        }
        md = generate_agents_md(["NO_GIT_PUSH"], snap)
        assert "write_implementation" in md
        assert "EDIT_TARGET_ONLY" in md


class TestDefaultArgvForWorker:
    def test_codex_returns_configured(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from nodeflow.workflows.dev_process import exec_policy

        monkeypatch.setattr(
            exec_policy,
            "WORKER_DEFAULT_ARGV",
            {"codex": ["codex", "exec", "--sandbox", "workspace-write"]},
        )
        argv = default_argv_for_worker("codex")
        assert argv == ["codex", "exec", "--sandbox", "workspace-write"]

    def test_unconfigured_worker_raises(self) -> None:
        from nodeflow.core.base_node import NodeExecutionFailure

        with pytest.raises(NodeExecutionFailure, match="no exec_argv configured"):
            default_argv_for_worker("unknown_worker")

    def test_production_empty_default_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from nodeflow.core.base_node import NodeExecutionFailure
        from nodeflow.workflows.dev_process import exec_policy

        monkeypatch.setattr(exec_policy, "WORKER_DEFAULT_ARGV", {})
        with pytest.raises(NodeExecutionFailure, match="no exec_argv configured"):
            default_argv_for_worker("codex")


class TestExecPolicySnapshotConstraints:
    def test_constraints_included_in_snapshot(self) -> None:
        overrides = {
            "constraints": ["NO_GIT_PUSH", "NO_TOUCH_EXISTING_UNTRACKED"],
            "constraint_defs": {"CUSTOM": "Custom rule."},
        }
        snap = build_exec_policy_snapshot(exec_policy_overrides=overrides)
        assert snap["constraints"] == ["NO_GIT_PUSH", "NO_TOUCH_EXISTING_UNTRACKED"]
        assert snap["constraint_defs"]["CUSTOM"] == "Custom rule."

    def test_no_overrides_no_constraints(self) -> None:
        snap = build_exec_policy_snapshot()
        assert "constraints" not in snap

    def test_strict_constraints_stored(self) -> None:
        overrides = {
            "constraints": ["NO_GIT_PUSH"],
            "strict_constraints": True,
        }
        snap = build_exec_policy_snapshot(exec_policy_overrides=overrides)
        assert snap["strict_constraints"] is True

    def test_strict_unknown_id_raises(self) -> None:
        from nodeflow.core.base_node import NodeExecutionFailure

        with pytest.raises(NodeExecutionFailure, match="unknown constraint"):
            load_exec_policy_file_content(
                {
                    "constraints": ["NONEXISTENT_RULE"],
                    "strict_constraints": True,
                }
            )


class TestExecPolicyValidation:
    def test_constraints_must_be_list(self, tmp_path: Path) -> None:
        from nodeflow.core.base_node import NodeExecutionFailure

        p = tmp_path / "bad.json"
        p.write_text(json.dumps({"constraints": "not a list"}))
        with pytest.raises(NodeExecutionFailure, match="must be a JSON array"):
            load_exec_policy_file(str(p))

    def test_constraint_defs_must_be_dict(self, tmp_path: Path) -> None:
        from nodeflow.core.base_node import NodeExecutionFailure

        p = tmp_path / "bad.json"
        p.write_text(json.dumps({"constraint_defs": ["not", "a", "dict"]}))
        with pytest.raises(NodeExecutionFailure, match="must be a JSON object"):
            load_exec_policy_file(str(p))

    def test_valid_constraints_pass(self, tmp_path: Path) -> None:
        p = tmp_path / "ok.json"
        p.write_text(
            json.dumps(
                {
                    "constraints": ["NO_GIT_PUSH"],
                    "constraint_defs": {"PROJ": "project rule"},
                }
            )
        )
        result = load_exec_policy_file(str(p))
        assert result["constraints"] == ["NO_GIT_PUSH"]


class TestWorkerDefaultArgv:
    pass


class TestReviewNodeImplicitConstraints:
    def test_review_spec_gets_read_only(self) -> None:
        snap: Dict[str, Any] = {"constraints": ["NO_GIT_PUSH"]}
        ids = resolve_constraints(snap, "review_spec")
        assert "READ_ONLY_NODE" in ids
        assert "NO_GIT_PUSH" in ids

    def test_review_requirements_gets_read_only(self) -> None:
        snap: Dict[str, Any] = {}
        ids = resolve_constraints(snap, "review_requirements")
        assert "READ_ONLY_NODE" in ids

    def test_write_node_no_implicit_read_only(self) -> None:
        snap: Dict[str, Any] = {"constraints": ["NO_GIT_PUSH"]}
        ids = resolve_constraints(snap, "write_implementation")
        assert "READ_ONLY_NODE" not in ids

    def test_no_duplicates_when_explicitly_set(self) -> None:
        snap: Dict[str, Any] = {
            "constraints": ["READ_ONLY_NODE"],
        }
        ids = resolve_constraints(snap, "review_spec")
        assert ids.count("READ_ONLY_NODE") == 1


class TestPerNodeConstraintsValidation:
    def test_per_node_constraints_not_list_raises(self) -> None:
        from nodeflow.core.base_node import NodeExecutionFailure

        content = {
            "nodes": {
                "write_implementation": {
                    "constraints": "NOT_A_LIST",
                }
            }
        }
        with pytest.raises(NodeExecutionFailure, match="must be a list"):
            load_exec_policy_file_content(content)

    def test_per_node_constraints_empty_string_raises(self) -> None:
        from nodeflow.core.base_node import NodeExecutionFailure

        content = {
            "nodes": {
                "write_implementation": {
                    "constraints": [""],
                }
            }
        }
        with pytest.raises(NodeExecutionFailure, match="non-empty string"):
            load_exec_policy_file_content(content)

    def test_per_node_strict_unknown_raises(self) -> None:
        from nodeflow.core.base_node import NodeExecutionFailure

        content = {
            "strict_constraints": True,
            "nodes": {
                "write_implementation": {
                    "constraints": ["TOTALLY_MADE_UP"],
                }
            },
        }
        with pytest.raises(NodeExecutionFailure, match="unknown constraint"):
            load_exec_policy_file_content(content)

    def test_per_node_valid_constraints_pass(self) -> None:
        content = {
            "nodes": {
                "write_implementation": {
                    "constraints": ["NO_GIT_PUSH", "EDIT_TARGET_ONLY"],
                }
            }
        }
        load_exec_policy_file_content(content)


class TestMutualExclusivePolicy:
    def test_path_and_inline_overrides_raises(self, tmp_path: Path) -> None:
        from nodeflow.core.base_node import NodeExecutionFailure
        from nodeflow.workflows.dev_process.flow_actions import run_flow

        policy_file = tmp_path / "policy.json"
        policy_file.write_text(json.dumps({"default_argv": ["echo", "hi"]}))

        with pytest.raises(NodeExecutionFailure, match="mutually exclusive"):
            run_flow(
                action="start",
                repo_root=tmp_path,
                task_prompt="test",
                exec_policy_path=str(policy_file),
                exec_policy_overrides={"default_argv": ["echo", "hi"]},
            )


class TestNoImplicitWorkerExec:
    def test_start_without_exec_argv_fails_noninteractive(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import subprocess

        from nodeflow.core.base_node import NodeExecutionFailure
        from nodeflow.workflows.dev_process import exec_policy

        subprocess.run(["git", "init"], cwd=str(tmp_path), check=True, capture_output=True)
        subprocess.run(
            ["git", "commit", "--allow-empty", "-m", "init"],
            cwd=str(tmp_path),
            check=True,
            capture_output=True,
        )
        monkeypatch.setattr(exec_policy, "WORKER_DEFAULT_ARGV", {})

        from nodeflow.workflows.dev_process.flow_actions import run_flow

        with pytest.raises(NodeExecutionFailure, match="no exec_argv configured"):
            run_flow(
                action="start",
                repo_root=tmp_path,
                task_prompt="test",
                interactive=False,
            )


class TestConstraintsAudit:
    """Verify _write_constraints_audit generates audit file with proper sections."""

    def test_audit_created_with_sections(self, tmp_path: Path) -> None:
        from nodeflow.workflows.dev_process.flow_actions import _write_constraints_audit

        body: Dict[str, Any] = {
            "run_context": {"artifact_root": str(tmp_path / "artifacts")},
            "dev_process": {
                "exec_policy_snapshot": {
                    "constraints": ["NO_GIT_PUSH"],
                },
            },
        }
        result = _write_constraints_audit(body)
        assert result is not None

        audit_file = tmp_path / "artifacts" / "agent_context" / "constraints_audit.md"
        assert audit_file.is_file()
        content = audit_file.read_text()
        assert "## Global constraints" in content
        assert "NO_GIT_PUSH" in content
        assert "## Effective constraints by node" in content
        assert "review_requirements" in content
        assert "READ_ONLY_NODE" in content

    def test_audit_separates_global_from_per_node(self, tmp_path: Path) -> None:
        from nodeflow.workflows.dev_process.flow_actions import _write_constraints_audit

        body: Dict[str, Any] = {
            "run_context": {"artifact_root": str(tmp_path / "artifacts")},
            "dev_process": {
                "exec_policy_snapshot": {
                    "constraints": ["NO_GIT_PUSH"],
                    "nodes": {"write_implementation": {"constraints": ["EDIT_TARGET_ONLY"]}},
                },
            },
        }
        _write_constraints_audit(body)
        content = (tmp_path / "artifacts" / "agent_context" / "constraints_audit.md").read_text()
        assert "## Global constraints" in content
        assert "## Effective constraints by node" in content
        assert "### write_implementation" in content
        assert "EDIT_TARGET_ONLY" in content
        assert "### review_requirements" in content

    def test_no_snapshot_returns_none(self, tmp_path: Path) -> None:
        from nodeflow.workflows.dev_process.flow_actions import _write_constraints_audit

        body: Dict[str, Any] = {
            "run_context": {"artifact_root": str(tmp_path / "artifacts")},
            "dev_process": {},
        }
        result = _write_constraints_audit(body)
        assert result is None


class TestValidationArtifact:
    """Verify _write_validation_artifact writes constraint validation results."""

    def test_success_artifact(self, tmp_path: Path) -> None:
        from nodeflow.workflows.dev_process.node_runner import _write_validation_artifact

        result = {"ok": True, "constraints": ["NO_GIT_PUSH"]}
        path = _write_validation_artifact(str(tmp_path), "write_spec", result)
        content = json.loads(Path(path).read_text())
        assert content["ok"] is True
        assert content["node_name"] == "write_spec"
        assert "validated_at" in content

    def test_failure_artifact(self, tmp_path: Path) -> None:
        from nodeflow.workflows.dev_process.node_runner import _write_validation_artifact

        result = {
            "ok": False,
            "violated": "READ_ONLY_NODE",
            "message": "repo changed",
            "constraints": ["READ_ONLY_NODE"],
        }
        path = _write_validation_artifact(str(tmp_path), "review_requirements", result)
        content = json.loads(Path(path).read_text())
        assert content["ok"] is False
        assert content["violated"] == "READ_ONLY_NODE"
        assert content["node_name"] == "review_requirements"


class TestPerNodeCodexHome:
    """Verify per-node CODEX_HOME/AGENTS.md generation in node_runner."""

    def test_write_node_codex_home_basic(self, tmp_path: Path) -> None:
        from nodeflow.workflows.dev_process.node_runner import _write_node_codex_home

        snapshot: Dict[str, Any] = {"constraints": ["NO_GIT_PUSH"]}
        codex_home, sha = _write_node_codex_home(
            artifact_root=str(tmp_path),
            node_name="write_implementation",
            constraint_ids=["NO_GIT_PUSH"],
            snapshot=snapshot,
        )
        agents_file = Path(codex_home) / "AGENTS.md"
        assert agents_file.is_file()
        content = agents_file.read_text()
        assert "Dev-process task instructions" in content
        assert "NO_GIT_PUSH" in content
        assert "READ_ONLY_NODE" not in content
        assert len(sha) == 64

    def test_review_node_gets_read_only(self, tmp_path: Path) -> None:
        from nodeflow.workflows.dev_process.node_runner import _write_node_codex_home

        snapshot: Dict[str, Any] = {"constraints": ["NO_GIT_PUSH"]}
        codex_home, _ = _write_node_codex_home(
            artifact_root=str(tmp_path),
            node_name="review_requirements",
            constraint_ids=["NO_GIT_PUSH", "READ_ONLY_NODE"],
            snapshot=snapshot,
        )
        content = (Path(codex_home) / "AGENTS.md").read_text()
        assert "READ_ONLY_NODE" in content
        assert "NO_GIT_PUSH" in content

    def test_per_node_isolation(self, tmp_path: Path) -> None:
        """write_implementation and review_requirements get different AGENTS.md files."""
        from nodeflow.workflows.dev_process.node_runner import _write_node_codex_home

        snapshot: Dict[str, Any] = {"constraints": ["NO_GIT_PUSH"]}

        impl_home, _ = _write_node_codex_home(
            artifact_root=str(tmp_path),
            node_name="write_implementation",
            constraint_ids=["NO_GIT_PUSH"],
            snapshot=snapshot,
        )
        review_home, _ = _write_node_codex_home(
            artifact_root=str(tmp_path),
            node_name="review_requirements",
            constraint_ids=["NO_GIT_PUSH", "READ_ONLY_NODE"],
            snapshot=snapshot,
        )

        impl_content = (Path(impl_home) / "AGENTS.md").read_text()
        review_content = (Path(review_home) / "AGENTS.md").read_text()

        assert "READ_ONLY_NODE" not in impl_content
        assert "READ_ONLY_NODE" in review_content
        assert impl_home != review_home

    def test_auth_files_linked(self, tmp_path: Path, monkeypatch: "pytest.MonkeyPatch") -> None:
        """_write_node_codex_home symlinks auth files from the default CODEX_HOME."""
        from nodeflow.workflows.dev_process.node_runner import _write_node_codex_home

        fake_codex_home = tmp_path / "fake_codex"
        fake_codex_home.mkdir()
        (fake_codex_home / "auth.json").write_text('{"token":"t"}')
        (fake_codex_home / "config.toml").write_text("[general]")
        monkeypatch.setenv("CODEX_HOME", str(fake_codex_home))

        artifact_root = tmp_path / "artifacts"
        codex_home, _ = _write_node_codex_home(
            artifact_root=str(artifact_root),
            node_name="spec_review",
            constraint_ids=["READ_ONLY_NODE"],
            snapshot={"constraints": ["READ_ONLY_NODE"]},
        )
        ch = Path(codex_home)
        assert (ch / "AGENTS.md").is_file()
        assert (ch / "auth.json").exists()
        assert (ch / "config.toml").exists()
        assert (ch / "auth.json").read_text() == '{"token":"t"}'

    def test_auth_missing_is_not_fatal(
        self, tmp_path: Path, monkeypatch: "pytest.MonkeyPatch"
    ) -> None:
        """No error when default CODEX_HOME has no auth files."""
        from nodeflow.workflows.dev_process.node_runner import _write_node_codex_home

        empty_codex_home = tmp_path / "empty_codex"
        empty_codex_home.mkdir()
        monkeypatch.setenv("CODEX_HOME", str(empty_codex_home))

        codex_home, _ = _write_node_codex_home(
            artifact_root=str(tmp_path / "art"),
            node_name="spec_review",
            constraint_ids=["READ_ONLY_NODE"],
            snapshot={"constraints": ["READ_ONLY_NODE"]},
        )
        assert (Path(codex_home) / "AGENTS.md").is_file()
        assert not (Path(codex_home) / "auth.json").exists()


def load_exec_policy_file_content(content: dict) -> dict:
    """Helper to validate policy content inline (simulates _validate_policy_overrides)."""
    from nodeflow.workflows.dev_process.exec_policy import _validate_policy_overrides

    _validate_policy_overrides(content)
    return content
