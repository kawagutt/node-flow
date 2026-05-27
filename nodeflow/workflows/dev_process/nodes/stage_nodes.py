"""dev_process leaf ActionNodes (one execution attempt = one node_runs entry)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Type

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.flow_context import _read_plan_text, _read_spec_text
from nodeflow.workflows.dev_process.node_runner import (
    record_local_node_run,
    record_skipped_node_run,
)
from nodeflow.workflows.dev_process.nodes._base import DevProcessLeafNode
from nodeflow.workflows.dev_process.nodes._ctx import (
    artifact_root_from_body,
    repo_root_from_ctx,
    review_artifact_root_from_ctx,
    run_id_from_body,
)
from nodeflow.workflows.dev_process.nodes._review import active_review_agents
from nodeflow.workflows.dev_process.review_config import review_node_name
from nodeflow.workflows.dev_process.stages.implementation import run_implementation_stage
from nodeflow.workflows.dev_process.stages.lint_fix import run_lint_fix_stage
from nodeflow.workflows.dev_process.stages.plan import run_plan_stage
from nodeflow.workflows.dev_process.stages.plan_review import run_plan_review_stage
from nodeflow.workflows.dev_process.stages.review_agent import run_one_review_agent_stage
from nodeflow.workflows.dev_process.stages.run_tests import run_run_tests_stage
from nodeflow.workflows.dev_process.stages.spec import run_spec_stage
from nodeflow.workflows.dev_process.stages.spec_review import run_spec_review_stage
from nodeflow.workflows.dev_process.stages.test_implementation import run_test_implementation_stage


def _run_context(body: dict[str, Any]) -> dict[str, Any]:
    rc = body.get("run_context")
    if not isinstance(rc, dict):
        raise NodeExecutionFailure("body.run_context required")
    return rc


class WriteSpecNode(DevProcessLeafNode):
    node_name = "write_spec"
    stage_key = "spec"

    def _execute(self, ctx, body, node_params, pipe_params, context) -> None:
        repo = repo_root_from_ctx(ctx)
        artifact_root = artifact_root_from_body(body)
        run_id = run_id_from_body(body)
        rc = _run_context(body)
        result = run_spec_stage(
            repo_root=repo,
            artifact_root=artifact_root,
            run_id=run_id,
            task_prompt=str(node_params.get("task_prompt") or body.get("task_prompt") or ""),
            base_revision=str(rc.get("source_base_revision") or ""),
            revision_context=node_params.get("revision_context"),
            notes=node_params.get("notes"),
            reference_materials=node_params.get("reference_materials"),
            previous_spec=node_params.get("previous_spec"),
            body=body,
        )
        body.setdefault("stages", {})["spec"] = result


class ReviewSpecNode(DevProcessLeafNode):
    node_name = "review_spec"
    stage_key = "spec_review"

    def _execute(self, ctx, body, node_params, pipe_params, context) -> None:
        repo = repo_root_from_ctx(ctx)
        artifact_root = artifact_root_from_body(body)
        run_id = run_id_from_body(body)
        spec_text = str(node_params.get("spec_text") or _read_spec_text(artifact_root))
        result = run_spec_review_stage(
            repo_root=repo,
            artifact_root=artifact_root,
            run_id=run_id,
            task_prompt=str(node_params.get("task_prompt") or body.get("task_prompt") or ""),
            spec_text=spec_text,
            body=body,
        )
        body.setdefault("stages", {})["spec_review"] = result


class WritePlanNode(DevProcessLeafNode):
    node_name = "write_plan"
    stage_key = "plan"

    def _execute(self, ctx, body, node_params, pipe_params, context) -> None:
        repo = repo_root_from_ctx(ctx)
        artifact_root = artifact_root_from_body(body)
        run_id = run_id_from_body(body)
        result = run_plan_stage(
            repo_root=repo,
            artifact_root=artifact_root,
            run_id=run_id,
            task_prompt=str(node_params.get("task_prompt") or body.get("task_prompt") or ""),
            approved_spec=str(node_params.get("approved_spec") or _read_spec_text(artifact_root)),
            revision_context=node_params.get("revision_context"),
            previous_plan=node_params.get("previous_plan"),
            body=body,
            completed_phases=node_params.get("completed_phases"),
            continuation_findings=node_params.get("continuation_findings"),
            continuation_start_index=node_params.get("continuation_start_index", 0),
            existing_plan=node_params.get("existing_plan"),
            existing_plan_text=node_params.get("existing_plan_text"),
            defer_plan_version_commit=node_params.get("defer_plan_version_commit", False),
        )
        body.setdefault("stages", {})["plan"] = result


class ReviewPlanNode(DevProcessLeafNode):
    node_name = "review_plan"
    stage_key = "plan_review"

    def _execute(self, ctx, body, node_params, pipe_params, context) -> None:
        repo = repo_root_from_ctx(ctx)
        artifact_root = artifact_root_from_body(body)
        run_id = run_id_from_body(body)
        result = run_plan_review_stage(
            repo_root=repo,
            artifact_root=artifact_root,
            run_id=run_id,
            task_prompt=str(node_params.get("task_prompt") or body.get("task_prompt") or ""),
            spec_text=str(node_params.get("spec_text") or _read_spec_text(artifact_root)),
            plan_text=str(node_params.get("plan_text") or _read_plan_text(artifact_root)),
            body=body,
        )
        body.setdefault("stages", {})["plan_review"] = result


class WriteImplementationNode(DevProcessLeafNode):
    node_name = "write_implementation"
    stage_key = "implementation"

    def _execute(self, ctx, body, node_params, pipe_params, context) -> None:
        dp = body.get("dev_process") if isinstance(body.get("dev_process"), dict) else {}
        phase_id = str(node_params.get("phase_id") or dp.get("current_phase_id") or "")
        if node_params.get("skip_implementation"):
            if phase_id:
                impl_cached = (body.get("stages") or {}).get("implementation") or {}
                impl_phase = impl_cached.get("phase_id", "")
                if impl_phase and impl_phase != phase_id:
                    raise NodeExecutionFailure(
                        f"skip_implementation: cached implementation is for {impl_phase!r}, "
                        f"not current phase {phase_id!r}; cannot skip safely"
                    )
            record_skipped_node_run(
                body,
                node_name=self.node_name,
                stage=self.stage_key,
                skip_reason="skip_implementation",
                artifact_root=artifact_root_from_body(body, phase=True),
            )
            return
        repo = repo_root_from_ctx(ctx)
        artifact_root = artifact_root_from_body(body, phase=True)
        run_id = run_id_from_body(body)
        base_rev = str(
            node_params.get("base_revision")
            or (dp.get("phase_results") or {}).get(phase_id, {}).get("phase_start_git_ref")
            or _run_context(body).get("source_base_revision")
            or ""
        )
        spec_text = str(
            node_params.get("approved_spec") or _read_spec_text(artifact_root_from_body(body))
        )
        plan_text = str(
            node_params.get("approved_plan") or _read_plan_text(artifact_root_from_body(body))
        )
        result = run_implementation_stage(
            repo_root=repo,
            artifact_root=artifact_root,
            run_id=run_id,
            task_prompt=str(node_params.get("task_prompt") or body.get("task_prompt") or ""),
            base_revision=base_rev,
            approved_spec=spec_text,
            approved_plan=plan_text,
            rework_context=node_params.get("rework_context"),
            body=body,
        )
        if phase_id:
            result["phase_id"] = phase_id
        body.setdefault("stages", {})["implementation"] = result


class WriteTestsNode(DevProcessLeafNode):
    node_name = "write_tests"
    stage_key = "test_implementation"

    def _execute(self, ctx, body, node_params, pipe_params, context) -> None:
        repo = repo_root_from_ctx(ctx)
        artifact_root = artifact_root_from_body(body, phase=True)
        run_id = run_id_from_body(body)
        spec_text = str(
            node_params.get("approved_spec") or _read_spec_text(artifact_root_from_body(body))
        )
        plan_text = str(
            node_params.get("phase_plan_text")
            or node_params.get("approved_plan")
            or _read_plan_text(artifact_root_from_body(body))
        )
        result = run_test_implementation_stage(
            repo_root=repo,
            artifact_root=artifact_root,
            run_id=run_id,
            approved_spec=spec_text,
            approved_plan=plan_text,
            rework_context=node_params.get("rework_context"),
            body=body,
        )
        body.setdefault("stages", {})["test_implementation"] = result


class LintFixNode(DevProcessLeafNode):
    node_name = "lint_fix"
    stage_key = "lint_fix"

    def _execute(self, ctx, body, node_params, pipe_params, context) -> None:
        from nodeflow.workflows.dev_process.phase_git import collect_phase_changed_paths

        repo = repo_root_from_ctx(ctx)
        dp = body.get("dev_process") if isinstance(body.get("dev_process"), dict) else {}
        phase_id = str(node_params.get("phase_id") or dp.get("current_phase_id") or "")
        artifact_root = artifact_root_from_body(body, phase=True)
        run_artifact_root = artifact_root_from_body(body, phase=False)
        override = node_params.get("changed_paths")
        if isinstance(override, list) and override:
            changed = list(override)
        else:
            changed = collect_phase_changed_paths(
                repo,
                artifact_roots=[run_artifact_root, artifact_root],
            )
        result = run_lint_fix_stage(
            repo_root=repo,
            changed_paths=changed,
            artifact_root=artifact_root,
            phase_id=phase_id,
        )
        evidence_paths = list(result.get("evidence_paths") or [])
        if not evidence_paths:
            ev_dir = Path(artifact_root) / "evidence"
            ev_dir.mkdir(parents=True, exist_ok=True)
            evidence_json = ev_dir / "lint_fix.json"
            evidence_json.write_text(json.dumps(result, indent=2), encoding="utf-8")
            evidence_paths = [str(evidence_json)]
        evidence_path = evidence_paths[0]
        record_local_node_run(
            body,
            node_name=self.node_name,
            stage=self.stage_key,
            evidence_path=evidence_path,
            argv=["ruff", "check", "--fix"],
        )
        body.setdefault("stages", {})["lint_fix"] = result


class RunTestsNode(DevProcessLeafNode):
    node_name = "run_tests"
    stage_key = "run_tests"

    def _execute(self, ctx, body, node_params, pipe_params, context) -> None:
        from nodeflow.workflows.dev_process.reuse import collect_diff

        repo = repo_root_from_ctx(ctx)
        artifact_root = artifact_root_from_body(body, phase=True)
        run_id = run_id_from_body(body)
        impl = (body.get("stages") or {}).get("implementation") or {}
        dp = body.get("dev_process") if isinstance(body.get("dev_process"), dict) else {}
        phase_id = str(node_params.get("phase_id") or dp.get("current_phase_id") or "")
        base_rev = str(
            node_params.get("base_revision")
            or (dp.get("phase_results") or {}).get(phase_id, {}).get("phase_start_git_ref")
            or _run_context(body).get("source_base_revision")
            or ""
        )
        override_diff = node_params.get("diff_result")
        if isinstance(override_diff, dict) and override_diff:
            pre_test_diff = override_diff
        else:
            pre_test_diff = collect_diff(repo_root=repo, base_revision=base_rev)
        raw_argv = node_params.get("test_argv")
        test_argv = raw_argv if isinstance(raw_argv, list) and raw_argv else None
        result = run_run_tests_stage(
            repo_root=repo,
            artifact_root=artifact_root,
            run_id=run_id,
            test_argv=test_argv,
            diff_result=pre_test_diff,
            execution_output=impl.get("execution_output") or {},
        )
        post_test_diff = collect_diff(repo_root=repo, base_revision=base_rev)
        result = dict(result)
        result["diff_result"] = post_test_diff
        result["pre_test_diff_result"] = pre_test_diff
        cp_path = result.get("stage_checkpoint_path") or ""
        evidence_path = str(cp_path) if cp_path else ""
        if not evidence_path:
            ev_dir = Path(artifact_root) / "evidence"
            ev_dir.mkdir(parents=True, exist_ok=True)
            evidence_json = ev_dir / "run_tests.json"
            evidence_json.write_text(
                json.dumps({"test_result": result.get("test_result")}, indent=2),
                encoding="utf-8",
            )
            evidence_path = str(evidence_json)
        record_local_node_run(
            body,
            node_name=self.node_name,
            stage=self.stage_key,
            evidence_path=evidence_path,
            argv=list(test_argv or []),
        )
        body.setdefault("stages", {})["run_tests"] = result


def _record_review_evidence_path(body: dict[str, Any], node_name: str, evidence_path: str) -> None:
    if not isinstance(evidence_path, str) or not evidence_path.strip():
        return
    store = body.setdefault("_review_evidence_paths", {})
    if not isinstance(store, dict):
        store = {}
        body["_review_evidence_paths"] = store
    store[node_name] = evidence_path.strip()


def _review_evidence_paths_for_segment(
    body: dict[str, Any], expected_node_names: list[str]
) -> list[str]:
    """Collect review evidence paths recorded during the current review segment only."""
    raw = body.get("_review_evidence_paths")
    if not isinstance(raw, dict):
        return []
    paths: list[str] = []
    for name in expected_node_names:
        ep = raw.get(name)
        if isinstance(ep, str) and ep.strip():
            paths.append(ep.strip())
    return paths


def _review_bundle(body: dict[str, Any], node_params: dict[str, Any]) -> dict[str, Any]:
    bundle = node_params.get("review_bundle")
    if isinstance(bundle, dict):
        return bundle
    stages = body.get("stages") if isinstance(body.get("stages"), dict) else {}
    impl = stages.get("implementation") or {}
    run_tests = stages.get("run_tests") or {}
    lint = stages.get("lint_fix") or {}
    return {
        "diff_result": (
            node_params.get("diff_result")
            or run_tests.get("diff_result")
            or impl.get("diff_result")
            or {}
        ),
        "test_result": run_tests.get("test_result") or node_params.get("test_result") or {},
        "lint_result": lint or node_params.get("lint_result"),
        "approved_spec": node_params.get("approved_spec"),
        "approved_plan": node_params.get("approved_plan"),
        "base_revision": node_params.get("base_revision"),
    }


def _lint_result_for_prompt(lint_result: dict[str, Any]) -> dict[str, Any]:
    """Compact lint payload for review prompts (avoid dumping large logs)."""
    summary: dict[str, Any] = {}
    if "lint_fix" in lint_result:
        summary["lint_fix"] = lint_result["lint_fix"]
    paths = lint_result.get("evidence_paths")
    if isinstance(paths, list):
        summary["evidence_paths"] = paths[:10]
        if len(paths) > 10:
            summary["evidence_paths_truncated_count"] = len(paths)
    for key in ("stderr_tail", "stdout_tail", "ruff_exit_code"):
        if key in lint_result:
            summary[key] = lint_result[key]
    return summary or lint_result


def _augment_review_plan(
    plan_text: str,
    node_params: dict[str, Any],
    lint_result: dict[str, Any] | None,
) -> str:
    supplement: list[str] = []

    targets = node_params.get("review_targets")
    if isinstance(targets, list) and targets:
        supplement.append("Review targets: " + ", ".join(str(x) for x in targets))

    checklist = node_params.get("review_checklist")
    if isinstance(checklist, list) and checklist:
        supplement.append("Review checklist:")
        supplement.extend(f"- {x}" for x in checklist)

    criteria = node_params.get("review_acceptance_criteria")
    if isinstance(criteria, list) and criteria:
        supplement.append("Review acceptance criteria:")
        supplement.extend(f"- {x}" for x in criteria)

    scope = node_params.get("review_scope")
    if isinstance(scope, str) and scope.strip():
        supplement.append(f"Review scope: {scope.strip()}")

    if isinstance(lint_result, dict) and lint_result:
        supplement.append("Lint result:")
        supplement.append(
            json.dumps(_lint_result_for_prompt(lint_result), ensure_ascii=False, indent=2)
        )

    if not supplement:
        return plan_text
    return plan_text + "\n\n---\n" + "\n".join(supplement)


class ReviewAggregateNode(DevProcessLeafNode):
    node_name = "review_aggregate"
    stage_key = "review"

    def _execute(self, ctx, body, node_params, pipe_params, context) -> None:
        from nodeflow.workflows.dev_process.paths import assert_path_under_run_dir
        from nodeflow.workflows.dev_process.reuse import aggregate_reviews, write_stage_checkpoint
        from nodeflow.workflows.dev_process.review_presets import normalize_preset
        from nodeflow.workflows.dev_process.synthesis import assign_owners_to_findings

        repo = repo_root_from_ctx(ctx)
        artifact_root = review_artifact_root_from_ctx(ctx, body)
        run_id = run_id_from_body(body)
        bundle = _review_bundle(body, node_params)
        agents = active_review_agents(body, node_params)
        expected = [review_node_name(a) for a in agents]
        raw_inputs = body.get("_review_inputs")
        if not isinstance(raw_inputs, dict):
            raw_inputs = {}
        expected_set = set(expected)
        review_inputs = {k: v for k, v in raw_inputs.items() if k in expected_set}
        preset = normalize_preset(
            str(
                node_params.get("review_depth_preset")
                or (body.get("dev_process") or {}).get("review_depth_preset")
                or "standard"
            )
        )
        review_result, checkpoint_request = aggregate_reviews(
            review_inputs=review_inputs,
            test_result=bundle.get("test_result") or {},
            diff_result=bundle.get("diff_result") or {},
            expected_review_keys=expected,
        )
        blocking = assign_owners_to_findings(list(review_result.get("blocking_findings") or []))
        review_result = dict(review_result)
        review_result["blocking_findings"] = blocking

        stage_result = write_stage_checkpoint(
            request=checkpoint_request,
            checkpoint_dir=str(Path(artifact_root) / "review"),
            run_id=run_id,
            stage="review",
            repo_root=repo,
            extra_inputs={"review_result": review_result},
        )
        cp_path = None
        for art in stage_result.get("artifacts") or []:
            if isinstance(art, dict) and art.get("kind") == "checkpoint":
                cp_path = art.get("path")
                break
        if cp_path:
            assert_path_under_run_dir(artifact_root, cp_path)

        aggregate_path = Path(artifact_root) / "review" / "aggregate.json"
        aggregate_path.parent.mkdir(parents=True, exist_ok=True)
        blocking_count = len(blocking)
        aggregate = {
            "blocking_count": blocking_count,
            "decision": review_result.get("decision"),
            "spec_revision_needed": review_result.get("spec_revision_needed"),
        }
        dp = body.get("dev_process") if isinstance(body.get("dev_process"), dict) else {}
        if dp:
            from nodeflow.workflows.dev_process.artifact_versions import review_aggregate_metadata

            scope = str(node_params.get("review_scope") or dp.get("review_scope") or "")
            aggregate.update(review_aggregate_metadata(dp, review_scope=scope))
        aggregate_path.write_text(json.dumps(aggregate, indent=2), encoding="utf-8")

        record_local_node_run(
            body,
            node_name=self.node_name,
            stage=self.stage_key,
            evidence_path=str(aggregate_path),
            kind="aggregate",
        )

        merge_ready = not blocking and review_result.get("decision") == "merge_ok"
        evidence_paths = _review_evidence_paths_for_segment(body, expected)
        body.setdefault("stages", {})["review"] = {
            "status": "completed",
            "stage_checkpoint_path": cp_path,
            "stage_result": stage_result,
            "review_result": review_result,
            "aggregate": aggregate,
            "merge_ready": merge_ready,
            "review_depth_preset": preset,
            "stale": False,
            "evidence_paths": evidence_paths,
        }
        # Avoid stale reviewer payloads leaking into later segments/phases.
        body["_review_inputs"] = {}
        body["_review_evidence_paths"] = {}


def _make_review_agent_node(agent: str) -> Type[DevProcessLeafNode]:
    agent_node_name = review_node_name(agent)

    class _ReviewAgentNode(DevProcessLeafNode):
        node_name = agent_node_name
        stage_key = "review"
        _agent = agent

        def _execute(self, ctx, body, node_params, pipe_params, context) -> None:
            agents = active_review_agents(body, node_params)
            artifact_root = review_artifact_root_from_ctx(ctx, body)
            if self._agent not in agents:
                skipped = record_skipped_node_run(
                    body,
                    node_name=self.node_name,
                    stage=self.stage_key,
                    skip_reason="inactive_review_agent",
                    artifact_root=artifact_root,
                )
                _record_review_evidence_path(body, self.node_name, skipped.evidence_path)
                return
            repo = repo_root_from_ctx(ctx)
            run_id = run_id_from_body(body)
            bundle = _review_bundle(body, node_params)
            spec_text = str(
                bundle.get("approved_spec") or _read_spec_text(artifact_root_from_body(body))
            )
            plan_text = str(
                bundle.get("approved_plan") or _read_plan_text(artifact_root_from_body(body))
            )
            plan_text = _augment_review_plan(
                plan_text,
                node_params,
                bundle.get("lint_result") if isinstance(bundle.get("lint_result"), dict) else None,
            )
            dp = body.get("dev_process") if isinstance(body.get("dev_process"), dict) else {}
            preset = str(
                node_params.get("review_depth_preset")
                or dp.get("review_depth_preset")
                or "standard"
            )
            base_rev = str(
                bundle.get("base_revision") or _run_context(body).get("source_base_revision") or ""
            )
            er, evidence_path = run_one_review_agent_stage(
                agent=self._agent,
                body=body,
                repo_root=repo,
                artifact_root=artifact_root,
                run_id=run_id,
                base_revision=base_rev,
                approved_spec=spec_text,
                approved_plan=plan_text,
                diff_result=bundle.get("diff_result") or {},
                test_result=bundle.get("test_result") or {},
                review_depth_preset=preset,
            )
            _record_review_evidence_path(body, agent_node_name, evidence_path)
            inputs = body.setdefault("_review_inputs", {})
            if not isinstance(inputs, dict):
                inputs = {}
                body["_review_inputs"] = inputs
            inputs[agent_node_name] = er

    _ReviewAgentNode.__name__ = f"DevProcess{''.join(p.title() for p in agent.split('_'))}Node"
    _ReviewAgentNode.__qualname__ = _ReviewAgentNode.__name__
    return _ReviewAgentNode


STAGE_NODE_CLASSES: List[Type[DevProcessLeafNode]] = [
    WriteSpecNode,
    ReviewSpecNode,
    WritePlanNode,
    ReviewPlanNode,
    WriteImplementationNode,
    WriteTestsNode,
    LintFixNode,
    RunTestsNode,
    ReviewAggregateNode,
]

STAGE_NODE_CLASSES.extend(
    _make_review_agent_node(agent)
    for agent in (
        "requirements",
        "architecture",
        "test_quality",
        "checklist_compliance",
        "impact",
        "diff_detail",
        "naming_doc",
    )
)

STAGE_NODE_REGISTRY: Dict[str, Type[DevProcessLeafNode]] = {
    cls.node_name: cls for cls in STAGE_NODE_CLASSES
}
