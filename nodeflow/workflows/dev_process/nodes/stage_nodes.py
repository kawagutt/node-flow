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
            node_params.get("approved_plan") or _read_plan_text(artifact_root_from_body(body))
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
        repo = repo_root_from_ctx(ctx)
        dp = body.get("dev_process") if isinstance(body.get("dev_process"), dict) else {}
        phase_id = str(node_params.get("phase_id") or dp.get("current_phase_id") or "")
        artifact_root = artifact_root_from_body(body, phase=True)
        changed = list(node_params.get("changed_paths") or [])
        result = run_lint_fix_stage(
            repo_root=repo,
            changed_paths=changed,
            artifact_root=artifact_root,
            phase_id=phase_id,
        )
        evidence_paths = list(result.get("evidence_paths") or [])
        evidence_path = evidence_paths[0] if evidence_paths else ""
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
        repo = repo_root_from_ctx(ctx)
        artifact_root = artifact_root_from_body(body, phase=True)
        run_id = run_id_from_body(body)
        impl = (body.get("stages") or {}).get("implementation") or {}
        result = run_run_tests_stage(
            repo_root=repo,
            artifact_root=artifact_root,
            run_id=run_id,
            test_argv=node_params.get("test_argv"),
            diff_result=node_params.get("diff_result"),
            execution_output=impl.get("execution_output") or {},
        )
        cp_path = result.get("stage_checkpoint_path") or ""
        record_local_node_run(
            body,
            node_name=self.node_name,
            stage=self.stage_key,
            evidence_path=str(cp_path),
            argv=list(node_params.get("test_argv") or []),
        )
        body.setdefault("stages", {})["run_tests"] = result


def _review_bundle(body: dict[str, Any], node_params: dict[str, Any]) -> dict[str, Any]:
    bundle = node_params.get("review_bundle")
    if isinstance(bundle, dict):
        return bundle
    stages = body.get("stages") if isinstance(body.get("stages"), dict) else {}
    impl = stages.get("implementation") or {}
    run_tests = stages.get("run_tests") or {}
    lint = stages.get("lint_fix") or {}
    return {
        "diff_result": impl.get("diff_result") or node_params.get("diff_result") or {},
        "test_result": run_tests.get("test_result") or node_params.get("test_result") or {},
        "lint_result": lint or node_params.get("lint_result"),
        "approved_spec": node_params.get("approved_spec"),
        "approved_plan": node_params.get("approved_plan"),
        "base_revision": node_params.get("base_revision"),
    }


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
        review_inputs = body.get("_review_inputs")
        if not isinstance(review_inputs, dict):
            review_inputs = {}
        agents = active_review_agents(body, node_params)
        expected = [review_node_name(a) for a in agents]
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
        body.setdefault("stages", {})["review"] = {
            "status": "completed",
            "stage_checkpoint_path": cp_path,
            "stage_result": stage_result,
            "review_result": review_result,
            "aggregate": aggregate,
            "merge_ready": merge_ready,
            "review_depth_preset": preset,
            "stale": False,
        }


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
                record_skipped_node_run(
                    body,
                    node_name=self.node_name,
                    stage=self.stage_key,
                    skip_reason="inactive_review_agent",
                    artifact_root=artifact_root,
                )
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
            dp = body.get("dev_process") if isinstance(body.get("dev_process"), dict) else {}
            preset = str(
                node_params.get("review_depth_preset")
                or dp.get("review_depth_preset")
                or "standard"
            )
            base_rev = str(
                bundle.get("base_revision") or _run_context(body).get("source_base_revision") or ""
            )
            er, _ep = run_one_review_agent_stage(
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
