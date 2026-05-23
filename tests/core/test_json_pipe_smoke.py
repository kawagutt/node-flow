"""Minimal JSON graph → load_pipeline / object load → Runner → pipe output (v1.7)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from nodeflow.core.loader import load_pipe_spec_from_json_object, load_pipeline
from nodeflow.core.registry import NodeRegistry
from nodeflow.core.runner import Runner
from tests.core.fixtures import SmokeCopyNode


def _run_until_outputs(runner: Runner, *, max_steps: int = 100) -> None:
    for _ in range(max_steps):
        if runner.all_pipe_outputs_filled():
            return
        if not runner.step():
            break
    if not runner.all_pipe_outputs_filled():
        pytest.fail("runner stalled before pipe outputs filled")


def _write_v17_copy_node(ws: Path) -> None:
    p = ws / "copy_def.json"
    p.write_text(
        json.dumps(
            {
                "kind": "node",
                "version": "1.7",
                "type": "nf_test_copy",
                "input_ports": ["in"],
                "output_ports": ["out"],
            }
        ),
        encoding="utf-8",
    )


def test_minimal_json_pipe_load_runner_smoke(tmp_path) -> None:
    reg = NodeRegistry()
    reg.register("nf_test_copy", SmokeCopyNode)
    ws = tmp_path / "ws"
    ws.mkdir()
    _write_v17_copy_node(ws)
    data = {
        "kind": "pipe",
        "version": "1.7",
        "pipe": {"outputs": {"echo": "n.out"}},
        "nodes": [
            {
                "id": "n",
                "path": "copy_def.json",
                "inputs": {"in": "input.payload"},
            },
        ],
    }
    spec = load_pipe_spec_from_json_object(data, reg=reg, workspace_dir=ws)
    runner = Runner.from_pipe_spec(spec, pipe_inputs={"payload": {"answer": 42}})
    _run_until_outputs(runner)
    assert runner.filled_pipe_outputs() == {"echo": {"answer": 42}}


def test_minimal_json_file_load_pipeline_smoke(tmp_path) -> None:
    reg = NodeRegistry()
    reg.register("nf_test_copy", SmokeCopyNode)
    ws = tmp_path / "ws"
    ws.mkdir()
    _write_v17_copy_node(ws)
    p = ws / "minimal.json"
    p.write_text(
        json.dumps(
            {
                "kind": "pipe",
                "version": "1.7",
                "pipe": {"outputs": {"echo": "copy.out"}},
                "nodes": [
                    {
                        "id": "copy",
                        "path": "copy_def.json",
                        "inputs": {"in": "input.payload"},
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    spec = load_pipeline(str(ws), str(p.name), reg=reg)
    runner = Runner.from_pipe_spec(spec, pipe_inputs={"payload": {"k": "v"}})
    _run_until_outputs(runner)
    assert runner.filled_pipe_outputs()["echo"] == {"k": "v"}


def test_examples_pipes_hello_json_load_and_run() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    spec = load_pipeline(str(repo_root), "examples/pipes/hello.json")
    runner = Runner.from_pipe_spec(spec, pipe_inputs={"incoming": {}})
    _run_until_outputs(runner)
    assert runner.filled_pipe_outputs() == {"greeting": {"data": "Hello from NodeFlow!"}}


def test_examples_pipes_hello_json_without_input_stays_not_done() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    spec = load_pipeline(str(repo_root), "examples/pipes/hello.json")
    runner = Runner.from_pipe_spec(spec, pipe_inputs={})
    assert runner.step() is False
    assert not runner.all_pipe_outputs_filled()


def test_smoke_copy_without_pipe_payload_does_not_fill_output(tmp_path) -> None:
    reg = NodeRegistry()
    reg.register("nf_test_copy", SmokeCopyNode)
    ws = tmp_path / "ws"
    ws.mkdir()
    _write_v17_copy_node(ws)
    data = {
        "kind": "pipe",
        "version": "1.7",
        "pipe": {"outputs": {"echo": "n.out"}},
        "nodes": [
            {
                "id": "n",
                "path": "copy_def.json",
                "inputs": {"in": "input.payload"},
            },
        ],
    }
    spec = load_pipe_spec_from_json_object(data, reg=reg, workspace_dir=ws)
    runner = Runner.from_pipe_spec(spec, pipe_inputs={})
    assert runner.step() is False
    assert not runner.all_pipe_outputs_filled()
