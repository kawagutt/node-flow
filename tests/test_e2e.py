"""
Tier 4: E2E — Script → LLM(mock) → Script。チェックリスト対応。
"""

from nodeflow.loader import load_pipeline
from nodeflow.nodes import LLMNode, PythonScriptNode
from nodeflow.pipeline_node import PipelineNode


def test_script_llm_script_in_memory(tmp_path):
    """Script → LLM(mock) → Script をコードで組み立てて実行。"""
    pre = tmp_path / "pre.py"
    pre.write_text(
        """
def main(inputs):
    data = inputs.get("raw_data", "")
    return {"prompt_text": "analyze: " + str(data)}
"""
    )
    post = tmp_path / "post.py"
    post.write_text(
        """
def main(inputs):
    r = inputs.get("data", "")
    return {"final": "result:" + str(r)}
"""
    )

    nodes = {
        "preprocess": PythonScriptNode(),
        "llm_call": LLMNode(),
        "postprocess": PythonScriptNode(),
    }
    # llm_call.prompt は preprocess.result.prompt_text、postprocess.data は llm_call.response.value
    node_input_bindings = {
        "preprocess": {"raw_data": ("inputs", "raw_data")},
        "llm_call": {"prompt": ("node", "preprocess", "result", "prompt_text")},
        "postprocess": {"data": ("node", "llm_call", "response", "value")},
    }
    node_param_definitions = {
        "preprocess": {"script": str(pre)},
        "llm_call": {},
        "postprocess": {"script": str(post)},
    }
    pipeline = PipelineNode(
        graph_node_order=["preprocess", "llm_call", "postprocess"],
        nodes=nodes,
        node_input_bindings=node_input_bindings,
        node_param_definitions=node_param_definitions,
        final_id="postprocess",
    )
    out = pipeline.execute({"raw_data": "hello"}, {})

    assert pipeline.read_status() == "done"
    for n in nodes.values():
        assert n.read_status() == "done"
    assert "result" in out
    assert "final" in out["result"]
    assert "result:" in out["result"]["final"]
    assert "mock:analyze: hello" in out["result"]["final"]
    assert "_meta" in out["result"]
    assert "revision" in out["result"]["_meta"]


def test_e2e_via_load_pipeline(tmp_path):
    """pipeline.yaml を load_pipeline で読み、Script → LLM(mock) → Script を実行。"""
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    scripts = workspace / "scripts"
    scripts.mkdir()

    pre = scripts / "preprocess.py"
    pre.write_text(
        """
def main(inputs):
    return {"prompt_text": "p:" + str(inputs.get("data", ""))}
"""
    )
    post = scripts / "postprocess.py"
    post.write_text(
        """
def main(inputs):
    return {"final": str(inputs.get("data", ""))}
"""
    )

    yaml_path = workspace / "pipeline.yaml"
    yaml_path.write_text(
        """
version: "1.4"
name: e2e

graph:
  nodes:
    - id: preprocess
      type: python_script
      inputs:
        data: ${inputs.raw_data}
      params:
        script: scripts/preprocess.py
    - id: llm_call
      type: llm
      inputs:
        prompt: ${preprocess.result.prompt_text}
      params: {}
    - id: postprocess
      type: python_script
      inputs:
        data: ${llm_call.response.value}
      params:
        script: scripts/postprocess.py
  final: postprocess
"""
    )

    root = load_pipeline(str(workspace), str(yaml_path))
    out = root.execute({"raw_data": "hello"}, {})

    assert root.read_status() == "done"
    assert "result" in out
    assert "final" in out["result"]
    assert "_meta" in out["result"]
    assert "revision" in out["result"]["_meta"]
