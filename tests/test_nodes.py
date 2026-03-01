"""
Tier 2: PythonScriptNode / LLMNode (mock) テスト。
"""



from nodeflow.nodes import LLMNode, PythonScriptNode


def test_python_script_returns_dict(tmp_path):
    script = tmp_path / "main.py"
    script.write_text(
        """
def main(inputs):
    return {"value": inputs.get("x", 0)}
"""
    )
    node = PythonScriptNode()
    out = node.execute({"x": 42}, {"script": str(script)})
    assert node.read_status() == "done"
    assert "result" in out
    assert out["result"]["value"] == 42


def test_python_script_raises_fatal(tmp_path):
    script = tmp_path / "fail.py"
    script.write_text(
        """
def main(inputs):
    raise RuntimeError("script error")
"""
    )
    node = PythonScriptNode()
    out = node.execute({}, {"script": str(script)})
    assert node.read_status() == "fatal"
    assert out == {}
    assert "script error" in str(node.read_error())


def test_python_script_non_dict_return_fatal(tmp_path):
    script = tmp_path / "bad.py"
    script.write_text(
        """
def main(inputs):
    return "not a dict"
"""
    )
    node = PythonScriptNode()
    out = node.execute({}, {"script": str(script)})
    assert node.read_status() == "fatal"
    assert out == {}
    assert isinstance(node.read_error(), TypeError)


def test_llm_node_mock_returns_response():
    node = LLMNode()
    out = node.execute({"prompt": "hello"}, {})
    assert node.read_status() == "done"
    assert "response" in out
    assert out["response"] == "mock:hello"


def test_llm_node_mock_empty_prompt():
    node = LLMNode()
    out = node.execute({}, {})
    assert node.read_status() == "done"
    assert out["response"] == "mock:"


def test_llm_max_tokens_limit():
    """post-limit では result を返す。status=limit になる。
    注: 現状 response は string のため _attach_revision は dict port にしか revision を付与せず、
    out2 に _meta.revision は付かない（既知のギャップ）。string port 対応後に assert _meta/revision を追加する。
    """
    node = LLMNode()
    params = {"limit": {"max_tokens": 20}}

    # 1 回目: "abc" -> prompt_tokens=3, completion="mock:abc" -> completion_tokens=8, total=11
    out1 = node.execute({"prompt": "abc"}, params)
    assert node.read_status() == "done"
    assert "response" in out1
    assert out1["response"] == "mock:abc"

    node.reset_status()

    # 2 回目: 累積 11+11=22 >= 20 → post-limit。result は返す
    out2 = node.execute({"prompt": "abc"}, params)
    assert node.read_status() == "limit"
    assert out2 != {}
    assert "response" in out2
    assert out2["response"] == "mock:abc"


def test_reset_token_limit_state():
    """token limit で limit になった後、reset_limit_state('tokens') で再実行可能。"""
    node = LLMNode()
    params = {"limit": {"max_tokens": 10}}

    node.execute(
        {"prompt": "ab"}, params
    )  # total_tokens = len("ab") + len("mock:ab") = 2 + 7 = 9
    node.reset_status()
    node.execute({"prompt": "ab"}, params)  # 累積 9+9=18 >= 10 → limit
    assert node.read_status() == "limit"

    node.reset_limit_state("tokens")
    node.reset_status()

    out = node.execute({"prompt": "x"}, params)
    assert node.read_status() == "done"
    assert "response" in out
    assert out["response"] == "mock:x"
