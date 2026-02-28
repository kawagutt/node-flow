"""
NodeFlow v1.41 — PythonScriptNode (§13.1).
指定した Python スクリプトを実行し、main(inputs) の戻り値を result port で返す。
"""

from __future__ import annotations

import importlib.util
from types import MappingProxyType
from typing import Any, Dict

from ..node import BaseNode, ExecutionContext


class PythonScriptNode(BaseNode):
    """Script は main(inputs: dict) -> dict を実装すること。"""

    def run(
        self,
        inputs: Dict[str, Any],
        params: MappingProxyType,
        context: ExecutionContext,
    ) -> Dict[str, Any]:
        script_path = params["script"]
        spec = importlib.util.spec_from_file_location("script", script_path)
        if spec is None or spec.loader is None:
            raise FileNotFoundError(f"Could not load script: {script_path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        result = module.main(inputs)
        if not isinstance(result, dict):
            raise TypeError(f"script must return dict, got {type(result)}")
        return {"result": result}
