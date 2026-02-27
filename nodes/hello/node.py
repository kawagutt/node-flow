"""
Hello Node — v1.2. run(inputs, params) -> dict.
"""

from typing import Any, Dict

from nodeflow.node import BaseNode


class HelloNode(BaseNode):
    def run(self, inputs: Dict[str, Any], params: Any) -> Dict[str, Any]:
        message = (
            params.get("message", "Hello, World!")
            if hasattr(params, "get")
            else "Hello, World!"
        )
        return {"message": {"data": message}}
