"""Hello Node."""
from nodeflow.node import BaseNode
from nodeflow.context import Context
from typing import Dict, Any


class HelloNode(BaseNode):
    DEFAULT_CONFIG = {"message": "Hello, World!"}

    def run(self, context: Context) -> Dict[str, Any]:
        message = self.config.get("message", "Hello, World!")
        return {
            "status": "success",
            "updates": [{"op": "set", "path": "artifacts.hello.message", "value": message}],
        }
