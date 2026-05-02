"""
Hello sample node — runnable type is packaged as ``HelloDemoNode``.

See ``examples/pipes/hello.json`` for a minimal v1.6 PipeSpec graph using registry key ``hello_demo``.
"""

from __future__ import annotations

from nodeflow.nodes.hello_demo import HelloDemoNode

HelloNode = HelloDemoNode

__all__ = ["HelloDemoNode", "HelloNode"]
