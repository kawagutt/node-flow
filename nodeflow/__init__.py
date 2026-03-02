"""
NodeFlow v1.4.4 (runtime-min) - Everything is a Node workflow execution engine.
"""

__version__ = "1.4.4"

# パッケージ利用時に built-in を登録（execution.loader でも明示的に import するので二重でも可）
import nodeflow.extensions  # noqa: F401, E402
