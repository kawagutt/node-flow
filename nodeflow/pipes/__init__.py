"""Named pipe dispatch for the NodeFlow CLI."""

from nodeflow.pipes.dispatch import dispatch_named_pipe
from nodeflow.pipes.registry import get_named_pipe, list_named_pipes

__all__ = ["dispatch_named_pipe", "get_named_pipe", "list_named_pipes"]
