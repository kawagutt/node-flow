"""
NodeFlow — NodeRegistry. Resolves a YAML `type` string to a node class.
"""

from __future__ import annotations

from typing import Any, Dict, Type

# BaseNode の型ヒント用（循環回避のため文字列で参照しない）
_NodeClass = Type[Any]


class UnknownNodeTypeError(ValueError):
    """Raised when resolve() is called with an unregistered type name."""

    def __init__(self, type_name: str):
        self.type_name = type_name
        super().__init__(f"Unknown node type: {type_name!r}")


class RegistryConflictError(ValueError):
    """Raised when register() is called with an existing type_name and override=False."""

    def __init__(self, type_name: str):
        self.type_name = type_name
        super().__init__(
            f"Type {type_name!r} already registered. Use override=True to replace."
        )


class NodeRegistry:
    """グローバルなノード型レジストリ。register で登録、resolve で解決。"""

    def __init__(self) -> None:
        self._store: Dict[str, _NodeClass] = {}

    def register(
        self, type_name: str, node_class: _NodeClass, *, override: bool = False
    ) -> None:
        """type 文字列と Node クラスを登録する。既存の場合は override=True のときだけ上書き。"""
        if type_name in self._store and not override:
            raise RegistryConflictError(type_name)
        self._store[type_name] = node_class

    def get(self, type_name: str) -> _NodeClass | None:
        """登録済みならクラスを返し、未登録なら None。"""
        return self._store.get(type_name)

    def unregister(self, type_name: str) -> None:
        """type の登録を削除する。未登録でもエラーにしない。"""
        self._store.pop(type_name, None)

    def clear(self) -> None:
        """全登録を削除する。テスト・lifecycle 用。"""
        self._store.clear()

    def resolve(self, type_name: str) -> _NodeClass:
        """type 文字列から Node クラスを返す。未登録なら UnknownNodeTypeError。"""
        if type_name not in self._store:
            raise UnknownNodeTypeError(type_name)
        return self._store[type_name]


# モジュールレベルで 1 インスタンス
registry = NodeRegistry()
