"""Typed FlowCtx helpers for dev-process subpipe boundaries."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Mapping

from nodeflow.core.base_node import NodeExecutionFailure
from nodeflow.workflows.dev_process.constants import SCHEMA_VERSION


@dataclass(frozen=True)
class SegmentParams:
    raw: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any] | None) -> "SegmentParams":
        if raw is None:
            return cls({})
        if not isinstance(raw, Mapping):
            raise NodeExecutionFailure("FlowCtx.params must be a dict")
        return cls(raw=deepcopy(dict(raw)))

    def to_dict(self) -> dict[str, Any]:
        return deepcopy(self.raw)

    def get_str(self, key: str, default: str = "") -> str:
        value = self.raw.get(key, default)
        return value if isinstance(value, str) else default

    def get_list(self, key: str) -> list[Any]:
        value = self.raw.get(key)
        return value if isinstance(value, list) else []


@dataclass(frozen=True)
class FlowCtx:
    body: dict[str, Any]
    segment: str = ""
    params: SegmentParams = field(default_factory=SegmentParams)
    schema_version: str = SCHEMA_VERSION

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "FlowCtx":
        if not isinstance(raw, Mapping):
            raise NodeExecutionFailure("FlowCtx must be a dict-like mapping")

        schema = raw.get("schema_version")
        if schema != SCHEMA_VERSION:
            raise NodeExecutionFailure(
                f"FlowCtx.schema_version must be {SCHEMA_VERSION!r}, got {schema!r}"
            )

        body = raw.get("body")
        if not isinstance(body, dict):
            raise NodeExecutionFailure("FlowCtx.body must be a dict")

        segment = raw.get("segment", "")
        if not isinstance(segment, str):
            raise NodeExecutionFailure("FlowCtx.segment must be a string")

        params = SegmentParams.from_mapping(raw.get("params", {}))
        return cls(
            schema_version=schema,
            body=deepcopy(body),
            segment=segment,
            params=params,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "body": deepcopy(self.body),
            "segment": self.segment,
            "params": self.params.to_dict(),
        }

    def with_body(self, body: dict[str, Any]) -> "FlowCtx":
        return FlowCtx(
            schema_version=self.schema_version,
            body=deepcopy(body),
            segment=self.segment,
            params=SegmentParams.from_mapping(self.params.raw),
        )


def require_flow_ctx(raw: Mapping[str, Any]) -> FlowCtx:
    return FlowCtx.from_dict(raw)


def copy_flow_ctx(raw: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    flow = require_flow_ctx(raw)
    ctx = flow.to_dict()
    body = ctx["body"]
    return ctx, body


def make_flow_ctx(
    body: dict[str, Any],
    *,
    segment: str = "",
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not isinstance(body, dict):
        raise NodeExecutionFailure("FlowCtx.body must be a dict")
    return FlowCtx(
        body=deepcopy(body),
        segment=segment,
        params=SegmentParams.from_mapping(params),
    ).to_dict()


def flow_params(ctx: dict[str, Any]) -> dict[str, Any]:
    flow = require_flow_ctx(ctx)
    return flow.params.to_dict()
