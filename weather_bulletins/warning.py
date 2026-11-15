"""来源预警观测的校验、规范化和证据摘要。

这里只做“来源说了什么”的结构化，不解释正文地名。区域范围必须来自结构化
字段 ``regions`` 与显式的 ``boundary_version``；正文仅原样保留作为证据，
绝不从中猜测影响范围。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

STATUS_VALUES = ("active", "cancelled")

_BODY_FIELDS = ("body", "text")


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def content_digest(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class Observation:
    source: str
    key: str
    revision: int
    value: Any


def inspect_record(raw: dict[str, Any]) -> dict[str, Any]:
    """校验并规范化一条来源观测，同时保留来源信息。"""
    required = ("source", "key", "revision", "value")
    missing = [name for name in required if name not in raw]
    if missing:
        raise ValueError("missing fields: " + ", ".join(missing))
    source, key = str(raw["source"]).strip(), str(raw["key"]).strip()
    if not source or not key:
        raise ValueError("source and key must be non-empty")
    revision = raw["revision"]
    if isinstance(revision, bool) or not isinstance(revision, int) or revision < 0:
        raise ValueError("revision must be a non-negative integer")
    item = Observation(source, key, revision, raw["value"])
    return {"source": item.source, "key": item.key, "revision": item.revision, "value": item.value, "event": "notification_revisions"}


def _text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _identity(raw: dict[str, Any]) -> tuple[str, str, int]:
    source = _text(raw.get("issuer")) or _text(raw.get("source"))
    alert = _text(raw.get("alert")) or _text(raw.get("key"))
    revision = raw.get("revision")
    missing = [name for name, value in (("issuer", source), ("alert", alert)) if not value]
    if "revision" not in raw:
        missing.append("revision")
    if missing:
        raise ValueError("missing identity fields: " + ", ".join(missing))
    if isinstance(revision, bool) or not isinstance(revision, int) or revision < 0:
        raise ValueError("revision must be a non-negative integer")
    return source, alert, revision


def inspect_fragment(raw: dict[str, Any], capture_id: int | None = None, observed_at: str | None = None) -> dict[str, Any]:
    """把一条来源记录规范化为可持久化的观测片段。

    身份字段（发行方、预警标识、修订号）不合法时直接拒绝，因为无法为它建立
    可核对的事件；其余配套字段缺失或类型不对则保留为 ``invalid``，由装配层
    明确报告缺什么，而不是猜测。
    """
    if not isinstance(raw, dict):
        raise TypeError("warning record must be an object")
    source, alert, revision = _identity(raw)
    invalid: list[str] = []

    status: str | None = None
    if "status" in raw:
        candidate = _text(raw.get("status")).lower()
        if candidate in STATUS_VALUES:
            status = candidate
        else:
            invalid.append("status")

    regions: list[str] | None = None
    regions_present = "regions" in raw
    if regions_present:
        candidate = raw.get("regions")
        if isinstance(candidate, list) and all(isinstance(item, str) and item.strip() for item in candidate):
            regions = sorted({item.strip() for item in candidate})
        else:
            invalid.append("regions")

    boundary_version: str | None = None
    boundary_present = "boundary_version" in raw
    if boundary_present:
        candidate = _text(raw.get("boundary_version"))
        if candidate:
            boundary_version = candidate
        else:
            invalid.append("boundary_version")

    bodies: list[str] = []
    invalid_evidence: list[str] = []
    for name in _BODY_FIELDS:
        if name in raw:
            if isinstance(raw[name], str):
                bodies.append(raw[name])
            else:
                # 正文只是证据，写坏了不阻止范围计算，但要单独留痕。
                invalid_evidence.append(name)

    normalized = {
        "source": source,
        "alert": alert,
        "revision": revision,
        "status": status,
        "status_present": "status" in raw,
        "regions": regions,
        "regions_present": regions_present,
        "boundary_version": boundary_version,
        "boundary_version_present": boundary_present,
        "bodies": bodies,
        "invalid": sorted(set(invalid)),
        "invalid_evidence": sorted(set(invalid_evidence)),
        "digest": content_digest(raw),
        "capture_id": capture_id,
        "observed_at": observed_at,
    }
    return normalized


def valid_records(records: list[dict[str, Any]], capture_id: int | None = None, observed_at: str | None = None) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """返回可入库片段与无法建立身份的记录。"""
    fragments: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for index, record in enumerate(records):
        try:
            fragments.append(inspect_fragment(record, capture_id, observed_at))
        except (TypeError, ValueError) as error:
            rejected.append({"index": index, "reason": str(error), "record": record})
    return fragments, rejected
