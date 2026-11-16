"""预警来源记录的规范化。

来源字段 ``issuer`` / ``alert`` / ``revision`` 是供方声明，这里只负责校验和
保留原文，不把它们直接当作业务实体结论。业务事件的拼装与区域解析在
``engine.py`` 中完成。
"""

import hashlib
import json
from dataclasses import dataclass

SUPPORTED_STATUSES = ("active", "cancelled")


class RecordError(ValueError):
    """来源记录不满足最低业务字段要求。"""


def _text(raw, name, required=True):
    value = raw.get(name)
    if value is None:
        if required:
            raise RecordError("missing fields: " + name)
        return None
    if not isinstance(value, str) or not value.strip():
        raise RecordError(name + " must be a non-empty string")
    return value.strip()


def canonical_fingerprint(raw):
    encoded = json.dumps(raw, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class WarningRecord:
    issuer: str
    alert: str
    event_key: str
    revision: int
    status: str
    regions: tuple
    boundary_version: str | None
    fingerprint: str
    raw: dict

    def as_dict(self):
        return {
            "issuer": self.issuer,
            "alert": self.alert,
            "event_key": self.event_key,
            "revision": self.revision,
            "status": self.status,
            "regions": list(self.regions),
            "boundary_version": self.boundary_version,
            "fingerprint": self.fingerprint,
        }


def parse_record(raw):
    """校验一条来源记录，保留原文并按内容计算指纹。"""
    if not isinstance(raw, dict):
        raise RecordError("record must be an object")
    issuer = _text(raw, "issuer")
    alert = _text(raw, "alert")
    revision = raw.get("revision")
    if isinstance(revision, bool) or not isinstance(revision, int) or revision < 0:
        raise RecordError("revision must be a non-negative integer")
    status = _text(raw, "status").lower()
    if status not in SUPPORTED_STATUSES:
        raise RecordError("unsupported status: " + status)
    regions = raw.get("regions", [])
    if not isinstance(regions, list):
        raise RecordError("regions must be an array")
    normalized_regions = []
    for region in regions:
        if not isinstance(region, str) or not region.strip():
            raise RecordError("regions entries must be non-empty strings")
        normalized_regions.append(region.strip())
    boundary_version = raw.get("boundary_version")
    if boundary_version is not None:
        if not isinstance(boundary_version, str) or not boundary_version.strip():
            raise RecordError("boundary_version must be a non-empty string when present")
        boundary_version = boundary_version.strip()
    if status == "active" and not normalized_regions:
        raise RecordError("active records must name at least one region")
    # 事件归并键可由来源显式声明；否则同一预警代码归入同一事件。这里只做
    # 归并，不代表已经验证两家来源在说同一个实体，结论仍按来源分别保留。
    event_name = raw.get("event")
    if event_name is None:
        event_key = alert
    elif isinstance(event_name, str) and event_name.strip():
        event_key = event_name.strip()
    else:
        raise RecordError("event must be a non-empty string when present")
    return WarningRecord(
        issuer=issuer,
        alert=alert,
        event_key=event_key,
        revision=revision,
        status=status,
        regions=tuple(normalized_regions),
        boundary_version=boundary_version,
        fingerprint=canonical_fingerprint(raw),
        raw=raw,
    )
