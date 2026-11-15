"""区域版本、行政区快照与订阅者花名册。

区域代码只在一个边界版本内有确定含义。行政区划调整后，同一个代码可能指向
不同的实际区域，因此任何影响计算都必须显式携带 ``boundary_version``，并在
该版本的快照内解析，不能回退到“今天的边界”。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable


class MaterialError(ValueError):
    """配套资料格式或内容不成立。"""


class UnknownRegion(LookupError):
    """在指定边界版本内找不到区域代码。"""


@dataclass(frozen=True)
class Region:
    """一个边界版本内的区域记录。

    ``code`` 是来源引用的区域代码，只在 ``boundary_version`` 内有效；
    ``area_id`` 是跨版本稳定的实际区域标识；
    ``parent`` 指向同版本内的父代码，``parent_area`` 用于表达跨版本的
    稳定父区域（例如某镇由旧县析出，而旧县代码在新版本中已另有含义）。
    """

    code: str
    area_id: str
    name: str
    kind: str
    parent: str | None
    parent_area: str | None = None

    def as_dict(self) -> dict[str, Any]:
        result = {
            "code": self.code,
            "area_id": self.area_id,
            "name": self.name,
            "kind": self.kind,
            "parent": self.parent,
        }
        if self.parent_area is not None:
            result["parent_area"] = self.parent_area
        return result


class BoundaryCatalog:
    """一个 boundary_version 下不可变的行政区快照。"""

    def __init__(self, version: str, regions: Iterable[Region]):
        if not isinstance(version, str) or not version.strip():
            raise MaterialError("boundary version must be a non-empty string")
        self.version = version.strip()
        self._regions: dict[str, Region] = {}
        area_ids: dict[str, str] = {}
        for region in regions:
            if not isinstance(region, Region):
                raise MaterialError("regions must contain Region records")
            if region.code in self._regions:
                raise MaterialError(f"duplicate region code in {self.version}: {region.code}")
            if region.area_id in area_ids:
                raise MaterialError(
                    f"area id {region.area_id} is used by both {area_ids[region.area_id]} and {region.code}"
                )
            self._regions[region.code] = region
            area_ids[region.area_id] = region.code
        for region in self._regions.values():
            if region.parent is not None and region.parent not in self._regions:
                raise MaterialError(
                    f"region {region.code} has unknown parent {region.parent} in {self.version}"
                )
            if region.parent is not None and region.parent_area is not None:
                raise MaterialError(
                    f"region {region.code} declares both parent and parent_area in {self.version}"
                )
        self._check_cycles()

    @classmethod
    def from_document(cls, document: Any) -> "BoundaryCatalog":
        if not isinstance(document, dict):
            raise MaterialError("boundary document must be an object")
        version = document.get("version")
        raw_regions = document.get("regions")
        if not isinstance(raw_regions, list):
            raise MaterialError("boundary document must contain a regions array")
        regions: list[Region] = []
        for index, raw in enumerate(raw_regions):
            if not isinstance(raw, dict):
                raise MaterialError(f"region {index} must be an object")
            missing = [name for name in ("code", "area_id", "name") if not _text(raw.get(name))]
            if missing:
                raise MaterialError(f"region {index} missing fields: {', '.join(missing)}")
            parent = raw.get("parent")
            if parent is not None and not _text(parent):
                raise MaterialError(f"region {index} has an invalid parent")
            parent_area = raw.get("parent_area")
            if parent_area is not None and not _text(parent_area):
                raise MaterialError(f"region {index} has an invalid parent_area")
            regions.append(
                Region(
                    code=_text(raw["code"]),
                    area_id=_text(raw["area_id"]),
                    name=_text(raw["name"]),
                    kind=_text(raw.get("kind")) or "region",
                    parent=_text(parent) if parent is not None else None,
                    parent_area=_text(parent_area) if parent_area is not None else None,
                )
            )
        return cls(_text(version), regions)

    def _check_cycles(self) -> None:
        for start in self._regions:
            seen: set[str] = set()
            current: str | None = start
            while current is not None:
                if current in seen:
                    raise MaterialError(f"parent cycle involving region {current} in {self.version}")
                seen.add(current)
                current = self._regions[current].parent

    def as_document(self) -> dict[str, Any]:
        return {"version": self.version, "regions": [region.as_dict() for region in self._regions.values()]}

    @property
    def regions(self) -> tuple[Region, ...]:
        return tuple(self._regions.values())

    def has(self, code: str) -> bool:
        return code in self._regions

    def region(self, code: str) -> Region:
        try:
            return self._regions[code]
        except KeyError:
            raise UnknownRegion(f"region {code} is not defined in boundary {self.version}") from None

    def descendants(self, code: str) -> tuple[Region, ...]:
        """返回该区域自身及其后代，后代按快照顺序排列。"""
        self.region(code)
        selected: list[Region] = []
        frontier = [code]
        while frontier:
            current = frontier.pop(0)
            selected.append(self._regions[current])
            frontier.extend(
                region.code for region in self._regions.values() if region.parent == current
            )
        return tuple(selected)

    def area_ids(self, code: str) -> frozenset[str]:
        """区域覆盖的实际区域标识集合（含自身）。"""
        return frozenset(region.area_id for region in self.descendants(code))


def _text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


@dataclass(frozen=True)
class Subscriber:
    subscriber_id: str
    region: str
    boundary_version: str

    def as_dict(self) -> dict[str, str]:
        return {
            "id": self.subscriber_id,
            "region": self.region,
            "boundary_version": self.boundary_version,
        }


class SubscriberRoster:
    """订阅者花名册；每条订阅声明其区域代码登记时的边界版本。"""

    def __init__(self, roster_version: str, subscribers: Iterable[Subscriber]):
        if not isinstance(roster_version, str) or not roster_version.strip():
            raise MaterialError("roster version must be a non-empty string")
        self.version = roster_version.strip()
        self._subscribers: dict[str, Subscriber] = {}
        for subscriber in subscribers:
            if not isinstance(subscriber, Subscriber):
                raise MaterialError("subscribers must contain Subscriber records")
            if not subscriber.subscriber_id or not subscriber.region or not subscriber.boundary_version:
                raise MaterialError("subscriber id, region and boundary_version are required")
            if subscriber.subscriber_id in self._subscribers:
                raise MaterialError(f"duplicate subscriber id: {subscriber.subscriber_id}")
            self._subscribers[subscriber.subscriber_id] = subscriber

    @classmethod
    def from_document(cls, document: Any) -> "SubscriberRoster":
        if not isinstance(document, dict):
            raise MaterialError("subscriber roster must be an object")
        raw_subscribers = document.get("subscribers")
        if not isinstance(raw_subscribers, list):
            raise MaterialError("subscriber roster must contain a subscribers array")
        subscribers: list[Subscriber] = []
        for index, raw in enumerate(raw_subscribers):
            if not isinstance(raw, dict):
                raise MaterialError(f"subscriber {index} must be an object")
            member_id = _text(raw.get("id"))
            if not member_id:
                raise MaterialError(f"subscriber {index} missing id")
            if raw.get("active", True) is False:
                continue
            subscribers.append(
                Subscriber(
                    subscriber_id=member_id,
                    region=_text(raw.get("region")),
                    boundary_version=_text(raw.get("boundary_version")),
                )
            )
        return cls(_text(document.get("roster_version")), subscribers)

    def as_document(self) -> dict[str, Any]:
        return {
            "roster_version": self.version,
            "subscribers": [subscriber.as_dict() for subscriber in self._subscribers.values()],
        }

    @property
    def subscribers(self) -> tuple[Subscriber, ...]:
        return tuple(self._subscribers.values())

    def __len__(self) -> int:
        return len(self._subscribers)


@dataclass(frozen=True)
class MissingMaterial:
    """一项尚不能用于计算的配套资料。``label`` 是面向值班员的简短标识。"""

    kind: str
    identifier: str | None = None
    detail: str | None = None

    @property
    def label(self) -> str:
        if self.kind == "boundary_catalog":
            return f"boundary:{self.identifier}"
        if self.kind == "region_reference":
            suffix = f"@{self.detail}" if self.detail else ""
            return f"region:{self.identifier}{suffix}"
        if self.kind == "roster_boundary":
            return f"subscriber-boundary:{self.identifier}"
        if self.kind == "roster_region":
            suffix = f"@{self.detail}" if self.detail else ""
            return f"subscriber-region:{self.identifier}{suffix}"
        if self.kind == "revision_conflict":
            return f"revision-conflict:{self.identifier}"
        if self.kind == "conflict":
            return f"conflict:{self.identifier}"
        if self.kind == "unreadable_record":
            return f"unreadable-record:{self.identifier}"
        if self.kind == "invalid_field":
            return f"invalid:{self.identifier}"
        return self.kind

    def as_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {"kind": self.kind, "label": self.label}
        if self.identifier is not None:
            result["identifier"] = self.identifier
        if self.detail is not None:
            result["detail"] = self.detail
        return result


def missing_labels(missing: Iterable[MissingMaterial]) -> list[str]:
    return sorted({item.label for item in missing})
