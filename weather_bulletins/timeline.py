"""订阅者影响时间线。

对每个来源的每一版有效范围，按订阅者覆盖的稳定地点计算其是否受影响。
地点集合的差集就是“扩大 / 缩小 / 撤销”的具体含义，不依赖区域名称或正文。
"""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Audience:
    subscriber: str
    name: str
    covered: frozenset
    affected: frozenset
    known: bool = True

    @property
    def impact(self):
        if not self.known:
            return "unknown"
        if not self.affected:
            return "unaffected"
        if self.affected == self.covered:
            return "fully_affected"
        return "partially_affected"


@dataclass
class Step:
    issuer: str
    revision: int
    status: str
    boundary_version: str | None
    places: frozenset | None
    unresolved: list
    audience: dict = field(default_factory=dict)
    transition: dict = field(default_factory=dict)

    def as_dict(self):
        return {
            "issuer": self.issuer,
            "revision": self.revision,
            "status": self.status,
            "boundary_version": self.boundary_version,
            "places": None if self.places is None else sorted(self.places),
            "unresolved": self.unresolved,
            "audience": {
                subscriber: {
                    "impact": member.impact,
                    "places": sorted(member.affected),
                    "name": member.name,
                }
                for subscriber, member in sorted(self.audience.items())
            },
            "transition": self.transition,
        }


def resolve_audience(places, subscribers):
    audience = {}
    for subscriber in subscribers:
        covered = frozenset(subscriber["covers"])
        affected = frozenset() if places is None else covered & places
        audience[subscriber["id"]] = Audience(
            subscriber=subscriber["id"],
            name=subscriber["name"],
            covered=covered,
            affected=affected,
            known=places is not None,
        )
    return audience


def _classify(previous_places, previous_status, current_places, current_status, audience):
    """给出逐订阅者的变化描述。资料缺失时标记 unknown，不冒充 unchanged。"""
    change = {}
    for subscriber, member in audience.items():
        if previous_places is None or current_places is None:
            change[subscriber] = {
                "change": "unknown",
                "added": [],
                "removed": [],
                "reason": "missing_boundary_materials",
            }
            continue
        before = member.covered & previous_places
        after = member.covered & current_places
        if current_status == "cancelled":
            kind = "cancelled" if before else "unchanged"
        elif previous_status == "cancelled" and current_status == "active":
            kind = "reinstated"
        elif not before and after:
            kind = "added"
        elif before and not after:
            kind = "removed"
        elif before == after:
            kind = "unchanged"
        elif before < after:
            kind = "expanded"
        elif after < before:
            kind = "narrowed"
        else:
            kind = "shifted"
        change[subscriber] = {
            "change": kind,
            "added": sorted(after - before),
            "removed": sorted(before - after),
        }
    return change


def build_timeline(observations, catalog, subscribers, resolve):
    """按来源分组、按修订号升序生成每一步的影响。

    ``observations`` 为按记录时间排序的观察；``resolve(codes, version)`` 返回
    ``(places, unresolved)``。同一来源相同修订的多条冲突观察都保留为独立步骤。
    """
    grouped = {}
    for item in observations:
        grouped.setdefault(item["issuer"], []).append(item)
    steps = []
    for issuer, items in grouped.items():
        items.sort(key=lambda item: (item["revision"], item["id"]))
        previous = None
        for item in items:
            if not item["regions"] and item["status"] == "active":
                places, unresolved = None, [{
                    "reason": "missing_regions",
                    "boundary_version": item["boundary_version"],
                }]
            else:
                places, unresolved = resolve(item["regions"], item["boundary_version"])
            audience = resolve_audience(places, subscribers)
            if previous is None:
                transition = {
                    subscriber: (
                        {
                            "change": "unknown",
                            "added": [],
                            "removed": [],
                            "reason": "missing_boundary_materials",
                        }
                        if places is None
                        else {
                            "change": (
                                "cancelled" if item["status"] == "cancelled"
                                else ("added" if member.affected else "unchanged")
                            ),
                            "added": sorted(member.affected) if item["status"] == "active" else [],
                            "removed": [],
                        }
                    )
                    for subscriber, member in audience.items()
                }
            else:
                previous_places, previous_status, previous_audience = previous
                transition = _classify(
                    previous_places, previous_status, places, item["status"], previous_audience
                )
            steps.append(
                Step(
                    issuer=issuer,
                    revision=item["revision"],
                    status=item["status"],
                    boundary_version=item["boundary_version"],
                    places=places,
                    unresolved=unresolved,
                    audience=audience,
                    transition=transition,
                )
            )
            previous = (places, item["status"], audience)
    return steps
