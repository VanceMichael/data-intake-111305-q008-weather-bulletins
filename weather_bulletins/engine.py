"""把来源观测装配成有依据的预警事件，并计算订阅者影响。

装配只使用结构化字段：``status``、``regions`` 与显式 ``boundary_version``。
同一来源同一修订的多份观测（例如正文与后续到达的范围文件）可以合并；字段
互相矛盾时保留冲突并以“缺资料”呈现，不凭正文猜范围。

影响计算在声明的边界版本快照内进行，订阅者也按其登记时的边界版本解析。
通知差异以“上一已应用修订 → 当前修订”为确定性依据，重复采集或进程重启
只会复用同一批通知，不会重复发出一轮更正。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable

from .boundaries import (
    BoundaryCatalog,
    MaterialError,
    MissingMaterial,
    Subscriber,
    SubscriberRoster,
    missing_labels,
)
from .store import StateStore
from .warning import content_digest

_ACTIVE = "active"
_CANCELLED = "cancelled"

# 会阻止“把这份观测当成完整业务事实”的缺口。
_BLOCKING_KINDS = {
    "missing_field",
    "invalid_field",
    "conflict",
    "boundary_catalog",
    "region_reference",
    "roster",
    "roster_boundary",
    "roster_region",
}


@dataclass
class Revision:
    source: str
    alert: str
    revision: int
    status: str | None = None
    regions: tuple[str, ...] | None = None
    boundary_version: str | None = None
    missing: list[MissingMaterial] = field(default_factory=list)
    fragments: list[dict[str, Any]] = field(default_factory=list)

    @property
    def blocking(self) -> list[MissingMaterial]:
        return [item for item in self.missing if item.kind in _BLOCKING_KINDS]

    @property
    def complete(self) -> bool:
        return not self.blocking


@dataclass
class Conclusion:
    source: str
    alert: str
    revision: int
    status: str
    complete: bool
    covered_areas: tuple[str, ...]
    impacted: tuple[str, ...]
    missing: list[MissingMaterial]
    boundary_version: str | None = None
    regions: tuple[str, ...] = ()
    subscriber_areas: dict[str, list[str]] = field(default_factory=dict)


def _field_values(fragments: Iterable[dict[str, Any]], name: str) -> tuple[list[Any], bool]:
    """收集某字段在所有片段中的有效取值，以及是否有片段声明了无效值。"""
    values: list[Any] = []
    invalid = False
    for fragment in fragments:
        if name in fragment["invalid"]:
            invalid = True
        value = fragment[name]
        if value is not None:
            values.append(value)
    return values, invalid


def _conflict(source: str, alert: str, revision: int, name: str) -> MissingMaterial:
    return MissingMaterial("conflict", f"{source}/{alert}/r{revision}/{name}")


def merge_revision(source: str, alert: str, revision: int, fragments: list[dict[str, Any]]) -> Revision:
    """合并同一来源同一修订的多个观测片段。

    只在片段对同一字段给出不同有效值时报告冲突；缺失的字段按状态决定是否
    构成阻断：``active`` 必须有非空范围和边界版本，``cancelled`` 可以没有范围。
    """
    merged = Revision(source=source, alert=alert, revision=revision, fragments=fragments)

    status_values, status_invalid = _field_values(fragments, "status")
    status_distinct = {value for value in status_values if isinstance(value, str)}
    if len(status_distinct) > 1:
        merged.missing.append(_conflict(source, alert, revision, "status"))
    elif status_distinct:
        merged.status = next(iter(status_distinct))
    elif status_invalid:
        merged.missing.append(MissingMaterial("invalid_field", "status"))
    else:
        merged.missing.append(MissingMaterial("missing_field", "status"))

    region_values, regions_invalid = _field_values(fragments, "regions")
    region_distinct = {tuple(value) for value in region_values}
    if len(region_distinct) > 1:
        merged.missing.append(_conflict(source, alert, revision, "regions"))
    elif region_distinct:
        merged.regions = next(iter(region_distinct))
    elif regions_invalid:
        merged.missing.append(MissingMaterial("invalid_field", "regions"))
    elif merged.status == _ACTIVE:
        merged.missing.append(MissingMaterial("missing_field", "regions"))

    boundary_values, boundary_invalid = _field_values(fragments, "boundary_version")
    boundary_distinct = {value for value in boundary_values if isinstance(value, str)}
    if len(boundary_distinct) > 1:
        merged.missing.append(_conflict(source, alert, revision, "boundary_version"))
    elif boundary_distinct:
        merged.boundary_version = next(iter(boundary_distinct))
    elif boundary_invalid:
        merged.missing.append(MissingMaterial("invalid_field", "boundary_version"))
    elif merged.status == _ACTIVE:
        merged.missing.append(MissingMaterial("missing_field", "boundary_version"))

    for fragment in fragments:
        for name in fragment["invalid"]:
            merged.missing.append(MissingMaterial("invalid_field", name))
    return merged


def assemble_source(observations: Iterable[dict[str, Any]]) -> list[Revision]:
    grouped: dict[int, list[dict[str, Any]]] = {}
    for observation in observations:
        grouped.setdefault(observation["revision"], []).append(observation)
    return [
        merge_revision(observations[0]["source"], observations[0]["alert"], revision, grouped[revision])
        for revision in sorted(grouped)
    ]


@dataclass
class Materials:
    catalogs: dict[str, BoundaryCatalog]
    roster: SubscriberRoster | None
    missing: list[MissingMaterial] = field(default_factory=list)

    @classmethod
    def load(cls, store: StateStore) -> "Materials":
        catalogs: dict[str, BoundaryCatalog] = {}
        missing: list[MissingMaterial] = []
        for version, document in store.boundary_documents().items():
            try:
                catalogs[version] = BoundaryCatalog.from_document(document)
            except (MaterialError, ValueError) as error:
                missing.append(MissingMaterial("boundary_catalog", version, str(error)))
        members: dict[str, Subscriber] = {}
        roster_version = None
        for document in store.roster_documents():
            try:
                roster = SubscriberRoster.from_document(document)
            except (MaterialError, ValueError) as error:
                missing.append(MissingMaterial("roster", None, str(error)))
                continue
            # 后登记的花名册版本覆盖先前的同一名单成员。
            roster_version = roster.version
            for subscriber in roster.subscribers:
                members[subscriber.subscriber_id] = subscriber
        roster = None
        if members:
            roster = SubscriberRoster(
                roster_version or "stored", [members[key] for key in sorted(members)]
            )
        return cls(catalogs=catalogs, roster=roster, missing=missing)


def resolve_conclusion(revision: Revision, materials: Materials) -> Conclusion:
    """在声明的边界版本内解析影响范围；缺任何一环都不给出“完整结论”。

    撤销不需要解析范围，也不需要花名册或边界快照：它只意味着此前受影响的
    订阅者不再受影响，因此不因这些资料缺失而卡住更正。``active`` 则需要
    范围、边界版本、花名册与每个订阅者的登记版本都齐全。
    """
    missing = list(revision.blocking)
    covered: frozenset[str] = frozenset()
    impacted: list[str] = []
    subscriber_areas: dict[str, list[str]] = {}

    if revision.status == _ACTIVE:
        if materials.roster is None:
            missing.append(MissingMaterial("roster"))
        catalog = None
        if revision.boundary_version:
            catalog = materials.catalogs.get(revision.boundary_version)
            if catalog is None:
                missing.append(MissingMaterial("boundary_catalog", revision.boundary_version))
        if catalog is not None and revision.regions is not None:
            unknown = [code for code in revision.regions if not catalog.has(code)]
            for code in unknown:
                missing.append(MissingMaterial("region_reference", code, revision.boundary_version))
            if not unknown:
                covered = frozenset().union(*(catalog.area_ids(code) for code in revision.regions))

        if materials.roster is not None:
            for subscriber in materials.roster.subscribers:
                catalog = materials.catalogs.get(subscriber.boundary_version)
                if catalog is None:
                    missing.append(MissingMaterial("roster_boundary", subscriber.boundary_version))
                    continue
                if not catalog.has(subscriber.region):
                    missing.append(
                        MissingMaterial("roster_region", subscriber.region, subscriber.boundary_version)
                    )
                    continue
                areas = catalog.area_ids(subscriber.region)
                subscriber_areas[subscriber.subscriber_id] = sorted(areas)
                if covered & areas:
                    impacted.append(subscriber.subscriber_id)

    complete = not missing
    return Conclusion(
        source=revision.source,
        alert=revision.alert,
        revision=revision.revision,
        status=revision.status or "unknown",
        complete=complete,
        covered_areas=tuple(sorted(covered)),
        impacted=tuple(sorted(impacted)),
        missing=missing,
        boundary_version=revision.boundary_version,
        regions=revision.regions or (),
        subscriber_areas=subscriber_areas,
    )


def _payload(conclusion: Conclusion) -> dict[str, Any]:
    return {
        "source": conclusion.source,
        "alert": conclusion.alert,
        "revision": conclusion.revision,
        "status": conclusion.status,
        "boundary_version": conclusion.boundary_version,
        "regions": list(conclusion.regions),
        "covered_areas": list(conclusion.covered_areas),
    }


def _queue_notice(
    store: StateStore,
    conclusion: Conclusion,
    subscriber: str,
    kind: str,
    identity: str,
    correction_of: int | None = None,
    reason: str | None = None,
    payload: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], bool]:
    document = {
        "dedupe_key": f"{conclusion.source}|{conclusion.alert}|{subscriber}|{identity}",
        "source": conclusion.source,
        "alert": conclusion.alert,
        "revision": conclusion.revision,
        "subscriber": subscriber,
        "kind": kind,
        "state": "pending",
        "boundary_version": conclusion.boundary_version,
        "regions": list(conclusion.regions),
        "covered_areas": list(conclusion.covered_areas),
        "payload": {**_payload(conclusion), **(payload or {})},
    }
    if correction_of is not None:
        document["correction_of"] = correction_of
    if reason is not None:
        document["reason"] = reason
    return store.add_notification(document)


def _pending_for(store: StateStore, source: str, alert: str, subscriber: str) -> list[dict[str, Any]]:
    return [
        notification
        for notification in store.notifications(alert=alert, source=source, state="pending")
        if notification["subscriber"] == subscriber
    ]


def _history(store: StateStore, source: str, alert: str, subscriber: str) -> list[dict[str, Any]]:
    return [
        notification
        for notification in store.notifications(alert=alert, source=source)
        if notification["subscriber"] == subscriber
    ]


def _latest_sent(store: StateStore, source: str, alert: str, subscriber: str) -> dict[str, Any] | None:
    sent = [item for item in _history(store, source, alert, subscriber) if item["state"] == "sent"]
    return sent[-1] if sent else None


def queue_diff(store: StateStore, conclusion: Conclusion, materials: Materials) -> list[dict[str, Any]]:
    """按“当前修订的实际影响”与历史通知生成确定性的通知差异。

    - 未发送且被新修订取代的通知直接撤下；
    - 仍受影响：没有通知则发警告，已有已发送通知则发更新；
    - 不再受影响：已发送的通知生成关联更正，未发送的只撤下。
    所有通知都以（来源、事件、订阅者、种类/修订）为去重键，重试或重启不会
    再发出一轮。
    """
    actions: list[dict[str, Any]] = []
    current_impacted = set(conclusion.impacted)
    roster_ids = (
        {subscriber.subscriber_id for subscriber in materials.roster.subscribers}
        if materials.roster is not None
        else None
    )
    known_ids = set(roster_ids or ())
    for notification in store.notifications(alert=conclusion.alert, source=conclusion.source):
        known_ids.add(notification["subscriber"])

    def note(action: str, notification: dict[str, Any], reason: str | None = None) -> dict[str, Any]:
        item = {
            "action": action,
            "notification": notification["id"],
            "subscriber": notification["subscriber"],
            "revision": conclusion.revision,
            "kind": notification["kind"],
        }
        if reason is not None:
            item["reason"] = reason
        actions.append(item)
        return item

    for subscriber in sorted(known_ids):
        # 先撤下所有被本修订取代的未发送通知。
        for pending in _pending_for(store, conclusion.source, conclusion.alert, subscriber):
            if pending["revision"] < conclusion.revision:
                withdrawn = store.withdraw(
                    pending["id"],
                    conclusion.revision,
                    f"superseded by revision {conclusion.revision}",
                )
                note("withdraw", withdrawn, "superseded")

        latest_sent = _latest_sent(store, conclusion.source, conclusion.alert, subscriber)
        if subscriber in current_impacted:
            if latest_sent is None or latest_sent["kind"] == "correction":
                # 从未收到，或上一次收到的是“已不再影响”的更正：这是一个新的警告。
                notification, created = _queue_notice(
                    store, conclusion, subscriber, "warning", f"warning@{conclusion.revision}"
                )
                if created:
                    note("queue", notification)
            else:
                # 已经发过且仍在范围内：只有范围或版本真的变化才发更新。
                changed = (
                    latest_sent["boundary_version"] != conclusion.boundary_version
                    or tuple(latest_sent["regions"]) != tuple(conclusion.regions)
                    or tuple(latest_sent["covered_areas"]) != tuple(conclusion.covered_areas)
                )
                if not changed:
                    continue
                notification, created = _queue_notice(
                    store, conclusion, subscriber, "update", f"update@{conclusion.revision}"
                )
                if created:
                    note("queue", notification)
        elif latest_sent is not None and latest_sent["kind"] in ("warning", "update"):
            if roster_ids is not None and subscriber not in roster_ids:
                # 订阅者已不在最新花名册：只撤下未发送通知，不臆造“已不再影响”的更正。
                continue
            reason = "cancelled" if conclusion.status == _CANCELLED else "narrowed"
            notification, created = _queue_notice(
                store,
                conclusion,
                subscriber,
                "correction",
                f"correction@{latest_sent['id']}->{conclusion.revision}",
                correction_of=latest_sent["id"],
                reason=reason,
                payload={
                    "original": {
                        "notification": latest_sent["id"],
                        "revision": latest_sent["revision"],
                        "kind": latest_sent["kind"],
                        "boundary_version": latest_sent["boundary_version"],
                        "regions": latest_sent["regions"],
                        "covered_areas": latest_sent["covered_areas"],
                    }
                },
            )
            if created:
                note("correct", notification, reason)

    return actions


def _evaluation(
    alert: str,
    conclusion: Conclusion,
    note: str,
    actions: list[dict[str, Any]],
    frontier_revision: int | None = None,
) -> dict[str, Any]:
    return {
        "source": conclusion.source,
        "alert": alert,
        "revision": conclusion.revision,
        "status": conclusion.status if conclusion.complete else None,
        "applied": note == "applied",
        "complete": conclusion.complete,
        "impacted": list(conclusion.impacted) if conclusion.complete else [],
        "missing": missing_labels(conclusion.missing),
        "conflicts": [],
        "actions": actions,
        "note": note,
        "boundary_version": conclusion.boundary_version,
        "regions": list(conclusion.regions),
        "covered_areas": list(conclusion.covered_areas),
        "frontier_revision": frontier_revision,
    }


def _record(store: StateStore, evaluation: dict[str, Any]) -> bool:
    previous = [
        row for row in store.evaluations(evaluation["alert"]) if row["source"] == evaluation["source"]
    ]
    if previous:
        last = previous[-1]
        same = all(
            last[name] == evaluation[name]
            for name in (
                "revision",
                "applied",
                "complete",
                "missing",
                "actions",
                "boundary_version",
                "regions",
                "covered_areas",
            )
        )
        if same:
            return False
    store.record_evaluation(evaluation)
    return True


def evaluate_alert(store: StateStore, alert: str, materials: Materials | None = None) -> dict[str, Any]:
    """按修订顺序推进，在第一个缺口处停下。

    缺资料的修订不应用，也不猜测；它之后的修订暂不推进，以免差异建立在错误
    的当前范围上。配套资料补齐后再次评估会从该修订继续，已应用的修订不会
    重复生成通知。
    """
    materials = materials or Materials.load(store)
    by_source: dict[str, list[dict[str, Any]]] = {}
    for observation in store.observations(alert):
        by_source.setdefault(observation["source"], []).append(observation)

    results: list[dict[str, Any]] = []
    for source in sorted(by_source):
        revisions = assemble_source(by_source[source])
        if not revisions:
            continue
        frontier = store.frontier(source, alert)
        last_applied = frontier["revision"] if frontier else -1
        applied: list[Conclusion] = []
        actions: list[dict[str, Any]] = []
        blocked: Conclusion | None = None

        for revision in revisions:
            if revision.revision <= last_applied:
                continue
            conclusion = resolve_conclusion(revision, materials)
            if not conclusion.complete:
                blocked = conclusion
                break
            revision_actions = queue_diff(store, conclusion, materials)
            actions.extend(revision_actions)
            prior = last_applied
            store.set_frontier(
                source,
                alert,
                conclusion.revision,
                conclusion.status,
                conclusion.impacted,
                content_digest(
                    {
                        "status": conclusion.status,
                        "regions": list(conclusion.regions),
                        "covered_areas": list(conclusion.covered_areas),
                    }
                ),
            )
            _record(store, _evaluation(alert, conclusion, "applied", revision_actions, prior))
            applied.append(conclusion)
            last_applied = conclusion.revision

        if blocked is not None:
            evaluation = _evaluation(alert, blocked, "materials-incomplete", [], last_applied)
            _record(store, evaluation)
            results.append(evaluation)
        elif applied:
            results.append(_evaluation(alert, applied[-1], "applied", actions, last_applied))
        else:
            fallback = resolve_conclusion(revisions[-1], materials)
            note = "already-applied" if fallback.complete else "materials-incomplete"
            results.append(_evaluation(alert, fallback, note, [], last_applied))

    conflicts = _source_conflicts(alert, results)
    for result in results:
        result["conflicts"] = conflicts if result["complete"] else []
    return {"alert": alert, "evaluations": results, "conflicts": conflicts}


def _source_conflicts(alert: str, results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """两家来源对同一事件的结论不同时，分别保留并显式列出。"""
    latest: dict[str, dict[str, Any]] = {}
    for item in results:
        if item["complete"]:
            latest[item["source"]] = item
    if len(latest) < 2:
        return []
    fingerprints = {
        (item["status"], item["boundary_version"], tuple(item["regions"]), tuple(item["covered_areas"]))
        for item in latest.values()
    }
    if len(fingerprints) < 2:
        return []
    return [
        {
            "alert": alert,
            "kind": "source-disagreement",
            "sources": [
                {
                    "source": item["source"],
                    "revision": item["revision"],
                    "status": item["status"],
                    "boundary_version": item["boundary_version"],
                    "regions": item["regions"],
                    "covered_areas": item["covered_areas"],
                }
                for item in sorted(latest.values(), key=lambda row: row["source"])
            ],
        }
    ]


def evaluate(store: StateStore, alert: str | None = None) -> dict[str, Any]:
    alerts = [alert] if alert else store.alerts()
    results = [evaluate_alert(store, name) for name in alerts]
    return {
        "alerts": alerts,
        "evaluations": [item for result in results for item in result["evaluations"]],
        "conflicts": [item for result in results for item in result["conflicts"]],
    }


def flush(store: StateStore, deliver=None) -> list[dict[str, Any]]:
    """发送所有待发通知；已发送的不会再次发送。"""
    delivered: list[dict[str, Any]] = []
    for notification in store.notifications(state="pending"):
        if deliver is not None:
            deliver(notification)
        delivered.append(store.mark_sent(notification["id"]))
    return delivered


def ingest_materials(
    store: StateStore,
    boundaries: Iterable[dict[str, Any]] = (),
    rosters: Iterable[dict[str, Any]] = (),
) -> dict[str, Any]:
    added_boundaries, added_rosters = [], []
    for document in boundaries:
        store.add_boundary(document, content_digest(document))
        added_boundaries.append(document.get("version"))
    for document in rosters:
        store.add_roster(document, content_digest(document))
        added_rosters.append(document.get("roster_version"))
    return {"boundaries": added_boundaries, "rosters": added_rosters}


def ingest_records(
    store: StateStore,
    records: list[dict[str, Any]],
    capture_id: int | None = None,
    observed_at: str | None = None,
) -> dict[str, Any]:
    from .warning import valid_records

    fragments, rejected = valid_records(records, capture_id, observed_at)
    created = 0
    for fragment in fragments:
        _, was_created = store.add_observation(fragment)
        created += int(was_created)
    return {
        "observed": len(records),
        "fragments": len(fragments),
        "new_fragments": created,
        "rejected": rejected,
    }


def report(store: StateStore, alert: str) -> dict[str, Any]:
    """按修订展示订阅者实际受到的版本影响、通知去向与未决缺口。"""
    materials = Materials.load(store)
    notifications = store.notifications(alert=alert)

    applied: list[dict[str, Any]] = []
    seen: set[tuple[str, int]] = set()
    for row in store.evaluations(alert):
        key = (row["source"], row["revision"])
        if row["applied"] and key not in seen:
            frontier = store.frontier(row["source"], alert)
            # 只保留真正推进过当前前沿的修订，避免重复评估同一修订造成重影。
            if frontier is not None and row["revision"] > frontier["revision"]:
                continue
            applied.append(row)
            seen.add(key)

    sources: dict[str, Any] = {}
    for row in applied:
        block = sources.setdefault(row["source"], {"source": row["source"], "revisions": [], "subscribers": {}})
        block["revisions"].append(
            {
                "revision": row["revision"],
                "status": row["status"],
                "boundary_version": row["boundary_version"],
                "regions": row["regions"],
                "covered_areas": row["covered_areas"],
                "impacted": row["impacted"],
                "evaluated_at": row["evaluated_at"],
            }
        )

    for block in sources.values():
        block["revisions"].sort(key=lambda item: item["revision"])
        revision_numbers = [item["revision"] for item in block["revisions"]]
        impacted_by_revision = {
            item["revision"]: set(item["impacted"]) for item in block["revisions"]
        }
        subscriber_ids = {
            subscriber.subscriber_id
            for subscriber in (materials.roster.subscribers if materials.roster is not None else ())
        }
        # 即使花名册暂时缺失，已经发过通知的人也要在时间线上可见。
        subscriber_ids |= {
            item["subscriber"]
            for item in notifications
            if item["source"] == block["source"]
        }
        for subscriber_id in sorted(subscriber_ids):
            subscriber = (
                next(
                    (item for item in materials.roster.subscribers if item.subscriber_id == subscriber_id),
                    None,
                )
                if materials.roster is not None
                else None
            )
            detail = block["subscribers"].setdefault(
                subscriber_id,
                {
                    "id": subscriber_id,
                    "registered_region": subscriber.region if subscriber else None,
                    "registered_boundary": subscriber.boundary_version if subscriber else None,
                    "revisions": {},
                },
            )
            for revision in revision_numbers:
                detail["revisions"][str(revision)] = {
                    "affected": subscriber_id in impacted_by_revision[revision],
                    "notice": _notice_for(
                        notifications,
                        source=block["source"],
                        subscriber=subscriber_id,
                        revision=revision,
                    ),
                }

    notification_rows = [
        {
            "id": item["id"],
            "source": item["source"],
            "alert": item["alert"],
            "subscriber": item["subscriber"],
            "revision": item["revision"],
            "kind": item["kind"],
            "state": item["state"],
            "correction_of": item["correction_of"],
            "reason": item["reason"],
            "boundary_version": item["boundary_version"],
            "regions": item["regions"],
            "covered_areas": item["covered_areas"],
            "created_at": item["created_at"],
            "sent_at": item["sent_at"],
            "withdrawn_at": item["withdrawn_at"],
        }
        for item in notifications
    ]

    latest_by_source: dict[str, dict[str, Any]] = {}
    for row in store.evaluations(alert):
        latest_by_source[row["source"]] = row
    missing = sorted(
        {label for row in latest_by_source.values() if not row["complete"] for label in row["missing"]}
    )
    latest_applied = _latest_per_source(applied)
    conflicts = _source_conflicts(alert, latest_applied)

    return {
        "alert": alert,
        "sources": sources,
        "missing": missing,
        "conflicts": conflicts,
        "notifications": notification_rows,
    }


def _latest_per_source(applied: list[dict[str, Any]]) -> list[dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for row in applied:
        latest[row["source"]] = {
            "source": row["source"],
            "revision": row["revision"],
            "status": row["status"],
            "boundary_version": row["boundary_version"],
            "regions": row["regions"],
            "covered_areas": row["covered_areas"],
            "complete": True,
        }
    return list(latest.values())


def _notice_for(
    notifications: list[dict[str, Any]], source: str, subscriber: str, revision: int
) -> dict[str, Any] | None:
    matches = [
        item
        for item in notifications
        if item["source"] == source
        and item["subscriber"] == subscriber
        and item["revision"] == revision
        and item["kind"] in ("warning", "update", "correction")
    ]
    if not matches:
        return None
    item = matches[-1]
    return {
        "id": item["id"],
        "kind": item["kind"],
        "state": item["state"],
        "correction_of": item["correction_of"],
    }


def timeline(store: StateStore, alert: str) -> dict[str, Any]:
    """按修订展示订阅者实际受到的版本影响及通知去向。"""
    return report(store, alert)
