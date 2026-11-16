"""面向值班员的事件报告。"""

from .engine import divergence, source_conclusions, timeline


def _unresolved_summary(entries):
    """把缺失资料整理成可读条目，明确“缺什么”。"""
    summary = []
    for entry in entries:
        reason = entry.get("reason")
        if reason == "missing_boundary_version":
            summary.append("缺少边界版本 " + str(entry.get("boundary_version")))
        elif reason == "missing_region":
            summary.append("边界版本 " + str(entry.get("boundary_version")) + " 缺区域 " + str(entry.get("region")))
        elif reason == "missing_regions":
            summary.append("活动预警未声明区域")
        else:
            summary.append(str(reason))
    return summary


def _boundary_status(conclusions, catalog):
    status = {}
    for issuer, item in sorted(conclusions.items()):
        version = item["boundary_version"]
        status[issuer] = {
            "boundary_version": version,
            "known": version in catalog if version else False,
            "resolved": item["places"] is not None,
            "missing": _unresolved_summary(item["unresolved"]),
        }
    return status


def event_report(store, event_key):
    catalog = store.boundary_catalog()
    conclusions = source_conclusions(store, event_key, catalog)
    disputed, detail = divergence(conclusions)
    steps = timeline(store, event_key, catalog)
    notifications = []
    for notification in store.notifications(event_key):
        targets = store.targets(notification["id"])
        notifications.append({**notification, "targets": targets})
    missing = []
    for issuer, item in sorted(conclusions.items()):
        if item["places"] is None:
            missing.append({"issuer": issuer, "unresolved": item["unresolved"]})
    return {
        "event": event_key,
        "sources": conclusions,
        "boundary": _boundary_status(conclusions, catalog),
        "disputed": disputed,
        "divergence": detail,
        "materials_missing": missing,
        "timeline": [step.as_dict() for step in steps],
        "notifications": notifications,
    }


def events_report(store):
    return {
        "events": [event_report(store, event_key) for event_key in store.event_keys()],
        "boundary_versions": store.boundary_versions(),
        "errors": store.errors(),
    }
