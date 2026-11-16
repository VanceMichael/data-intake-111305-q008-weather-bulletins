"""业务处理：把来源观察拼成有依据的事件，并计算通知差异。

处理原则：

* 影响范围只由“区域代码 + 该记录声明的边界版本”解析得到，不用正文猜。
* 每家来源各自保留结论；来源之间不一致时标记 ``disputed`` 并并列展示，
  不合成“唯一真相”。
* 通知按业务键去重，采集重试不会重复发出一轮通知或更正。
* 撤销时未发送的通知转为 ``retracted``；已发送的通知至少生成一条关联更正，
  更正可指向“同一事件的最新撤销结论”，重复处理只会补挂新的原通知。
"""

from .boundaries import resolve_regions
from .domain import RecordError, parse_record
from .timeline import build_timeline, resolve_audience


def ingest_records(store, payload, source="feed"):
    """写入一批来源记录，返回 ``(inserted, errors)``，坏记录单独记入错误表。"""
    items = payload.get("items") if isinstance(payload, dict) else payload
    if not isinstance(items, list):
        raise ValueError("source payload must contain an items array")
    inserted = 0
    errors = 0
    for index, raw in enumerate(items):
        try:
            record = parse_record(raw)
        except RecordError as error:
            store.record_error(
                source, "invalid_record", f"items[{index}]: {error}", raw if isinstance(raw, dict) else None
            )
            errors += 1
            continue
        if store.insert_observation(record):
            inserted += 1
    return inserted, errors


def ingest_boundary_documents(store, loaded, errors=()):
    """写入边界版本；相同版本文本未变时跳过。返回 ``(applied, failed)``。"""
    for error in errors:
        store.record_error(error["source"], error["reason"], error["detail"])
    applied = 0
    for item in loaded:
        document = item["document"]
        if store.boundary_digest(document["version"]) == item["digest"]:
            continue
        store.upsert_boundaries(document, item["digest"], item["source"])
        applied += 1
    return applied, len(errors)


def _resolve(catalog, item):
    if item["status"] == "cancelled" and not item["regions"]:
        return frozenset(), []
    return resolve_regions(catalog, item["boundary_version"], item["regions"])


def event_keys(store):
    return store.event_keys()


def source_conclusions(store, event_key, catalog):
    """每个来源的最新结论，含区域解析结果与同修订冲突。"""
    latest = store.latest_per_source(event_key)
    conclusions = {}
    for issuer, item in latest.items():
        places, unresolved = _resolve(catalog, item)
        same_revision = store.revisions_for(event_key, issuer, item["revision"])
        conflicts = [
            {"fingerprint": row["fingerprint"], "raw": row["raw"]}
            for row in same_revision
            if row["fingerprint"] != item["fingerprint"]
        ]
        conclusions[issuer] = {
            "issuer": issuer,
            "alert": item["alert"],
            "revision": item["revision"],
            "status": item["status"],
            "regions": item["regions"],
            "boundary_version": item["boundary_version"],
            "places": None if places is None else sorted(places),
            "unresolved": unresolved,
            "same_revision_conflicts": conflicts,
        }
    return conclusions


def divergence(conclusions):
    """比较各来源结论，返回 ``(disputed, detail)``。

    资料未到齐的来源不参与比较，但会列在 ``pending_materials`` 中——未解析
    不等于一致。
    """
    resolved = [item for item in conclusions.values() if item["places"] is not None]
    pending = [item["issuer"] for item in conclusions.values() if item["places"] is None]
    if len(resolved) < 2:
        return False, {"pending_materials": pending}
    signatures = {(item["status"], tuple(item["places"])) for item in resolved}
    detail = {
        "by_issuer": {
            item["issuer"]: {"status": item["status"], "places": item["places"]}
            for item in sorted(resolved, key=lambda entry: entry["issuer"])
        },
        "pending_materials": pending,
    }
    return len(signatures) > 1, detail


def timeline(store, event_key, catalog):
    subscribers = store.subscribers()

    def resolve(codes, version):
        return resolve_regions(catalog, version, codes)

    return build_timeline(store.observations(event_key), catalog, subscribers, resolve)


def process_event(store, event_key, catalog, subscribers):
    """按来源最新修订维护通知，返回本事件的通知动作。"""
    actions = []
    for issuer, item in store.latest_per_source(event_key).items():
        revision = item["revision"]
        actions.extend(_retract_superseded(store, event_key, issuer, revision))
        places, unresolved = _resolve(catalog, item)
        if item["status"] == "cancelled":
            actions.extend(_cancel(store, event_key, issuer, revision))
        elif places is None:
            actions.append(_block(store, event_key, issuer, revision, unresolved))
        else:
            actions.extend(_publish(store, event_key, issuer, revision, places, subscribers))
    return actions


def _retract_superseded(store, event_key, issuer, revision):
    """更高修订到达时，撤下更早修订中尚未发出的通知（含因缺资料暂缓的）。"""
    actions = []
    for notification in store.notifications(event_key, issuer):
        if notification["reason"] != "alert" or notification["status"] not in ("pending", "blocked"):
            continue
        if notification["revision"] < revision and store.transition(notification["id"], notification["status"], "retracted"):
            actions.append({"action": "retracted", "notification": notification["id"], "reason": "superseded"})
    return actions


def _block(store, event_key, issuer, revision, unresolved):
    existing = store.find_notification(event_key, issuer, revision, "alert")
    if existing is not None and existing["status"] not in ("blocked", "sent"):
        # 先前队列中的通知现在缺资料；撤下并说明缺什么。
        store.replace_notification(existing["id"], "blocked", [], unresolved, {})
        return {"action": "blocked", "notification": existing["id"], "missing": unresolved}
    if existing is not None:
        return {"action": "unchanged", "notification": existing["id"]}
    notification_id = store.insert_notification(
        event_key, issuer, revision, "alert", None, "blocked", [], unresolved, {}
    )
    return {"action": "blocked", "notification": notification_id, "missing": unresolved}


def _publish(store, event_key, issuer, revision, places, subscribers):
    audience = resolve_audience(places, subscribers)
    targets = {}
    for subscriber, member in audience.items():
        if member.affected:
            previous = store.prior_sent_subscriber(event_key, issuer, subscriber, revision)
            targets[subscriber] = (sorted(member.affected), previous)
    existing = store.find_notification(event_key, issuer, revision, "alert")
    actions = []
    if existing is not None:
        if existing["status"] == "blocked":
            if targets:
                store.replace_notification(existing["id"], "pending", sorted(places), [], targets)
                actions.append({"action": "released", "notification": existing["id"]})
            else:
                store.replace_notification(existing["id"], "empty", sorted(places), [], {})
                actions.append({"action": "empty", "notification": existing["id"]})
        else:
            actions.append({"action": "unchanged", "notification": existing["id"]})
    elif not targets:
        # 有边界资料但无人受影响：记录结论，不排队通知。
        notification_id = store.insert_notification(
            event_key, issuer, revision, "alert", None, "empty", sorted(places), [], {}
        )
        actions.append({"action": "empty", "notification": notification_id})
    else:
        notification_id = store.insert_notification(
            event_key, issuer, revision, "alert", None, "pending", sorted(places), [], targets
        )
        actions.append({"action": "queued", "notification": notification_id, "revision": revision})
    # 范围缩小可能把上一版已发送的订阅者排除在外：为每个被排除的订阅者生成
    # 与原通知关联的更正，说明其不再受影响。
    actions.extend(_narrowed_corrections(store, event_key, issuer, revision, set(targets)))
    return actions


def _narrowed_corrections(store, event_key, issuer, revision, affected):
    """对“最后一次送达仍在本版范围内、但本版已排除”的订阅者生成关联更正。

    只有订阅者收到的最后一条通知仍被视为现行；更早的通知已被后续修订取代，
    不逐条更正，因此每个订阅者至多收到一轮更正。
    """
    actions = []
    for notification_id, targets in store.latest_sent_targets(event_key, issuer, revision).items():
        already = store.corrected_subscribers(notification_id)
        dropped = {
            subscriber: (places, notification_id)
            for subscriber, places in targets.items()
            if subscriber not in affected and subscriber not in already
        }
        if not dropped:
            continue
        correction = store.first_correction(notification_id, "narrowed")
        if correction is not None:
            store.add_targets(correction["id"], dropped)
            actions.append({"action": "amended", "notification": correction["id"], "corrects": notification_id})
            continue
        correction_id = store.insert_notification(
            event_key, issuer, revision, "correction", notification_id, "pending", [], [], dropped, cause="narrowed"
        )
        actions.append({"action": "corrected", "notification": correction_id, "corrects": notification_id})
    return actions


def _cancel(store, event_key, issuer, revision):
    actions = []
    for notification in store.notifications(event_key, issuer):
        if notification["reason"] != "alert" or notification["status"] != "pending":
            continue
        if store.transition(notification["id"], "pending", "retracted"):
            actions.append({"action": "retracted", "notification": notification["id"], "reason": "cancelled"})
    # 只有每个订阅者最后一条仍现行的已送达通知需要更正；更早的已被取代。
    for notification_id, targets in store.latest_sent_targets(event_key, issuer, revision).items():
        already = store.corrected_subscribers(notification_id)
        pending_targets = {
            subscriber: (places, notification_id)
            for subscriber, places in targets.items()
            if subscriber not in already
        }
        if not pending_targets:
            continue
        correction = store.first_correction(notification_id, "cancelled")
        if correction is not None:
            store.add_targets(correction["id"], pending_targets)
            actions.append({"action": "amended", "notification": correction["id"], "corrects": notification_id})
            continue
        correction_id = store.insert_notification(
            event_key, issuer, revision, "correction", notification_id, "pending", [], [], pending_targets, cause="cancelled"
        )
        actions.append({"action": "corrected", "notification": correction_id, "corrects": notification_id})
    return actions


def deliver(store, notification_id):
    """把一条待发送通知标记为已发送。重复调用返回 ``False``，不会重发。"""
    return store.transition(notification_id, "pending", "sent")
