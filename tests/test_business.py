import json
import tempfile
import unittest
from pathlib import Path

from weather_bulletins.boundaries import BoundaryError, load_documents, normalize_document, resolve_regions
from weather_bulletins.domain import RecordError, parse_record
from weather_bulletins.engine import (
    deliver,
    divergence,
    ingest_records,
    process_event,
    source_conclusions,
    timeline,
)
from weather_bulletins.report import event_report
from weather_bulletins.store import Store

FIXTURES = Path(__file__).parents[1] / "fixtures"


def load_boundaries(*versions):
    documents = {}
    for version in versions:
        document = normalize_document(
            json.loads((FIXTURES / "boundaries" / (version + ".json")).read_text(encoding="utf-8"))
        )
        documents[version] = document
    return documents


def catalog_of(versions):
    return {
        version: {region["code"]: {"name": region["name"], "covers": region["covers"]} for region in document["regions"]}
        for version, document in versions.items()
    }


def seed(store, versions, records, subscribers=None):
    for version, document in versions.items():
        store.upsert_boundaries(document, "digest-" + version, "test")
    if subscribers is None:
        subscribers = json.loads((FIXTURES / "subscribers.json").read_text(encoding="utf-8"))
    store.upsert_subscribers(subscribers)
    ingest_records(store, {"items": records})


def process(store):
    return process_event(store, "RAIN-5", store.boundary_catalog(), store.subscribers())


def record(issuer, revision, regions, status="active", boundary_version="2026-b", **extra):
    return {
        "issuer": issuer,
        "alert": "RAIN-5",
        "revision": revision,
        "regions": regions,
        "boundary_version": boundary_version,
        "status": status,
        **extra,
    }


class BoundaryTests(unittest.TestCase):
    def test_region_code_is_resolved_with_declared_version(self):
        versions = load_boundaries("2026-a", "2026-c")
        old = resolve_regions(catalog_of(versions), "2026-a", ["county-1"])
        new = resolve_regions(catalog_of(versions), "2026-c", ["county-1"])
        self.assertEqual(old[0], frozenset({"place-n", "place-e", "place-c", "place-s", "place-w", "place-f"}))
        self.assertEqual(new[0], frozenset({"place-f", "place-g"}))
        self.assertNotEqual(old[0], new[0])

    def test_missing_version_is_not_guessed_from_text(self):
        places, missing = resolve_regions({}, "2026-z", ["county-1"])
        self.assertIsNone(places)
        self.assertEqual(missing[0]["reason"], "missing_boundary_version")

    def test_missing_region_lists_what_is_missing(self):
        catalog = {"2026-b": {"town-11": {"name": "十一", "covers": ["place-n"]}}}
        places, missing = resolve_regions(catalog, "2026-b", ["town-11", "town-99"])
        self.assertIsNone(places)
        self.assertIn({"reason": "missing_region", "boundary_version": "2026-b", "region": "town-99"}, missing)

    def test_invalid_document_is_rejected_not_silently_emptied(self):
        for document in ({}, {"version": "x"}, {"version": "x", "regions": []}, {"version": "x", "regions": [{"code": "a", "covers": []}]}):
            with self.subTest(document=document), self.assertRaises(BoundaryError):
                normalize_document(document)

    def test_duplicate_codes_are_rejected(self):
        with self.assertRaisesRegex(BoundaryError, "duplicate region code"):
            normalize_document({"version": "x", "regions": [
                {"code": "a", "covers": ["one"]}, {"code": "a", "covers": ["two"]},
            ]})

    def test_bad_boundary_file_is_reported_not_fatal(self):
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            (folder / "good.json").write_text(json.dumps({"version": "v1", "regions": [{"code": "a", "covers": ["x"]}]}), encoding="utf-8")
            (folder / "bad.json").write_text("{not json", encoding="utf-8")
            loaded, errors = load_documents([folder])
            self.assertEqual([item["document"]["version"] for item in loaded], ["v1"])
            self.assertEqual(len(errors), 1)
            self.assertEqual(errors[0]["reason"], "invalid_boundary_document")


class RecordTests(unittest.TestCase):
    def test_active_record_requires_regions(self):
        with self.assertRaisesRegex(RecordError, "at least one region"):
            parse_record(record("weather-a", 1, []))

    def test_cancelled_record_may_be_empty(self):
        parsed = parse_record(record("weather-a", 3, [], status="cancelled"))
        self.assertEqual(parsed.status, "cancelled")

    def test_unknown_status_is_rejected(self):
        with self.assertRaisesRegex(RecordError, "unsupported status"):
            parse_record(record("weather-a", 1, ["county-1"], status="paused"))

    def test_fingerprint_is_content_based(self):
        first = parse_record(record("weather-a", 2, ["town-11"]))
        second = parse_record(record("weather-a", 2, ["town-11"]))
        third = parse_record(record("weather-a", 2, ["town-11", "town-12"]))
        self.assertEqual(first.fingerprint, second.fingerprint)
        self.assertNotEqual(first.fingerprint, third.fingerprint)


class EventAssemblyTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temporary.name) / "state.sqlite3")

    def tearDown(self):
        self.store.close()
        self.temporary.cleanup()

    def test_out_of_order_capture_uses_highest_revision(self):
        seed(self.store, load_boundaries("2026-a", "2026-b"), [
            record("weather-a", 1, ["county-1"], boundary_version="2026-a"),
            record("weather-a", 2, ["town-11", "town-12"], boundary_version="2026-b"),
        ])
        conclusions = source_conclusions(self.store, "RAIN-5", self.store.boundary_catalog())
        self.assertEqual(conclusions["weather-a"]["revision"], 2)
        self.assertEqual(
            conclusions["weather-a"]["places"],
            ["place-c", "place-e", "place-n", "place-s", "place-w"],
        )

    def test_late_arriving_older_revision_does_not_win(self):
        seed(self.store, load_boundaries("2026-a", "2026-b"), [
            record("weather-a", 2, ["town-11", "town-12"], boundary_version="2026-b"),
        ])
        ingest_records(self.store, {"items": [record("weather-a", 1, ["county-1"], boundary_version="2026-a")]})
        conclusions = source_conclusions(self.store, "RAIN-5", self.store.boundary_catalog())
        self.assertEqual(conclusions["weather-a"]["revision"], 2)

    def test_missing_boundary_materials_are_reported(self):
        seed(self.store, {}, [record("weather-a", 2, ["town-11"], boundary_version="2099-x")])
        conclusions = source_conclusions(self.store, "RAIN-5", self.store.boundary_catalog())
        self.assertIsNone(conclusions["weather-a"]["places"])
        self.assertEqual(conclusions["weather-a"]["unresolved"][0]["reason"], "missing_boundary_version")
        report = event_report(self.store, "RAIN-5")
        self.assertEqual(report["boundary"]["weather-a"]["missing"], ["缺少边界版本 2099-x"])

    def test_same_revision_conflict_is_kept_not_overwritten(self):
        seed(self.store, load_boundaries("2026-a", "2026-b"), [
            record("weather-a", 2, ["town-11"]),
            record("weather-a", 2, ["town-12"]),
        ])
        conclusions = source_conclusions(self.store, "RAIN-5", self.store.boundary_catalog())
        self.assertEqual(len(conclusions["weather-a"]["same_revision_conflicts"]), 1)

    def test_two_sources_disagree_keeps_both(self):
        seed(self.store, load_boundaries("2026-a", "2026-b"), [
            record("weather-a", 2, ["town-11", "town-12"], boundary_version="2026-b"),
            record("weather-b", 2, ["county-1"], boundary_version="2026-a"),
        ])
        conclusions = source_conclusions(self.store, "RAIN-5", self.store.boundary_catalog())
        disputed, detail = divergence(conclusions)
        self.assertTrue(disputed)
        self.assertEqual(set(detail["by_issuer"]), {"weather-a", "weather-b"})
        report = event_report(self.store, "RAIN-5")
        self.assertTrue(report["disputed"])
        self.assertEqual(report["sources"]["weather-b"]["regions"], ["county-1"])

    def test_pending_material_is_not_treated_as_agreement(self):
        seed(self.store, load_boundaries("2026-a"), [
            record("weather-a", 2, ["county-1"], boundary_version="2026-a"),
            record("weather-b", 2, ["town-11"], boundary_version="2099-x"),
        ])
        conclusions = source_conclusions(self.store, "RAIN-5", self.store.boundary_catalog())
        disputed, detail = divergence(conclusions)
        self.assertFalse(disputed)
        self.assertEqual(detail["pending_materials"], ["weather-b"])


class NotificationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.state = Path(self.temporary.name) / "state.sqlite3"
        self.store = Store(self.state)

    def tearDown(self):
        self.store.close()
        self.temporary.cleanup()

    def reopen(self):
        self.store.close()
        self.store = Store(self.state)

    def test_narrowing_nobody_affected_emits_correction_for_dropped_subscriber(self):
        seed(self.store, load_boundaries("2026-a", "2026-b"), [
            record("weather-a", 1, ["county-1"], boundary_version="2026-a"),
        ])
        actions = process(self.store)
        self.assertEqual(actions[0]["action"], "queued")
        sent_id = actions[0]["notification"]
        self.assertTrue(deliver(self.store, sent_id))

        ingest_records(self.store, {"items": [record("weather-a", 2, ["town-11", "town-12"], boundary_version="2026-b")]})
        actions = process(self.store)
        corrected = [item for item in actions if item["action"] == "corrected" and item["corrects"] == sent_id]
        self.assertEqual(len(corrected), 1)
        correction = self.store.get_notification(corrected[0]["notification"])
        self.assertEqual(correction["reason"], "correction")
        self.assertEqual(correction["link_id"], sent_id)
        self.assertEqual([target["subscriber"] for target in self.store.targets(correction["id"])], ["subscriber-far"])

    def test_cancel_retracts_pending_and_corrects_sent(self):
        seed(self.store, load_boundaries("2026-a", "2026-b"), [
            record("weather-a", 1, ["county-1"], boundary_version="2026-a"),
        ])
        first = process(self.store)[0]["notification"]
        deliver(self.store, first)
        ingest_records(self.store, {"items": [record("weather-a", 2, ["town-11", "town-12"], boundary_version="2026-b")]})
        second = [item for item in process(self.store) if item["action"] == "queued"][0]["notification"]

        ingest_records(self.store, {"items": [record("weather-a", 3, [], status="cancelled")]})
        actions = process(self.store)
        retracted = [item for item in actions if item["action"] == "retracted"]
        self.assertEqual([item["notification"] for item in retracted], [second])
        self.assertEqual(self.store.get_notification(second)["status"], "retracted")
        corrections = [item for item in actions if item["action"] == "corrected"]
        self.assertEqual([item["corrects"] for item in corrections], [first])

    def test_retry_after_cancel_does_not_duplicate_correction(self):
        seed(self.store, load_boundaries("2026-a", "2026-b"), [
            record("weather-a", 1, ["county-1"], boundary_version="2026-a"),
        ])
        first = process(self.store)[0]["notification"]
        deliver(self.store, first)
        ingest_records(self.store, {"items": [record("weather-a", 3, [], status="cancelled")]})
        process(self.store)
        before = self.store.notifications()
        ingest_records(self.store, {"items": [record("weather-a", 3, [], status="cancelled")]})
        process(self.store)
        process(self.store)
        self.assertEqual(len(self.store.notifications()), len(before))
        self.assertEqual(len([item for item in self.store.notifications() if item["reason"] == "correction"]), 1)

    def test_blocked_notification_is_retracted_by_newer_revision(self):
        seed(self.store, {}, [record("weather-a", 2, ["town-11"], boundary_version="2099-x")])
        blocked = process(self.store)[0]["notification"]
        ingest_records(self.store, {"items": [record("weather-a", 3, [], status="cancelled")]})
        actions = process(self.store)
        self.assertIn({"action": "retracted", "notification": blocked, "reason": "superseded"}, actions)
        self.assertEqual(self.store.get_notification(blocked)["status"], "retracted")

    def test_duplicate_capture_does_not_queue_second_notification(self):
        seed(self.store, load_boundaries("2026-b"), [record("weather-a", 2, ["town-11"])])
        first = process(self.store)
        inserted = ingest_records(self.store, {"items": [record("weather-a", 2, ["town-11"])]})
        second = process(self.store)
        self.assertEqual(inserted, (0, 0))
        self.assertEqual([item["action"] for item in second], ["unchanged"])
        self.assertEqual(len(self.store.notifications()), len(first))

    def test_blocked_notification_released_when_boundary_arrives(self):
        seed(self.store, {}, [record("weather-a", 2, ["town-11"], boundary_version="2099-x")])
        actions = process(self.store)
        self.assertEqual(actions[0]["action"], "blocked")
        self.assertEqual(actions[0]["missing"][0]["reason"], "missing_boundary_version")
        blocked_id = actions[0]["notification"]

        document = normalize_document({"version": "2099-x", "regions": [{"code": "town-11", "covers": ["place-n"]}]})
        self.store.upsert_boundaries(document, "digest", "test")
        actions = process(self.store)
        self.assertEqual(actions[0]["action"], "released")
        self.assertEqual(actions[0]["notification"], blocked_id)
        self.assertEqual(self.store.get_notification(blocked_id)["status"], "pending")

    def test_progressive_narrowing_amends_single_correction(self):
        seed(self.store, load_boundaries("2026-a", "2026-b"), [
            record("weather-a", 1, ["county-1"], boundary_version="2026-a"),
        ])
        first = process(self.store)[0]["notification"]
        self.assertTrue(deliver(self.store, first))

        # 修订 2 只覆盖两个乡镇，外县组被排除。
        ingest_records(self.store, {"items": [record("weather-a", 2, ["town-11", "town-12"], boundary_version="2026-b")]})
        corrections = [item for item in process(self.store) if item["action"] == "corrected" and item["corrects"] == first]
        self.assertEqual(len(corrections), 1)
        correction_id = corrections[0]["notification"]
        self.assertEqual(
            [target["subscriber"] for target in self.store.targets(correction_id)], ["subscriber-far"]
        )

        # 修订 3 进一步只保留北区，南区新被排除；更正应追加而不是新建。
        ingest_records(self.store, {"items": [record("weather-a", 3, ["town-11"], boundary_version="2026-b")]})
        actions = process(self.store)
        amended = [item for item in actions if item["action"] == "amended"]
        self.assertEqual([item["notification"] for item in amended], [correction_id])
        self.assertEqual(
            [target["subscriber"] for target in self.store.targets(correction_id)],
            ["subscriber-far", "subscriber-south"],
        )
        self.assertEqual(len([item for item in self.store.notifications() if item["reason"] == "correction"]), 1)

    def test_cancellation_does_not_repeat_narrowing_correction(self):
        seed(self.store, load_boundaries("2026-a", "2026-b"), [
            record("weather-a", 1, ["county-1"], boundary_version="2026-a"),
        ])
        first = process(self.store)[0]["notification"]
        deliver(self.store, first)
        ingest_records(self.store, {"items": [record("weather-a", 2, ["town-11", "town-12"], boundary_version="2026-b")]})
        process(self.store)
        ingest_records(self.store, {"items": [record("weather-a", 3, [], status="cancelled")]})
        actions = process(self.store)
        cancelled = [
            item for item in actions
            if item["action"] == "corrected" and item["corrects"] == first
            and self.store.get_notification(item["notification"])["cause"] == "cancelled"
        ]
        self.assertEqual(len(cancelled), 1)
        subscribers = [target["subscriber"] for target in self.store.targets(cancelled[0]["notification"])]
        # 外县组已因范围缩小收到更正，撤销不再重复通知。
        self.assertEqual(subscribers, ["subscriber-north", "subscriber-south"])

    def test_state_survives_restart_and_continues(self):
        seed(self.store, load_boundaries("2026-a", "2026-b"), [
            record("weather-a", 1, ["county-1"], boundary_version="2026-a"),
        ])
        first = process(self.store)[0]["notification"]
        deliver(self.store, first)
        self.reopen()
        ingest_records(self.store, {"items": [record("weather-a", 2, ["town-11", "town-12"], boundary_version="2026-b")]})
        actions = process(self.store)
        self.assertEqual([item["action"] for item in actions], ["queued", "corrected"])
        self.reopen()
        self.assertEqual(self.store.get_notification(first)["status"], "sent")
        self.assertEqual(len(self.store.notifications()), 3)

    def test_restart_between_capture_and_processing_does_not_duplicate(self):
        # 模拟采集已入库但进程在发布前退出；重启后重新处理仍只发一轮。
        seed(self.store, load_boundaries("2026-b"), [record("weather-a", 2, ["town-11"])])
        self.reopen()
        first = process(self.store)
        self.assertEqual([item["action"] for item in first], ["queued"])
        self.reopen()
        # 采集重试写入同一记录，再次处理不新增。
        inserted = ingest_records(self.store, {"items": [record("weather-a", 2, ["town-11"])]})
        self.assertEqual(inserted, (0, 0))
        second = process(self.store)
        self.assertEqual([item["action"] for item in second], ["unchanged"])
        self.assertEqual(len(self.store.notifications()), 1)

    def test_pending_notification_survives_restart_unsent(self):
        seed(self.store, load_boundaries("2026-b"), [record("weather-a", 2, ["town-11"])])
        notification = process(self.store)[0]["notification"]
        self.reopen()
        self.assertEqual(self.store.get_notification(notification)["status"], "pending")
        self.assertEqual(len(self.store.targets(notification)), 1)
        self.assertTrue(deliver(self.store, notification))
        self.reopen()
        self.assertEqual(self.store.get_notification(notification)["status"], "sent")
        self.assertFalse(deliver(self.store, notification))


class TimelineTests(unittest.TestCase):
    def test_timeline_marks_missing_materials_as_unknown(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = Store(Path(temporary) / "state.sqlite3")
            try:
                seed(store, {}, [record("weather-a", 2, ["town-11"], boundary_version="2099-x")])
                steps = timeline(store, "RAIN-5", store.boundary_catalog())
                self.assertEqual(steps[0].audience["subscriber-north"].impact, "unknown")
                self.assertEqual(steps[0].transition["subscriber-north"]["change"], "unknown")
                report = event_report(store, "RAIN-5")
                self.assertEqual(report["materials_missing"][0]["unresolved"][0]["reason"], "missing_boundary_version")
            finally:
                store.close()

    def test_timeline_shows_expand_narrow_cancel_per_subscriber(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = Store(Path(temporary) / "state.sqlite3")
            try:
                seed(store, load_boundaries("2026-a", "2026-b", "2026-c"), [
                    record("weather-a", 1, ["county-1"], boundary_version="2026-a"),
                    record("weather-a", 2, ["town-11", "town-12"], boundary_version="2026-b"),
                    record("weather-a", 3, [], status="cancelled"),
                ])
                steps = timeline(store, "RAIN-5", store.boundary_catalog())
                self.assertEqual([step.revision for step in steps], [1, 2, 3])
                far = [step.audience["subscriber-far"].impact for step in steps]
                self.assertEqual(far, ["partially_affected", "unaffected", "unaffected"])
                transitions = [step.transition["subscriber-far"]["change"] for step in steps]
                self.assertEqual(transitions, ["added", "removed", "unchanged"])
                north = [step.transition["subscriber-north"]["change"] for step in steps]
                self.assertEqual(north, ["added", "unchanged", "cancelled"])
            finally:
                store.close()

    def test_code_reuse_across_versions_changes_audience(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = Store(Path(temporary) / "state.sqlite3")
            try:
                seed(store, load_boundaries("2026-a", "2026-c"), [
                    record("weather-a", 1, ["county-1"], boundary_version="2026-a"),
                    record("weather-a", 2, ["county-1"], boundary_version="2026-c"),
                ])
                steps = timeline(store, "RAIN-5", store.boundary_catalog())
                # 2026-a 的 county-1 含 place-f；2026-c 的 county-1 只含 place-f/place-g。
                self.assertEqual(steps[0].transition["subscriber-south"]["change"], "added")
                self.assertEqual(steps[1].transition["subscriber-south"]["change"], "removed")
                # 外县组在 2026-a 的 county-1 里已含 place-f，换成 2026-c 后新增 place-g。
                self.assertEqual(steps[1].transition["subscriber-far"]["change"], "expanded")
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
