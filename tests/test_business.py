import json
import tempfile
import unittest
from pathlib import Path

from weather_bulletins.boundaries import (
    BoundaryCatalog,
    MaterialError,
    SubscriberRoster,
    UnknownRegion,
)
from weather_bulletins.engine import assemble_source, ingest_materials, ingest_records, merge_revision
from weather_bulletins.origin import serve
from weather_bulletins.pipeline import Pipeline
from weather_bulletins.store import StateStore


def boundary(version, regions):
    return {"version": version, "regions": regions}


def region(code, area_id, name=None, parent=None, **extra):
    return {"code": code, "area_id": area_id, "name": name or code, "parent": parent, **extra}


def materials():
    path = Path(__file__).parents[1] / "fixtures" / "materials.json"
    return json.loads(path.read_text(encoding="utf-8"))


def scenario(routes):
    return {
        "identity_fields": ["issuer", "alert", "revision"],
        "source_records": [],
        "portal": {"csrf": "test"},
        "routes": routes,
    }


def feed(*items):
    return {"json": {"items": list(items)}}


def record(revision, regions, version="2026-a", status="active", issuer="weather-a", alert="RAIN-5", body=None):
    item = {
        "issuer": issuer,
        "alert": alert,
        "revision": revision,
        "regions": regions,
        "boundary_version": version,
        "status": status,
    }
    if body is not None:
        item["body"] = body
    return item


class BoundaryCatalogTests(unittest.TestCase):
    def test_area_identity_is_scoped_to_one_version(self):
        first = BoundaryCatalog.from_document(
            boundary("v1", [region("town-9", "area-west")])
        )
        second = BoundaryCatalog.from_document(
            boundary("v2", [region("town-9", "area-east")])
        )
        self.assertEqual(first.area_ids("town-9"), frozenset({"area-west"}))
        self.assertEqual(second.area_ids("town-9"), frozenset({"area-east"}))

    def test_missing_region_is_not_guessed(self):
        catalog = BoundaryCatalog.from_document(boundary("v1", [region("town-1", "a-1")]))
        self.assertFalse(catalog.has("town-2"))
        with self.assertRaises(UnknownRegion):
            catalog.region("town-2")

    def test_duplicate_code_and_area_are_rejected(self):
        with self.assertRaisesRegex(MaterialError, "duplicate region code"):
            BoundaryCatalog.from_document(boundary("v1", [region("a", "1"), region("a", "2")]))
        with self.assertRaisesRegex(MaterialError, "area id"):
            BoundaryCatalog.from_document(boundary("v1", [region("a", "1"), region("b", "1")]))

    def test_parent_must_exist_and_be_acyclic(self):
        with self.assertRaisesRegex(MaterialError, "unknown parent"):
            BoundaryCatalog.from_document(boundary("v1", [region("a", "1", parent="missing")]))
        with self.assertRaisesRegex(MaterialError, "cycle"):
            BoundaryCatalog.from_document(
                boundary("v1", [region("a", "1", parent="b"), region("b", "2", parent="a")])
            )

    def test_descendants_carry_subscription_downwards(self):
        catalog = BoundaryCatalog.from_document(
            boundary(
                "v1",
                [
                    region("county", "c"),
                    region("town-1", "t1", parent="county"),
                    region("town-2", "t2", parent="county"),
                ],
            )
        )
        self.assertEqual(
            catalog.area_ids("county"), frozenset({"c", "t1", "t2"})
        )


class RosterTests(unittest.TestCase):
    def test_inactive_subscribers_are_skipped(self):
        roster = SubscriberRoster.from_document(
            {
                "roster_version": "r1",
                "subscribers": [
                    {"id": "on", "region": "town-1", "boundary_version": "v1"},
                    {"id": "off", "region": "town-1", "boundary_version": "v1", "active": False},
                ],
            }
        )
        self.assertEqual([item.subscriber_id for item in roster.subscribers], ["on"])

    def test_duplicate_subscriber_is_rejected(self):
        with self.assertRaisesRegex(MaterialError, "duplicate subscriber"):
            SubscriberRoster.from_document(
                {
                    "roster_version": "r1",
                    "subscribers": [
                        {"id": "a", "region": "r", "boundary_version": "v1"},
                        {"id": "a", "region": "r", "boundary_version": "v1"},
                    ],
                }
            )


class RevisionMergeTests(unittest.TestCase):
    def fragment(self, **overrides):
        base = {
            "source": "weather-a",
            "alert": "RAIN-5",
            "revision": 1,
            "status": "active",
            "status_present": True,
            "regions": ["town-1"],
            "regions_present": True,
            "boundary_version": "v1",
            "boundary_version_present": True,
            "bodies": [],
            "invalid": [],
            "digest": "d",
            "capture_id": 1,
            "observed_at": "t",
        }
        base.update(overrides)
        return base

    def test_body_only_fragment_does_not_block_complete_range_file(self):
        text = self.fragment(
            regions=None,
            regions_present=False,
            boundary_version=None,
            boundary_version_present=False,
            status=None,
            status_present=False,
            bodies=["全县注意"],
            digest="body",
        )
        range_file = self.fragment(regions=["town-1", "town-2"], digest="range")
        merged = merge_revision("weather-a", "RAIN-5", 1, [text, range_file])
        self.assertTrue(merged.complete)
        self.assertEqual(merged.regions, ("town-1", "town-2"))

    def test_text_alone_cannot_supply_scope(self):
        text = self.fragment(
            regions=None,
            regions_present=False,
            boundary_version=None,
            boundary_version_present=False,
            bodies=["影响甲县全部地区"],
            digest="body",
        )
        merged = merge_revision("weather-a", "RAIN-5", 1, [text])
        self.assertFalse(merged.complete)
        self.assertIn("regions", {item.identifier for item in merged.missing})

    def test_conflicting_scope_is_blocking(self):
        first = self.fragment(regions=["town-1"], digest="a")
        second = self.fragment(regions=["town-2"], digest="b")
        merged = merge_revision("weather-a", "RAIN-5", 1, [first, second])
        self.assertFalse(merged.complete)
        self.assertTrue(any(item.kind == "conflict" for item in merged.missing))

    def test_cancellation_does_not_require_regions(self):
        cancelled = self.fragment(status="cancelled", regions=[], digest="c")
        merged = merge_revision("weather-a", "RAIN-5", 3, [cancelled])
        self.assertTrue(merged.complete)
        self.assertEqual(merged.status, "cancelled")


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.raw = self.root / "raw.sqlite3"
        self.state = self.root / "state.sqlite3"

    def tearDown(self):
        self.temporary.cleanup()

    def open(self):
        pipeline = Pipeline.open(self.raw, self.state)
        pipeline.load_materials(materials())
        return pipeline

    def test_ingestion_is_idempotent_across_restarts(self):
        with serve(scenario({"/feed": feed(record(1, ["county-1"]))})) as origin:
            pipeline = self.open()
            pipeline.pull(origin.base_url, "weather-a")
            pipeline.close()
            pipeline = self.open()
            pipeline.pull(origin.base_url, "weather-a")
            self.assertEqual(len(pipeline.store.observations("RAIN-5")), 1)
            pipeline.close()

    def test_repeated_evaluation_does_not_duplicate_notifications(self):
        with serve(scenario({"/feed": feed(record(1, ["county-1"]))})) as origin:
            pipeline = self.open()
            pipeline.pull(origin.base_url, "weather-a")
            first = pipeline.evaluate("RAIN-5")
            second = pipeline.evaluate("RAIN-5")
            self.assertTrue(first["evaluations"][0]["applied"])
            self.assertEqual(second["evaluations"][0]["note"], "already-applied")
            self.assertEqual(len(pipeline.store.notifications()), 4)
            pipeline.close()

    def test_missing_material_reports_labels_and_resumes(self):
        partial = materials()
        partial["boundaries"] = []
        with serve(scenario({"/feed": feed(record(1, ["county-1"]))})) as origin:
            pipeline = Pipeline.open(self.raw, self.state)
            pipeline.load_materials(partial)
            pipeline.pull(origin.base_url, "weather-a")
            evaluation = pipeline.evaluate("RAIN-5")
            self.assertEqual(evaluation["evaluations"][0]["note"], "materials-incomplete")
            self.assertIn("boundary:2026-a", evaluation["evaluations"][0]["missing"])
            self.assertEqual(pipeline.store.notifications(), [])
            pipeline.close()

            pipeline = self.open()
            evaluation = pipeline.evaluate("RAIN-5")
            self.assertEqual(evaluation["evaluations"][0]["note"], "applied")
            self.assertEqual(pipeline.report("RAIN-5")["missing"], [])
            pipeline.close()

    def test_unknown_region_is_reported_not_inferred_from_body(self):
        with serve(
            scenario(
                {
                    "/feed": feed(
                        record(1, ["town-404"], body="影响甲县一镇和甲县二镇")
                    )
                }
            )
        ) as origin:
            pipeline = self.open()
            pipeline.pull(origin.base_url, "weather-a")
            evaluation = pipeline.evaluate("RAIN-5")
            self.assertFalse(evaluation["evaluations"][0]["complete"])
            self.assertIn("region:town-404@2026-a", evaluation["evaluations"][0]["missing"])
            self.assertEqual(pipeline.store.notifications(), [])
            pipeline.close()

    def test_narrowing_withdraws_unsent_notices(self):
        routes = {
            "/wide": feed(record(1, ["county-1"])),
            "/narrow": feed(record(2, ["town-11", "town-12"], version="2026-b")),
        }
        with serve(scenario(routes)) as origin:
            pipeline = self.open()
            pipeline.pull(origin.base_url, "weather-a", "/wide")
            pipeline.evaluate("RAIN-5")
            pipeline.pull(origin.base_url, "weather-a", "/narrow")
            evaluation = pipeline.evaluate("RAIN-5")
            actions = evaluation["evaluations"][0]["actions"]
            withdrawn = [item for item in actions if item["action"] == "withdraw"]
            # 所有未发送的 revision 1 通知都撤下；仍在范围内的人另发 revision 2 通知。
            self.assertEqual(
                {item["subscriber"] for item in withdrawn},
                {"sub-county", "sub-town-11", "sub-town-12", "sub-west"},
            )
            for notification in pipeline.store.notifications(state="withdrawn"):
                self.assertIsNotNone(notification["withdrawn_at"])
            sent = pipeline.flush()
            self.assertEqual(
                {item["subscriber"] for item in sent},
                {"sub-county", "sub-town-11", "sub-town-12"},
            )
            self.assertNotIn("sub-west", {item["subscriber"] for item in sent})
            pipeline.close()

    def test_sent_notices_get_linked_corrections(self):
        routes = {
            "/wide": feed(record(1, ["county-1"])),
            "/narrow": feed(record(2, ["town-11", "town-12"], version="2026-b")),
        }
        with serve(scenario(routes)) as origin:
            pipeline = self.open()
            pipeline.pull(origin.base_url, "weather-a", "/wide")
            pipeline.evaluate("RAIN-5")
            pipeline.flush()
            pipeline.pull(origin.base_url, "weather-a", "/narrow")
            pipeline.evaluate("RAIN-5")
            corrections = [
                item
                for item in pipeline.store.notifications(state="pending")
                if item["kind"] == "correction"
            ]
            self.assertEqual([item["subscriber"] for item in corrections], ["sub-west"])
            correction = corrections[0]
            self.assertEqual(correction["reason"], "narrowed")
            original = pipeline.store.notification(correction["correction_of"])
            self.assertEqual((original["subscriber"], original["revision"]), ("sub-west", 1))
            self.assertEqual(correction["payload"]["original"]["notification"], original["id"])
            pipeline.close()

    def test_cancellation_corrects_everyone_and_retry_is_idempotent(self):
        routes = {
            "/wide": feed(record(1, ["county-1"])),
            "/narrow": feed(record(2, ["town-11", "town-12"], version="2026-b")),
            "/cancel": feed(record(3, [], version="2026-b", status="cancelled")),
        }
        with serve(scenario(routes)) as origin:
            pipeline = self.open()
            pipeline.pull(origin.base_url, "weather-a", "/wide")
            pipeline.evaluate("RAIN-5")
            pipeline.flush()
            pipeline.pull(origin.base_url, "weather-a", "/narrow")
            pipeline.evaluate("RAIN-5")
            pipeline.flush()
            pipeline.pull(origin.base_url, "weather-a", "/cancel")
            pipeline.evaluate("RAIN-5")
            sent = pipeline.flush()
            self.assertEqual(len(sent), 3)
            for item in sent:
                self.assertEqual(item["kind"], "correction")
                self.assertEqual(item["reason"], "cancelled")
                self.assertIsNotNone(item["correction_of"])
            before = len(pipeline.store.notifications())
            for _ in range(3):
                pipeline.evaluate("RAIN-5")
            self.assertEqual(pipeline.flush(), [])
            self.assertEqual(len(pipeline.store.notifications()), before)
            pipeline.close()

    def test_restart_replays_pending_diff_once(self):
        routes = {
            "/wide": feed(record(1, ["county-1"])),
            "/narrow": feed(record(2, ["town-11", "town-12"], version="2026-b")),
        }
        with serve(scenario(routes)) as origin:
            pipeline = self.open()
            pipeline.pull(origin.base_url, "weather-a", "/wide")
            pipeline.evaluate("RAIN-5")
            pipeline.flush()
            pipeline.pull(origin.base_url, "weather-a", "/narrow")
            pipeline.evaluate("RAIN-5")
            pipeline.close()

            pipeline = self.open()
            self.assertEqual(pipeline.evaluate("RAIN-5")["evaluations"][0]["actions"], [])
            sent = pipeline.flush()
            self.assertEqual(len(sent), 4)
            self.assertEqual(pipeline.flush(), [])
            pipeline.close()

    def test_source_disagreement_is_preserved_per_source(self):
        routes = {
            "/a": feed(record(3, [], status="cancelled", issuer="weather-a")),
            "/b": feed(record(2, ["county-1"], issuer="weather-b")),
        }
        with serve(scenario(routes)) as origin:
            pipeline = self.open()
            pipeline.pull(origin.base_url, "weather-a", "/a")
            pipeline.pull(origin.base_url, "weather-b", "/b")
            evaluation = pipeline.evaluate("RAIN-5")
            self.assertEqual(evaluation["conflicts"][0]["kind"], "source-disagreement")
            conclusions = {
                item["source"]: item["status"] for item in evaluation["evaluations"]
            }
            self.assertEqual(conclusions, {"weather-a": "cancelled", "weather-b": "active"})
            report = pipeline.report("RAIN-5")
            self.assertEqual(len(report["conflicts"]), 1)
            self.assertEqual(
                {item["source"]: item["status"] for item in report["conflicts"][0]["sources"]},
                {"weather-a": "cancelled", "weather-b": "active"},
            )
            pipeline.close()

    def test_re_expansion_after_correction_sends_new_warning(self):
        routes = {
            "/wide": feed(record(1, ["county-1"])),
            "/narrow": feed(record(2, ["town-11"], version="2026-b")),
            "/again": feed(record(3, ["county-1"])),
        }
        with serve(scenario(routes)) as origin:
            pipeline = self.open()
            for path in ("/wide", "/narrow", "/again"):
                pipeline.pull(origin.base_url, "weather-a", path)
                pipeline.evaluate("RAIN-5")
                pipeline.flush()
            west = [
                item
                for item in pipeline.store.notifications()
                if item["subscriber"] == "sub-west"
            ]
            self.assertEqual(
                [(item["revision"], item["kind"]) for item in west],
                [(1, "warning"), (2, "correction"), (3, "warning")],
            )
            pipeline.close()

    def test_invalid_body_does_not_block_scope(self):
        with serve(
            scenario({"/feed": feed({**record(1, ["county-1"]), "body": {"unexpected": True}})})
        ) as origin:
            pipeline = self.open()
            pipeline.pull(origin.base_url, "weather-a")
            evaluation = pipeline.evaluate("RAIN-5")
            self.assertEqual(evaluation["evaluations"][0]["note"], "applied")
            observation = pipeline.store.observations("RAIN-5")[0]
            self.assertEqual(observation["bodies"], [])
            self.assertEqual(observation["invalid_evidence"], ["body"])
            pipeline.close()

    def test_cancellation_does_not_wait_for_materials(self):
        routes = {
            "/wide": feed(record(1, ["county-1"])),
            "/cancel": feed(record(2, [], version="2026-b", status="cancelled")),
        }
        with serve(scenario(routes)) as origin:
            pipeline = self.open()
            pipeline.pull(origin.base_url, "weather-a", "/wide")
            pipeline.evaluate("RAIN-5")
            pipeline.flush()
            with pipeline.store.connection:
                pipeline.store.connection.execute("DELETE FROM boundaries")
                pipeline.store.connection.execute("DELETE FROM rosters")
            pipeline.pull(origin.base_url, "weather-a", "/cancel")
            evaluation = pipeline.evaluate("RAIN-5")
            self.assertEqual(evaluation["evaluations"][0]["note"], "applied")
            sent = pipeline.flush()
            self.assertEqual(len(sent), 4)
            self.assertTrue(all(item["kind"] == "correction" for item in sent))
            pipeline.close()

    def test_timeline_shows_who_was_affected_by_each_revision(self):
        routes = {
            "/wide": feed(record(1, ["county-1"])),
            "/narrow": feed(record(2, ["town-11", "town-12"], version="2026-b")),
            "/cancel": feed(record(3, [], version="2026-b", status="cancelled")),
        }
        with serve(scenario(routes)) as origin:
            pipeline = self.open()
            for path in ("/wide", "/narrow", "/cancel"):
                pipeline.pull(origin.base_url, "weather-a", path)
                pipeline.evaluate("RAIN-5")
                pipeline.flush()
            timeline = pipeline.report("RAIN-5")["sources"]["weather-a"]
            self.assertEqual([item["revision"] for item in timeline["revisions"]], [1, 2, 3])
            self.assertEqual(
                timeline["revisions"][0]["impacted"],
                ["sub-county", "sub-town-11", "sub-town-12", "sub-west"],
            )
            self.assertEqual(
                timeline["revisions"][1]["impacted"],
                ["sub-county", "sub-town-11", "sub-town-12"],
            )
            self.assertEqual(timeline["revisions"][2]["impacted"], [])
            self.assertTrue(timeline["subscribers"]["sub-west"]["revisions"]["1"]["affected"])
            self.assertFalse(timeline["subscribers"]["sub-west"]["revisions"]["2"]["affected"])
            self.assertEqual(
                timeline["subscribers"]["sub-west"]["revisions"]["2"]["notice"]["kind"],
                "correction",
            )
            pipeline.close()


class StoreTests(unittest.TestCase):
    def test_digest_deduplicates_identical_observations(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = StateStore(Path(temporary) / "state.sqlite3")
            first = ingest_records(store, [record(1, ["county-1"])], capture_id=1, observed_at="t")
            second = ingest_records(store, [record(1, ["county-1"])], capture_id=2, observed_at="t")
            self.assertEqual(len(store.observations("RAIN-5")), 1)
            self.assertEqual(first["new_fragments"], 1)
            self.assertEqual(second["new_fragments"], 0)
            self.assertEqual(first["rejected"], [])
            store.close()

    def test_identity_less_records_are_rejected_not_dropped_silently(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = StateStore(Path(temporary) / "state.sqlite3")
            result = ingest_records(store, [{"alert": "RAIN-5", "regions": ["x"]}])
            self.assertEqual(result["fragments"], 0)
            self.assertEqual(len(result["rejected"]), 1)
            self.assertIn("missing identity", result["rejected"][0]["reason"])
            store.close()


if __name__ == "__main__":
    unittest.main()
