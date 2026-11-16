import argparse
import json
import tempfile
from pathlib import Path

from .archive import Archive
from .boundaries import load_documents
from .client import Client, collect
from .decoding import records
from .engine import (
    deliver,
    event_keys,
    ingest_boundary_documents,
    ingest_records,
    process_event,
)
from .origin import fixtures_directory, load_scenario, serve
from .report import events_report
from .store import Store


def capture(base_url, archive_path, source, path):
    archive = Archive(archive_path)
    try:
        capture_id, response = collect(Client(base_url), archive, source, path)
        result = {"capture_id": capture_id, "status": response.status, "bytes": len(response.body), "url": response.url}
        if response.status == 200:
            result["source_records"] = records(response.body, response.headers.get("content-type", ""))
        result["archived_observations"] = len(archive.captures())
        return result
    finally:
        archive.close()


def ingest(base_url, archive_path, state_path, source, path, boundary_paths):
    """采集一次并入业务状态，边界资料随后加载；返回本次处理摘要。

    即使本次来源请求失败，边界资料仍会加载并重新评估事件，因此“资料补齐”
    与“来源暂时不可用”可以分开重试。
    """
    archive = Archive(archive_path)
    store = Store(state_path)
    try:
        capture_id, response = collect(Client(base_url), archive, source, path)
        summary = {
            "capture_id": capture_id,
            "status": response.status,
            "url": response.url,
        }
        if response.status == 200:
            payload = records(response.body, response.headers.get("content-type", ""))
            inserted, invalid = ingest_records(store, payload, source)
        else:
            inserted, invalid = 0, 0
        loaded, boundary_errors = load_documents(boundary_paths)
        applied, boundary_failed = ingest_boundary_documents(store, loaded, boundary_errors)
        actions = []
        catalog = store.boundary_catalog()
        subscribers = store.subscribers()
        for event_key in event_keys(store):
            actions.extend(process_event(store, event_key, catalog, subscribers))
        summary["records"] = {"inserted": inserted, "invalid": invalid}
        summary["boundaries"] = {"applied": applied, "failed": boundary_failed}
        summary["notifications"] = actions
        summary["errors"] = store.errors()
        return summary
    finally:
        store.close()
        archive.close()


def load_subscribers(path):
    path = Path(path)
    if not path.is_file():
        raise SystemExit("subscriber directory not found: " + str(path))
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list) or not payload:
        raise SystemExit("subscriber directory must be a non-empty array")
    subscribers = []
    for index, item in enumerate(payload):
        if not isinstance(item, dict):
            raise SystemExit(f"subscribers[{index}] must be an object")
        identity = item.get("id")
        covers = item.get("covers")
        if not isinstance(identity, str) or not identity.strip():
            raise SystemExit(f"subscribers[{index}].id must be a non-empty string")
        if not isinstance(covers, list) or not covers or any(not isinstance(place, str) or not place.strip() for place in covers):
            raise SystemExit(f"subscribers[{index}].covers must list stable place identifiers")
        subscribers.append({"id": identity.strip(), "name": item.get("name", identity), "covers": [place.strip() for place in covers]})
    identifiers = {subscriber["id"] for subscriber in subscribers}
    if len(identifiers) != len(subscribers):
        raise SystemExit("subscriber ids must be unique")
    return subscribers


def show(state_path, event=None):
    store = Store(state_path)
    try:
        report = events_report(store)
        if event is None:
            return report
        matching = [item for item in report["events"] if item["event"] == event]
        if not matching:
            raise SystemExit("event not found: " + event)
        return matching[0]
    finally:
        store.close()


def send(state_path, notification_id):
    store = Store(state_path)
    try:
        notification = store.get_notification(notification_id)
        if notification is None:
            raise SystemExit(f"notification not found: {notification_id}")
        changed = deliver(store, notification_id)
        return {"notification": notification_id, "status": store.get_notification(notification_id)["status"], "changed": changed}
    finally:
        store.close()


def demo():
    """从空目录启动本地来源，按真实交错顺序重放一次完整预警修订。

    FLOOD-7 走完“先小范围 → 扩大 → 缩小 → 撤销”的全程，逐步展示每个订阅
    者受到哪一版影响；RAIN-5 展示同一事件两家来源结论不一致时并列保留。
    每个观察之后推进通知队列，让撤销同时遇到“已发出”和“未发送”。
    """
    root_fixtures = fixtures_directory()
    scenario = load_scenario(root_fixtures / "demo_scenario.json")
    script = [
        "/flood-1", "/flood-2", "/flood-3", "/flood-4",
        "/rain-1", "/rain-2", "/rain-2-other-source",
    ]
    subscribers = load_subscribers(root_fixtures / "subscribers.json")
    with tempfile.TemporaryDirectory() as temporary, serve(scenario) as origin:
        root = Path(temporary)
        state_path = root / "state.sqlite3"
        store = Store(state_path)
        store.upsert_subscribers(subscribers)
        store.close()
        results = []
        for path in script:
            step = ingest(
                origin.base_url,
                root / "archive.sqlite3",
                state_path,
                "local-source",
                path,
                [root_fixtures / "boundaries"],
            )
            # 每一版活跃通知先发出，下一版到达时就会走“已发出 → 更正”的路径。
            sent = []
            for action in step["notifications"]:
                if action["action"] in ("queued", "released"):
                    sent.append(send(state_path, action["notification"]))
            results.append({
                "observation": path,
                "status": step["status"],
                "records": step["records"],
                "boundaries": step["boundaries"],
                "notifications": step["notifications"],
                "sent": sent,
            })
        final = show(state_path)
    return {
        "operation": "revision_impact",
        "steps": results,
        "report": final,
    }


def main():
    parser = argparse.ArgumentParser(description="来源响应接收、预警修订解析与通知差异")
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser("demo")
    collector = subcommands.add_parser("ingest")
    collector.add_argument("--base-url", required=True)
    collector.add_argument("--archive", type=Path, required=True)
    collector.add_argument("--state", type=Path, required=True)
    collector.add_argument("--source", required=True)
    collector.add_argument("--path", default="/feed")
    collector.add_argument("--subscribers", type=Path, required=True)
    collector.add_argument("--boundaries", type=Path, nargs="+", required=True)
    shower = subcommands.add_parser("show")
    shower.add_argument("--state", type=Path, required=True)
    shower.add_argument("--event")
    sender = subcommands.add_parser("send")
    sender.add_argument("--state", type=Path, required=True)
    sender.add_argument("--notification", type=int, required=True)
    server = subcommands.add_parser("serve")
    server.add_argument("--scenario", type=Path)
    args = parser.parse_args()
    if args.command == "demo":
        print(json.dumps(demo(), ensure_ascii=False, indent=2))
    elif args.command == "ingest":
        store = Store(args.state)
        try:
            store.upsert_subscribers(load_subscribers(args.subscribers))
        finally:
            store.close()
        print(json.dumps(
            ingest(args.base_url, args.archive, args.state, args.source, args.path, args.boundaries),
            ensure_ascii=False, indent=2,
        ))
    elif args.command == "show":
        print(json.dumps(show(args.state, args.event), ensure_ascii=False, indent=2))
    elif args.command == "send":
        print(json.dumps(send(args.state, args.notification), ensure_ascii=False, indent=2))
    else:
        with serve(load_scenario(args.scenario)) as origin:
            print(json.dumps({"base_url": origin.base_url}, ensure_ascii=False), flush=True)
            try:
                import threading
                threading.Event().wait()
            except KeyboardInterrupt:
                pass


if __name__ == "__main__":
    main()
