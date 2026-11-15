"""演示与命令行入口。

``demo`` 使用本地来源完整走一遍“扩大 → 缩小 → 撤销”，并在每一步打印
订阅者实际受到的影响与通知差异。``pipeline`` 子命令把同样的阶段拆开，
便于在进程重启后从持久状态继续。
"""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

from .archive import Archive
from .client import Client, collect
from .decoding import records
from .origin import load_scenario, serve
from .pipeline import Pipeline


def capture(base_url, archive_path, source, path):
    archive = Archive(archive_path)
    try:
        capture_id, response = collect(Client(base_url), archive, source, path)
        result = {
            "capture_id": capture_id,
            "status": response.status,
            "bytes": len(response.body),
            "url": response.url,
        }
        if response.status == 200:
            result["source_records"] = records(response.body, response.headers.get("content-type", ""))
        result["archived_observations"] = len(archive.captures())
        return result
    finally:
        archive.close()


def _dump(value):
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=False)


def _demo_materials():
    material_path = Path(__file__).parent / "fixtures" / "materials.json"
    if not material_path.is_file():
        material_path = Path(__file__).parents[1] / "fixtures" / "materials.json"
    return json.loads(material_path.read_text(encoding="utf-8"))


def demo() -> dict:
    scenario = {
        "identity_fields": ["issuer", "alert", "revision"],
        "source_records": [],
        "portal": {"csrf": "demo"},
        "routes": {
            "/expansion": {
                "json": {
                    "items": [
                        {
                            "issuer": "weather-a", "alert": "RAIN-5", "revision": 1,
                            "regions": ["county-1"], "boundary_version": "2026-a",
                            "status": "active", "body": "甲县全部范围",
                        }
                    ]
                }
            },
            "/narrowing": {
                "json": {
                    "items": [
                        {
                            "issuer": "weather-a", "alert": "RAIN-5", "revision": 2,
                            "regions": ["town-11", "town-12"], "boundary_version": "2026-b",
                            "status": "active", "body": "影响范围缩小至两镇",
                        }
                    ]
                }
            },
            "/cancellation": {
                "json": {
                    "items": [
                        {
                            "issuer": "weather-a", "alert": "RAIN-5", "revision": 3,
                            "regions": [], "boundary_version": "2026-b",
                            "status": "cancelled", "body": "预警撤销",
                        }
                    ]
                }
            },
        },
    }
    outcome = {"operation": "warning_timeline", "alert": "RAIN-5", "stages": []}
    with tempfile.TemporaryDirectory() as temporary, serve(scenario) as origin:
        pipeline = Pipeline.open(Path(temporary) / "raw.sqlite3", Path(temporary) / "state.sqlite3")
        try:
            pipeline.load_materials(_demo_materials())
            for stage, path, deliver in (
                ("expansion", "/expansion", True),
                ("narrowing", "/narrowing", False),
                ("cancellation", "/cancellation", True),
            ):
                capture = pipeline.pull(origin.base_url, "weather-a", path)
                evaluation = pipeline.evaluate("RAIN-5")
                sent = pipeline.flush() if deliver else []
                outcome["stages"].append(
                    {
                        "stage": stage,
                        "capture": {key: capture[key] for key in ("capture_id", "status", "bytes")},
                        "revision": evaluation["evaluations"][0]["revision"],
                        "missing": evaluation["evaluations"][0]["missing"],
                        "actions": evaluation["evaluations"][0]["actions"],
                        "sent": [
                            {
                                "subscriber": item["subscriber"],
                                "revision": item["revision"],
                                "kind": item["kind"],
                                "state": item["state"],
                                "correction_of": item["correction_of"],
                            }
                            for item in sent
                        ],
                    }
                )
            outcome["report"] = pipeline.report("RAIN-5")
        finally:
            pipeline.close()
    return outcome


def _pipeline(args):
    pipeline = Pipeline.open(args.archive, args.state)
    try:
        if args.command == "resolve":
            if args.materials:
                pipeline.load_materials_file(args.materials)
            print(_dump({"alert": args.alert, "missing": pipeline.missing(args.alert)}))
        elif args.command == "evaluate":
            print(_dump(pipeline.evaluate(args.alert)))
        elif args.command == "report":
            print(_dump(pipeline.report(args.alert)))
        elif args.command == "notify":
            print(_dump(pipeline.flush()))
        elif args.command == "pull":
            print(_dump(pipeline.pull(args.base_url, args.source, args.path)))
    finally:
        pipeline.close()


def main(argv=None):
    import argparse

    parser = argparse.ArgumentParser(description="来源响应接收、预警修订装配与通知差异")
    subcommands = parser.add_subparsers(dest="command", required=True)

    subcommands.add_parser("demo", help="演示扩大、缩小与撤销的订阅者影响")

    collector = subcommands.add_parser("capture", help="采集原始响应并归档")
    collector.add_argument("--base-url", required=True)
    collector.add_argument("--archive", type=Path, required=True)
    collector.add_argument("--source", required=True)
    collector.add_argument("--path", default="/feed")

    server = subcommands.add_parser("serve", help="启动本地来源")
    server.add_argument("--scenario", type=Path)

    for name, help_text in (
        ("pull", "采集一次来源并写入业务状态"),
        ("resolve", "显示某事件尚缺的配套资料"),
        ("evaluate", "装配事件、计算影响并处理通知差异"),
        ("report", "按修订展示订阅者影响与通知"),
        ("notify", "发送待发通知"),
    ):
        command = subcommands.add_parser(name, help=help_text)
        command.add_argument("--archive", type=Path, required=True)
        command.add_argument("--state", type=Path, required=True)
        if name == "pull":
            command.add_argument("--base-url", required=True)
            command.add_argument("--source", required=True)
            command.add_argument("--path", default="/feed")
        elif name == "resolve":
            command.add_argument("--alert", required=True)
            command.add_argument("--materials", type=Path, help="同时载入一份配套资料")
        elif name == "evaluate":
            command.add_argument("--alert")
        else:
            command.add_argument("--alert", required=True)

    args = parser.parse_args(argv)
    if args.command == "demo":
        print(_dump(demo()))
    elif args.command == "capture":
        print(_dump(capture(args.base_url, args.archive, args.source, args.path)))
    elif args.command == "serve":
        with serve(load_scenario(args.scenario)) as origin:
            print(json.dumps({"base_url": origin.base_url}, ensure_ascii=False), flush=True)
            try:
                import threading

                threading.Event().wait()
            except KeyboardInterrupt:
                pass
    else:
        return _pipeline(args)
    return None


if __name__ == "__main__":
    sys.exit(main())
