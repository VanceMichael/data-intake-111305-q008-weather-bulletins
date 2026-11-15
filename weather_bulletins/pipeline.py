"""端到端业务流水线：采集、装配、评估、通知与报表。

每一阶段都只依赖 :class:`~weather_bulletins.store.StateStore` 中的持久状态，
因此可以在任意阶段中断，进程重启后继续收资料或继续处理通知差异。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from .archive import Archive
from .client import Client, collect
from .decoding import records as decode_records
from .engine import evaluate, flush, ingest_materials, ingest_records, report
from .store import StateStore


@dataclass
class Pipeline:
    archive: Archive
    store: StateStore
    delivered: list[dict[str, Any]] = field(default_factory=list)

    @classmethod
    def open(cls, archive_path, state_path) -> "Pipeline":
        return cls(Archive(archive_path), StateStore(state_path))

    def close(self) -> None:
        self.archive.close()
        self.store.close()

    def __enter__(self) -> "Pipeline":
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()

    # -- 配套资料 -----------------------------------------------------
    def load_materials(self, document: dict[str, Any]) -> dict[str, Any]:
        return ingest_materials(
            self.store,
            boundaries=document.get("boundaries", []),
            rosters=document.get("rosters", []),
        )

    def load_materials_file(self, path) -> dict[str, Any]:
        return self.load_materials(_read_json(Path(path)))

    # -- 采集 ---------------------------------------------------------
    def pull(self, base_url: str, source: str, path: str = "/feed") -> dict[str, Any]:
        capture_id, response = collect(Client(base_url), self.archive, source, path)
        result = {
            "capture_id": capture_id,
            "source": source,
            "status": response.status,
            "bytes": len(response.body),
            "url": response.url,
        }
        if response.status == 200:
            observed_at = self.archive.captures(source)[-1]["received_at"]
            decoded = decode_records(response.body, response.headers.get("content-type", ""))
            result["ingest"] = ingest_records(self.store, decoded, capture_id, observed_at)
        return result

    # -- 评估与通知 ---------------------------------------------------
    def evaluate(self, alert: str | None = None) -> dict[str, Any]:
        return evaluate(self.store, alert)

    def flush(self, deliver: Callable[[dict[str, Any]], Any] | None = None) -> list[dict[str, Any]]:
        sent = flush(self.store, deliver)
        self.delivered.extend(sent)
        return sent

    def report(self, alert: str) -> dict[str, Any]:
        return report(self.store, alert)

    def missing(self, alert: str) -> list[str]:
        latest: dict[str, dict[str, Any]] = {}
        for row in self.store.evaluations(alert):
            latest[row["source"]] = row
        return sorted(
            {label for row in latest.values() if not row["complete"] for label in row["missing"]}
        )

    def advance(self, base_url: str, source: str, alert: str, path: str = "/feed") -> dict[str, Any]:
        """一次完整推进：采集 → 评估 → 发送。"""
        capture = self.pull(base_url, source, path)
        evaluation = self.evaluate(alert)
        sent = self.flush()
        return {"capture": capture, "evaluation": evaluation, "sent": sent}


def _read_json(path: Path) -> Any:
    import json

    return json.loads(path.read_text(encoding="utf-8"))
