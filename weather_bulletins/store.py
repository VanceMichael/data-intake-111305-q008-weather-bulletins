"""业务状态库：观测、配套资料、评估结果与通知队列。

原始响应仍由 :mod:`weather_bulletins.archive` 只追加保存；这里保存“已经解释
过的业务事实”以及可恢复的处理进度。所有写入都在事务内完成，进程重启后
可以直接从库中继续收资料、重算影响和处理通知差异。
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable

from .warning import canonical_json


def _loads(value: str | None, default: Any) -> Any:
    return default if value is None else json.loads(value)


class StateStore:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.path, timeout=5)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS observations (
                id INTEGER PRIMARY KEY,
                source TEXT NOT NULL,
                alert TEXT NOT NULL,
                revision INTEGER NOT NULL,
                status TEXT,
                status_present INTEGER NOT NULL,
                regions TEXT,
                regions_present INTEGER NOT NULL,
                boundary_version TEXT,
                boundary_version_present INTEGER NOT NULL,
                bodies TEXT NOT NULL,
                invalid TEXT NOT NULL,
                invalid_evidence TEXT NOT NULL DEFAULT '[]',
                digest TEXT NOT NULL,
                capture_id INTEGER,
                observed_at TEXT,
                UNIQUE(source, alert, revision, digest)
            );
            CREATE INDEX IF NOT EXISTS observations_alert ON observations(alert, source, revision);

            CREATE TABLE IF NOT EXISTS boundaries (
                version TEXT PRIMARY KEY,
                document TEXT NOT NULL,
                digest TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS rosters (
                roster_version TEXT PRIMARY KEY,
                document TEXT NOT NULL,
                digest TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS frontiers (
                source TEXT NOT NULL,
                alert TEXT NOT NULL,
                revision INTEGER NOT NULL,
                status TEXT NOT NULL,
                impacted TEXT NOT NULL,
                fingerprint TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                PRIMARY KEY (source, alert)
            );

            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY,
                dedupe_key TEXT NOT NULL UNIQUE,
                source TEXT NOT NULL,
                alert TEXT NOT NULL,
                revision INTEGER NOT NULL,
                subscriber TEXT NOT NULL,
                kind TEXT NOT NULL,
                state TEXT NOT NULL,
                boundary_version TEXT,
                regions TEXT,
                covered_areas TEXT,
                correction_of INTEGER REFERENCES notifications(id),
                reason TEXT,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                sent_at TEXT,
                withdrawn_at TEXT,
                withdrawn_revision INTEGER
            );
            CREATE INDEX IF NOT EXISTS notifications_target
                ON notifications(source, alert, subscriber, kind, state);

            CREATE TABLE IF NOT EXISTS queue_events (
                id INTEGER PRIMARY KEY,
                notification_id INTEGER NOT NULL REFERENCES notifications(id),
                action TEXT NOT NULL,
                revision INTEGER,
                detail TEXT,
                at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            );

            CREATE TABLE IF NOT EXISTS evaluations (
                id INTEGER PRIMARY KEY,
                source TEXT NOT NULL,
                alert TEXT NOT NULL,
                revision INTEGER NOT NULL,
                status TEXT,
                applied INTEGER NOT NULL,
                complete INTEGER NOT NULL,
                impacted TEXT NOT NULL,
                missing TEXT NOT NULL,
                conflicts TEXT NOT NULL,
                actions TEXT NOT NULL,
                boundary_version TEXT,
                regions TEXT NOT NULL DEFAULT '[]',
                covered_areas TEXT NOT NULL DEFAULT '[]',
                note TEXT,
                evaluated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            );
            CREATE INDEX IF NOT EXISTS evaluations_alert ON evaluations(alert, source, revision);
            """
        )

    def close(self):
        self.connection.close()

    @contextmanager
    def transaction(self):
        with self.connection:
            yield self.connection

    # -- observations -------------------------------------------------
    def add_observation(self, fragment: dict[str, Any]) -> tuple[int, bool]:
        """写入一个观测片段；相同来源、修订与摘要只保留一次。"""
        with self.transaction() as connection:
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO observations(
                    source, alert, revision, status, status_present,
                    regions, regions_present, boundary_version, boundary_version_present,
                    bodies, invalid, invalid_evidence, digest, capture_id, observed_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    fragment["source"], fragment["alert"], fragment["revision"],
                    fragment["status"], int(fragment["status_present"]),
                    canonical_json(fragment["regions"]) if fragment["regions"] is not None else None,
                    int(fragment["regions_present"]),
                    fragment["boundary_version"], int(fragment["boundary_version_present"]),
                    canonical_json(fragment["bodies"]), canonical_json(fragment["invalid"]),
                    canonical_json(fragment.get("invalid_evidence", [])),
                    fragment["digest"], fragment["capture_id"], fragment["observed_at"],
                ),
            )
            created = cursor.rowcount == 1
            row = connection.execute(
                "SELECT id FROM observations WHERE source=? AND alert=? AND revision=? AND digest=?",
                (fragment["source"], fragment["alert"], fragment["revision"], fragment["digest"]),
            ).fetchone()
        return row["id"], created

    def observations(self, alert: str | None = None) -> list[dict[str, Any]]:
        query = "SELECT * FROM observations"
        params: tuple[Any, ...] = ()
        if alert is not None:
            query += " WHERE alert=?"
            params = (alert,)
        rows = self.connection.execute(query + " ORDER BY source, revision, id", params).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            for name in ("regions", "bodies", "invalid", "invalid_evidence"):
                item[name] = _loads(item[name], None if name == "regions" else [])
            item["status_present"] = bool(item["status_present"])
            item["regions_present"] = bool(item["regions_present"])
            item["boundary_version_present"] = bool(item["boundary_version_present"])
            result.append(item)
        return result

    def alerts(self) -> list[str]:
        rows = self.connection.execute("SELECT DISTINCT alert FROM observations ORDER BY alert").fetchall()
        return [row["alert"] for row in rows]

    # -- materials ----------------------------------------------------
    def add_boundary(self, document: dict[str, Any], digest: str) -> None:
        from .boundaries import BoundaryCatalog  # 局部导入避免循环

        catalog = BoundaryCatalog.from_document(document)
        with self.transaction() as connection:
            connection.execute(
                "INSERT INTO boundaries(version, document, digest) VALUES (?,?,?) "
                "ON CONFLICT(version) DO UPDATE SET document=excluded.document, digest=excluded.digest",
                (catalog.version, canonical_json(catalog.as_document()), digest),
            )

    def add_roster(self, document: dict[str, Any], digest: str) -> None:
        from .boundaries import SubscriberRoster

        roster = SubscriberRoster.from_document(document)
        with self.transaction() as connection:
            connection.execute(
                "INSERT INTO rosters(roster_version, document, digest) VALUES (?,?,?) "
                "ON CONFLICT(roster_version) DO UPDATE SET document=excluded.document, digest=excluded.digest",
                (roster.version, canonical_json(roster.as_document()), digest),
            )

    def boundary_documents(self) -> dict[str, dict[str, Any]]:
        rows = self.connection.execute("SELECT version, document FROM boundaries ORDER BY version").fetchall()
        return {row["version"]: json.loads(row["document"]) for row in rows}

    def roster_documents(self) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT roster_version, document FROM rosters ORDER BY roster_version"
        ).fetchall()
        return [json.loads(row["document"]) for row in rows]

    # -- frontiers ----------------------------------------------------
    def frontier(self, source: str, alert: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT * FROM frontiers WHERE source=? AND alert=?", (source, alert)
        ).fetchone()
        if row is None:
            return None
        result = dict(row)
        result["impacted"] = _loads(result["impacted"], [])
        return result

    def set_frontier(
        self,
        source: str,
        alert: str,
        revision: int,
        status: str,
        impacted: Iterable[str],
        fingerprint: str = "",
    ) -> None:
        with self.transaction() as connection:
            connection.execute(
                "INSERT INTO frontiers(source, alert, revision, status, impacted, fingerprint) "
                "VALUES (?,?,?,?,?,?) "
                "ON CONFLICT(source, alert) DO UPDATE SET "
                "revision=excluded.revision, status=excluded.status, impacted=excluded.impacted, "
                "fingerprint=excluded.fingerprint, "
                "updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')",
                (source, alert, revision, status, canonical_json(sorted(impacted)), fingerprint),
            )

    # -- notifications ------------------------------------------------
    def notification(self, notification_id: int) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT * FROM notifications WHERE id=?", (notification_id,)
        ).fetchone()
        return self._notification(row) if row else None

    def _notification(self, row: sqlite3.Row) -> dict[str, Any]:
        result = dict(row)
        for name in ("regions", "covered_areas"):
            result[name] = _loads(result[name], [])
        result["payload"] = _loads(result["payload"], {})
        return result

    def add_notification(self, notification: dict[str, Any]) -> tuple[dict[str, Any], bool]:
        with self.transaction() as connection:
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO notifications(
                    dedupe_key, source, alert, revision, subscriber, kind, state,
                    boundary_version, regions, covered_areas, correction_of, reason, payload
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    notification["dedupe_key"], notification["source"], notification["alert"],
                    notification["revision"], notification["subscriber"], notification["kind"],
                    notification.get("state", "pending"), notification.get("boundary_version"),
                    canonical_json(notification.get("regions", [])),
                    canonical_json(notification.get("covered_areas", [])),
                    notification.get("correction_of"), notification.get("reason"),
                    canonical_json(notification.get("payload", {})),
                ),
            )
            created = cursor.rowcount == 1
            row = connection.execute(
                "SELECT * FROM notifications WHERE dedupe_key=?", (notification["dedupe_key"],)
            ).fetchone()
            if created:
                connection.execute(
                    "INSERT INTO queue_events(notification_id, action, revision, detail) VALUES (?,?,?,?)",
                    (row["id"], "queued", notification["revision"], notification["kind"]),
                )
        return self._notification(row), created

    def notifications(self, alert: str | None = None, source: str | None = None, state: str | None = None) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        for column, value in (("alert", alert), ("source", source), ("state", state)):
            if value is not None:
                clauses.append(f"{column}=?")
                params.append(value)
        query = "SELECT * FROM notifications"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        rows = self.connection.execute(query + " ORDER BY id", tuple(params)).fetchall()
        return [self._notification(row) for row in rows]

    def mark_sent(self, notification_id: int) -> dict[str, Any]:
        with self.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM notifications WHERE id=?", (notification_id,)
            ).fetchone()
            if row is None:
                raise KeyError(notification_id)
            if row["state"] != "pending":
                raise ValueError(f"notification {notification_id} is {row['state']}, not pending")
            connection.execute(
                "UPDATE notifications SET state='sent', sent_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') "
                "WHERE id=?",
                (notification_id,),
            )
            connection.execute(
                "INSERT INTO queue_events(notification_id, action, revision) VALUES (?,?,?)",
                (notification_id, "sent", row["revision"]),
            )
        return self.notification(notification_id)

    def withdraw(self, notification_id: int, revision: int, reason: str) -> dict[str, Any]:
        with self.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM notifications WHERE id=?", (notification_id,)
            ).fetchone()
            if row is None:
                raise KeyError(notification_id)
            if row["state"] == "withdrawn":
                return self.notification(notification_id)
            if row["state"] != "pending":
                raise ValueError(f"notification {notification_id} is {row['state']}, not pending")
            connection.execute(
                "UPDATE notifications SET state='withdrawn', reason=?, "
                "withdrawn_at=strftime('%Y-%m-%dT%H:%M:%fZ','now'), withdrawn_revision=? WHERE id=?",
                (reason, revision, notification_id),
            )
            connection.execute(
                "INSERT INTO queue_events(notification_id, action, revision, detail) VALUES (?,?,?,?)",
                (notification_id, "withdrawn", revision, reason),
            )
        return self.notification(notification_id)

    # -- evaluations --------------------------------------------------
    def record_evaluation(self, evaluation: dict[str, Any]) -> int:
        with self.transaction() as connection:
            cursor = connection.execute(
                """
                INSERT INTO evaluations(
                    source, alert, revision, status, applied, complete,
                    impacted, missing, conflicts, actions,
                    boundary_version, regions, covered_areas, note
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    evaluation["source"], evaluation["alert"], evaluation["revision"],
                    evaluation.get("status"), int(evaluation.get("applied", False)),
                    int(evaluation.get("complete", False)),
                    canonical_json(evaluation.get("impacted", [])),
                    canonical_json(evaluation.get("missing", [])),
                    canonical_json(evaluation.get("conflicts", [])),
                    canonical_json(evaluation.get("actions", [])),
                    evaluation.get("boundary_version"),
                    canonical_json(evaluation.get("regions", [])),
                    canonical_json(evaluation.get("covered_areas", [])),
                    evaluation.get("note"),
                ),
            )
        return cursor.lastrowid

    def evaluations(self, alert: str | None = None) -> list[dict[str, Any]]:
        query = "SELECT * FROM evaluations"
        params: tuple[Any, ...] = ()
        if alert is not None:
            query += " WHERE alert=?"
            params = (alert,)
        rows = self.connection.execute(query + " ORDER BY id", params).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            for name in ("impacted", "missing", "conflicts", "actions", "regions", "covered_areas"):
                item[name] = _loads(item[name], [])
            item["applied"] = bool(item["applied"])
            item["complete"] = bool(item["complete"])
            result.append(item)
        return result

    def queue_events(self, notification_id: int | None = None) -> list[dict[str, Any]]:
        query = "SELECT * FROM queue_events"
        params: tuple[Any, ...] = ()
        if notification_id is not None:
            query += " WHERE notification_id=?"
            params = (notification_id,)
        rows = self.connection.execute(query + " ORDER BY id", params).fetchall()
        return [dict(row) for row in rows]
