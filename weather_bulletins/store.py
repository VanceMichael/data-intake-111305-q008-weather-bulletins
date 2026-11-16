"""业务状态存储。

原始观察只追加；“有效结论”和“通知差异”是可从原始观察重建的投影，存放在
同一个 SQLite 文件中，进程重启后继续处理。所有写入都以业务键去重，因此
采集重试不会重复产出通知或更正。
"""

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS boundary_versions (
    version TEXT PRIMARY KEY,
    digest TEXT NOT NULL,
    source TEXT NOT NULL,
    received_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE TABLE IF NOT EXISTS boundary_regions (
    version TEXT NOT NULL REFERENCES boundary_versions(version) ON DELETE CASCADE,
    code TEXT NOT NULL,
    name TEXT NOT NULL,
    covers TEXT NOT NULL,
    PRIMARY KEY (version, code)
);
CREATE TABLE IF NOT EXISTS subscribers (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    covers TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS ingest_errors (
    id INTEGER PRIMARY KEY,
    source TEXT NOT NULL,
    reason TEXT NOT NULL,
    detail TEXT NOT NULL,
    record TEXT,
    seen_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE TABLE IF NOT EXISTS source_observations (
    id INTEGER PRIMARY KEY,
    event_key TEXT NOT NULL,
    issuer TEXT NOT NULL,
    alert TEXT NOT NULL,
    revision INTEGER NOT NULL,
    status TEXT NOT NULL,
    regions TEXT NOT NULL,
    boundary_version TEXT,
    fingerprint TEXT NOT NULL,
    raw TEXT NOT NULL,
    seen_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE (event_key, issuer, revision, fingerprint)
);
CREATE TABLE IF NOT EXISTS notifications (
    id INTEGER PRIMARY KEY,
    event_key TEXT NOT NULL,
    issuer TEXT NOT NULL,
    revision INTEGER NOT NULL,
    reason TEXT NOT NULL,
    cause TEXT,
    link_id INTEGER REFERENCES notifications(id),
    status TEXT NOT NULL,
    places TEXT NOT NULL,
    unresolved TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE UNIQUE INDEX IF NOT EXISTS notifications_business_key
    ON notifications(event_key, issuer, revision, reason, IFNULL(cause, ''), IFNULL(link_id, -1));
CREATE TABLE IF NOT EXISTS notification_targets (
    id INTEGER PRIMARY KEY,
    notification_id INTEGER NOT NULL REFERENCES notifications(id) ON DELETE CASCADE,
    subscriber TEXT NOT NULL,
    places TEXT NOT NULL,
    previous_id INTEGER,
    UNIQUE (notification_id, subscriber)
);
"""


class Store:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.path, timeout=5)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.executescript(SCHEMA)

    def close(self):
        self.connection.close()

    @contextmanager
    def transaction(self):
        with self.connection:
            yield self.connection

    # ---- 边界资料 ----
    def boundary_digest(self, version):
        row = self.connection.execute(
            "SELECT digest FROM boundary_versions WHERE version=?", (version,)
        ).fetchone()
        return None if row is None else row["digest"]

    def upsert_boundaries(self, document, digest, source):
        with self.transaction() as connection:
            connection.execute(
                "INSERT INTO boundary_versions(version,digest,source) VALUES (?,?,?) "
                "ON CONFLICT(version) DO UPDATE SET digest=excluded.digest, source=excluded.source, "
                "received_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')",
                (document["version"], digest, source),
            )
            connection.execute("DELETE FROM boundary_regions WHERE version=?", (document["version"],))
            connection.executemany(
                "INSERT INTO boundary_regions(version,code,name,covers) VALUES (?,?,?,?)",
                [
                    (document["version"], region["code"], region["name"], json.dumps(region["covers"], ensure_ascii=False))
                    for region in document["regions"]
                ],
            )

    def boundary_catalog(self):
        rows = self.connection.execute(
            "SELECT version,code,name,covers FROM boundary_regions ORDER BY version,code"
        ).fetchall()
        catalog = {}
        for row in rows:
            catalog.setdefault(row["version"], {})[row["code"]] = {
                "name": row["name"],
                "covers": json.loads(row["covers"]),
            }
        return catalog

    def boundary_versions(self):
        rows = self.connection.execute(
            "SELECT version,digest,source,received_at FROM boundary_versions ORDER BY version"
        ).fetchall()
        return [dict(row) for row in rows]

    # ---- 订阅者 ----
    def upsert_subscribers(self, subscribers):
        with self.transaction() as connection:
            for subscriber in subscribers:
                connection.execute(
                    "INSERT INTO subscribers(id,name,covers) VALUES (?,?,?) "
                    "ON CONFLICT(id) DO UPDATE SET name=excluded.name, covers=excluded.covers",
                    (subscriber["id"], subscriber["name"], json.dumps(subscriber["covers"], ensure_ascii=False)),
                )

    def subscribers(self):
        rows = self.connection.execute("SELECT id,name,covers FROM subscribers ORDER BY id").fetchall()
        return [{"id": row["id"], "name": row["name"], "covers": json.loads(row["covers"])} for row in rows]

    # ---- 采集错误 ----
    def record_error(self, source, reason, detail, record=None):
        with self.transaction() as connection:
            connection.execute(
                "INSERT INTO ingest_errors(source,reason,detail,record) VALUES (?,?,?,?)",
                (source, reason, detail, None if record is None else json.dumps(record, ensure_ascii=False, sort_keys=True)),
            )

    def errors(self):
        rows = self.connection.execute("SELECT * FROM ingest_errors ORDER BY id").fetchall()
        return [
            {**dict(row), "record": None if row["record"] is None else json.loads(row["record"])}
            for row in rows
        ]

    # ---- 来源观察 ----
    def insert_observation(self, record):
        with self.transaction() as connection:
            cursor = connection.execute(
                "INSERT OR IGNORE INTO source_observations"
                "(event_key,issuer,alert,revision,status,regions,boundary_version,fingerprint,raw) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    record.event_key,
                    record.issuer,
                    record.alert,
                    record.revision,
                    record.status,
                    json.dumps(list(record.regions), ensure_ascii=False),
                    record.boundary_version,
                    record.fingerprint,
                    json.dumps(record.raw, ensure_ascii=False, sort_keys=True),
                ),
            )
            return cursor.rowcount == 1

    def observations(self, event_key=None):
        query = "SELECT * FROM source_observations"
        params = ()
        if event_key is not None:
            query += " WHERE event_key=?"
            params = (event_key,)
        rows = self.connection.execute(query + " ORDER BY id", params).fetchall()
        return [_observation(row) for row in rows]

    def latest_per_source(self, event_key):
        """每个来源的最新修订，保留来源各自结论。

        以来源声明的最大修订号为准：后到的新版范围会取代先到的旧正文。
        修订号相同而内容不同的记录不会互相覆盖，全部保留供值班员处置。
        """
        latest = {}
        for item in self.observations(event_key):
            current = latest.get(item["issuer"])
            if current is None or item["revision"] > current["revision"]:
                latest[item["issuer"]] = item
        return latest

    def revisions_for(self, event_key, issuer, revision):
        rows = self.connection.execute(
            "SELECT * FROM source_observations WHERE event_key=? AND issuer=? AND revision=? ORDER BY id",
            (event_key, issuer, revision),
        ).fetchall()
        return [_observation(row) for row in rows]

    def event_keys(self):
        rows = self.connection.execute(
            "SELECT DISTINCT event_key FROM source_observations ORDER BY event_key"
        ).fetchall()
        return [row["event_key"] for row in rows]

    # ---- 通知 ----
    def find_notification(self, event_key, issuer, revision, reason):
        row = self.connection.execute(
            "SELECT * FROM notifications WHERE event_key=? AND issuer=? AND revision=? AND reason=? "
            "AND link_id IS NULL",
            (event_key, issuer, revision, reason),
        ).fetchone()
        return None if row is None else _notification(row)

    def get_notification(self, notification_id):
        row = self.connection.execute(
            "SELECT * FROM notifications WHERE id=?", (notification_id,)
        ).fetchone()
        return None if row is None else _notification(row)

    def insert_notification(self, event_key, issuer, revision, reason, link_id, status, places, unresolved, targets, cause=None):
        with self.transaction() as connection:
            cursor = connection.execute(
                "INSERT INTO notifications"
                "(event_key,issuer,revision,reason,cause,link_id,status,places,unresolved) VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    event_key,
                    issuer,
                    revision,
                    reason,
                    cause,
                    link_id,
                    status,
                    json.dumps(sorted(places), ensure_ascii=False),
                    json.dumps(unresolved, ensure_ascii=False),
                ),
            )
            notification_id = cursor.lastrowid
            connection.executemany(
                "INSERT INTO notification_targets(notification_id,subscriber,places,previous_id) VALUES (?,?,?,?)",
                [
                    (notification_id, subscriber, json.dumps(places, ensure_ascii=False), previous_id)
                    for subscriber, (places, previous_id) in sorted(targets.items())
                ],
            )
        return notification_id

    def add_targets(self, notification_id, targets):
        with self.transaction() as connection:
            connection.executemany(
                "INSERT OR IGNORE INTO notification_targets(notification_id,subscriber,places,previous_id) VALUES (?,?,?,?)",
                [
                    (notification_id, subscriber, json.dumps(places, ensure_ascii=False), previous_id)
                    for subscriber, (places, previous_id) in sorted(targets.items())
                ],
            )

    def replace_notification(self, notification_id, status, places, unresolved, targets):
        """改写一条尚未发送的通知，并整体替换其订阅者目标。"""
        with self.transaction() as connection:
            connection.execute(
                "UPDATE notifications SET status=?, places=?, unresolved=? WHERE id=?",
                (status, json.dumps(sorted(places), ensure_ascii=False), json.dumps(unresolved, ensure_ascii=False), notification_id),
            )
            connection.execute("DELETE FROM notification_targets WHERE notification_id=?", (notification_id,))
            connection.executemany(
                "INSERT INTO notification_targets(notification_id,subscriber,places,previous_id) VALUES (?,?,?,?)",
                [
                    (notification_id, subscriber, json.dumps(places, ensure_ascii=False), previous_id)
                    for subscriber, (places, previous_id) in sorted(targets.items())
                ],
            )

    def corrected_subscribers(self, notification_id, cause=None):
        """已经针对某条原通知、某类更正收到过通知的订阅者，避免重复通知同一结论。"""
        query = (
            "SELECT DISTINCT t.subscriber FROM notification_targets t "
            "JOIN notifications n ON n.id=t.notification_id "
            "WHERE n.reason='correction' AND n.link_id=?"
        )
        params = [notification_id]
        if cause is not None:
            query += " AND n.cause=?"
            params.append(cause)
        rows = self.connection.execute(query, params).fetchall()
        return {row["subscriber"] for row in rows}

    def first_correction(self, notification_id, cause=None):
        """某条原通知最早的同因更正；后续更正只补挂新的订阅者。"""
        query = "SELECT * FROM notifications WHERE reason='correction' AND link_id=?"
        params = [notification_id]
        if cause is not None:
            query += " AND cause=?"
            params.append(cause)
        row = self.connection.execute(query + " ORDER BY id LIMIT 1", params).fetchone()
        return None if row is None else _notification(row)

    def prior_sent_subscriber(self, event_key, issuer, subscriber, revision):
        """该订阅者在更早修订中最后一次被实际发送的通知。"""
        row = self.connection.execute(
            "SELECT t.notification_id FROM notification_targets t "
            "JOIN notifications n ON n.id=t.notification_id "
            "WHERE n.event_key=? AND n.issuer=? AND n.reason='alert' AND n.status='sent' "
            "AND n.revision< ? AND t.subscriber=? ORDER BY n.revision DESC, n.id DESC LIMIT 1",
            (event_key, issuer, revision, subscriber),
        ).fetchone()
        return None if row is None else row["notification_id"]

    def latest_sent_targets(self, event_key, issuer, revision):
        """每个订阅者在早于 ``revision`` 的修订中最后一次实际送达的通知。

        返回 ``{notification_id: {subscriber: places}}``。更早的通知已被这一版
        取代，无需再逐条更正。
        """
        rows = self.connection.execute(
            "SELECT t.subscriber, t.notification_id, t.places, n.revision FROM notification_targets t "
            "JOIN notifications n ON n.id=t.notification_id "
            "WHERE n.event_key=? AND n.issuer=? AND n.reason='alert' AND n.status='sent' "
            "AND n.revision< ? ORDER BY n.revision DESC, n.id DESC",
            (event_key, issuer, revision),
        ).fetchall()
        latest = {}
        for row in rows:
            if row["subscriber"] in latest:
                continue
            latest[row["subscriber"]] = (row["notification_id"], json.loads(row["places"]))
        grouped = {}
        for subscriber, (notification_id, places) in latest.items():
            grouped.setdefault(notification_id, {})[subscriber] = places
        return grouped

    def notifications(self, event_key=None, issuer=None):
        query = "SELECT * FROM notifications"
        clauses, params = [], []
        if event_key is not None:
            clauses.append("event_key=?")
            params.append(event_key)
        if issuer is not None:
            clauses.append("issuer=?")
            params.append(issuer)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        rows = self.connection.execute(query + " ORDER BY id", params).fetchall()
        return [_notification(row) for row in rows]

    def targets(self, notification_id):
        rows = self.connection.execute(
            "SELECT subscriber,places,previous_id FROM notification_targets "
            "WHERE notification_id=? ORDER BY subscriber",
            (notification_id,),
        ).fetchall()
        return [
            {"subscriber": row["subscriber"], "places": json.loads(row["places"]), "previous": row["previous_id"]}
            for row in rows
        ]

    def transition(self, notification_id, expected, new):
        """仅当状态为 ``expected`` 时改为 ``new``；返回是否改动。"""
        with self.transaction() as connection:
            cursor = connection.execute(
                "UPDATE notifications SET status=? WHERE id=? AND status=?", (new, notification_id, expected)
            )
            return cursor.rowcount == 1


def _observation(row):
    item = dict(row)
    item["regions"] = json.loads(item["regions"])
    item["raw"] = json.loads(item["raw"])
    return item


def _notification(row):
    item = dict(row)
    item["places"] = json.loads(item["places"])
    item["unresolved"] = json.loads(item["unresolved"])
    return item
