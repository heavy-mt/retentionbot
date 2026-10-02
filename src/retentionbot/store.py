"""Metadata only: no message bodies, ciphertext, keys or Matrix access tokens."""

from __future__ import annotations

import time
from pathlib import Path

from .database import Database


def now_ms() -> int:
    return time.time_ns() // 1_000_000


class Store:
    def __init__(self, target: str | Path, namespace: str = "server"):
        self.db = Database(target, namespace)
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS rooms(
                room_id TEXT PRIMARY KEY, min_lifetime BIGINT, max_lifetime BIGINT,
                after_ms BIGINT, error TEXT);
            CREATE TABLE IF NOT EXISTS events(
                event_id TEXT PRIMARY KEY, room_id TEXT NOT NULL REFERENCES rooms(room_id),
                sender TEXT NOT NULL, ts BIGINT NOT NULL, kind TEXT NOT NULL,
                anchor_ts BIGINT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
                queued_at BIGINT NOT NULL DEFAULT 0, next_try BIGINT NOT NULL DEFAULT 0,
                attempts BIGINT NOT NULL DEFAULT 0, error TEXT, redaction_id TEXT,
                completed_at BIGINT);
            CREATE INDEX IF NOT EXISTS pending_deadline
                ON events(room_id,anchor_ts,event_id) WHERE status='pending';
            CREATE INDEX IF NOT EXISTS completed_metadata
                ON events(completed_at) WHERE status<>'pending';
        """)
        self.db.initialized(namespace)

    def close(self):
        self.db.close()

    def get(self, key: str):
        row = self.db.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return row[0] if row else None

    def set(self, key: str, value):
        self.db.execute(
            "INSERT INTO settings VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)),
        )

    def bootstrap(self, cursor: int, since_ts: int):
        with self.db:
            self.set("cursor", cursor)
            self.set("since_ts", since_ts)

    def policy(self, room_id: str, data: dict):
        self.db.execute(
            """
            INSERT INTO rooms VALUES(?,?,?,?,?) ON CONFLICT(room_id) DO UPDATE SET
            min_lifetime=excluded.min_lifetime,max_lifetime=excluded.max_lifetime,
            after_ms=excluded.after_ms,error=excluded.error
        """,
            (
                room_id,
                data.get("min_lifetime"),
                data.get("max_lifetime"),
                data.get("redact_after_ms"),
                data.get("policy_error"),
            ),
        )

    def ingest(self, page: dict, policies: dict):
        since = int(self.get("since_ts"))
        with self.db:
            for room, policy in policies.items():
                self.policy(room, policy)
            for event in page["events"]:
                if event["ts"] < since or event["kind"] == "m.room.retention":
                    continue
                self.db.execute(
                    """
                    INSERT INTO events(event_id,room_id,sender,ts,kind,anchor_ts)
                    VALUES(?,?,?,?,?,?) ON CONFLICT(event_id) DO NOTHING
                """,
                    (
                        event["event_id"],
                        event["room_id"],
                        event["sender"],
                        event["ts"],
                        event["kind"],
                        event["anchor_ts"],
                    ),
                )
            self.set("cursor", page["cursor"])
            self.set("last_poll_at", now_ms())

    def due(self, at: int, limit: int = 1000):
        rooms = self.db.execute(
            "SELECT * FROM rooms WHERE max_lifetime IS NOT NULL "
            "AND after_ms IS NOT NULL AND error IS NULL "
            "ORDER BY room_id"
        ).fetchall()
        offset = int(self.get("scheduler_cursor") or 0) % max(1, len(rooms))
        jobs = []
        for room in rooms[offset:] + rooms[:offset]:
            jobs.extend(
                self.db.execute(
                    """
                SELECT e.* FROM events e WHERE room_id=? AND status='pending'
                AND anchor_ts<=? AND ts<=? AND next_try<=? AND (queued_at=0 OR queued_at<=?)
                ORDER BY anchor_ts,event_id LIMIT ?
            """,
                    (
                        room["room_id"],
                        at - room["after_ms"],
                        at - (room["min_lifetime"] or 0),
                        at,
                        at - 300_000,
                        limit - len(jobs),
                    ),
                ).fetchall()
            )
            if len(jobs) >= limit:
                break
        with self.db:
            self.set("scheduler_cursor", offset + 1)
        return jobs

    def event(self, event_id: str):
        return self.db.execute("SELECT * FROM events WHERE event_id=?", (event_id,)).fetchone()

    def queued(self, event_id: str, at: int):
        with self.db:
            self.db.execute(
                "UPDATE events SET queued_at=? WHERE event_id=? AND status='pending'",
                (at, event_id),
            )

    def retry(self, event_id: str, at: int, code: str):
        with self.db:
            self.db.execute(
                "UPDATE events SET next_try=?,error=?,attempts=attempts+1,queued_at=0 "
                "WHERE event_id=? AND status='pending'",
                (at, code, event_id),
            )

    def finish(self, event_id: str, result: dict, at: int):
        with self.db:
            self.db.execute(
                "UPDATE events SET status=?,error=?,redaction_id=?,completed_at=?,"
                "queued_at=0 WHERE event_id=? AND status='pending'",
                (result["status"], result.get("code"), result.get("redaction_id"), at, event_id),
            )

    def counts(self):
        return {
            row["status"]: row["n"]
            for row in self.db.execute("SELECT status,COUNT(*) AS n FROM events GROUP BY status")
        }

    def compact(self, before: int, limit: int = 1000):
        with self.db:
            self.db.execute(
                "DELETE FROM events WHERE event_id IN (SELECT event_id FROM events "
                "WHERE status='done' AND completed_at<? ORDER BY completed_at LIMIT ?)",
                (before, limit),
            )
