from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path


def now_ms() -> int:
    return time.time_ns() // 1_000_000


class Store:
    """Durable metadata queue. Never stores decrypted message bodies or attachment keys."""

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.execute("PRAGMA busy_timeout=5000")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS rooms(
                room_id TEXT PRIMARY KEY, since_ts INTEGER NOT NULL, lifetime INTEGER,
                last_event TEXT, error TEXT, joined INTEGER NOT NULL DEFAULT 1,
                policy_ts INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS events(
                event_id TEXT PRIMARY KEY, room_id TEXT NOT NULL REFERENCES rooms(room_id),
                ts INTEGER NOT NULL, parent TEXT, kind TEXT,
                decoded INTEGER NOT NULL, ciphertext TEXT,
                redacted INTEGER NOT NULL DEFAULT 0, next_try INTEGER NOT NULL DEFAULT 0,
                attempts INTEGER NOT NULL DEFAULT 0, error TEXT,
                queued_at INTEGER NOT NULL DEFAULT 0);
            CREATE INDEX IF NOT EXISTS event_room_ts ON events(room_id,ts);
            CREATE INDEX IF NOT EXISTS event_parent ON events(parent);
            CREATE TABLE IF NOT EXISTS media(
                uri TEXT PRIMARY KEY, protected INTEGER NOT NULL DEFAULT 0,
                deleted INTEGER NOT NULL DEFAULT 0, next_try INTEGER NOT NULL DEFAULT 0,
                error TEXT, queued_at INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS media_refs(
                uri TEXT NOT NULL REFERENCES media(uri),
                event_id TEXT NOT NULL REFERENCES events(event_id),
                PRIMARY KEY(uri,event_id));
            CREATE TABLE IF NOT EXISTS commands(
                event_id TEXT PRIMARY KEY, room_id TEXT NOT NULL, sender TEXT NOT NULL,
                action TEXT NOT NULL, argument TEXT, done INTEGER NOT NULL DEFAULT 0,
                ts INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS outbox(
                event_id TEXT PRIMARY KEY, room_id TEXT NOT NULL, body TEXT NOT NULL,
                delivered INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS device_keys(
                user_id TEXT NOT NULL, device_id TEXT NOT NULL, ed25519 TEXT NOT NULL,
                PRIMARY KEY(user_id,device_id));
        """)
        for table, column in [("rooms", "policy_ts"), ("commands", "ts")]:
            if column not in {r[1] for r in self.db.execute(f"PRAGMA table_info({table})")}:
                self.db.execute(
                    f"ALTER TABLE {table} ADD COLUMN {column} INTEGER NOT NULL DEFAULT 0"
                )
        self.db.commit()
        if self.get("started_at") is None:
            self.set("started_at", str(now_ms()))

    def close(self):
        self.db.close()

    def get(self, key: str) -> str | None:
        row = self.db.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return row[0] if row else None

    def set(self, key: str, value: str):
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO settings VALUES(?,?)", (key, value))

    def enroll(self, room_id: str, lifetime: int | None, since_ts: int | None = None):
        with self.db:
            self.db.execute(
                "INSERT OR IGNORE INTO rooms(room_id,since_ts,lifetime) VALUES(?,?,?)",
                (room_id, since_ts if since_ts is not None else now_ms(), lifetime),
            )

    def room(self, room_id: str):
        return self.db.execute("SELECT * FROM rooms WHERE room_id=?", (room_id,)).fetchone()

    def room_error(self, room_id: str, error: str | None):
        with self.db:
            self.db.execute("UPDATE rooms SET error=? WHERE room_id=?", (error, room_id))

    def record(
        self,
        room_id: str,
        event_id: str,
        ts: int,
        *,
        kind: str,
        parent: str | None = None,
        decoded: bool = True,
        ciphertext: dict | None = None,
        media: set[str] | None = None,
    ) -> bool:
        room = self.room(room_id)
        if not room or ts < room["since_ts"]:
            return False
        with self.db:
            self.db.execute(
                """
                INSERT INTO events(event_id,room_id,ts,parent,kind,decoded,ciphertext)
                VALUES(?,?,?,?,?,?,?) ON CONFLICT(event_id) DO UPDATE SET
                    parent=COALESCE(excluded.parent,events.parent),
                    kind=CASE WHEN excluded.decoded THEN excluded.kind ELSE events.kind END,
                    decoded=MAX(events.decoded,excluded.decoded),
                    ciphertext=CASE WHEN excluded.decoded THEN NULL ELSE events.ciphertext END
            """,
                (
                    event_id,
                    room_id,
                    ts,
                    parent,
                    kind,
                    decoded,
                    json.dumps(ciphertext) if ciphertext is not None else None,
                ),
            )
            for uri in media or ():
                self.db.execute("INSERT OR IGNORE INTO media(uri) VALUES(?)", (uri,))
                self.db.execute("INSERT OR IGNORE INTO media_refs VALUES(?,?)", (uri, event_id))
        return True

    def protect(self, uri: str):
        with self.db:
            self.db.execute(
                """
                INSERT INTO media(uri,protected) VALUES(?,1)
                ON CONFLICT(uri) DO UPDATE SET protected=1
            """,
                (uri,),
            )

    def last_event(self, room_id: str, event_id: str):
        with self.db:
            self.db.execute("UPDATE rooms SET last_event=? WHERE room_id=?", (event_id, room_id))

    def queue_command(
        self, event_id: str, room_id: str, sender: str, action: str, argument: str | None
    ):
        event = self.db.execute("SELECT ts FROM events WHERE event_id=?", (event_id,)).fetchone()
        ts = event[0] if event else now_ms()
        with self.db:
            self.db.execute(
                """
                INSERT OR IGNORE INTO commands(event_id,room_id,sender,action,argument,ts)
                VALUES(?,?,?,?,?,?)
            """,
                (event_id, room_id, sender, action, argument, ts),
            )

    def finish_command(
        self, command, body: str, *, change: bool = False, lifetime: int | None = None
    ):
        with self.db:
            if change:
                self.db.execute(
                    "UPDATE rooms SET lifetime=?,policy_ts=? WHERE room_id=?",
                    (lifetime, command["ts"], command["room_id"]),
                )
            self.db.execute("UPDATE commands SET done=1 WHERE event_id=?", (command["event_id"],))
            self.db.execute(
                "INSERT OR IGNORE INTO outbox(event_id,room_id,body) VALUES(?,?,?)",
                (command["event_id"], command["room_id"], body),
            )

    def due_events(self, at: int, limit: int = 100):
        # Edits/reactions expire with their original when it was observed in this room.
        return self.db.execute(
            """
            SELECT e.* FROM events e JOIN rooms r USING(room_id)
            LEFT JOIN events parent ON parent.event_id=e.parent AND parent.room_id=e.room_id
            WHERE r.lifetime IS NOT NULL AND r.joined=1 AND r.error IS NULL
              AND e.redacted=0 AND e.decoded=1 AND e.next_try<=?
              AND e.queued_at<?
              AND COALESCE(parent.ts,e.ts)+r.lifetime<=?
            ORDER BY COALESCE(parent.ts,e.ts),e.event_id LIMIT ?
        """,
            (at, at - 300_000, at, limit),
        ).fetchall()

    def event_due(self, event_id: str, at: int):
        return self.db.execute(
            """
            SELECT e.* FROM events e JOIN rooms r USING(room_id)
            LEFT JOIN events parent ON parent.event_id=e.parent AND parent.room_id=e.room_id
            WHERE e.event_id=? AND r.lifetime IS NOT NULL AND r.joined=1 AND r.error IS NULL
              AND e.redacted=0 AND e.decoded=1 AND e.next_try<=?
              AND COALESCE(parent.ts,e.ts)+r.lifetime<=?
        """,
            (event_id, at, at),
        ).fetchone()

    def mark_redacted(self, event_id: str, at: int):
        with self.db:
            self.db.execute(
                """
                UPDATE events SET redacted=1,
                    ciphertext=CASE WHEN decoded=1 THEN NULL ELSE ciphertext END,
                    error=NULL,queued_at=0 WHERE event_id=?
            """,
                (event_id,),
            )
            self.db.execute(
                """
                UPDATE media SET next_try=MAX(next_try,?) WHERE uri IN
                    (SELECT uri FROM media_refs WHERE event_id=?)
            """,
                (at, event_id),
            )

    def retry(self, event_id: str, at: int, error: str):
        with self.db:
            self.db.execute(
                """
                UPDATE events SET next_try=?,attempts=attempts+1,error=?,queued_at=0
                WHERE event_id=?
            """,
                (at, error, event_id),
            )

    def media_candidates(self, at: int, limit: int = 100):
        return self.db.execute(
            """
            SELECT m.* FROM media m WHERE m.protected=0 AND m.deleted=0 AND m.next_try<=?
              AND m.queued_at<?
              AND EXISTS(SELECT 1 FROM media_refs refs WHERE refs.uri=m.uri)
              AND NOT EXISTS(SELECT 1 FROM media_refs refs JOIN events e USING(event_id)
                             WHERE refs.uri=m.uri AND e.redacted=0)
            LIMIT ?
        """,
            (at, at - 300_000, limit),
        ).fetchall()

    def media_eligible(self, uri: str, at: int) -> bool:
        return bool(
            self.db.execute(
                """
            SELECT 1 FROM media m WHERE m.uri=? AND m.protected=0 AND m.deleted=0
              AND m.next_try<=? AND EXISTS(SELECT 1 FROM media_refs refs WHERE refs.uri=m.uri)
              AND NOT EXISTS(SELECT 1 FROM media_refs refs JOIN events e USING(event_id)
                             WHERE refs.uri=m.uri AND e.redacted=0)
        """,
                (uri, at),
            ).fetchone()
        )

    def queued(self, kind: str, key: str, at: int):
        table, column = ("events", "event_id") if kind == "redact" else ("media", "uri")
        with self.db:
            self.db.execute(f"UPDATE {table} SET queued_at=? WHERE {column}=?", (at, key))

    def release(self, kind: str, key: str):
        self.queued(kind, key, 0)

    def media_done(self, uri: str, *, protected: bool = False):
        with self.db:
            self.db.execute(
                """
                UPDATE media SET deleted=?,protected=MAX(protected,?),queued_at=0,error=NULL
                WHERE uri=?
            """,
                (not protected, protected, uri),
            )

    def media_retry(self, uri: str, at: int, error: str):
        with self.db:
            self.db.execute(
                "UPDATE media SET next_try=?,error=?,queued_at=0 WHERE uri=?", (at, error, uri)
            )

    def cleanup_ready(self, at: int) -> bool:
        last_sync = int(self.get("last_sync_at") or "0")
        coverage = self.get("coverage_ok") == "1"
        return (
            coverage
            and at - last_sync < 90_000
            and not self.undecoded(1)
            and not self.db.execute(
                "SELECT 1 FROM rooms WHERE error IS NOT NULL LIMIT 1"
            ).fetchone()
        )

    def undecoded(self, limit: int = 100):
        return self.db.execute(
            """
            SELECT * FROM events WHERE decoded=0 AND ciphertext IS NOT NULL
            ORDER BY next_try,ts LIMIT ?
        """,
            (limit,),
        ).fetchall()

    def counts(self, room_id: str | None = None) -> dict:
        where, args = ("WHERE room_id=?", (room_id,)) if room_id else ("", ())
        row = self.db.execute(
            f"""
            SELECT COUNT(*) AS total,
                COALESCE(SUM(redacted=0),0) AS pending,
                COALESCE(SUM(redacted=0 AND decoded=0),0) AS missing_keys,
                COALESCE(SUM(redacted=0 AND error IS NOT NULL),0) AS errors
            FROM events {where}
        """,
            args,
        ).fetchone()
        return dict(row)
