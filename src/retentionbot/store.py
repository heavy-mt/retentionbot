from __future__ import annotations

import json
import time
from pathlib import Path

from .database import Database


def now_ms() -> int:
    return time.time_ns() // 1_000_000


class Store:
    """Durable metadata queue. Never stores decrypted message bodies or attachment keys."""

    def __init__(self, target: str | Path, namespace: str = "bot"):
        self.db = Database(target, namespace)
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS rooms(
                room_id TEXT PRIMARY KEY, since_ts BIGINT NOT NULL, lifetime BIGINT,
                last_event TEXT, error TEXT, joined BIGINT NOT NULL DEFAULT 1,
                policy_ts BIGINT NOT NULL DEFAULT 0, min_lifetime BIGINT, redact_after BIGINT,
                activated BIGINT NOT NULL DEFAULT 1);
            CREATE TABLE IF NOT EXISTS events(
                event_id TEXT PRIMARY KEY, room_id TEXT NOT NULL REFERENCES rooms(room_id),
                ts BIGINT NOT NULL, parent TEXT, kind TEXT,
                decoded BIGINT NOT NULL, ciphertext TEXT,
                redacted BIGINT NOT NULL DEFAULT 0, next_try BIGINT NOT NULL DEFAULT 0,
                attempts BIGINT NOT NULL DEFAULT 0, error TEXT,
                anchor_ts BIGINT, queued_at BIGINT NOT NULL DEFAULT 0,
                key_next_try BIGINT NOT NULL DEFAULT 0);
            CREATE INDEX IF NOT EXISTS event_room_ts ON events(room_id,ts);
            CREATE INDEX IF NOT EXISTS event_parent ON events(parent);
            CREATE INDEX IF NOT EXISTS event_pending ON events(room_id,ts) WHERE redacted=0;
            CREATE TABLE IF NOT EXISTS media(
                uri TEXT PRIMARY KEY, protected BIGINT NOT NULL DEFAULT 0,
                deleted BIGINT NOT NULL DEFAULT 0, next_try BIGINT NOT NULL DEFAULT 0,
                error TEXT, queued_at BIGINT NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS media_refs(
                uri TEXT NOT NULL REFERENCES media(uri),
                event_id TEXT NOT NULL REFERENCES events(event_id),
                PRIMARY KEY(uri,event_id));
            CREATE INDEX IF NOT EXISTS media_ref_event ON media_refs(event_id);
            CREATE TABLE IF NOT EXISTS commands(
                event_id TEXT PRIMARY KEY, room_id TEXT NOT NULL, sender TEXT NOT NULL,
                action TEXT NOT NULL, argument TEXT, done BIGINT NOT NULL DEFAULT 0,
                ts BIGINT NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS outbox(
                event_id TEXT PRIMARY KEY, room_id TEXT NOT NULL, body TEXT NOT NULL,
                delivered BIGINT NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS device_keys(
                user_id TEXT NOT NULL, device_id TEXT NOT NULL, ed25519 TEXT NOT NULL,
                PRIMARY KEY(user_id,device_id));
        """)
        for table, column, default in [
            ("rooms", "policy_ts", 0),
            ("commands", "ts", 0),
            ("rooms", "activated", 1),
            ("rooms", "min_lifetime", None),
            ("rooms", "redact_after", None),
            ("events", "anchor_ts", None),
            ("events", "key_next_try", 0),
        ]:
            if column not in self.db.columns(table):
                self.db.execute(
                    f"ALTER TABLE {table} ADD COLUMN {column} BIGINT"
                    + (f" NOT NULL DEFAULT {default}" if default is not None else "")
                )
        self.db.execute(
            "CREATE INDEX IF NOT EXISTS pending_keys ON events(key_next_try,ts) "
            "WHERE decoded=0 AND ciphertext IS NOT NULL"
        )
        if self.get("schema_version") != "2":
            self.db.execute("UPDATE rooms SET redact_after=lifetime WHERE redact_after IS NULL")
            self.db.execute(
                "UPDATE events SET anchor_ts=COALESCE("
                "(SELECT p.ts FROM events p WHERE p.event_id=events.parent "
                "AND p.room_id=events.room_id),ts) WHERE anchor_ts IS NULL"
            )
            self.set("schema_version", "2")
        self.db.execute(
            "CREATE INDEX IF NOT EXISTS pending_deadline ON events(room_id,anchor_ts,event_id) "
            "WHERE redacted=0"
        )
        self.db.initialized(namespace)
        if self.get("started_at") is None:
            self.set("started_at", str(now_ms()))

    def close(self):
        self.db.close()

    def get(self, key: str) -> str | None:
        row = self.db.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return row[0] if row else None

    def set(self, key: str, value: str):
        with self.db:
            self.db.execute(
                "INSERT INTO settings VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )

    def enroll(
        self,
        room_id: str,
        lifetime: int | None,
        since_ts: int | None = None,
        *,
        active: bool = True,
    ):
        cutoff = since_ts if since_ts is not None else now_ms()
        with self.db:
            self.db.execute(
                "INSERT INTO rooms(room_id,since_ts,lifetime,redact_after,activated,joined) "
                "VALUES(?,?,?,?,?,?) ON CONFLICT(room_id) DO NOTHING",
                (room_id, cutoff, lifetime, lifetime, active, active),
            )
            if active:
                self.db.execute(
                    "UPDATE rooms SET since_ts=?,activated=1,joined=1 "
                    "WHERE room_id=? AND activated=0",
                    (cutoff, room_id),
                )

    def room(self, room_id: str):
        return self.db.execute("SELECT * FROM rooms WHERE room_id=?", (room_id,)).fetchone()

    def room_error(self, room_id: str, error: str | None):
        with self.db:
            self.db.execute("UPDATE rooms SET error=? WHERE room_id=?", (error, room_id))

    def policy(
        self,
        room_id: str,
        minimum: int | None,
        maximum: int | None,
        ts: int = 0,
        *,
        lead_ms: int = 0,
    ):
        after = None if maximum is None else max(minimum or 0, maximum - lead_ms)
        if maximum is not None and lead_ms and after >= maximum:
            raise ValueError("Между min_lifetime и max_lifetime нужен запас для доставки удаления.")
        with self.db:
            self.db.execute(
                "UPDATE rooms SET min_lifetime=?,lifetime=?,redact_after=?,"
                "policy_ts=GREATEST(policy_ts,?) WHERE room_id=?",
                (minimum, maximum, after, ts, room_id),
            )

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
        if not room or not room["activated"] or ts < room["since_ts"]:
            return False
        parent_row = (
            self.db.execute(
                "SELECT ts FROM events WHERE event_id=? AND room_id=?", (parent, room_id)
            ).fetchone()
            if parent
            else None
        )
        anchor = parent_row["ts"] if parent_row else ts
        with self.db:
            self.db.execute(
                """
                INSERT INTO events(event_id,room_id,ts,parent,kind,decoded,ciphertext,anchor_ts)
                VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(event_id) DO UPDATE SET
                    parent=COALESCE(excluded.parent,events.parent),
                    anchor_ts=CASE WHEN excluded.decoded=1
                        THEN excluded.anchor_ts ELSE events.anchor_ts END,
                    kind=CASE WHEN excluded.decoded=1 THEN excluded.kind ELSE events.kind END,
                    decoded=GREATEST(events.decoded,excluded.decoded),
                    ciphertext=CASE WHEN excluded.decoded=1 THEN NULL ELSE events.ciphertext END
            """,
                (
                    event_id,
                    room_id,
                    ts,
                    parent,
                    kind,
                    decoded,
                    json.dumps(ciphertext) if ciphertext is not None else None,
                    anchor,
                ),
            )
            self.db.execute(
                "UPDATE events SET anchor_ts=? WHERE parent=? AND room_id=?",
                (ts, event_id, room_id),
            )
            for uri in media or ():
                self.db.execute(
                    "INSERT INTO media(uri) VALUES(?) ON CONFLICT(uri) DO NOTHING", (uri,)
                )
                self.db.execute(
                    "INSERT INTO media_refs VALUES(?,?) ON CONFLICT(uri,event_id) DO NOTHING",
                    (uri, event_id),
                )
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
                INSERT INTO commands(event_id,room_id,sender,action,argument,ts)
                VALUES(?,?,?,?,?,?) ON CONFLICT(event_id) DO NOTHING
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
                "INSERT INTO outbox(event_id,room_id,body) VALUES(?,?,?) "
                "ON CONFLICT(event_id) DO NOTHING",
                (command["event_id"], command["room_id"], body),
            )

    def due_events(self, at: int, limit: int = 1000):
        rooms = self.db.execute(
            "SELECT * FROM rooms WHERE lifetime IS NOT NULL AND redact_after IS NOT NULL "
            "AND joined=1 AND (error IS NULL OR error='GAP_ANCHOR_UNAVAILABLE') ORDER BY room_id"
        ).fetchall()
        if not rooms:
            return []
        # Indexed range lookup per room; avoid scanning every unexpired event on each tick.
        offset = int(self.get("scheduler_cursor") or "0") % len(rooms)
        rooms = rooms[offset:] + rooms[:offset]
        jobs = []
        for room in rooms:
            jobs.extend(
                self.db.execute(
                    "SELECT * FROM events WHERE room_id=? AND redacted=0 AND next_try<=? "
                    "AND queued_at<? AND anchor_ts<=? AND ts<=? "
                    "ORDER BY anchor_ts,event_id LIMIT ?",
                    (
                        room["room_id"],
                        at,
                        at - 300_000,
                        at - room["redact_after"],
                        at - (room["min_lifetime"] or 0),
                        limit - len(jobs),
                    ),
                ).fetchall()
            )
            if len(jobs) >= limit:
                break
        self.set("scheduler_cursor", str(offset + 1))
        return jobs

    def event_due(self, event_id: str, at: int):
        return self.db.execute(
            """
            SELECT e.* FROM events e JOIN rooms r USING(room_id)
            WHERE e.event_id=? AND r.lifetime IS NOT NULL
              AND r.redact_after IS NOT NULL AND r.joined=1
              AND (r.error IS NULL OR r.error='GAP_ANCHOR_UNAVAILABLE')
              AND e.redacted=0 AND e.next_try<=?
              AND e.anchor_ts<=?-r.redact_after
              AND e.ts<=?-COALESCE(r.min_lifetime,0)
        """,
            (event_id, at, at, at),
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
                UPDATE media SET next_try=GREATEST(next_try,?) WHERE uri IN
                    (SELECT uri FROM media_refs WHERE event_id=?)
            """,
                (at, event_id),
            )

    def key_retry(self, event_id: str, at: int):
        with self.db:
            self.db.execute(
                "UPDATE events SET key_next_try=?,error='MISSING_ROOM_KEY' WHERE event_id=?",
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

    def media_candidates(self, at: int, limit: int = 1000):
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
                UPDATE media SET deleted=?,protected=GREATEST(protected,?),queued_at=0,error=NULL
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

    def undecoded(self, limit: int = 1000):
        return self.db.execute(
            """
            SELECT * FROM events WHERE decoded=0 AND ciphertext IS NOT NULL
            ORDER BY key_next_try,ts LIMIT ?
        """,
            (limit,),
        ).fetchall()

    def counts(self, room_id: str | None = None) -> dict:
        where, args = ("WHERE room_id=?", (room_id,)) if room_id else ("", ())
        row = self.db.execute(
            f"""
            SELECT COUNT(*) AS total,
                COALESCE(SUM(CASE WHEN redacted=0 THEN 1 ELSE 0 END),0) AS pending,
                COALESCE(SUM(CASE WHEN decoded=0 AND ciphertext IS NOT NULL
                    THEN 1 ELSE 0 END),0) AS missing_keys,
                COALESCE(SUM(CASE WHEN redacted=0 AND error IS NOT NULL
                    THEN 1 ELSE 0 END),0) AS errors
            FROM events {where}
        """,
            args,
        ).fetchone()
        return dict(row)
