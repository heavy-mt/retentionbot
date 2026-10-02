from .db import open_store

ROOM = "!room:example.org"
POLICY = {"min_lifetime": 100, "max_lifetime": 1000, "redact_after_ms": 800}


def event(key="$new", ts=200, **kwargs):
    return (
        dict(
            event_id=key,
            room_id=ROOM,
            sender="@a:example.org",
            ts=ts,
            anchor_ts=ts,
            kind="m.room.message",
        )
        | kwargs
    )


def ingest(store, *events):
    store.ingest({"events": list(events), "cursor": 10}, {ROOM: POLICY})


def test_no_retroactive_and_durable_cursor(store, tmp_path):
    ingest(store, event("$old", 99), event(), event())
    assert store.event("$old") is None
    assert store.counts() == {"pending": 1}
    assert store.get("cursor") == "10"
    path = tmp_path / "restart.db"
    other = open_store(path)
    other.bootstrap(5, 100)
    ingest(other, event())
    other.close()
    other = open_store(path)
    assert other.get("cursor") == "10"
    assert other.event("$new")
    other.close()


def test_cursor_and_page_commit_atomically(store):
    import pytest

    ingest(store, event())
    with pytest.raises(KeyError):
        store.ingest({"cursor": 999, "events": [event("$a"), {"ts": 200}]}, {ROOM: POLICY})
    assert store.get("cursor") == "10"
    assert store.event("$a") is None


def test_minimum_applies_to_late_edit(store):
    ingest(store, event("$original", 200), event("$edit", 2000, anchor_ts=200))
    assert {r["event_id"] for r in store.due(2099)} == {"$original"}
    assert {r["event_id"] for r in store.due(2100)} == {"$original", "$edit"}


def test_lease_and_retry_recover_without_duplicate_completion(store):
    ingest(store, event())
    store.queued("$new", 400000)
    assert not store.due(400001)
    assert store.due(700001)
    store.retry("$new", 800000, "TEMPORARY")
    assert not store.due(799999)
    assert store.due(800000)
    store.finish("$new", {"status": "done", "redaction_id": "$r"}, 800001)
    assert not store.due(900000)


def test_off_or_invalid_policy_prevents_scheduling(store):
    ingest(store, event())
    with store.db:
        store.policy(ROOM, {"max_lifetime": None})
    assert not store.due(900000)
    with store.db:
        store.policy(ROOM, POLICY | {"policy_error": "INVALID_RETENTION_WINDOW"})
    assert not store.due(900000)


def test_archival_keeps_pending_and_missed_jobs(store):
    ingest(store, event("$done"), event("$missed"), event("$pending"))
    store.finish("$done", {"status": "done", "redaction_id": "$r"}, 1000)
    store.finish("$missed", {"status": "missed", "code": "EVENT_PURGED"}, 1000)
    store.compact(2000)
    assert store.counts() == {"missed": 1, "pending": 1}
