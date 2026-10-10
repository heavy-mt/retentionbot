from unittest.mock import AsyncMock

import pytest

from retentionbot.api import ApiError
from retentionbot.invalidation_cleanup import (
    METADATA_WINDOW_MS,
    compact_receipts_txn,
    validate_receipts,
)
from retentionbot.service import Observer
from retentionbot.store import now_ms
from retentionbot.worker import Worker

from .test_queue import event, ingest


class Transaction:
    def __init__(self, db):
        self.db = db

    def execute(self, query, params=()):
        self.cursor = self.db.execute(query, params)

    def fetchone(self):
        return self.cursor.fetchone()


def receipt(key="$new", generation=1):
    return {"event_id": key, "room_id": "!room:test", "generation": generation, "completed_at": 1}


@pytest.mark.parametrize(
    "change",
    [
        {"generation": True},
        {"generation": 0},
        {"completed_at": METADATA_WINDOW_MS},
        {"completed_at": None},
        {"event_id": "bad"},
        {"room_id": "bad"},
    ],
)
def test_receipt_rejects_recent_or_invalid_completion(change):
    with pytest.raises(ValueError):
        validate_receipts([receipt() | change], METADATA_WINDOW_MS + 1)


def test_receipt_boundary_and_batch_limit():
    validate_receipts([receipt()], METADATA_WINDOW_MS + 2)
    with pytest.raises(ValueError):
        validate_receipts([receipt()] * 1001, METADATA_WINDOW_MS + 2)
    with pytest.raises(ValueError):
        validate_receipts([], METADATA_WINDOW_MS + 2)


@pytest.fixture
def synapse_txn(store):
    store.db.executescript("""
        CREATE TABLE retentionbot_event_invalidations(
            event_id TEXT PRIMARY KEY, room_id TEXT, generation BIGINT);
        CREATE TABLE retentionbot_cache_reset_rooms(room_id TEXT PRIMARY KEY,generation BIGINT);
        INSERT INTO retentionbot_event_invalidations VALUES('$new','!room:test',1);
        INSERT INTO retentionbot_event_invalidations VALUES('$other','!room:test',2);
        INSERT INTO retentionbot_cache_reset_rooms VALUES('!room:test',2);
    """)
    return Transaction(store.db)


def test_cleanup_replay_keeps_room_generation_and_other_records(store, synapse_txn):
    with store.db:
        assert compact_receipts_txn(synapse_txn, [receipt()], True)
        assert compact_receipts_txn(synapse_txn, [receipt()], True)
    assert (
        store.db.execute("SELECT generation FROM retentionbot_cache_reset_rooms").fetchone()[0] == 2
    )
    assert (
        store.db.execute("SELECT count(*) FROM retentionbot_event_invalidations").fetchone()[0] == 1
    )


@pytest.mark.parametrize("reason", ["mismatch", "unpersisted_reset", "original_present"])
def test_cleanup_defers_whole_batch_when_any_receipt_is_unsafe(store, synapse_txn, reason):
    bad = receipt("$other", 2)
    if reason == "mismatch":
        bad["generation"] = 3
    elif reason == "unpersisted_reset":
        store.db.execute("UPDATE retentionbot_cache_reset_rooms SET generation=1")
    else:
        ingest(store, event("$other"))
    with store.db:
        assert not compact_receipts_txn(synapse_txn, [receipt(), bad], True)
    assert (
        store.db.execute("SELECT count(*) FROM retentionbot_event_invalidations").fetchone()[0] == 2
    )


async def test_coordinated_cleanup_retry_and_late_delivery(config, store):
    ingest(store, event())
    store.finish("$new", {"status": "missed", "code": "EVENT_PURGED"}, 1)
    store.invalidation_finish("$new", {"generation": 7}, 1)
    api = AsyncMock()
    api.compact_invalidations.side_effect = ApiError(503, "TEMPORARY")
    observer = Observer(config, store, api, AsyncMock())
    with pytest.raises(ApiError):
        await observer.schedule()
    assert store.event("$new") is not None
    api.compact_invalidations.side_effect = None
    api.compact_invalidations.return_value = {"status": "deferred"}
    with pytest.raises(ApiError):
        await observer.schedule()
    assert store.invalidation("$new") is not None
    api.compact_invalidations.return_value = {"status": "done"}
    await observer.schedule()
    assert store.event("$new") is None
    assert store.invalidation("$new") is None
    await Worker(store, api).handle_invalidation("$new")
    api.invalidate.assert_not_awaited()


def test_pending_and_recent_completion_are_not_offered_for_remote_cleanup(store):
    ingest(store, event("$pending"), event("$recent"), event("$eligible"))
    for key in ("$pending", "$recent", "$eligible"):
        store.finish(key, {"status": "done", "redaction_id": "$r"}, 1)
    store.invalidation_finish("$recent", {"generation": 2}, now_ms())
    store.invalidation_finish("$eligible", {"generation": 3}, 1)
    candidates = store.compaction_candidates(now_ms() - METADATA_WINDOW_MS)
    assert [row["event_id"] for row in candidates] == ["$eligible"]
    store.compact_confirmed(candidates, now_ms() - METADATA_WINDOW_MS)
    assert store.event("$pending") and store.event("$recent")
