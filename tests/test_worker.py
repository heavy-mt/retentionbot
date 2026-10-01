from unittest.mock import AsyncMock

from retentionbot.api import ApiError
from retentionbot.store import now_ms
from retentionbot.worker import Worker, transaction


async def test_disabled_policy_cancels_already_queued_job(config, store):
    store.record("!room:example.org", "$a", 0, kind="m.room.message")
    matrix = AsyncMock()
    worker = Worker(config, store, matrix, AsyncMock())
    with store.db:
        store.db.execute("UPDATE rooms SET lifetime=NULL")
    await worker.handle("redact", "$a")
    matrix.redact.assert_not_called()


async def test_increased_period_postpones_queued_job(config, store):
    at = now_ms()
    store.record("!room:example.org", "$a", at - 2000, kind="m.room.message")
    store.policy("!room:example.org", None, 5000)
    matrix = AsyncMock()
    await Worker(config, store, matrix, AsyncMock()).handle("redact", "$a")
    matrix.redact.assert_not_called()


async def test_duplicate_delivery_does_not_redact_twice(config, store):
    store.record("!room:example.org", "$a", 0, kind="m.room.message")
    matrix = AsyncMock()
    worker = Worker(config, store, matrix, AsyncMock())
    await worker.handle("redact", "$a")
    await worker.handle("redact", "$a")
    matrix.redact.assert_awaited_once_with("!room:example.org", "$a", transaction("$a"))


async def test_rate_limit_persists_retry_and_does_not_mark_success(config, store):
    store.record("!room:example.org", "$a", 0, kind="m.room.message")
    matrix = AsyncMock()
    matrix.redact.side_effect = ApiError(429, "M_LIMIT_EXCEEDED", 120_000)
    await Worker(config, store, matrix, AsyncMock()).handle("redact", "$a")
    row = store.db.execute("SELECT * FROM events").fetchone()
    assert row["redacted"] == 0
    assert row["next_try"] >= now_ms() + 119_000
    assert row["queued_at"] == 0


async def test_new_reference_cancels_already_queued_media_job(config, store):
    uri = "mxc://example.org/file"
    store.set("last_sync_at", str(now_ms()))
    store.set("coverage_ok", "1")
    store.record("!room:example.org", "$a", 0, kind="m.room.message", media={uri})
    store.mark_redacted("$a", 0)
    store.record("!room:example.org", "$b", 0, kind="m.room.message", media={uri})
    gateway = AsyncMock()
    await Worker(config, store, AsyncMock(), gateway).handle("media", uri)
    gateway.delete_media.assert_not_called()


async def test_physical_cleanup_after_last_reference(config, store):
    uri = "mxc://example.org/file"
    store.set("last_sync_at", str(now_ms()))
    store.set("coverage_ok", "1")
    store.record("!room:example.org", "$a", 0, kind="m.room.message", media={uri})
    store.mark_redacted("$a", 0)
    gateway = AsyncMock()
    gateway.delete_media.return_value = {"deleted": True}
    await Worker(config, store, AsyncMock(), gateway).handle("media", uri)
    gateway.delete_media.assert_awaited_once_with(uri)
    assert store.db.execute("SELECT deleted FROM media").fetchone()[0] == 1


async def test_unconfirmed_media_response_is_retried(config, store):
    uri = "mxc://example.org/file"
    store.set("last_sync_at", str(now_ms()))
    store.set("coverage_ok", "1")
    store.record("!room:example.org", "$a", 0, kind="m.room.message", media={uri})
    store.mark_redacted("$a", 0)
    gateway = AsyncMock()
    gateway.delete_media.return_value = {}
    await Worker(config, store, AsyncMock(), gateway).handle("media", uri)
    row = store.db.execute("SELECT * FROM media").fetchone()
    assert row["deleted"] == 0
    assert row["error"] == "MEDIA_DELETE_UNCONFIRMED"
    assert row["next_try"] > now_ms()
