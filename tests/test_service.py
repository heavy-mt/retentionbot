from unittest.mock import AsyncMock

import pytest

from retentionbot.service import Observer

from .test_queue import POLICY, ROOM, event


async def test_feed_failure_does_not_advance_checkpoint(config, store):
    api = AsyncMock()
    api.feed.return_value = {"events": [event()], "cursor": 25, "caught_up": True}
    api.policy.side_effect = RuntimeError("unavailable")
    observer = Observer(config, store, api, AsyncMock())
    with pytest.raises(RuntimeError):
        await observer.poll_once()
    assert store.get("cursor") == "0"
    api.policy.side_effect = None
    api.policy.return_value = POLICY
    assert await observer.poll_once()
    assert store.get("cursor") == "25"


async def test_publish_failure_releases_lease(config, store):
    store.ingest({"events": [event(ts=100)], "cursor": 1}, {ROOM: POLICY})
    broker = AsyncMock()
    broker.publish.side_effect = RuntimeError("unavailable")
    observer = Observer(config, store, AsyncMock(), broker)
    with pytest.raises(RuntimeError):
        await observer.schedule()
    assert store.event("$new")["queued_at"] == 0
