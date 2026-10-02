from unittest.mock import AsyncMock

import pytest

from retentionbot.api import ApiError
from retentionbot.worker import Worker

from .test_queue import event, ingest


@pytest.mark.parametrize(
    "result",
    [
        {"status": "deferred", "code": "NOT_DUE", "retry_at_ms": 9999999999999},
        {"status": "done"},
        {},
    ],
)
async def test_unconfirmed_or_postponed_deletion_stays_pending(store, result):
    ingest(store, event())
    api = AsyncMock()
    api.redact.return_value = result
    await Worker(store, api).handle("$new")
    assert store.event("$new")["status"] == "pending"
    assert store.event("$new")["error"]


async def test_completed_job_is_idempotent_and_missed_is_explicit(store):
    ingest(store, event("$a"), event("$b"))
    api = AsyncMock()
    api.redact.side_effect = [
        {"status": "done", "redaction_id": "$redaction"},
        {"status": "missed", "code": "EVENT_PURGED"},
    ]
    worker = Worker(store, api)
    await worker.handle("$a")
    await worker.handle("$a")
    await worker.handle("$b")
    assert api.redact.await_count == 2
    assert store.counts() == {"done": 1, "missed": 1}


async def test_transient_api_error_keeps_retryable_job(store):
    ingest(store, event())
    api = AsyncMock()
    api.redact.side_effect = ApiError(429, "M_LIMIT_EXCEEDED", 100000)
    await Worker(store, api).handle("$new")
    assert store.event("$new")["status"] == "pending"
    assert store.event("$new")["attempts"] == 1
