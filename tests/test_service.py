from types import SimpleNamespace
from unittest.mock import AsyncMock

from retentionbot.service import Service


async def test_admin_authority_is_read_from_server_and_membership(config, store):
    matrix = AsyncMock()
    matrix.power_levels.return_value = {
        "users": {"@admin:example.org": 100, "@user:example.org": 0}
    }
    matrix.member.return_value = {"membership": "join"}
    service = Service(config, store, AsyncMock(), matrix, AsyncMock(), AsyncMock())
    store.queue_command("$denied", "!room:example.org", "@user:example.org", "off", None)
    await service.commands()
    assert store.room("!room:example.org")["lifetime"] == 1000
    store.queue_command("$allowed", "!room:example.org", "@admin:example.org", "set", "7d")
    await service.commands()
    assert store.room("!room:example.org")["lifetime"] == 604_800_000
    matrix.member.return_value = {"membership": "leave"}
    store.queue_command("$left", "!room:example.org", "@admin:example.org", "off", None)
    await service.commands()
    assert store.room("!room:example.org")["lifetime"] == 604_800_000


async def test_replayed_command_does_not_overwrite_newer_policy(config, store):
    matrix = AsyncMock()
    matrix.power_levels.return_value = {"users": {"@admin:example.org": 100}}
    matrix.member.return_value = {"membership": "join"}
    service = Service(config, store, AsyncMock(), matrix, AsyncMock(), AsyncMock())
    store.queue_command("$a", "!room:example.org", "@admin:example.org", "set", "7d")
    await service.commands()
    store.queue_command("$b", "!room:example.org", "@admin:example.org", "off", None)
    await service.commands()
    store.queue_command("$a", "!room:example.org", "@admin:example.org", "set", "7d")
    await service.commands()
    assert store.room("!room:example.org")["lifetime"] is None


async def test_late_decrypted_command_does_not_overwrite_newer_policy(config, store):
    matrix = AsyncMock()
    matrix.power_levels.return_value = {"users": {"@admin:example.org": 100}}
    matrix.member.return_value = {"membership": "join"}
    service = Service(config, store, AsyncMock(), matrix, AsyncMock(), AsyncMock())
    store.record("!room:example.org", "$newer", 200, kind="m.room.message")
    store.queue_command("$newer", "!room:example.org", "@admin:example.org", "off", None)
    await service.commands()
    store.record("!room:example.org", "$late-key", 100, kind="m.room.message")
    store.queue_command("$late-key", "!room:example.org", "@admin:example.org", "set", "7d")
    await service.commands()
    assert store.room("!room:example.org")["lifetime"] is None


def test_observer_stores_metadata_not_plaintext_or_attachment_keys(config, store):
    service = Service(config, store, AsyncMock(), AsyncMock(), AsyncMock(), AsyncMock())
    source = {
        "type": "m.room.message",
        "event_id": "$a",
        "sender": "@user:example.org",
        "origin_server_ts": 100,
        "content": {
            "msgtype": "m.file",
            "body": "secret.txt",
            "file": {"url": "mxc://example.org/file", "key": {"k": "secret-key"}},
        },
    }
    service.observe("!room:example.org", SimpleNamespace(source=source))
    dump = "\n".join(store.db.iterdump())
    assert "secret.txt" not in dump
    assert "secret-key" not in dump
    assert "mxc://example.org/file" in dump


async def test_publish_failure_returns_job_to_scheduler(config, store):
    store.record("!room:example.org", "$a", 0, kind="m.room.message")
    broker = AsyncMock()
    broker.publish.side_effect = RuntimeError("broker down")
    service = Service(config, store, AsyncMock(), AsyncMock(), AsyncMock(), broker)
    import pytest

    with pytest.raises(RuntimeError):
        await service.schedule()
    assert store.db.execute("SELECT queued_at FROM events").fetchone()[0] == 0
