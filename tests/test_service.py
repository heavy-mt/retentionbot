from types import SimpleNamespace
from unittest.mock import AsyncMock

from retentionbot.service import Service

from .db import snapshot


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
    store.queue_command(
        "$allowed", "!room:example.org", "@admin:example.org", "set", '{"max_lifetime":"7d"}'
    )
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
    store.queue_command(
        "$a", "!room:example.org", "@admin:example.org", "set", '{"max_lifetime":"7d"}'
    )
    await service.commands()
    store.queue_command("$b", "!room:example.org", "@admin:example.org", "off", None)
    await service.commands()
    store.queue_command(
        "$a", "!room:example.org", "@admin:example.org", "set", '{"max_lifetime":"7d"}'
    )
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
    store.queue_command(
        "$late-key", "!room:example.org", "@admin:example.org", "set", '{"max_lifetime":"7d"}'
    )
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
    dump = snapshot(store)
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


async def test_bot_policy_state_does_not_change_command_order(config, store):
    matrix = AsyncMock()
    matrix.power_levels.return_value = {"users": {"@admin:example.org": 100}}
    matrix.member.return_value = {"membership": "join"}
    service = Service(config, store, AsyncMock(), matrix, AsyncMock(), AsyncMock())
    store.policy("!room:example.org", None, 1000, ts=100)
    service.observe(
        "!room:example.org",
        SimpleNamespace(
            source={
                "type": "m.room.retention",
                "state_key": "",
                "sender": config.user_id,
                "event_id": "$bot-policy",
                "origin_server_ts": 10000,
                "content": {"max_lifetime": 1000},
            }
        ),
    )
    store.record("!room:example.org", "$new-command", 200, kind="m.room.message")
    store.queue_command(
        "$new-command", "!room:example.org", "@admin:example.org", "set", '{"max_lifetime":"1d"}'
    )
    await service.commands()
    assert store.room("!room:example.org")["lifetime"] == 86400000


async def test_purged_gap_is_visible_and_does_not_freeze_new_sync(config, store):
    from nio import RoomMessagesResponse, SyncResponse

    from retentionbot.store import now_ms

    store.record("!room:example.org", "$old-anchor", 0, kind="m.room.message")
    store.last_event("!room:example.org", "$old-anchor")
    client = AsyncMock()
    client.should_upload_keys = client.should_query_keys = client.should_claim_keys = False
    client.room_messages.return_value = RoomMessagesResponse("!room:example.org", [], "gap", None)
    client.sync.return_value = SyncResponse.from_dict(
        {
            "next_batch": "next",
            "rooms": {
                "join": {
                    "!room:example.org": {
                        "state": {"events": []},
                        "timeline": {
                            "limited": True,
                            "prev_batch": "gap",
                            "events": [
                                {
                                    "type": "m.room.message",
                                    "event_id": "$fresh",
                                    "sender": "@user:example.org",
                                    "origin_server_ts": now_ms(),
                                    "content": {"msgtype": "m.text", "body": "fresh"},
                                }
                            ],
                        },
                    }
                }
            },
        }
    )
    service = Service(config, store, client, AsyncMock(), AsyncMock(), AsyncMock())
    await service.sync_once()
    assert store.get("sync_token") == "next"
    assert store.room("!room:example.org")["error"] == "GAP_ANCHOR_UNAVAILABLE"
    assert store.db.execute("SELECT 1 FROM events WHERE event_id=?", ("$fresh",)).fetchone()
    assert store.event_due("$old-anchor", now_ms())
    assert not store.cleanup_ready(now_ms())


def test_decrypting_an_already_deleted_command_does_not_execute_it(config, store):
    service = Service(config, store, AsyncMock(), AsyncMock(), AsyncMock(), AsyncMock())
    store.record(
        "!room:example.org",
        "$deleted-command",
        200,
        kind="m.room.encrypted",
        decoded=False,
        ciphertext={"ciphertext": "encrypted"},
    )
    store.mark_redacted("$deleted-command", 0)
    service.observe(
        "!room:example.org",
        SimpleNamespace(
            source={
                "type": "m.room.message",
                "event_id": "$deleted-command",
                "sender": "@admin:example.org",
                "origin_server_ts": 200,
                "content": {
                    "msgtype": "m.text",
                    "body": '{"command":"retention","action":"set","max_lifetime":"7d"}',
                },
            }
        ),
    )
    assert not store.db.execute("SELECT 1 FROM commands").fetchone()
