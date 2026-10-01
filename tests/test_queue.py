from retentionbot.media import attachment_uris, split_mxc
from retentionbot.service import parse_command

from .db import open_store


def test_no_retroactive_processing_and_restart(tmp_path):
    path = tmp_path / "state.db"
    db = open_store(path)
    db.enroll("!r:example.org", 1000, since_ts=2000)
    assert not db.record("!r:example.org", "$old", 1999, kind="m.room.message")
    assert db.record("!r:example.org", "$new", 2000, kind="m.room.message")
    db.set("sync_token", "checkpoint")
    db.close()
    db = open_store(path)
    db.enroll("!r:example.org", 5000, since_ts=9000)
    assert db.room("!r:example.org")["since_ts"] == 2000
    assert db.room("!r:example.org")["lifetime"] == 1000
    assert db.get("sync_token") == "checkpoint"
    assert [e["event_id"] for e in db.due_events(400_000)] == ["$new"]
    db.close()


def test_failed_first_enrollment_starts_only_after_success(store):
    room = "!pending:example.org"
    store.enroll(room, 1000, since_ts=100, active=False)
    assert not store.record(room, "$during-failure", 200, kind="m.room.message")
    store.enroll(room, 1000, since_ts=300)
    assert not store.record(room, "$before-join", 299, kind="m.room.message")
    assert store.record(room, "$after-join", 300, kind="m.room.message")
    store.enroll(room, 5000, since_ts=1000)
    assert store.room(room)["since_ts"] == 300
    assert store.room(room)["lifetime"] == 1000


def test_edit_expires_with_original(store):
    store.record("!room:example.org", "$original", 100, kind="m.room.message")
    store.record("!room:example.org", "$edit", 900, kind="m.replace", parent="$original")
    assert {e["event_id"] for e in store.due_events(400_000)} == {"$original", "$edit"}
    assert not store.event_due("$edit", 1099)
    assert store.event_due("$edit", 1100)


def test_shared_attachment_survives_until_last_reference(store):
    uri = "mxc://example.org/shared"
    for event in ["$a", "$b"]:
        store.record("!room:example.org", event, 0, kind="m.room.message", media={uri})
    store.mark_redacted("$a", 0)
    assert not store.media_candidates(400_000)
    store.mark_redacted("$b", 0)
    assert [m["uri"] for m in store.media_candidates(400_000)] == [uri]
    store.protect(uri)
    assert not store.media_candidates(400_000)


def test_shared_attachment_in_room_with_retention_off(store):
    uri = "mxc://example.org/shared"
    store.enroll("!other:example.org", None, since_ts=0)
    store.record("!room:example.org", "$a", 0, kind="m.room.message", media={uri})
    store.record("!other:example.org", "$b", 0, kind="m.room.message", media={uri})
    store.mark_redacted("$a", 0)
    assert not store.media_eligible(uri, 400_000)


def test_unreadable_ciphertext_blocks_physical_cleanup(store):
    store.set("coverage_ok", "1")
    store.set("last_sync_at", "400000")
    assert store.cleanup_ready(400_001)
    store.record(
        "!room:example.org",
        "$unknown",
        0,
        kind="m.room.encrypted",
        decoded=False,
        ciphertext={"ciphertext": "encrypted"},
    )
    assert not store.cleanup_ready(400_001)
    store.record("!room:example.org", "$unknown", 0, kind="m.room.message", decoded=True)
    assert store.cleanup_ready(400_001)
    assert store.db.execute("SELECT ciphertext FROM events").fetchone()[0] is None
    assert not store.cleanup_ready(500_001)


def test_broker_lease_recovers_without_losing_jobs(store):
    store.record("!room:example.org", "$a", 0, kind="m.room.message")
    store.queued("redact", "$a", 400_000)
    assert not store.due_events(400_001)
    assert store.event_due("$a", 400_001)
    assert store.due_events(700_001)


def test_encrypted_attachments_edits_and_thumbnails():
    content = {
        "file": {"url": "mxc://example.org/file", "key": {"k": "secret"}},
        "info": {"thumbnail_file": {"url": "mxc://example.org/thumb"}},
        "m.new_content": {"url": "mxc://example.org/replacement"},
    }
    assert attachment_uris(content) == {
        "mxc://example.org/file",
        "mxc://example.org/thumb",
        "mxc://example.org/replacement",
    }
    assert not attachment_uris({"body": "mxc://example.org/quoted"})


def test_mxc_path_traversal_and_remote_urls_rejected():
    import pytest

    for uri in [
        "mxc://example.org/../x",
        "mxc://example.org/%2fetc",
        "file:///etc/passwd",
        "https://example.org/x",
        "mxc://user@example.org/x",
        "mxc://example.org/x?q=1",
    ]:
        with pytest.raises(ValueError):
            split_mxc(uri)


def test_command_parsing_is_exact():
    import json

    command = {"command": "retention", "action": "set", "min_lifetime": "1h", "max_lifetime": "7d"}
    action, argument = parse_command(json.dumps(command))
    assert action == "set"
    assert json.loads(argument) == {"min_lifetime": "1h", "max_lifetime": "7d"}
    assert parse_command('{"command":"retention","action":"set"}') == ("invalid", None)
    assert parse_command("hello !retention off") is None
    assert parse_command('{"command":"other"}') is None
    assert parse_command('{"command":"retention","action":"off","extra":true}') == ("invalid", None)
