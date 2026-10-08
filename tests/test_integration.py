"""Real Synapse integration: no bot user, no extra member, no decryption by the service."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import socket
import sqlite3
import subprocess
from dataclasses import replace
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import uuid4

import aiohttp
import pytest
import yaml
from nio import AsyncClient, AsyncClientConfig, ErrorResponse

from retentionbot.api import PREFIX, ApiError, JsonApi, ServerApi, segment
from retentionbot.broker import Broker
from retentionbot.service import Observer
from retentionbot.store import now_ms
from retentionbot.worker import Worker

from .db import open_store

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not os.getenv("SYNAPSE_PYTHON"), reason="Set SYNAPSE_PYTHON"),
]
SECRET = "disposable-module-secret-" + "x" * 40


@pytest.fixture
async def homeserver(tmp_path):
    python = str(Path(os.environ["SYNAPSE_PYTHON"]).absolute())
    config = tmp_path / "homeserver.yaml"
    subprocess.run(
        [
            python,
            "-m",
            "synapse.app.homeserver",
            "--server-name",
            "test.local",
            "--config-path",
            str(config),
            "--generate-config",
            "--report-stats=no",
        ],
        check=True,
        cwd=tmp_path,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    (tmp_path / "module-secret").write_text(SECRET)
    data = yaml.safe_load(config.read_text())
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    data.update(
        database={"name": "sqlite3", "args": {"database": str(tmp_path / "synapse.db")}},
        media_store_path=str(tmp_path / "media"),
        listeners=[
            {
                "port": port,
                "tls": False,
                "type": "http",
                "bind_addresses": ["127.0.0.1"],
                "resources": [{"names": ["client"], "compress": False}],
            }
        ],
        registration_shared_secret="integration-secret",
        trusted_key_servers=[],
        retention={
            "enabled": True,
            "default_policy": {"max_lifetime": "7d"},
            "purge_jobs": [{"interval": 500}],
        },
        modules=[
            {
                "module": "synapse_retention.module.RetentionModule",
                "config": {
                    "secret_file": str(tmp_path / "module-secret"),
                    "cutoff_file": str(tmp_path / "retention-cutoff"),
                    "redaction_lead": 1500,
                    "command_bot_user_id": "@retention:test.local",
                    "element_x_cache_reset": {
                        "enabled": True,
                        "sentinel_user_id": "@__retention_cache_reset:test.local",
                        "debounce": 100,
                        "min_interval": 100,
                        "poll_interval": 100,
                    },
                },
            }
        ],
        redaction_retention_period=0,
        rc_message={"per_second": 1000, "burst_count": 10000},
        rc_registration={"per_second": 1000, "burst_count": 10000},
        rc_joins={
            "local": {"per_second": 1000, "burst_count": 10000},
            "remote": {"per_second": 1000, "burst_count": 10000},
        },
    )
    config.write_text(yaml.safe_dump(data))
    output = (tmp_path / "process.log").open("w")
    process = subprocess.Popen(
        [python, "-m", "synapse.app.homeserver", "--config-path", str(config)],
        cwd=tmp_path,
        stdout=output,
        stderr=output,
    )
    base = f"http://127.0.0.1:{port}"
    try:
        async with aiohttp.ClientSession() as session:
            for _ in range(100):
                try:
                    async with session.get(base + "/_matrix/client/versions") as response:
                        if response.status == 200:
                            break
                except aiohttp.ClientError:
                    pass
                if process.poll() is not None:
                    pytest.fail((tmp_path / "process.log").read_text()[-6000:])
                await asyncio.sleep(0.2)
            else:
                pytest.fail("Synapse startup timed out")
        yield base, tmp_path
    finally:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
        output.close()


async def register(base, user):
    async with aiohttp.ClientSession() as session:
        async with session.get(base + "/_synapse/admin/v1/register") as response:
            nonce = (await response.json())["nonce"]
        password = "disposable-integration-password"
        message = "\0".join([nonce, user, password, "notadmin"])
        mac = hmac.new(b"integration-secret", message.encode(), hashlib.sha1).hexdigest()
        async with session.post(
            base + "/_synapse/admin/v1/register",
            json={
                "nonce": nonce,
                "username": user,
                "password": password,
                "admin": False,
                "mac": mac,
            },
        ) as response:
            assert response.status == 200, await response.text()
            return await response.json()


async def dm(alice, bob, encrypted=False, room_version="10"):
    initial = (
        [
            {
                "type": "m.room.encryption",
                "state_key": "",
                "content": {"algorithm": "m.megolm.v1.aes-sha2"},
            }
        ]
        if encrypted
        else []
    )
    room = (
        await alice.request(
            "POST",
            "/_matrix/client/v3/createRoom",
            json={
                "preset": "private_chat",
                "is_direct": True,
                "room_version": room_version,
                "invite": [bob.user_id],
                "initial_state": initial,
            },
        )
    )["room_id"]
    await bob.request("POST", "/_matrix/client/v3/join/" + segment(room), json={})
    return room


async def people(base, session):
    a, b = await register(base, "alice"), await register(base, "bob")
    alice, bob = (
        JsonApi(base, a["access_token"], session),
        JsonApi(base, b["access_token"], session),
    )
    alice.user_id, bob.user_id = a["user_id"], b["user_id"]
    return alice, bob, a, b


async def setting(api, room, maximum=2000, minimum=100):
    return await api.request(
        "POST",
        PREFIX + "/command",
        json={
            "command": "retention",
            "action": "set",
            "room_id": room,
            "min_lifetime": minimum,
            "max_lifetime": maximum,
        },
    )


async def send(api, room, text, txn=None):
    return (
        await api.request(
            "PUT",
            f"/_matrix/client/v3/rooms/{segment(room)}/send/m.room.message/{txn or uuid4().hex}",
            json={"msgtype": "m.text", "body": text},
        )
    )["event_id"]


async def wait_due(store, key, delay=510):
    delta = store.event(key)["ts"] + delay - now_ms()
    if delta > 0:
        await asyncio.sleep(delta / 1000)


@pytest.mark.parametrize("room_version", ["10", "12"])
async def test_dm_only_two_members_admin_json_and_redaction_sync(homeserver, config, room_version):
    base, path = homeserver
    store = open_store(path / "metadata.db")
    async with aiohttp.ClientSession() as session:
        alice, bob, _, _ = await people(base, session)
        room = await dm(alice, bob, room_version=room_version)
        old = await send(alice, room, "before monitoring")
        api = ServerApi(base, SECRET, session)
        observer = Observer(replace(config, synapse_url=base), store, api, AsyncMock())
        await observer.poll_once()
        assert store.event(old) is None
        await setting(alice, room)
        with pytest.raises(ApiError) as error:
            await setting(bob, room)
        assert error.value.code == "NOT_ROOM_ADMIN"
        first = await send(alice, room, "alice message")
        second = await send(bob, room, "bob message")
        await observer.poll_once()
        before = await alice.request("GET", "/_matrix/client/v3/sync", params={"timeout": "0"})
        devices = (await bob.request("GET", "/_matrix/client/v3/devices"))["devices"]
        await wait_due(store, second)
        worker = Worker(store, api)
        await asyncio.gather(worker.handle(first), worker.handle(first))
        await worker.handle(second)
        await worker.handle(first)
        assert store.counts() == {"done": 2}
        sync = await alice.request(
            "GET",
            "/_matrix/client/v3/sync",
            params={"since": before["next_batch"], "timeout": "100"},
        )
        timeline = sync["rooms"]["join"][room]["timeline"]["events"]
        removed = {
            e.get("redacts", e["content"].get("redacts"))
            for e in timeline
            if e["type"] == "m.room.redaction"
        }
        assert {first, second} <= removed
        members = await alice.request(
            "GET", f"/_matrix/client/v3/rooms/{segment(room)}/joined_members"
        )
        assert set(members["joined"]) == {alice.user_id, bob.user_id}
        assert len((await bob.request("GET", "/_matrix/client/v3/devices"))["devices"]) == len(
            devices
        )
        repeat = await api.redact(first)
        assert repeat["redaction_id"] == store.event(first)["redaction_id"]
        assert "alice message" not in json.dumps(
            list(store.db.execute("SELECT * FROM events")), ensure_ascii=False
        )
    store.close()


async def test_real_e2ee_dm_without_server_keys(homeserver, config):
    base, path = homeserver
    store = open_store(path / "encrypted.db")
    clients = []
    async with aiohttp.ClientSession() as session:
        alice, bob, a, b = await people(base, session)
        room = await dm(alice, bob, encrypted=True)
        for user in (a, b):
            directory = path / user["user_id"].split(":")[0][1:]
            directory.mkdir()
            client = AsyncClient(
                base,
                user["user_id"],
                store_path=str(directory),
                config=AsyncClientConfig(encryption_enabled=True, pickle_key="disposable"),
            )
            client.restore_login(user["user_id"], user["device_id"], user["access_token"])
            clients.append(client)
        sender, recipient = clients
        try:
            for client in clients:
                await client.sync(timeout=0)
                if client.should_upload_keys:
                    await client.keys_upload()
            for client in clients:
                await client.keys_query()
            for device in sender.device_store.active_user_devices(bob.user_id):
                sender.verify_device(device)
            api = ServerApi(base, SECRET, session)
            observer = Observer(replace(config, synapse_url=base), store, api, AsyncMock())
            await observer.poll_once()
            sent = await sender.room_send(
                room,
                "m.room.message",
                {"msgtype": "m.text", "body": "confidential plaintext known only to clients"},
            )
            assert not isinstance(sent, ErrorResponse)
            decrypted = await recipient.sync(timeout=0)
            assert any(
                getattr(e, "body", "") == "confidential plaintext known only to clients"
                for e in decrypted.rooms.join[room].timeline.events
            )
            await setting(alice, room)
            await observer.poll_once()
            row = store.event(sent.event_id)
            assert row["kind"] == "m.room.encrypted"
            snapshot = json.dumps(list(store.db.execute("SELECT * FROM events")))
            assert "confidential" not in snapshot and "ciphertext" not in snapshot
            await wait_due(store, sent.event_id)
            await Worker(store, api).handle(sent.event_id)
            assert store.event(sent.event_id)["status"] == "done"
            synced = await recipient.sync(timeout=0)
            assert any(
                e.source.get("type") == "m.room.redaction"
                and e.source.get("redacts", e.source["content"].get("redacts")) == sent.event_id
                for e in synced.rooms.join[room].timeline.events
            )
            members = await alice.request(
                "GET", f"/_matrix/client/v3/rooms/{segment(room)}/joined_members"
            )
            assert set(members["joined"]) == {alice.user_id, bob.user_id}
        finally:
            for client in clients:
                await client.close()
    store.close()


async def test_departed_local_author_and_state_protection(homeserver):
    base, _ = homeserver
    async with aiohttp.ClientSession() as session:
        alice, bob, _, _ = await people(base, session)
        room = await dm(alice, bob)
        await setting(alice, room)
        target = await send(bob, room, "departed author's message")
        state = (
            await alice.request(
                "PUT",
                f"/_matrix/client/v3/rooms/{segment(room)}/state/m.room.topic",
                json={"topic": "must stay"},
            )
        )["event_id"]
        await bob.request("POST", f"/_matrix/client/v3/rooms/{segment(room)}/leave", json={})
        await asyncio.sleep(0.6)
        api = ServerApi(base, SECRET, session)
        assert (await api.redact(state))["code"] == "EVENT_TYPE_EXCLUDED"
        result = await api.redact(target)
        assert result["status"] == "done", result
        assert result["sender"] == bob.user_id
        members = await alice.request(
            "GET", f"/_matrix/client/v3/rooms/{segment(room)}/joined_members"
        )
        assert set(members["joined"]) == {alice.user_id}
        assert (await api.redact("$unknown"))["status"] == "missed"


async def test_bulk_local_redactions_do_not_fork_room_dag(homeserver):
    # Regression: creating a separate historical branch for every redaction
    # made event_forward_extremities grow linearly with redaction count and
    # stream_ordering_to_exterm grow quadratically.
    base, path = homeserver
    async with aiohttp.ClientSession() as session:
        alice, bob, _, _ = await people(base, session)
        room = await dm(alice, bob)
        await setting(alice, room)
        targets = [await send(bob, room, f"bulk-target-{i}") for i in range(24)]
        await asyncio.sleep(0.6)
        api = ServerApi(base, SECRET, session)
        for event_id in targets:
            result = await api.redact(event_id)
            assert result["status"] == "done", result

        with sqlite3.connect(path / "synapse.db") as db:
            extremities = db.execute(
                "SELECT count(*) FROM event_forward_extremities WHERE room_id=?",
                (room,),
            ).fetchone()[0]
        assert extremities <= 5, (
            f"Ordinary redactions forked the room DAG: {extremities} extremities"
        )


async def test_server_rechecks_extended_policy_and_secret_scope(homeserver):
    base, _ = homeserver
    async with aiohttp.ClientSession() as session:
        alice, bob, _, _ = await people(base, session)
        room = await dm(alice, bob)
        await setting(alice, room)
        target = await send(alice, room, "new deadline")
        await asyncio.sleep(0.6)
        await setting(alice, room, maximum="1h")
        api = ServerApi(base, SECRET, session)
        assert (await api.redact(target))["status"] == "deferred"
        with pytest.raises(ApiError) as error:
            await ServerApi(base, alice.token, session).feed(None)
        assert error.value.status == 401
        with pytest.raises(ApiError):
            await api.request(
                "POST",
                PREFIX + "/command",
                json={"command": "retention", "action": "status", "room_id": room},
            )
        with pytest.raises(ApiError) as error:
            await setting(alice, room, maximum=1000, minimum=1000)
        assert error.value.code == "BAD_POLICY"
        with pytest.raises(ApiError):
            await alice.request(
                "POST",
                PREFIX + "/command",
                json={"command": "retention", "action": [], "room_id": room},
            )


async def test_cursor_recovers_many_events_without_bodies(homeserver, config):
    base, path = homeserver
    store = open_store(path / "catchup.db")
    async with aiohttp.ClientSession() as session:
        alice, bob, _, _ = await people(base, session)
        room = await dm(alice, bob)
        api = ServerApi(base, SECRET, session)
        observer = Observer(replace(config, batch_size=10), store, api, AsyncMock())
        await observer.poll_once()
        sent = {await send(alice, room, "private body") for _ in range(125)}
        for _ in range(40):
            if await observer.poll_once():
                break
        assert set(row["event_id"] for row in store.db.execute("SELECT * FROM events")) == sent
        assert "private body" not in json.dumps(await api.feed(0, 5000))
    store.close()


@pytest.mark.parametrize("timing", ["before", "after"])
async def test_native_purge_stays_enabled_and_late_job_is_not_success(homeserver, config, timing):
    base, path = homeserver
    store = open_store(path / "purge.db")
    async with aiohttp.ClientSession() as session:
        alice, bob, _, _ = await people(base, session)
        room = await dm(alice, bob)
        api = ServerApi(base, SECRET, session)
        observer = Observer(config, store, api, AsyncMock())
        await observer.poll_once()
        await setting(alice, room)
        target = await send(alice, room, "expires")
        await send(alice, room, "anchor one")
        await send(alice, room, "anchor two")
        await observer.poll_once()
        worker = Worker(store, api)
        if timing == "before":
            await wait_due(store, target)
            await worker.handle(target)
            assert store.event(target)["status"] == "done"
        with sqlite3.connect(path / "synapse.db") as server_db:
            for _ in range(100):
                if not server_db.execute(
                    "SELECT 1 FROM event_json WHERE event_id=?", (target,)
                ).fetchone():
                    break
                await asyncio.sleep(0.1)
            else:
                pytest.fail("Native retention did not purge target")
        if timing == "after":
            await worker.handle(target)
            assert store.event(target)["status"] == "missed"
            assert store.event(target)["error"] == "EVENT_PURGED"
    store.close()


async def test_post_purge_invalidation_pulses_ignore_list_once_for_local_members(
    homeserver,
):
    base, _ = homeserver
    sentinel_prefix = "@__retention_cache_reset-"
    async with aiohttp.ClientSession() as session:
        alice, bob, _, _ = await people(base, session)
        room = await dm(alice, bob)
        await alice.request(
            "PUT",
            f"/_matrix/client/v3/user/{segment(alice.user_id)}/account_data/m.ignored_user_list",
            json={"ignored_users": {"@already-blocked:test.local": {}}},
        )

        api = ServerApi(base, SECRET, session)
        event_id = "$cache-reset-" + uuid4().hex
        result = await api.invalidate(event_id, room)
        assert result["status"] == "done"
        assert result["cache_reset_queued"] is True

        second_event_id = "$cache-reset-" + uuid4().hex
        second = await api.invalidate(second_event_id, room)
        assert second["status"] == "done"
        assert second["cache_reset_queued"] is True

        async def ignored(client):
            try:
                data = await client.request(
                    "GET",
                    f"/_matrix/client/v3/user/{segment(client.user_id)}"
                    "/account_data/m.ignored_user_list",
                )
            except ApiError as error:
                if error.status == 404:
                    return {}
                raise
            return data.get("ignored_users", {})

        def reset_entries(ignored_users):
            return {
                user_id
                for user_id in ignored_users
                if user_id.startswith(sentinel_prefix)
            }

        for _ in range(100):
            alice_ignored = await ignored(alice)
            bob_ignored = await ignored(bob)
            alice_reset = reset_entries(alice_ignored)
            bob_reset = reset_entries(bob_ignored)
            if alice_reset and bob_reset:
                break
            await asyncio.sleep(0.05)
        else:
            pytest.fail("Element X cache-reset account-data pulse was not emitted")

        assert "@already-blocked:test.local" in alice_ignored
        assert len(alice_reset) == 1
        assert len(bob_reset) == 1
        assert set(alice_ignored) == {"@already-blocked:test.local"} | alice_reset
        assert set(bob_ignored) == bob_reset

        # Two different purged events in one room are aggregated into one account-data
        # pulse. A second pulse would advance the reserved generation again.
        first_alice_reset = alice_reset
        first_bob_reset = bob_reset
        await asyncio.sleep(0.5)
        assert reset_entries(await ignored(alice)) == first_alice_reset
        assert reset_entries(await ignored(bob)) == first_bob_reset

        # Retrying the same post-purge invalidation is idempotent and must not queue
        # another user reset generation.
        repeated = await api.invalidate(event_id, room)
        assert repeated["status"] == "done"
        assert repeated["cache_reset_queued"] is False


@pytest.mark.skipif(not os.getenv("RABBITMQ_TEST_URL"), reason="Set RABBITMQ_TEST_URL")
async def test_real_rabbitmq_persistent_redelivery():
    broker = await Broker().connect(os.environ["RABBITMQ_TEST_URL"])
    key = "$integration-" + uuid4().hex
    try:
        await broker.publish("redact", key)
        first = await broker.queue.get(timeout=5)
        assert first.delivery_mode == 2
        await first.nack(requeue=True)
        second = await broker.queue.get(timeout=5)
        assert second.redelivered and key.encode() in second.body
        await second.ack()
    finally:
        await broker.close()


async def test_existing_local_moderator_fallback(homeserver):
    base, _ = homeserver
    async with aiohttp.ClientSession() as session:
        alice, bob, _, _ = await people(base, session)
        room = await dm(alice, bob)
        prefix = f"/_matrix/client/v3/rooms/{segment(room)}/state/m.room.power_levels"
        levels = await alice.request("GET", prefix)
        levels.setdefault("events", {})["m.room.redaction"] = 100
        await alice.request("PUT", prefix, json=levels)
        await setting(alice, room)
        target = await send(bob, room, "requires moderator")
        await asyncio.sleep(0.6)
        result = await ServerApi(base, SECRET, session).redact(target)
        assert result["status"] == "done", result
        assert result["sender"] == alice.user_id
        members = await alice.request(
            "GET", f"/_matrix/client/v3/rooms/{segment(room)}/joined_members"
        )
        assert set(members["joined"]) == {alice.user_id, bob.user_id}
