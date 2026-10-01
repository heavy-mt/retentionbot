"""Opt-in tests against real Synapse and RabbitMQ. All credentials are disposable."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import os
import subprocess
from dataclasses import replace
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import uuid4

import aiohttp
import pytest
import yaml
from nio import AsyncClient, AsyncClientConfig, ErrorResponse

from retentionbot.api import GatewayApi, JsonApi, MatrixApi, segment
from retentionbot.broker import Broker
from retentionbot.gateway import AdminGateway
from retentionbot.service import Service
from retentionbot.store import Store, now_ms
from retentionbot.worker import Worker

from .test_gateway import start_app

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.getenv("SYNAPSE_PYTHON"), reason="Set SYNAPSE_PYTHON for real server tests"
    ),
]


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
    data = yaml.safe_load(config.read_text())
    # Free port allocated in the same network namespace as the subprocess and clients.
    import socket

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
        retention={"enabled": False},
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
                    pytest.fail((tmp_path / "process.log").read_text()[-4000:])
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


async def register(base: str, user: str, admin: bool = False):
    async with aiohttp.ClientSession() as session:
        async with session.get(base + "/_synapse/admin/v1/register") as response:
            nonce = (await response.json())["nonce"]
        password = "disposable-integration-password"
        message = "\0".join([nonce, user, password, "admin" if admin else "notadmin"])
        mac = hmac.new(b"integration-secret", message.encode(), hashlib.sha1).hexdigest()
        async with session.post(
            base + "/_synapse/admin/v1/register",
            json={
                "nonce": nonce,
                "username": user,
                "password": password,
                "admin": admin,
                "mac": mac,
            },
        ) as response:
            assert response.status == 200, await response.text()
            return await response.json()


async def test_private_room_commands_redaction_and_physical_media(homeserver, config):
    base, path = homeserver
    human = await register(base, "human", admin=True)
    bot = await register(base, "retention")
    user = await register(base, "member")
    config = replace(
        config,
        homeserver=base,
        server_name="test.local",
        user_id=bot["user_id"],
        media_grace_seconds=1,
    )
    store = Store(path / "retention.db")
    gateway_store = Store(path / "gateway.db")
    client = AsyncClient(
        base,
        bot["user_id"],
        store_path=str(path),
        config=AsyncClientConfig(encryption_enabled=True, pickle_key="test-key", request_timeout=5),
    )
    client.restore_login(bot["user_id"], bot["device_id"], bot["access_token"])
    async with aiohttp.ClientSession() as session:
        admin = JsonApi(base, human["access_token"], session)
        runner, gateway_url = await start_app(AdminGateway(config, admin, gateway_store).app())
        gateway = GatewayApi(gateway_url, config.gateway_secret, session)
        human_api = MatrixApi(base, human["access_token"], session)
        bot_api = MatrixApi(base, bot["access_token"], session)
        service = Service(config, store, client, bot_api, gateway, AsyncMock())
        try:
            room = (
                await human_api.request(
                    "POST", "/_matrix/client/v3/createRoom", json={"preset": "private_chat"}
                )
            )["room_id"]
            prefix = f"/_matrix/client/v3/rooms/{segment(room)}"
            await human_api.request(
                "PUT",
                prefix + "/send/m.room.message/old",
                json={"msgtype": "m.text", "body": "old history"},
            )
            await service.discover()
            await service.sync_once()
            assert store.room(room)
            assert not store.counts()["total"]  # Old history is outside scope.
            assert (await bot_api.power_levels(room))["users"][bot["user_id"]] == 50
            await human_api.request("POST", prefix + "/invite", json={"user_id": user["user_id"]})
            member_api = MatrixApi(base, user["access_token"], session)
            await member_api.request("POST", f"/_matrix/client/v3/join/{segment(room)}", json={})
            await member_api.request(
                "PUT",
                prefix + "/send/m.room.message/denied",
                json={"msgtype": "m.text", "body": "!retention off"},
            )
            await service.sync_once()
            assert store.room(room)["lifetime"] == config.default_lifetime
            await human_api.request(
                "PUT",
                prefix + "/send/m.room.message/setting",
                json={"msgtype": "m.text", "body": "!retention set 1h"},
            )
            await service.sync_once()
            assert store.room(room)["lifetime"] == 3_600_000
            missed = []
            for i in range(125):
                sent = await human_api.request(
                    "PUT", prefix + f"/send/m.room.message/gap-{i}",
                    json={"msgtype": "m.text", "body": f"gap test {i}"},
                )
                missed.append(sent["event_id"])
            await service.sync_once()
            for event_id in missed:
                assert store.db.execute(
                    "SELECT 1 FROM events WHERE event_id=?", (event_id,)
                ).fetchone()
            async with session.post(
                base + "/_matrix/media/v3/upload",
                headers={
                    "Authorization": "Bearer " + human["access_token"],
                    "Content-Type": "application/octet-stream",
                },
                data=b"disposable test attachment",
            ) as response:
                assert response.status == 200
                uri = (await response.json())["content_uri"]
            sent = await human_api.request(
                "PUT",
                prefix + "/send/m.room.message/file",
                json={"msgtype": "m.file", "body": "test.bin", "url": uri},
            )
            await service.sync_once()
            # Test clock: change the per-room lifetime without waiting an hour.
            with store.db:
                store.db.execute("UPDATE rooms SET lifetime=1 WHERE room_id=?", (room,))
            await asyncio.sleep(0.01)
            worker = Worker(config, store, bot_api, gateway)
            await worker.handle("redact", sent["event_id"])
            event = await human_api.request("GET", prefix + f"/event/{segment(sent['event_id'])}")
            assert not event["content"]
            store.set("last_sync_at", str(now_ms()))
            with store.db:
                store.db.execute("UPDATE media SET next_try=0")
            await worker.handle("media", uri)
            assert store.db.execute("SELECT deleted FROM media WHERE uri=?", (uri,)).fetchone()[0]
            media_id = uri.rsplit("/", 1)[1]
            assert not list((path / "media" / "local_content").rglob(media_id[4:]))
            async with session.get(
                base + f"/_matrix/client/v1/media/download/test.local/{media_id}",
                headers={"Authorization": "Bearer " + human["access_token"]},
            ) as r:
                assert r.status == 404
        finally:
            await client.close()
            await runner.cleanup()
            store.close()
            gateway_store.close()


async def test_e2ee_commands_and_attachment_metadata(homeserver, config):
    base, path = homeserver
    human = await register(base, "human", admin=True)
    bot = await register(base, "retention")
    config = replace(config, homeserver=base, server_name="test.local", user_id=bot["user_id"])
    store = Store(path / "retention.db")
    gateway_store = Store(path / "gateway.db")
    clients = []
    for identity in [human, bot]:
        crypto = path / identity["user_id"].split(":")[0].removeprefix("@")
        crypto.mkdir()
        client = AsyncClient(
            base,
            identity["user_id"],
            store_path=str(crypto),
            config=AsyncClientConfig(
                encryption_enabled=True, pickle_key="test-key", request_timeout=5
            ),
        )
        client.restore_login(identity["user_id"], identity["device_id"], identity["access_token"])
        clients.append(client)
    sender, observer = clients
    async with aiohttp.ClientSession() as session:
        admin = MatrixApi(base, human["access_token"], session)
        runner, gateway_url = await start_app(AdminGateway(config, admin, gateway_store).app())
        gateway = GatewayApi(gateway_url, config.gateway_secret, session)
        service = Service(
            config,
            store,
            observer,
            MatrixApi(base, bot["access_token"], session),
            gateway,
            AsyncMock(),
        )
        try:
            room = (
                await admin.request(
                    "POST",
                    "/_matrix/client/v3/createRoom",
                    json={
                        "preset": "private_chat",
                        "initial_state": [
                            {
                                "type": "m.room.encryption",
                                "state_key": "",
                                "content": {"algorithm": "m.megolm.v1.aes-sha2"},
                            }
                        ],
                    },
                )
            )["room_id"]
            await service.discover()
            await service.sync_once()
            await sender.sync(timeout=0)
            if sender.should_upload_keys:
                await sender.keys_upload()
            await sender.keys_query()
            for device in sender.device_store.active_user_devices(bot["user_id"]):
                sender.verify_device(device)
            sent = await sender.room_send(
                room, "m.room.message", {"msgtype": "m.text", "body": "!retention set 1d"}
            )
            assert not isinstance(sent, ErrorResponse)
            await service.sync_once()
            await service.retry_decryption()
            await service.commands()
            assert store.room(room)["lifetime"] == 86_400_000
            sent_file = await sender.room_send(
                room,
                "m.room.message",
                {
                    "msgtype": "m.file",
                    "body": "encrypted.bin",
                    "file": {
                        "url": "mxc://test.local/encryptedFile",
                        "v": "v2",
                        "key": {
                            "kty": "oct",
                            "key_ops": ["encrypt", "decrypt"],
                            "alg": "A256CTR",
                            "k": "A" * 43,
                            "ext": True,
                        },
                        "iv": "A" * 22,
                        "hashes": {"sha256": "A" * 43},
                    },
                },
            )
            assert not isinstance(sent_file, ErrorResponse)
            await service.sync_once()
            await service.retry_decryption()
            assert store.db.execute(
                "SELECT uri FROM media WHERE uri=?", ("mxc://test.local/encryptedFile",)
            ).fetchone()
            assert "encrypted.bin" not in "\n".join(store.db.iterdump())
        finally:
            for client in clients:
                await client.close()
            await runner.cleanup()
            store.close()
            gateway_store.close()


@pytest.mark.skipif(not os.getenv("RABBITMQ_TEST_URL"), reason="Set RABBITMQ_TEST_URL")
async def test_real_rabbitmq_redelivery_and_persistent_job():
    broker = await Broker().connect(os.environ["RABBITMQ_TEST_URL"])
    key = "$integration-" + uuid4().hex
    try:
        await broker.publish("redact", key)
        first = await broker.queue.get(timeout=5)
        assert first.delivery_mode == 2
        await first.nack(requeue=True)
        second = await broker.queue.get(timeout=5)
        assert second.redelivered
        assert key.encode() in second.body
        await second.ack()
    finally:
        await broker.close()
