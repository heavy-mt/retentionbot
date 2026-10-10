import json
import os

import aiohttp
import pytest
from nio import AsyncClient, AsyncClientConfig, ErrorResponse

from retentionbot.api import PREFIX, ApiError, JsonApi, segment
from retentionbot.command_bot import BotConfig, CommandBot, shortcut
from retentionbot.room_reference import reference

from .test_integration import dm, people, register, send, setting
from .test_integration import homeserver as homeserver


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("https://matrix.to/#/!room:example.org?via=example.org", ("id", "!room:example.org")),
        ("https://matrix.to/#/%23team:example.org", ("alias", "#team:example.org")),
        ("matrix:roomid/room:example.org", ("id", "!room:example.org")),
        ("matrix:r/team:example.org", ("alias", "#team:example.org")),
        ("!" + "A" * 43, ("id", "!" + "A" * 43)),
        ("https://matrix.to/#/!" + "A" * 43 + "?via=example.org", ("id", "!" + "A" * 43)),
        ("matrix:roomid/" + "A" * 43, ("id", "!" + "A" * 43)),
        ("!" + "aB0_-" * 8 + "abc", ("id", "!" + "aB0_-" * 8 + "abc")),
        ("Отдел продаж", ("name", "Отдел продаж")),
    ],
)
def test_room_reference(value, expected):
    assert reference(value) == expected


def test_user_links_and_arbitrary_urls_are_not_rooms():
    for value in (
        "https://example.org/secret",
        "https://matrix.to/#/@alice:example.org",
        "!bad",
        "!" + "A" * 42,
        "!" + "A" * 44,
        "!" + "A" * 42 + "+",
        "#" + "A" * 43,
    ):
        with pytest.raises(ValueError):
            reference(value)
    assert shortcut("7 дней") == "7d"
    assert shortcut("24 часа") == "24h"
    assert shortcut("30 минут") == "30m"


requires_synapse = pytest.mark.skipif(not os.getenv("SYNAPSE_PYTHON"), reason="Set SYNAPSE_PYTHON")


async def bot_person(base, session):
    user = await register(base, "retention")
    api = JsonApi(base, user["access_token"], session)
    api.user_id = user["user_id"]
    return user, api


def answer(bot, event_id):
    return json.loads(bot.store.saved(event_id)["body"])


@requires_synapse
@pytest.mark.integration
async def test_personal_bot_guides_selects_sets_and_rechecks_rights(homeserver):
    base, path = homeserver
    async with aiohttp.ClientSession() as session:
        alice, bob, _, _ = await people(base, session)
        bot_user, bot_api = await bot_person(base, session)
        bot = CommandBot(
            BotConfig(base, bot_user["user_id"], bot_user["access_token"], path / "bot")
        )
        try:
            await bot.start()
            await bot.sync_once(wait_ms=0)
            target = await dm(alice, bob)
            await setting(alice, target, maximum="2d", minimum="1h")
            await alice.request(
                "PUT",
                f"/_matrix/client/v3/rooms/{segment(target)}/state/m.room.name",
                json={"name": "Отдел"},
            )
            # Let the bot accept an actual personal invitation, rather than joining the target.
            personal = (
                await alice.request(
                    "POST",
                    "/_matrix/client/v3/createRoom",
                    json={
                        "preset": "private_chat",
                        "is_direct": True,
                        "invite": [bot_user["user_id"]],
                    },
                )
            )["room_id"]
            await bot.sync_once(wait_ms=0)
            key = await send(alice, personal, "ссылка")
            await bot.sync_once(wait_ms=0)
            assert "нажмите её название" in answer(bot, key)["message"]
            key = await send(alice, personal, "Отдел")
            await bot.sync_once(wait_ms=0)
            assert answer(bot, key)["room_id"] == target
            key = await send(alice, personal, "7 дней")
            await bot.sync_once(wait_ms=0)
            assert answer(bot, key)["ok"]
            policy = await alice.request(
                "GET", f"/_matrix/client/v3/rooms/{segment(target)}/state/m.room.retention"
            )
            assert policy["max_lifetime"] == 7 * 86400000
            assert policy["min_lifetime"] == 3600000
            assert policy["org.retentionbot.command_event_id"] == key
            members = await alice.request(
                "GET", f"/_matrix/client/v3/rooms/{segment(target)}/joined_members"
            )
            assert set(members["joined"]) == {alice.user_id, bob.user_id}
            # Two rooms with the same title require a numbered choice.
            second = await dm(alice, bob)
            await alice.request(
                "PUT",
                f"/_matrix/client/v3/rooms/{segment(second)}/state/m.room.name",
                json={"name": "Отдел"},
            )
            key = await send(alice, personal, "Отдел")
            await bot.sync_once(wait_ms=0)
            assert len(answer(bot, key)["choices"]) == 2
            choices = bot.store.state(personal, alice.user_id)["candidates"]
            number = next(i for i, room in enumerate(choices, 1) if room["room_id"] == target)
            key = await send(alice, personal, str(number))
            await bot.sync_once(wait_ms=0)
            assert answer(bot, key)["room_id"] == target
            # Cached selection grants no rights after a power-level change.
            levels_path = f"/_matrix/client/v3/rooms/{segment(target)}/state/m.room.power_levels"
            levels = await alice.request("GET", levels_path)
            levels["users"][alice.user_id] = 50
            levels["users"][bob.user_id] = 100
            await alice.request("PUT", levels_path, json=levels)
            key = await send(alice, personal, "3d")
            await bot.sync_once(wait_ms=0)
            assert answer(bot, key)["code"] == "NOT_ROOM_ADMIN"
            assert (
                await bob.request(
                    "GET", f"/_matrix/client/v3/rooms/{segment(target)}/state/m.room.retention"
                )
            )["max_lifetime"] == 7 * 86400000
        finally:
            await bot.close()


@requires_synapse
@pytest.mark.integration
async def test_bot_delegation_binds_sender_and_replay_does_not_revert_policy(homeserver):
    base, path = homeserver
    async with aiohttp.ClientSession() as session:
        alice, bob, _, _ = await people(base, session)
        bot_user, bot_api = await bot_person(base, session)
        target = await dm(alice, bob)
        personal = await dm(alice, bot_api)
        trigger = await send(alice, personal, "7d")
        command = {"command": "retention", "action": "set", "room_id": target, "max_lifetime": "7d"}
        data = {"dm_room_id": personal, "event_id": trigger, "command": command}
        result = await bot_api.request("POST", PREFIX + "/bot/command", json=data)
        assert result["max_lifetime"] == 7 * 86400000
        await setting(alice, target, maximum="5d")
        replay = await bot_api.request("POST", PREFIX + "/bot/command", json=data)
        assert replay["max_lifetime"] == 5 * 86400000
        assert "уже выполнена" in replay["message"]
        with pytest.raises(ApiError) as error:
            await alice.request("POST", PREFIX + "/bot/command", json=data)
        assert error.value.code == "NOT_COMMAND_BOT"
        outside = await send(alice, target, "outside the management chat")
        with pytest.raises(ApiError) as error:
            await bot_api.request(
                "POST", PREFIX + "/bot/command", json={**data, "event_id": outside}
            )
        assert error.value.code == "BAD_COMMAND_EVENT"
        bob_dm = await dm(bob, bot_api)
        bob_event = await send(bob, bob_dm, "pretend to be Alice")
        with pytest.raises(ApiError) as error:
            await bot_api.request(
                "POST",
                PREFIX + "/bot/command",
                json={
                    **data,
                    "dm_room_id": bob_dm,
                    "event_id": bob_event,
                    "actor": alice.user_id,
                },
            )
        assert error.value.code == "NOT_ROOM_ADMIN"
        # Alias/share links resolve without the bot entering the protected room.
        alias = "#team:test.local"
        await alice.request(
            "PUT", "/_matrix/client/v3/directory/room/" + segment(alias), json={"room_id": target}
        )
        resolved = await bot_api.request(
            "POST",
            PREFIX + "/bot/resolve",
            json={
                "dm_room_id": personal,
                "event_id": trigger,
                "query": "https://matrix.to/#/%23team:test.local",
            },
        )
        assert resolved["rooms"][0]["room_id"] == target
        await alice.request(
            "POST",
            f"/_matrix/client/v3/rooms/{segment(personal)}/invite",
            json={"user_id": bob.user_id},
        )
        with pytest.raises(ApiError) as error:
            await bot_api.request("POST", PREFIX + "/bot/command", json=data)
        assert error.value.code == "NOT_PERSONAL_CHAT"
        bot = CommandBot(
            BotConfig(base, bot_user["user_id"], bot_user["access_token"], path / "bot")
        )
        try:
            await bot.start()
            group = (
                await alice.request(
                    "POST",
                    "/_matrix/client/v3/createRoom",
                    json={
                        "preset": "private_chat",
                        "invite": [bob.user_id, bot_user["user_id"]],
                    },
                )
            )["room_id"]
            await bot.sync_once(wait_ms=0)
            members = await alice.request(
                "GET",
                f"/_matrix/client/v3/rooms/{segment(group)}/state/m.room.member/{segment(bot_user['user_id'])}",
            )
            assert members["membership"] == "leave"
        finally:
            await bot.close()


@requires_synapse
@pytest.mark.integration
async def test_encrypted_personal_commands_and_restart_preserve_selection(homeserver):
    base, path = homeserver
    async with aiohttp.ClientSession() as session:
        alice, bob, a, _ = await people(base, session)
        bot_user, bot_api = await bot_person(base, session)
        config = BotConfig(base, bot_user["user_id"], bot_user["access_token"], path / "bot")
        bot = CommandBot(config)
        client = None
        try:
            await bot.start()
            await bot.sync_once(wait_ms=0)
            target = await dm(alice, bob, encrypted=True)
            personal = await dm(alice, bot_api, encrypted=True)
            await bot.sync_once(wait_ms=0)
            crypto = path / "alice-crypto"
            crypto.mkdir()
            client = AsyncClient(
                base,
                alice.user_id,
                store_path=str(crypto),
                config=AsyncClientConfig(encryption_enabled=True, pickle_key="test"),
            )
            client.restore_login(alice.user_id, a["device_id"], a["access_token"])
            await client.sync(timeout=0)
            await client.keys_upload()
            await client.keys_query()
            selected = await client.room_send(
                personal,
                "m.room.message",
                {
                    "msgtype": "m.text",
                    "body": "https://matrix.to/#/" + target,
                },
                ignore_unverified_devices=True,
            )
            assert not isinstance(selected, ErrorResponse)
            await bot.sync_once(wait_ms=0)
            assert answer(bot, selected.event_id)["room_id"] == target
            sent = await client.room_send(
                personal,
                "m.room.message",
                {"msgtype": "m.text", "body": "3d"},
                ignore_unverified_devices=True,
            )
            await bot.sync_once(wait_ms=0)
            assert answer(bot, sent.event_id)["ok"]
            received = await client.sync(timeout=0)
            assert any(
                "Срок хранения: 3 дня" in e.body
                for e in received.rooms.join[personal].timeline.events
                if e.sender == bot_user["user_id"] and hasattr(e, "body")
            )
            await bot.close()
            bot = CommandBot(config)
            await bot.start()
            request = await client.room_send(
                personal,
                "m.room.message",
                {"msgtype": "m.text", "body": "статус"},
                ignore_unverified_devices=True,
            )
            await bot.sync_once(wait_ms=0)
            assert answer(bot, request.event_id)["max_lifetime"] == "3d"
            raw = await alice.request(
                "GET",
                f"/_matrix/client/v3/rooms/{segment(personal)}/messages",
                params={"dir": "b", "limit": "30"},
            )
            assert all(
                e["type"] == "m.room.encrypted"
                for e in raw["chunk"]
                if e["sender"] == bot_user["user_id"] and "state_key" not in e
            )
            members = await alice.request(
                "GET", f"/_matrix/client/v3/rooms/{segment(target)}/joined_members"
            )
            assert set(members["joined"]) == {alice.user_id, bob.user_id}
        finally:
            if client:
                await client.close()
            await bot.close()


@requires_synapse
@pytest.mark.integration
async def test_crash_after_apply_and_limited_sync_recover_commands(homeserver, monkeypatch):
    base, path = homeserver
    async with aiohttp.ClientSession() as session:
        alice, bob, _, _ = await people(base, session)
        user, bot_api = await bot_person(base, session)
        config = BotConfig(base, user["user_id"], user["access_token"], path / "bot")
        bot = CommandBot(config)
        try:
            await bot.start()
            await bot.sync_once(wait_ms=0)
            target = await dm(alice, bob)
            personal = await dm(alice, bot_api)
            await bot.sync_once(wait_ms=0)
            await send(alice, personal, "https://matrix.to/#/" + target)
            await bot.sync_once(wait_ms=0)
            command = await send(alice, personal, "7d")
            original = bot.store.save

            def crash(*args):
                raise OSError("simulated process failure")

            monkeypatch.setattr(bot.store, "save", crash)
            with pytest.raises(OSError):
                await bot.sync_once(wait_ms=0)
            monkeypatch.setattr(bot.store, "save", original)
            await setting(alice, target, maximum="5d")
            await bot.close()
            bot = CommandBot(config)
            await bot.start()
            await bot.sync_once(wait_ms=0)
            assert answer(bot, command)["max_lifetime"] == "5d"
            assert "уже выполнена" in answer(bot, command)["message"]
            # The bot is offline for more messages than one /sync timeline contains.
            missed = [await send(alice, personal, "статус") for _ in range(110)]
            await bot.sync_once(wait_ms=0)
            assert all(bot.store.saved(key)["sent"] for key in missed)
        finally:
            await bot.close()

