import hashlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from retentionbot.bot_messages import display_duration, reply_content
from retentionbot.command_bot import BotStore, CommandBot


@pytest.mark.parametrize(("value", "expected"), [
    ("1s", "1 секунда"), ("30s", "30 секунд"), ("31s", "31 секунда"),
    ("5m", "5 минут"), ("1h", "1 час"), ("3d", "3 дня"),
    ("11d", "11 дней"), ("21d", "21 день"), ("не задан", "не задан"),
])
def test_display_duration(value, expected):
    assert display_duration(value) == expected


def test_policy_reply_has_readable_fallback_and_matrix_html():
    content = reply_content({
        "ok": True, "message": "Настройка сохранена.", "room": "Отдел",
        "room_id": "!room:example.org", "min_lifetime": "30s", "max_lifetime": "5m",
    })
    assert content["body"] == (
        "Отдел\n\nНастройка сохранена.\n\n"
        "Срок хранения: 5 минут\nМинимальный срок: 30 секунд"
    )
    assert content["format"] == "org.matrix.custom.html"
    assert "<strong>Отдел</strong>" in content["formatted_body"]
    assert "<strong>5 минут</strong>" in content["formatted_body"]
    assert "https://matrix.to/#/%21room%3Aexample.org" in content["formatted_body"]
    assert "room_id" not in content["body"]


def test_html_escapes_names_messages_and_choices():
    content = reply_content({
        "room": '<img src=x onerror="bad">', "message": "<script>&",
        "choices": ["1. <b>Отдел</b>"],
    })
    assert "<img" not in content["formatted_body"]
    assert "<script>" not in content["formatted_body"]
    assert "&lt;script&gt;&amp;" in content["formatted_body"]
    assert "&lt;b&gt;Отдел&lt;/b&gt;" in content["formatted_body"]


def test_policy_error_explains_next_step_without_internal_fields():
    content = reply_content({
        "ok": False, "code": "BAD_POLICY",
        "message": "Нужен запас между min_lifetime и max_lifetime.",
    })
    assert "Максимальный срок должен быть больше минимального" in content["body"]
    assert "Пришлите срок" in content["body"]
    assert "BAD_POLICY" not in content["body"]
    assert "min_lifetime" not in content["body"]


def test_choices_keep_numbers_and_plain_text_fallback():
    content = reply_content({
        "ok": True, "message": "Выберите комнату.",
        "choices": ["1. Отдел — !a:example.org", "2. Отдел — !b:example.org"],
    })
    assert "1. Отдел — !a:example.org\n2. Отдел — !b:example.org" in content["body"]


def test_permission_error_does_not_expose_internal_code():
    content = reply_content({"ok": False, "code": "NOT_ROOM_ADMIN", "message": "denied"})
    assert "администратор выбранной комнаты" in content["body"]
    assert "NOT_ROOM_ADMIN" not in content["body"]


async def test_saved_outbox_is_rendered_when_sending_and_not_resent(tmp_path):
    bot = object.__new__(CommandBot)
    bot.store = BotStore(tmp_path / "bot")
    bot.client = SimpleNamespace(room_send=AsyncMock(return_value=SimpleNamespace()))
    result = {"ok": True, "message": "Настройка сохранена.", "max_lifetime": "5m"}
    try:
        bot.store.save("!dm:example.org", "@admin:example.org", "$command", {}, result)
        # This is also the outbox format used before human-readable replies were added.
        assert json.loads(bot.store.saved("$command")["body"]) == result
        await bot.reply("$command")
        content = bot.client.room_send.call_args.args[2]
        assert "Срок хранения: 5 минут" in content["body"]
        assert content["format"] == "org.matrix.custom.html"
        assert bot.client.room_send.call_args.kwargs["tx_id"] == (
            "retention_" + hashlib.sha256(b"$command").hexdigest()
        )
        await bot.reply("$command")
        assert bot.client.room_send.await_count == 1
        assert bot.store.saved("$command")["sent"] == 1
    finally:
        bot.store.close()
