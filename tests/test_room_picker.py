import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from retentionbot.api import ApiError
from retentionbot.command_bot import HELP, BotStore, CommandBot


@pytest.fixture
def bot(tmp_path):
    result = object.__new__(CommandBot)
    result.store = BotStore(tmp_path)
    result.server = AsyncMock()
    yield result
    result.store.close()


async def test_paginated_picker_uses_current_page_and_preserves_it_after_restart(bot, tmp_path):
    first = {"room_id": "!first:test", "name": "Отдел", "participants": ["Анна"],
             "participant_ids": ["@anna:test"], "topic": "Закупки", "member_count": 2}
    second = {**first, "room_id": "!second:test", "topic": "Продажи"}
    state = {"selected": {"room_id": "!old:test", "name": "Старая"}}
    bot.server.return_value = {"rooms": [first], "next": "!cursor:test"}
    reply = await bot.conversation("!dm:test", SimpleNamespace(body="Перечислить комнаты"), state)
    assert "Анна" in reply["choices"][0] and "Закупки" in reply["choices"][0]
    assert "selected" not in state
    bot.store.save("!dm:test", "@actor:test", "$list", state, reply)
    bot.store.close()
    bot.store = BotStore(tmp_path)
    state = bot.store.state("!dm:test", "@actor:test")
    bot.server.return_value = {"rooms": [second], "next": None}
    await bot.conversation("!dm:test", SimpleNamespace(body="Далее"), state)
    assert bot.server.call_args.kwargs["after"] == "!cursor:test"
    bot.server.side_effect = [
        {"rooms": [second]}, {"message": "Текущая политика", "max_lifetime": 300000},
    ]
    chosen = await bot.conversation("!dm:test", SimpleNamespace(body="1"), state)
    assert chosen["room_id"] == second["room_id"]
    assert "Пока настройки не изменены" in chosen["message"]
    assert "candidates" not in state


async def test_number_selection_rechecks_rights_before_saving_selection(bot):
    room = {"room_id": "!target:test", "name": "Отдел"}
    state = {"candidates": [room]}
    bot.server.side_effect = ApiError(403, "NOT_ROOM_ADMIN")
    with pytest.raises(ApiError):
        await bot.conversation("!dm:test", SimpleNamespace(body="1"), state)
    assert bot.server.call_args.args[0] == "resolve"
    assert "selected" not in state


async def test_empty_list_and_invalid_number_do_not_choose_other_room(bot):
    bot.server.return_value = {"rooms": [], "next": None}
    state = {}
    reply = await bot.conversation("!dm:test", SimpleNamespace(body="комнаты"), state)
    assert "Доступных комнат не найдено" in reply["message"]
    assert not state.get("selected")
    with pytest.raises(ValueError, match="число|Число"):
        await bot.conversation("!dm:test", SimpleNamespace(body="1"), state)
    with pytest.raises(ValueError, match="Следующей страницы нет"):
        await bot.conversation("!dm:test", SimpleNamespace(body="Далее"), state)


async def test_duration_without_selection_explains_number_and_unit(bot):
    with pytest.raises(ValueError, match="Сначала выберите комнату"):
        await bot.conversation("!dm:test", SimpleNamespace(body="5 минут"), {})
    assert bot.server.await_count == 0
    assert "Перечислить комнаты" in HELP
    assert "Скопировать ссылку" not in HELP


async def test_confirmation_discloses_server_clamping_and_live_limits(bot):
    room = {"room_id": "!target:test", "name": "Отдел"}
    bot.server.return_value = {"message": "Настройка сохранена.", "max_lifetime": 60000,
                               "allowed_lifetime_min": 60000,
                               "allowed_lifetime_max": 31536000000, "redaction_lead_ms": 5000}
    result = await bot.conversation("!dm:test", SimpleNamespace(body="1s"), {"selected": room})
    assert "ограничен правилами сервера" in result["message"]
    assert result["max_lifetime"] == "1m"
    assert result["redaction_lead"] == "5s"
    assert json.loads(json.dumps(result))["allowed_min"] == "1m"
