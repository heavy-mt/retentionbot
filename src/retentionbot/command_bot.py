"""Personal-chat UI. Target-room membership and deletion remain server-side."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import secrets
import sqlite3
from dataclasses import dataclass
from pathlib import Path

import aiohttp
from nio import AsyncClient, AsyncClientConfig, ErrorResponse, MegolmEvent, RoomMessageText

from .api import PREFIX, ApiError, JsonApi
from .config import base_url, secret
from .policy import UNITS, duration
from .store import now_ms

logger = logging.getLogger(__name__)
HELP = (
    "Пришлите название комнаты или ссылку на неё. Чтобы получить ссылку в Element X: "
    "откройте нужную комнату, нажмите её название вверху и найдите «Поделиться» / "
    "«Скопировать ссылку». Названия пунктов зависят от версии приложения. "
    "Если ссылки нет, пришлите название комнаты, а для личного чата — имя собеседника. "
    "После выбора я покажу текущий срок и попрошу новый. "
    "Настройку меняет администратор выбранной комнаты. «Статус» показывает срок; "
    "«Другая комната» начинает новый выбор."
)


def human_duration(value):
    if value is None:
        return "не задан"
    if value:
        for unit in ("d", "h", "m", "s"):
            if value % UNITS[unit] == 0:
                return f"{value // UNITS[unit]}{unit}"
    return f"{value}ms"


def shortcut(value):
    match = re.fullmatch(
        r"(\d+)\s*(ms|s|m|h|d|w|y|мс|сек(?:унд[аы]?)?|мин(?:ут[аы]?)?|ч(?:ас(?:а|ов)?)?|"
        r"д(?:н(?:я|ей)?)?|день|нед(?:ел[яьи])?)",
        value.casefold(),
    )
    if not match:
        return None
    number, unit = match.groups()
    if unit not in UNITS:
        unit = (
            "ms"
            if unit == "мс"
            else "s"
            if unit.startswith("сек")
            else "m"
            if unit.startswith("мин")
            else "h"
            if unit.startswith("ч")
            else "w"
            if unit.startswith("нед")
            else "d"
        )
    result = number + unit
    duration(result, minimum=1)
    return result


@dataclass(frozen=True)
class BotConfig:
    synapse_url: str
    user_id: str
    access_token: str
    data_dir: Path

    @classmethod
    def from_env(cls):
        return cls(
            base_url(os.environ["SYNAPSE_URL"]),
            os.environ["COMMAND_BOT_USER_ID"],
            secret("COMMAND_BOT_ACCESS_TOKEN"),
            Path(os.getenv("DATA_DIR", "/data")),
        )


class BotStore:
    def __init__(self, directory):
        directory.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(directory / "commands.db", timeout=5)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS dialogs(dm TEXT PRIMARY KEY,
                actor TEXT NOT NULL, state TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS replies(event_id TEXT PRIMARY KEY, dm TEXT NOT NULL,
                body TEXT NOT NULL, sent INTEGER NOT NULL DEFAULT 0);
        """)

    def get(self, key):
        row = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row[0] if row else None

    def set(self, key, value):
        with self.db:
            self.db.execute(
                "INSERT INTO meta VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, str(value)),
            )

    def state(self, room, actor):
        row = self.db.execute("SELECT * FROM dialogs WHERE dm=?", (room,)).fetchone()
        return json.loads(row["state"]) if row and row["actor"] == actor else {}

    def saved(self, event_id):
        return self.db.execute("SELECT * FROM replies WHERE event_id=?", (event_id,)).fetchone()

    def save(self, dm, actor, event_id, state, reply):
        # Store the resulting dialog/outbox, not the text of the received command.
        with self.db:
            self.db.execute(
                "INSERT INTO dialogs VALUES(?,?,?) ON CONFLICT(dm) DO UPDATE SET "
                "actor=excluded.actor,state=excluded.state",
                (dm, actor, json.dumps(state, ensure_ascii=False)),
            )
            self.db.execute(
                "INSERT INTO replies(event_id,dm,body) VALUES(?,?,?) "
                "ON CONFLICT(event_id) DO NOTHING",
                (event_id, dm, json.dumps(reply, ensure_ascii=False, indent=2)),
            )

    def sent(self, event_id):
        with self.db:
            self.db.execute("UPDATE replies SET sent=1 WHERE event_id=?", (event_id,))

    def close(self):
        self.db.close()


class CommandBot:
    def __init__(self, config):
        self.config = config
        self.store = BotStore(config.data_dir)
        self.client = self.http = self.api = None
        self.rehydrate = True

    @staticmethod
    def checked(response):
        if isinstance(response, ErrorResponse):
            code = response.status_code
            status = 401 if code in {"M_UNKNOWN_TOKEN", "M_MISSING_TOKEN"} else 502
            raise ApiError(status, "MATRIX_REQUEST_FAILED", response.retry_after_ms or 0)
        return response

    async def start(self):
        self.http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=45))
        self.api = JsonApi(self.config.synapse_url, self.config.access_token, self.http)
        who = await self.api.request("GET", "/_matrix/client/v3/account/whoami")
        if who.get("user_id") != self.config.user_id or not who.get("device_id"):
            raise ValueError("Command bot token must match its user and a persistent device")
        await self.api.request("POST", PREFIX + "/bot/info", json={})
        crypto = self.config.data_dir / "crypto"
        crypto.mkdir(exist_ok=True)
        key_path = crypto / "pickle-key"
        if not key_path.exists():
            if list(crypto.glob("*.db")):
                raise ValueError("Restore the existing crypto pickle key")
            descriptor = os.open(key_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "w") as handle:
                handle.write(secrets.token_hex(32))
        self.client = AsyncClient(
            self.config.synapse_url,
            self.config.user_id,
            store_path=str(crypto),
            config=AsyncClientConfig(
                encryption_enabled=True,
                pickle_key=key_path.read_text(),
                store_sync_tokens=False,
                request_timeout=45,
            ),
        )
        self.client.restore_login(who["user_id"], who["device_id"], self.config.access_token)
        if self.store.get("started_at") is None:
            self.store.set("started_at", now_ms())

    async def close(self):
        if self.client:
            await self.client.close()
        if self.http:
            await self.http.close()
        self.store.close()

    async def server(self, suffix, dm, event, **data):
        return await self.api.request(
            "POST",
            PREFIX + "/bot/" + suffix,
            json={"dm_room_id": dm, "event_id": event.event_id, **data},
        )

    async def reply(self, event_id):
        saved = self.store.saved(event_id)
        if not saved or saved["sent"]:
            return
        self.checked(
            await self.client.room_send(
                saved["dm"],
                "m.room.message",
                {"msgtype": "m.notice", "body": saved["body"]},
                tx_id="retention_" + hashlib.sha256(event_id.encode()).hexdigest(),
                ignore_unverified_devices=True,
            )
        )
        self.store.sent(event_id)

    async def policy(self, dm, event, selected, action="status", **arguments):
        result = await self.server(
            "command",
            dm,
            event,
            command={
                "command": "retention",
                "action": action,
                "room_id": selected["room_id"],
                **arguments,
            },
        )
        return {
            "ok": True,
            "message": result["message"],
            "room": selected["name"],
            "room_id": selected["room_id"],
            "min_lifetime": human_duration(result.get("min_lifetime")),
            "max_lifetime": human_duration(result.get("max_lifetime")),
        }

    async def choose(self, dm, event, state, selected):
        result = await self.policy(dm, event, selected)
        state.clear()
        state["selected"] = selected
        result["message"] = (
            "Комната выбрана. Пришлите максимальный срок, например 7d или «7 дней». "
            "Минимальный срок сохранится. Оба срока можно задать JSON-командой: "
            '{"command":"retention","action":"set","min_lifetime":"1h","max_lifetime":"7d"}. '
            "s — секунды, m — минуты, h — часы, d — дни, w — недели."
        )
        return result

    async def conversation(self, dm, event, state):
        text = event.body.strip()
        if not text or len(text.encode()) > 4096:
            raise ValueError("Пришлите короткую команду, название комнаты или ссылку.")
        folded = text.casefold()
        if folded in {
            "help",
            "/help",
            "start",
            "/start",
            "помощь",
            "привет",
            "здравствуйте",
            "добрый день",
            "?",
            "ссылка",
            "где взять ссылку",
            "где взять ссылку?",
        }:
            return {"ok": True, "message": HELP}
        if folded in {"другая комната", "отмена", "назад"}:
            state.clear()
            return {"ok": True, "message": HELP}
        data = None
        if text.startswith("{"):
            try:
                data = json.loads(text)
            except ValueError:
                raise ValueError(
                    "Не удалось прочитать JSON. Проверьте кавычки и запятые."
                ) from None
            if not isinstance(data, dict) or data.get("command", "retention") != "retention":
                raise ValueError("Нужна JSON-команда retention.")
        candidates = state.get("candidates", [])
        if candidates and text.isdecimal():
            number = int(text)
            if not 1 <= number <= len(candidates):
                raise ValueError("Выберите номер из предложенного списка.")
            return await self.choose(dm, event, state, candidates[number - 1])
        selected = state.get("selected")
        query = data.get("room", data.get("room_id")) if data else None
        if data and data.get("action") == "help":
            return {"ok": True, "message": HELP}
        value = shortcut(text) if data is None else None
        if query is None and (
            not selected
            or (
                data is None
                and value is None
                and folded
                not in {
                    "status",
                    "статус",
                    "off",
                    "выключить",
                }
            )
        ):
            query = text
        if query is not None:
            resolved = await self.server("resolve", dm, event, query=query)
            rooms = resolved["rooms"]
            if not rooms:
                raise ValueError(
                    "Комната не найдена среди тех, где вы администратор. Проверьте название "
                    "или пришлите ссылку. " + HELP
                )
            if len(rooms) > 1 or resolved.get("truncated"):
                state.clear()
                state["candidates"] = rooms
                return {
                    "ok": True,
                    "message": "Есть комнаты с одинаковым названием. Пришлите номер нужной."
                    if not resolved.get("truncated")
                    else "Совпадений много. Пришлите точную ссылку.",
                    "choices": [
                        f"{i}. {room['name']} — {room['room_id']}"
                        for i, room in enumerate(rooms, 1)
                    ],
                }
            selected = rooms[0]
            result = await self.choose(dm, event, state, selected)
            if not data or not data.get("action"):
                return result
        if not selected:
            return {"ok": True, "message": HELP}
        if data:
            action = data.get("action", "status")
            args = {k: data[k] for k in ("min_lifetime", "max_lifetime") if k in data}
        else:
            action = "set" if value else "off" if folded in {"off", "выключить"} else "status"
            args = {"max_lifetime": value} if value else {}
        return await self.policy(dm, event, selected, action, **args)

    async def handle(self, dm, event):
        if event.sender == self.config.user_id or event.server_timestamp < int(
            self.store.get("started_at")
        ):
            return
        # Even help/replies require a live private chat; leave group invitations/chats.
        try:
            await self.server("context", dm, event)
        except ApiError as error:
            if error.code in {"NOT_PERSONAL_CHAT", "BAD_DM_ACTOR"}:
                self.checked(await self.client.room_leave(dm))
                return
            if error.code == "BAD_COMMAND_EVENT":
                return
            raise
        if self.store.saved(event.event_id):
            await self.reply(event.event_id)
            return
        state = self.store.state(dm, event.sender)
        try:
            if isinstance(event, MegolmEvent):
                result = {
                    "ok": False,
                    "message": "Не удалось расшифровать команду. "
                    "Дождитесь подключения бота и отправьте её заново.",
                }
            else:
                result = await self.conversation(dm, event, state)
        except ValueError as error:
            result = {"ok": False, "message": str(error)}
        except ApiError as error:
            if error.status >= 500 or error.status == 429:
                raise
            result = {
                "ok": False,
                "message": error.message or "Сервер отклонил команду.",
                "code": error.code,
            }
        self.store.save(dm, event.sender, event.event_id, state, result)
        await self.reply(event.event_id)
        logger.info(
            "Личная команда обработана",
            extra={"event": "command.processed", "event_id": event.event_id},
        )

    async def history(self, room, timeline):
        events = []
        token = timeline.prev_batch
        floor = int(self.store.get("started_at"))
        while token:
            page = self.checked(await self.client.room_messages(room, start=token, limit=100))
            stop = False
            for event in page.chunk:
                if isinstance(event, (RoomMessageText, MegolmEvent)):
                    if self.store.saved(event.event_id) or event.server_timestamp < floor:
                        stop = True
                        break
                    if event.sender != self.config.user_id:
                        events.append(event)
            if stop or not page.chunk or page.end == token:
                break
            token = page.end
        return list(reversed(events))

    async def sync_once(self, wait_ms=30000):
        await self.api.request("POST", PREFIX + "/bot/info", json={})
        saved_token = self.store.get("sync")
        self.client.next_batch = saved_token
        response = self.checked(
            await self.client.sync(
                timeout=wait_ms,
                since=saved_token,
                full_state=self.rehydrate,
                sync_filter={"room": {"timeline": {"limit": 100}}},
            )
        )
        if self.client.should_upload_keys:
            self.checked(await self.client.keys_upload())
        if self.client.should_query_keys:
            self.checked(await self.client.keys_query())
        for outgoing in await self.client.send_to_device_messages():
            self.checked(outgoing)
        for room in response.rooms.invite:
            try:
                await self.api.request("POST", PREFIX + "/bot/invite", json={"dm_room_id": room})
            except ApiError as error:
                if error.code not in {"NOT_PERSONAL_CHAT", "BAD_DM_ACTOR"}:
                    raise
                self.checked(await self.client.room_leave(room))
                continue
            self.checked(await self.client.join(room))
        for room, joined in response.rooms.join.items():
            try:
                await self.api.request("POST", PREFIX + "/bot/chat", json={"dm_room_id": room})
            except ApiError as error:
                if error.code not in {"NOT_PERSONAL_CHAT", "BAD_DM_ACTOR"}:
                    raise
                self.checked(await self.client.room_leave(room))
                continue
            if saved_token is not None:
                events = (
                    await self.history(room, joined.timeline) if joined.timeline.limited else []
                )
                events.extend(joined.timeline.events)
                for event in events:
                    if isinstance(event, (RoomMessageText, MegolmEvent)):
                        await self.handle(room, event)
        self.store.set("sync", response.next_batch)
        self.store.set("last_sync", now_ms())
        self.rehydrate = False


async def run(config):
    bot = CommandBot(config)
    try:
        await bot.start()
        logger.info("Бот личных команд подключён", extra={"event": "command_bot.started"})
        while True:
            try:
                await bot.sync_once()
            except (ApiError, TimeoutError, aiohttp.ClientError) as error:
                if isinstance(error, ApiError) and error.status in {401, 403}:
                    raise
                logger.warning(
                    "Команды будут получены повторно",
                    extra={
                        "event": "command_bot.retry",
                        "code": error.code if isinstance(error, ApiError) else type(error).__name__,
                    },
                )
                await asyncio.sleep(5)
    finally:
        await bot.close()
