from __future__ import annotations

import asyncio
import hashlib
import json
import logging

import aiohttp
from nio import (
    AsyncClient,
    AsyncClientConfig,
    EncryptionError,
    ErrorResponse,
    MegolmEvent,
    RoomMessagesResponse,
    SyncResponse,
)
from nio.exceptions import OlmUnverifiedDeviceError

from .api import ApiError, GatewayApi, MatrixApi, segment
from .broker import Broker
from .config import PERIODS, Config, secret
from .media import attachment_uris, relation, split_mxc
from .policy import ServerPolicy
from .store import Store, now_ms

log = logging.getLogger(__name__)


def answer(message: str, **fields) -> str:
    return json.dumps({"message": message, **fields}, ensure_ascii=False, separators=(",", ":"))


HELP = answer(
    "Настройки меняет администратор комнаты. Сроки: строки 1h, 1d, 7d, 30d или миллисекунды.",
    command="retention",
    actions=["help", "status", "set", "off"],
    example={"command": "retention", "action": "set", "min_lifetime": "1h", "max_lifetime": "7d"},
)
MESSAGE_TYPES = {
    "m.room.message",
    "m.sticker",
    "m.reaction",
    "org.matrix.msc3381.poll.start",
    "org.matrix.msc3381.poll.response",
    "org.matrix.msc3381.poll.end",
    "m.poll.start",
    "m.poll.response",
    "m.poll.end",
}


def parse_command(body: str) -> tuple[str, str | None] | None:
    if not isinstance(body, str) or not body.lstrip().startswith("{"):
        return None
    try:
        data = json.loads(body)
    except ValueError:
        return ("invalid", None) if '"retention"' in body else None
    if not isinstance(data, dict) or data.get("command") != "retention":
        return None
    action = data.get("action", "help")
    if not isinstance(action, str):
        return "invalid", None
    if action in {"help", "status", "off"} and set(data) <= {"command", "action"}:
        return action, None
    if (
        action == "set"
        and "max_lifetime" in data
        and set(data) <= {"command", "action", "min_lifetime", "max_lifetime"}
    ):
        return action, json.dumps(
            {k: data[k] for k in ("min_lifetime", "max_lifetime") if k in data}
        )
    return "invalid", None


def period_name(lifetime: int | None) -> str:
    return next(
        (name for name, value in PERIODS.items() if value == lifetime),
        "отключено" if lifetime is None else f"{lifetime // 1000} секунд",
    )


class Service:
    def __init__(
        self,
        config: Config,
        store: Store,
        client: AsyncClient,
        matrix: MatrixApi,
        gateway: GatewayApi,
        broker: Broker,
    ):
        self.config, self.store, self.client = config, store, client
        self.matrix, self.gateway, self.broker = matrix, gateway, broker
        self.lock = asyncio.Lock()
        self.first_sync = True
        self.server_policy = ServerPolicy(default_max=config.default_lifetime)

    def observe(self, room_id: str, event):
        source = event.source
        event_id, ts = source.get("event_id"), source.get("origin_server_ts")
        if not isinstance(event_id, str) or not isinstance(ts, int):
            return
        content = source.get("content", {})
        if "state_key" in source:
            if source.get("type") == "m.room.retention" and self.store.room(room_id):
                try:
                    policy = self.server_policy.effective(content)
                    self.store.policy(
                        room_id,
                        policy.min_lifetime,
                        policy.max_lifetime,
                        0 if source.get("sender") == self.config.user_id else ts,
                        lead_ms=self.config.redaction_lead_ms,
                    )
                    if self.store.room(room_id)["error"] == "INVALID_RETENTION_POLICY":
                        self.store.room_error(room_id, None)
                except ValueError:
                    self.store.room_error(room_id, "INVALID_RETENTION_POLICY")
            if source.get("type") in {"m.room.avatar", "m.room.member"}:
                uri = content.get("url") or content.get("avatar_url")
                if isinstance(uri, str):
                    try:
                        split_mxc(uri)
                        self.store.protect(uri)
                    except ValueError:
                        pass
            return
        if source.get("type") == "m.room.redaction":
            target = content.get("redacts", source.get("redacts"))
            if isinstance(target, str):
                self.store.mark_redacted(target, now_ms() + self.config.media_grace_seconds * 1000)
            return
        if isinstance(event, MegolmEvent):
            self.store.record(
                room_id, event_id, ts, kind="m.room.encrypted", decoded=False, ciphertext=source
            )
            return
        kind = source.get("type")
        if kind not in MESSAGE_TYPES:
            # A decrypted RTC/custom event is not a message to expire. Drop its saved ciphertext.
            with self.store.db:
                self.store.db.execute(
                    "UPDATE events SET decoded=1,redacted=1,ciphertext=NULL "
                    "WHERE event_id=? AND decoded=0",
                    (event_id,),
                )
            return
        related_kind, parent = relation(content)
        recorded = self.store.record(
            room_id,
            event_id,
            ts,
            kind=related_kind or kind,
            parent=parent,
            media=attachment_uris(content),
        )
        if not recorded:
            return
        if "redacted_because" in source.get("unsigned", {}):
            self.store.mark_redacted(event_id, now_ms() + self.config.media_grace_seconds * 1000)
        if kind == "m.room.message" and content.get("msgtype") == "m.text" and not parent:
            command = parse_command(content.get("body", ""))
            if (
                command
                and not self.store.db.execute(
                    "SELECT redacted FROM events WHERE event_id=?", (event_id,)
                ).fetchone()["redacted"]
            ):
                self.store.queue_command(event_id, room_id, source["sender"], *command)

    async def commands(self):
        for command in self.store.db.execute(
            "SELECT * FROM commands WHERE done=0 ORDER BY ts,event_id"
        ).fetchall():
            action, room_id = command["action"], command["room_id"]
            if action in {"set", "off"}:
                if command["ts"] < self.store.room(room_id)["policy_ts"]:
                    self.store.finish_command(
                        command,
                        answer(
                            "Команда пропущена: администратор уже изменил срок позднее.",
                            ok=False,
                            code="STALE_COMMAND",
                        ),
                    )
                    continue
                # Never trust cached power levels or display names for authorisation.
                levels = await self.matrix.power_levels(room_id)
                member = await self.matrix.member(room_id, command["sender"])
                power = levels.get("users", {}).get(
                    command["sender"], levels.get("users_default", 0)
                )
                if member.get("membership") != "join" or power < self.config.admin_power_level:
                    self.store.finish_command(
                        command,
                        answer(
                            "Изменять срок может администратор комнаты.", ok=False, code="FORBIDDEN"
                        ),
                    )
                    continue
                try:
                    if action == "off":
                        if self.server_policy.default_max is not None:
                            raise ValueError(
                                "Отключение невозможно: действует серверный срок по умолчанию."
                            )
                        content = {}
                    else:
                        content = self.server_policy.requested(command["argument"])
                    policy = self.server_policy.effective(content)
                    if (
                        policy.max_lifetime is not None
                        and policy.min_lifetime == policy.max_lifetime
                    ):
                        raise ValueError("Между min_lifetime и max_lifetime нужен запас доставки.")
                except (ValueError, TypeError, KeyError) as error:
                    self.store.finish_command(
                        command, answer(str(error), ok=False, code="INVALID_POLICY")
                    )
                    continue
                await self.matrix.set_retention(room_id, content)
                self.store.policy(
                    room_id,
                    policy.min_lifetime,
                    policy.max_lifetime,
                    command["ts"],
                    lead_ms=self.config.redaction_lead_ms,
                )
                self.store.finish_command(
                    command,
                    answer(
                        "Политика Synapse обновлена. Старую историю бот не обрабатывает.",
                        ok=True,
                        min_lifetime=policy.min_lifetime,
                        max_lifetime=policy.max_lifetime,
                    ),
                )
            elif action == "status":
                room, counts = self.store.room(room_id), self.store.counts(room_id)
                errors = room["error"] or "нет"
                body = answer(
                    "Текущая политика комнаты и состояние обработки.",
                    ok=True,
                    redact_after=room["redact_after"],
                    min_lifetime=room["min_lifetime"],
                    max_lifetime=room["lifetime"],
                    queued=counts["pending"],
                    missing_keys=counts["missing_keys"],
                    deletion_errors=counts["errors"],
                    room_error=errors,
                )
                self.store.finish_command(command, body)
            else:
                self.store.finish_command(command, HELP)

    async def trust(self):
        if self.client.should_query_keys:
            response = await self.client.keys_query()
            if isinstance(response, ErrorResponse):
                raise ApiError(502, "KEY_QUERY_FAILED")
        if self.config.trust_devices != "tofu":
            return
        for user_id in self.client.device_store.users:
            for device in self.client.device_store.active_user_devices(user_id):
                row = self.store.db.execute(
                    "SELECT ed25519 FROM device_keys WHERE user_id=? AND device_id=?",
                    (user_id, device.device_id),
                ).fetchone()
                if row and row[0] != device.ed25519:
                    self.client.unverify_device(device)
                    log.error(
                        "Device identity changed user=%s device=%s", user_id, device.device_id
                    )
                    continue
                with self.store.db:
                    self.store.db.execute(
                        "INSERT INTO device_keys VALUES(?,?,?) "
                        "ON CONFLICT(user_id,device_id) DO NOTHING",
                        (user_id, device.device_id, device.ed25519),
                    )
                self.client.verify_device(device)

    async def replies(self):
        pending = self.store.db.execute("SELECT * FROM outbox WHERE delivered=0").fetchall()
        if pending:
            await self.trust()
        for reply in pending:
            try:
                response = await self.client.room_send(
                    reply["room_id"],
                    "m.room.message",
                    {"msgtype": "m.notice", "body": reply["body"]},
                    tx_id="reply-" + hashlib.sha256(reply["event_id"].encode()).hexdigest(),
                )
                if isinstance(response, ErrorResponse):
                    log.warning(
                        "Reply failed room=%s code=%s", reply["room_id"], response.status_code
                    )
                    continue
            except (EncryptionError, OlmUnverifiedDeviceError):
                log.warning("Encrypted reply waiting for trusted devices room=%s", reply["room_id"])
                continue
            with self.store.db:
                self.store.db.execute(
                    "UPDATE outbox SET delivered=1 WHERE event_id=?", (reply["event_id"],)
                )

    async def repair_gap(self, room_id: str, token: str, anchor: str | None):
        room = self.store.room(room_id)
        seen_tokens = set()
        while token not in seen_tokens:
            seen_tokens.add(token)
            response = await self.client.room_messages(room_id, start=token, limit=100)
            if not isinstance(response, RoomMessagesResponse):
                raise ApiError(502, "GAP_BACKFILL_FAILED")
            for event in response.chunk:
                if event.source.get("event_id") == anchor:
                    return
                ts = event.source.get("origin_server_ts", 0)
                if ts < room["since_ts"]:
                    # Arrival order and origin timestamps can differ. Continue through the
                    # page; an observed anchor is preferred when a room already has one.
                    continue
                self.observe(room_id, event)
            if not response.chunk or not response.end or response.end == token:
                if anchor:
                    raise ApiError(409, "GAP_ANCHOR_UNAVAILABLE")
                return
            if not anchor and all(
                e.source.get("origin_server_ts", 0) < room["since_ts"] for e in response.chunk
            ):
                return
            token = response.end
        raise ApiError(409, "GAP_PAGINATION_STALLED")

    async def sync_once(self):
        response = await self.client.sync(
            timeout=0 if self.first_sync else 30_000,
            since=self.store.get("sync_token"),
            full_state=self.first_sync,
            sync_filter={"room": {"timeline": {"limit": 100}}},
            set_presence="offline",
        )
        if not isinstance(response, SyncResponse):
            raise ApiError(502, "SYNC_FAILED")
        async with self.lock:
            for room_id, info in response.rooms.join.items():
                # Activate previously failed enrollment only when membership is established.
                # Manually invited rooms start now, never from historic sync events.
                self.store.enroll(room_id, self.server_policy.effective({}).max_lifetime)
                for event in info.state:
                    self.observe(room_id, event)
                if info.timeline.limited:
                    anchor = self.store.room(room_id)["last_event"]
                    try:
                        await self.repair_gap(room_id, info.timeline.prev_batch, anchor)
                    except ApiError as error:
                        self.store.room_error(room_id, error.code)
                        if error.code != "GAP_ANCHOR_UNAVAILABLE":
                            raise
                        log.error(
                            "History gap cannot be restored after native purge room=%s", room_id
                        )
                for event in info.timeline.events:
                    self.observe(room_id, event)
                if info.timeline.events:
                    self.store.last_event(room_id, info.timeline.events[-1].source["event_id"])
                if self.store.room(room_id)["error"] not in {
                    "INVALID_RETENTION_POLICY",
                    "GAP_ANCHOR_UNAVAILABLE",
                }:
                    self.store.room_error(room_id, None)
            for room_id in response.rooms.leave:
                with self.store.db:
                    self.store.db.execute(
                        "UPDATE rooms SET joined=0,error='BOT_LEFT' WHERE room_id=?", (room_id,)
                    )
            self.store.set("sync_token", response.next_batch)
            self.store.set("last_sync_at", str(now_ms()))
            self.first_sync = False
            await self.commands()
            await self.replies()
        if self.client.should_upload_keys:
            await self.client.keys_upload()
        if self.client.should_query_keys:
            await self.client.keys_query()
        if self.client.should_claim_keys:
            await self.client.keys_claim(self.client.get_users_for_key_claiming())
        await self.client.send_to_device_messages()

    async def retry_decryption(self):
        for row in self.store.undecoded():
            source = json.loads(row["ciphertext"])
            source["room_id"] = row["room_id"]
            event = MegolmEvent.from_dict(source)
            try:
                decoded = self.client.decrypt_event(event)
            except EncryptionError:
                # Request from the sender, not only other devices of the bot account.
                if row["key_next_try"] <= now_ms():
                    request = event.as_key_request(
                        user_id=event.sender,
                        requesting_device_id=self.client.device_id,
                        request_id="retention-key-"
                        + hashlib.sha256(row["event_id"].encode()).hexdigest(),
                    )
                    await self.client.to_device(request)
                    self.store.key_retry(row["event_id"], now_ms() + 60_000)
                continue
            self.observe(row["room_id"], decoded)

    async def discover(self):
        self.store.set("coverage_ok", "0")
        settings = await self.gateway.retention_config()
        self.server_policy = ServerPolicy(**settings)
        self.store.set("server_retention", json.dumps(settings))
        rooms = await self.gateway.rooms()
        success = True
        for room in rooms:
            room_id = room["room_id"]
            if room.get("room_type") == "m.space" or not room.get("joined_local_members", 0):
                continue
            try:
                # Read membership from the server, so a kick/demotion is noticed on each scan.
                try:
                    member = await self.matrix.member(room_id, self.config.user_id)
                    joined = member.get("membership") == "join"
                except ApiError as error:
                    if error.status not in {403, 404}:
                        raise
                    joined = False
                levels = await self.matrix.power_levels(room_id) if joined else {}
                required = max(
                    levels.get("redact", 50),
                    levels.get("events", {}).get(
                        "m.room.retention", levels.get("state_default", 50)
                    ),
                    levels.get("events_default", 0),
                    levels.get("events", {}).get("m.room.redaction", 0),
                    levels.get("events", {}).get("m.room.message", 0),
                    levels.get("events", {}).get("m.room.encrypted", 0),
                )
                power = levels.get("users", {}).get(self.config.user_id, 0)
                if not joined or power < required:
                    await self.gateway.enroll(room_id)
                if not joined:
                    response = await self.client.join(room_id)
                    if isinstance(response, ErrorResponse):
                        raise ApiError(403, "BOT_JOIN_FAILED")
                self.store.enroll(room_id, self.server_policy.effective({}).max_lifetime)
                with self.store.db:
                    self.store.db.execute("UPDATE rooms SET joined=1 WHERE room_id=?", (room_id,))
                # Reduce the temporary maximum granted by make_room_admin to required power.
                levels = await self.matrix.power_levels(room_id)
                required = max(
                    levels.get("redact", 50),
                    levels.get("events", {}).get(
                        "m.room.retention", levels.get("state_default", 50)
                    ),
                    levels.get("events_default", 0),
                    *(
                        levels.get("events", {}).get(t, 0)
                        for t in ("m.room.redaction", "m.room.message", "m.room.encrypted")
                    ),
                )
                if levels.get("users", {}).get(self.config.user_id, 0) < required:
                    raise ApiError(403, "BOT_INSUFFICIENT_POWER")
                if levels.get("users", {}).get(self.config.user_id, 0) > required:
                    levels.setdefault("users", {})[self.config.user_id] = required
                    await self.matrix.request(
                        "PUT",
                        f"/_matrix/client/v3/rooms/{segment(room_id)}/state/m.room.power_levels",
                        json=levels,
                    )
                policy = self.server_policy.effective(await self.matrix.retention(room_id))
                self.store.policy(
                    room_id,
                    policy.min_lifetime,
                    policy.max_lifetime,
                    lead_ms=self.config.redaction_lead_ms,
                )
                # Preserve an existing sync error until sync/backfill actually succeeds.
                if self.store.room(room_id)["error"] in {
                    "BOT_LEFT",
                    "ENROLL_FAILED",
                    "INVALID_RETENTION_POLICY",
                }:
                    self.store.room_error(room_id, None)
            except ValueError:
                success = False
                self.store.room_error(room_id, "INVALID_RETENTION_POLICY")
                log.error("Invalid native retention policy room=%s", room_id)
            except (ApiError, aiohttp.ClientError, TimeoutError) as error:
                success = False
                self.store.enroll(
                    room_id, self.server_policy.effective({}).max_lifetime, active=False
                )
                self.store.room_error(room_id, "ENROLL_FAILED")
                log.warning(
                    "Room enrollment failed room=%s code=%s",
                    room_id,
                    error.code if isinstance(error, ApiError) else "NETWORK_ERROR",
                )
        self.store.set("coverage_ok", "1" if success else "0")
        self.store.set("discovery_at", str(now_ms()))

    async def schedule(self):
        at = now_ms()
        for event in self.store.due_events(at):
            # Mark before publishing; crash recovery re-enqueues after the five-minute lease.
            # Publisher confirms must complete before assuming the task reached RabbitMQ.
            self.store.queued("redact", event["event_id"], at)
            try:
                await self.broker.publish("redact", event["event_id"])
            except Exception:
                self.store.release("redact", event["event_id"])
                raise
        if self.store.cleanup_ready(at):
            for media in self.store.media_candidates(at):
                self.store.queued("media", media["uri"], at)
                try:
                    await self.broker.publish("media", media["uri"])
                except Exception:
                    self.store.release("media", media["uri"])
                    raise

    async def sync_loop(self):
        while True:
            try:
                await self.sync_once()
            except (ApiError, aiohttp.ClientError, TimeoutError, EncryptionError) as error:
                log.warning(
                    "Sync/commands paused code=%s",
                    error.code if isinstance(error, ApiError) else type(error).__name__,
                )
                await asyncio.sleep(5)

    async def maintenance(self):
        while True:
            try:
                async with self.lock:
                    if now_ms() - int(self.store.get("discovery_at") or "0") >= (
                        self.config.discover_seconds * 1000
                    ):
                        await self.discover()
                    await self.retry_decryption()
                    await self.commands()
                    await self.replies()
                    await self.schedule()
            except (ApiError, aiohttp.ClientError, TimeoutError, EncryptionError) as error:
                log.warning(
                    "Maintenance paused code=%s",
                    error.code if isinstance(error, ApiError) else type(error).__name__,
                )
            await asyncio.sleep(self.config.poll_seconds)


async def run(config: Config):
    config.data_dir.mkdir(parents=True, exist_ok=True)
    crypto_dir = config.data_dir / "crypto"
    crypto_dir.mkdir(exist_ok=True)
    store = Store(config.database_url or config.data_dir / "retention.db")
    client = AsyncClient(
        config.homeserver,
        config.user_id,
        store_path=str(crypto_dir),
        config=AsyncClientConfig(
            encryption_enabled=True,
            store_sync_tokens=False,
            pickle_key=secret("CRYPTO_STORE_KEY"),
            max_limit_exceeded=1,
            max_timeouts=1,
            request_timeout=45,
        ),
    )
    client.restore_login(config.user_id, secret("BOT_DEVICE_ID"), secret("BOT_ACCESS_TOKEN"))
    broker = await Broker().connect(config.rabbitmq_url)
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=45)) as session:
        service = Service(
            config,
            store,
            client,
            MatrixApi(config.homeserver, client.access_token, session),
            GatewayApi(config.gateway_url, config.gateway_secret, session),
            broker,
        )
        try:
            await service.discover()
            log.info("Observer started user=%s device=%s", config.user_id, client.device_id)
            async with asyncio.TaskGroup() as tasks:
                tasks.create_task(service.sync_loop())
                tasks.create_task(service.maintenance())
        finally:
            await client.close()
            await broker.close()
            store.close()
