from __future__ import annotations

import hmac
import json
import logging
import os
import re
import tempfile
from dataclasses import asdict
from importlib.metadata import version as package_version
from pathlib import Path

from synapse.api.errors import SynapseError
from synapse.events.py_protocol import supports_msc4242_state_dag
from synapse.http.server import JsonResource
from synapse.http.servlet import parse_integer, parse_string
from synapse.types import StreamKeyType, UserID, create_requester

from retentionbot.cache_reset import toggle_ignored_user_sentinel
from retentionbot.jsonlog import JsonFormatter
from retentionbot.policy import ServerPolicy, duration

from .commands import BotCommands

PREFIX = "/_synapse/retention/v1"
IGNORED_USER_LIST = "m.ignored_user_list"
MESSAGE_TYPES = frozenset({"m.room.message", "m.room.encrypted", "m.reaction", "m.sticker"})
logger = logging.getLogger(__name__)
_ACTIVE_MODULE = None
SYNAPSE_MIN_VERSION = (1, 161, 0)


def synapse_release_tuple(value: str) -> tuple[int, int, int]:
    match = re.match(r"^(\d+)\.(\d+)\.(\d+)", value)
    if not match:
        raise ValueError(f"Unsupported Synapse version string: {value}")
    return tuple(int(part) for part in match.groups())


async def get_unseen_invalidation_rooms(requester, room_ids) -> set[str]:
    """Return relevant rooms with a purge generation unseen by this Matrix device."""
    module = _ACTIVE_MODULE
    if module is None:
        return set()
    return await module.get_unseen_invalidation_rooms(requester, room_ids)


async def should_force_limited(requester, room_id: str) -> bool:
    """Return whether Sliding Sync should force a limited room timeline.

    A purge generation is consumed per Matrix device for a bounded grace window.
    This lets matrix-rust-sdk rebuild its linked chunk after server-side retention
    removed history, without keeping every retention room permanently limited.
    """
    module = _ACTIVE_MODULE
    if module is None:
        return False
    return await module.should_force_limited(requester, room_id)


def cutoff(path: Path, now: int) -> int:
    """Create a shared, complete first-install marker atomically, including across workers."""
    if path.exists():
        value = int(path.read_text().strip())
        if value <= 0:
            raise ValueError("Invalid first-install cutoff")
        return value
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, delete=False) as handle:
        temporary = Path(handle.name)
        try:
            handle.write(str(now))
            handle.flush()
            os.fsync(handle.fileno())
            try:
                os.link(temporary, path)
                directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
            except FileExistsError:
                pass
        finally:
            temporary.unlink()
    value = int(path.read_text().strip())
    if value <= 0:
        raise ValueError("Invalid first-install cutoff")
    return value


def body(request):
    try:
        raw = request.content.read(8193)
        if len(raw) > 8192:
            raise ValueError()
        result = json.loads(raw)
        if not isinstance(result, dict):
            raise ValueError()
        return result
    except (ValueError, UnicodeDecodeError):
        raise SynapseError(400, "Требуется JSON-объект размером до 8 КБ.", "BAD_JSON") from None


class RetentionModule:
    """Read-only event feed + native event creation for Synapse 1.161.0 and newer.

    The sender is a real local author/moderator. No extra room member or crypto device
    is registered. Internal handler calls follow Synapse's administrative redaction path.
    """

    @staticmethod
    def parse_config(config):
        if not isinstance(config, dict) or not isinstance(config.get("secret_file"), str):
            raise ValueError("Set retention module secret_file")
        lead = duration(config.get("redaction_lead", "5m"))
        admin = config.get("room_admin_power_level", 100)
        if type(admin) is not int or admin < 1:
            raise ValueError("Invalid room_admin_power_level")
        bot_user = config.get("command_bot_user_id")
        if bot_user is not None:
            UserID.from_string(bot_user)

        cache_reset = config.get("element_x_cache_reset", {})
        if not isinstance(cache_reset, dict):
            raise ValueError("element_x_cache_reset must be an object")
        cache_reset_enabled = cache_reset.get("enabled", False)
        if type(cache_reset_enabled) is not bool:
            raise ValueError("element_x_cache_reset.enabled must be boolean")
        cache_reset_sentinel = cache_reset.get("sentinel_user_id")
        if cache_reset_enabled:
            if not isinstance(cache_reset_sentinel, str):
                raise ValueError(
                    "element_x_cache_reset.sentinel_user_id is required when enabled"
                )
            UserID.from_string(cache_reset_sentinel)
        cache_reset_debounce_ms = duration(cache_reset.get("debounce", "10s"))
        cache_reset_min_interval_ms = duration(cache_reset.get("min_interval", "5m"))
        cache_reset_poll_ms = duration(cache_reset.get("poll_interval", "2s"))
        if not 100 <= cache_reset_debounce_ms <= 300_000:
            raise ValueError("element_x_cache_reset.debounce must be between 100ms and 5m")
        if not 100 <= cache_reset_min_interval_ms <= 3_600_000:
            raise ValueError("element_x_cache_reset.min_interval must be between 100ms and 1h")
        if not 100 <= cache_reset_poll_ms <= 60_000:
            raise ValueError("element_x_cache_reset.poll_interval must be between 100ms and 1m")

        return {
            "secret_file": config["secret_file"],
            "lead_ms": lead,
            "admin_level": admin,
            "cutoff_file": config.get("cutoff_file", "/data/retention-cutoff"),
            "bot_user": bot_user,
            "cache_reset_enabled": cache_reset_enabled,
            "cache_reset_sentinel": cache_reset_sentinel,
            "cache_reset_debounce_ms": cache_reset_debounce_ms,
            "cache_reset_min_interval_ms": cache_reset_min_interval_ms,
            "cache_reset_poll_ms": cache_reset_poll_ms,
        }

    def __init__(self, config, api):
        installed_synapse = package_version("matrix-synapse")
        if synapse_release_tuple(installed_synapse) < SYNAPSE_MIN_VERSION:
            raise ValueError("Retention module requires Synapse 1.161.0 or newer")
        self.api, self.hs = api, api._hs
        self.store = self.hs.get_datastores().main
        native = self.hs.config.retention
        if not native.retention_enabled:
            raise ValueError("Native Synapse retention.enabled must remain true")
        self.server_policy = ServerPolicy(
            native.retention_default_min_lifetime,
            native.retention_default_max_lifetime,
            native.retention_allowed_lifetime_min,
            native.retention_allowed_lifetime_max,
        )
        self.secret = Path(config["secret_file"]).read_text().strip()
        if len(self.secret) < 32:
            raise ValueError("Retention module secret must have at least 32 characters")
        self.lead_ms, self.admin_level = config["lead_ms"], config["admin_level"]
        if config["bot_user"] and not self.hs.is_mine_id(config["bot_user"]):
            raise ValueError("Command bot must be a local Matrix user")
        self.cache_reset_enabled = config["cache_reset_enabled"]
        self.cache_reset_sentinel = config["cache_reset_sentinel"]
        self.cache_reset_debounce_ms = config["cache_reset_debounce_ms"]
        self.cache_reset_min_interval_ms = config["cache_reset_min_interval_ms"]
        self.cache_reset_poll_ms = config["cache_reset_poll_ms"]
        if self.cache_reset_enabled and not self.hs.is_mine_id(self.cache_reset_sentinel):
            raise ValueError("Element X cache-reset sentinel must be a local Matrix user ID")
        self.commands = BotCommands(self, config["bot_user"])
        self._invalidation_schema_ready = False
        global _ACTIVE_MODULE
        _ACTIVE_MODULE = self
        self.since_ts = cutoff(Path(config["cutoff_file"]), self.now())
        # Only module records get this formatter; do not change Synapse's own logging.
        handler = logging.StreamHandler()
        handler.setFormatter(JsonFormatter())
        logger.handlers[:] = [handler]
        logger.propagate = False
        logger.setLevel(logging.INFO)
        resource = JsonResource(self.hs, canonical_json=False)
        for method, suffix, callback in (
            ("GET", "/internal/feed", self.feed),
            ("GET", "/internal/policy", self.policy_endpoint),
            ("POST", "/internal/redact", self.redact_endpoint),
            ("POST", "/internal/invalidate", self.invalidate_endpoint),
            ("POST", "/command", self.command),
            ("POST", "/bot/invite", self.commands.invite),
            ("POST", "/bot/info", self.commands.info),
            ("POST", "/bot/context", self.commands.context),
            ("POST", "/bot/chat", self.commands.chat),
            ("POST", "/bot/resolve", self.commands.resolve),
            ("POST", "/bot/command", self.commands.command),
        ):
            import re

            resource.register_paths(
                method, [re.compile("^" + PREFIX + suffix + "$")], callback, "retention_server"
            )
        api.register_web_resource(PREFIX, resource)
        if self.cache_reset_enabled:
            api.looping_background_call(
                self.flush_cache_resets,
                self.cache_reset_poll_ms,
                desc="retention_element_x_cache_reset",
            )

    def now(self):
        return self.hs.get_clock().time_msec()

    def authenticate(self, request):
        token = request.getHeader("Authorization") or ""
        if not hmac.compare_digest(token.encode(), ("Bearer " + self.secret).encode()):
            raise SynapseError(401, "Неверный ключ серверной службы.", "UNAUTHORIZED")

    async def effective(self, room_id: str):
        await self.store.get_room_version(room_id)
        native = await self.store.get_retention_policy_for_room(room_id)
        return self.server_policy.effective(
            {"min_lifetime": native.min_lifetime, "max_lifetime": native.max_lifetime}
        )

    async def policy_data(self, room_id: str):
        result = {"room_id": room_id, "policy_error": None}
        try:
            policy = await self.effective(room_id)
            result.update(asdict(policy))
            after = (
                None
                if policy.max_lifetime is None
                else max(policy.min_lifetime or 0, policy.max_lifetime - self.lead_ms)
            )
            if self.lead_ms and after is not None and after >= policy.max_lifetime:
                raise ValueError("No redaction window between min and max")
            result["redact_after_ms"] = after
        except ValueError:
            result["redact_after_ms"] = None
            result["policy_error"] = "INVALID_RETENTION_WINDOW"
        return result

    async def policy_endpoint(self, request):
        self.authenticate(request)
        return 200, await self.policy_data(parse_string(request, "room_id", required=True))

    async def anchor(self, event):
        relation = event.content.get("m.relates_to", {})
        if not isinstance(relation, dict) or relation.get("rel_type") not in {
            "m.replace",
            "m.annotation",
        }:
            return event.origin_server_ts
        parent_id = relation.get("event_id")
        if not isinstance(parent_id, str):
            return event.origin_server_ts
        parent = await self.store.get_event(parent_id, allow_none=True, check_room_id=event.room_id)
        return parent.origin_server_ts if parent else event.origin_server_ts

    async def feed(self, request):
        self.authenticate(request)
        after = parse_integer(request, "after", default=None)
        limit = parse_integer(request, "limit", default=1000)
        if limit < 1 or limit > 5000 or (after is not None and after < 0):
            raise SynapseError(400, "Недопустимые параметры потока.", "BAD_FEED")
        # The minimum committed multi-writer position avoids advancing past in-flight events.
        ceiling = self.store.get_room_max_token().stream
        if after is None:
            return 200, {"cursor": ceiling, "now_ms": self.now(), "events": [], "caught_up": True}
        if after > ceiling:
            raise SynapseError(409, "Позиция потока опережает Synapse.", "CURSOR_AHEAD")

        def select(txn):
            txn.execute(
                "SELECT stream_ordering,event_id FROM events WHERE stream_ordering>? "
                "AND stream_ordering<=? ORDER BY stream_ordering LIMIT ?",
                (after, ceiling, limit),
            )
            return txn.fetchall()

        rows = await self.api.run_db_interaction("retention_metadata_feed", select)
        events = await self.store.get_events([row[1] for row in rows])
        output = []
        for _, event_id in rows:
            event = events.get(event_id)
            if event is None or event.origin_server_ts < self.since_ts:
                continue
            if event.type == "m.room.retention" or (
                event.type in MESSAGE_TYPES
                and not event.is_state()
                and "redacted_because" not in event.unsigned
            ):
                output.append(
                    {
                        "event_id": event_id,
                        "room_id": event.room_id,
                        "sender": event.sender,
                        "ts": event.origin_server_ts,
                        "kind": event.type,
                        "anchor_ts": await self.anchor(event),
                    }
                )
        cursor = rows[-1][0] if len(rows) == limit else ceiling
        return 200, {
            "cursor": cursor,
            "events": output,
            "caught_up": cursor >= ceiling,
            "now_ms": self.now(),
        }

    async def local_moderator(self, room_id: str):
        state = await self.api.get_room_state(
            room_id, [("m.room.member", None), ("m.room.power_levels", ""), ("m.room.create", "")]
        )
        levels = state.get(("m.room.power_levels", ""))
        create = state.get(("m.room.create", ""))
        power = levels.content if levels else {}
        users = power.get("users", {})
        needed = max(
            power.get("redact", 50),
            power.get("events", {}).get("m.room.redaction", power.get("events_default", 0)),
        )
        creators = (
            {create.sender, *create.content.get("additional_creators", [])} if create else set()
        )
        version = await self.store.get_room_version(room_id)
        for (kind, user), member in sorted(state.items()):
            if (
                kind == "m.room.member"
                and self.hs.is_mine_id(user)
                and member.content.get("membership") == "join"
            ):
                level = users.get(user, power.get("users_default", 0))
                if version.msc4289_creator_power_enabled and user in creators:
                    level = float("inf")
                if not levels and create and user == create.sender:
                    level = 100
                if level >= needed:
                    return user
        return None

    async def redact_endpoint(self, request):
        self.authenticate(request)
        event_id = body(request).get("event_id")
        if not isinstance(event_id, str) or not event_id.startswith("$") or len(event_id) > 1024:
            raise SynapseError(400, "Требуется event_id сообщения.", "BAD_EVENT_ID")
        # Native distributed locks serialize duplicates across Synapse workers.
        async with self.hs.get_worker_locks_handler().acquire_lock("retention_redact", event_id):
            return await self.redact_event(event_id)

    async def redact_event(self, event_id: str):
        event = await self.store.get_event(event_id, allow_none=True)
        if event is None:
            return 200, {"status": "missed", "code": "EVENT_PURGED"}
        if event.is_state() or event.type not in MESSAGE_TYPES:
            return 200, {"status": "blocked", "code": "EVENT_TYPE_EXCLUDED"}
        if event.origin_server_ts < self.since_ts:
            return 200, {"status": "blocked", "code": "BEFORE_START"}
        already = event.unsigned.get("redacted_because")
        if isinstance(already, dict) and already.get("event_id"):
            return 200, {
                "status": "done",
                "redaction_id": already["event_id"],
                "code": "ALREADY_REDACTED",
            }

        # Another worker's event cache may not have received invalidation yet.
        # Check persisted redactions under the lock and verify native authorization.
        def previous(txn):
            txn.execute("SELECT event_id FROM redactions WHERE redacts=?", (event_id,))
            return [row[0] for row in txn.fetchall()]

        for previous_id in await self.api.run_db_interaction(
            "retention_previous_redaction", previous
        ):
            previous_event = await self.store.get_event(previous_id, allow_none=True)
            if previous_event and previous_event.redacts == event_id:
                return 200, {
                    "status": "done",
                    "redaction_id": previous_id,
                    "code": "ALREADY_REDACTED",
                }
        policy = await self.policy_data(event.room_id)
        if policy["policy_error"]:
            return 200, {
                "status": "deferred",
                "code": policy["policy_error"],
                "retry_at_ms": self.now() + 60_000,
            }
        if policy["redact_after_ms"] is None:
            return 200, {
                "status": "deferred",
                "code": "RETENTION_OFF",
                "retry_at_ms": self.now() + 60_000,
            }
        deadline = max(
            await self.anchor(event) + policy["redact_after_ms"],
            event.origin_server_ts + (policy["min_lifetime"] or 0),
        )
        if deadline > self.now():
            return 200, {"status": "deferred", "code": "NOT_DUE", "retry_at_ms": deadline}
        local = self.hs.is_mine_id(event.sender)
        sender = event.sender if local else await self.local_moderator(event.room_id)
        if sender is None:
            return 200, {
                "status": "deferred",
                "code": "NO_LOCAL_MODERATOR",
                "retry_at_ms": self.now() + 60_000,
            }
        requester = create_requester(sender, authenticated_entity=self.api.server_name)
        version = await self.store.get_room_version(event.room_id)
        data = {
            "type": "m.room.redaction",
            "room_id": event.room_id,
            "sender": sender,
            "content": {"reason": "Message retention policy"},
        }
        if version.updated_redaction_rules:
            data["content"]["redacts"] = event_id
        else:
            data["redacts"] = event_id
        # Same historical branch technique as Synapse's admin redaction, allowing local
        # authors' messages to be removed even after they left the room.
        kwargs = {"prev_event_ids": [event_id]} if local else {}
        if local and supports_msc4242_state_dag(event):
            kwargs["prev_state_events"] = event.prev_state_events
        try:
            (
                redaction,
                _,
            ) = await self.hs.get_event_creation_handler().create_and_send_nonmember_event(
                requester,
                data,
                ratelimit=False,
                ignore_shadow_ban=True,
                **kwargs,
            )
        except SynapseError as error:
            # Never echo upstream exception text; it may include user-controlled content.
            if error.code != 403:
                raise
            # A room can require a high power level even for self-redaction.
            # Try an existing local moderator against the current room state.
            moderator = await self.local_moderator(event.room_id) if local else None
            if moderator and moderator != sender:
                sender = moderator
                requester = create_requester(sender, authenticated_entity=self.api.server_name)
                data["sender"] = sender
                try:
                    (
                        redaction,
                        _,
                    ) = await self.hs.get_event_creation_handler().create_and_send_nonmember_event(
                        requester, data, ratelimit=False, ignore_shadow_ban=True
                    )
                except SynapseError as fallback:
                    if fallback.code != 403:
                        raise
                    return 200, {
                        "status": "deferred",
                        "code": "REDACTION_FORBIDDEN",
                        "retry_at_ms": self.now() + 60_000,
                    }
            else:
                return 200, {
                    "status": "deferred",
                    "code": "REDACTION_FORBIDDEN",
                    "retry_at_ms": self.now() + 60_000,
                }
        visible = await self.store.get_event(redaction.event_id, allow_none=True)
        if visible is None or visible.type != "m.room.redaction":
            raise SynapseError(502, "Redaction недоступна клиентам.", "REDACTION_NOT_VISIBLE")
        logger.info(
            "Создано событие удаления",
            extra={"event": "message.redacted", "room_id": event.room_id, "event_id": event_id},
        )
        return 200, {"status": "done", "redaction_id": redaction.event_id, "sender": sender}


    async def ensure_invalidation_schema(self):
        if self._invalidation_schema_ready:
            return

        def create(txn):
            txn.execute(
                """
                CREATE TABLE IF NOT EXISTS retentionbot_room_invalidations(
                    room_id TEXT PRIMARY KEY,
                    generation BIGINT NOT NULL,
                    updated_ts BIGINT NOT NULL
                )
                """
            )
            txn.execute(
                """
                CREATE TABLE IF NOT EXISTS retentionbot_event_invalidations(
                    event_id TEXT PRIMARY KEY,
                    room_id TEXT NOT NULL,
                    generation BIGINT NOT NULL
                )
                """
            )
            txn.execute(
                """
                CREATE TABLE IF NOT EXISTS retentionbot_client_invalidations(
                    user_id TEXT NOT NULL,
                    device_id TEXT NOT NULL,
                    room_id TEXT NOT NULL,
                    generation BIGINT NOT NULL,
                    force_until BIGINT NOT NULL,
                    PRIMARY KEY(user_id, device_id, room_id)
                )
                """
            )
            txn.execute(
                """
                CREATE TABLE IF NOT EXISTS retentionbot_cache_reset_events(
                    event_id TEXT PRIMARY KEY,
                    room_id TEXT NOT NULL
                )
                """
            )
            txn.execute(
                """
                CREATE TABLE IF NOT EXISTS retentionbot_cache_reset_rooms(
                    room_id TEXT PRIMARY KEY,
                    generation BIGINT NOT NULL,
                    expanded_generation BIGINT NOT NULL,
                    due_ts BIGINT NOT NULL
                )
                """
            )
            txn.execute(
                """
                CREATE TABLE IF NOT EXISTS retentionbot_cache_reset_users(
                    user_id TEXT PRIMARY KEY,
                    generation BIGINT NOT NULL,
                    reset_generation BIGINT NOT NULL,
                    due_ts BIGINT NOT NULL,
                    last_reset_ts BIGINT NOT NULL
                )
                """
            )

        await self.api.run_db_interaction("retention_invalidation_schema", create)
        self._invalidation_schema_ready = True

    async def queue_cache_reset(self, event_id: str, room_id: str) -> bool:
        """Queue one debounced Element X EventCache reset for this room.

        Event invalidations are frequent, so the request path only dirties one room
        row. The background task expands due rooms to affected local users once per
        debounce window, then performs at most one account-data pulse per user.
        """
        if not self.cache_reset_enabled:
            return False
        await self.ensure_invalidation_schema()
        due = self.now() + self.cache_reset_debounce_ms

        def queue(txn):
            txn.execute(
                "SELECT room_id FROM retentionbot_cache_reset_events WHERE event_id=?",
                (event_id,),
            )
            previous = txn.fetchone()
            if previous:
                if previous[0] != room_id:
                    raise ValueError("Cache-reset room mismatch")
                return False

            txn.execute(
                "SELECT generation,expanded_generation,due_ts "
                "FROM retentionbot_cache_reset_rooms WHERE room_id=?",
                (room_id,),
            )
            row = txn.fetchone()
            if row:
                generation = row[0] + 1
                pending = row[0] > row[1]
                due_ts = row[2] if pending and row[2] > 0 else due
                txn.execute(
                    "UPDATE retentionbot_cache_reset_rooms "
                    "SET generation=?,due_ts=? WHERE room_id=?",
                    (generation, due_ts, room_id),
                )
            else:
                txn.execute(
                    "INSERT INTO retentionbot_cache_reset_rooms"
                    "(room_id,generation,expanded_generation,due_ts) VALUES(?,?,?,?)",
                    (room_id, 1, 0, due),
                )

            txn.execute(
                "INSERT INTO retentionbot_cache_reset_events(event_id,room_id) VALUES(?,?)",
                (event_id, room_id),
            )
            return True

        return await self.api.run_db_interaction("retention_cache_reset_queue", queue)

    async def _pulse_cache_reset(self, user_id: str) -> None:
        content = await self.api.account_data_manager.get_global(user_id, IGNORED_USER_LIST)
        updated = toggle_ignored_user_sentinel(content, self.cache_reset_sentinel)
        await self.api.account_data_manager.put_global(user_id, IGNORED_USER_LIST, updated)

    async def _expand_due_cache_reset_rooms(self, now: int) -> int:
        sentinel = self.cache_reset_sentinel

        def expand(txn):
            txn.execute(
                "SELECT room_id,generation FROM retentionbot_cache_reset_rooms "
                "WHERE generation>expanded_generation AND due_ts>0 AND due_ts<=? "
                "ORDER BY due_ts LIMIT 100",
                (now,),
            )
            rooms = txn.fetchall()
            affected_users = 0
            for room_id, target_generation in rooms:
                txn.execute(
                    "SELECT user_id FROM local_current_membership WHERE room_id=?",
                    (room_id,),
                )
                users = sorted({row[0] for row in txn.fetchall() if row[0] != sentinel})
                for user_id in users:
                    txn.execute(
                        "SELECT generation,reset_generation,due_ts,last_reset_ts "
                        "FROM retentionbot_cache_reset_users WHERE user_id=?",
                        (user_id,),
                    )
                    row = txn.fetchone()
                    if row:
                        generation = row[0] + 1
                        pending = row[0] > row[1]
                        due_ts = (
                            row[2]
                            if pending and row[2] > 0
                            else max(now, row[3] + self.cache_reset_min_interval_ms)
                        )
                        txn.execute(
                            "UPDATE retentionbot_cache_reset_users "
                            "SET generation=?,due_ts=? WHERE user_id=?",
                            (generation, due_ts, user_id),
                        )
                    else:
                        txn.execute(
                            "INSERT INTO retentionbot_cache_reset_users"
                            "(user_id,generation,reset_generation,due_ts,last_reset_ts) "
                            "VALUES(?,?,?,?,?)",
                            (user_id, 1, 0, now, 0),
                        )
                    affected_users += 1

                txn.execute(
                    "UPDATE retentionbot_cache_reset_rooms "
                    "SET expanded_generation=?,due_ts=0 "
                    "WHERE room_id=? AND generation=?",
                    (target_generation, room_id, target_generation),
                )

            return affected_users

        return await self.api.run_db_interaction(
            "retention_cache_reset_expand_rooms", expand
        )

    async def flush_cache_resets(self) -> None:
        if not self.cache_reset_enabled:
            return
        await self.ensure_invalidation_schema()

        # A registered sentinel would turn the workaround into a real ignore rule.
        # Fail closed and leave dirty generations queued until configuration is fixed.
        if await self.api.get_userinfo_by_id(self.cache_reset_sentinel) is not None:
            logger.error(
                "Cache-reset sentinel зарегистрирован; workaround остановлен",
                extra={
                    "event": "client_cache.sentinel_registered",
                    "user_id": self.cache_reset_sentinel,
                },
            )
            return

        # Only Synapse's designated background-task process drains this shared queue.
        # Keep the distributed lock as a second guard for topology/config changes.
        async with self.hs.get_worker_locks_handler().acquire_lock(
            "retention_cache_reset_flush", "global"
        ):
            await self._expand_due_cache_reset_rooms(self.now())
            now = self.now()

            def due_users(txn):
                txn.execute(
                    "SELECT user_id FROM retentionbot_cache_reset_users "
                    "WHERE generation>reset_generation AND due_ts>0 AND due_ts<=? "
                    "ORDER BY due_ts LIMIT 100",
                    (now,),
                )
                return [row[0] for row in txn.fetchall()]

            users = await self.api.run_db_interaction(
                "retention_cache_reset_due_users", due_users
            )
            for user_id in users:
                now = self.now()

                def current(txn, target_user_id=user_id):
                    txn.execute(
                        "SELECT generation,reset_generation,due_ts "
                        "FROM retentionbot_cache_reset_users WHERE user_id=?",
                        (target_user_id,),
                    )
                    return txn.fetchone()

                row = await self.api.run_db_interaction(
                    "retention_cache_reset_current", current
                )
                if not row or row[0] <= row[1] or row[2] <= 0 or row[2] > now:
                    continue
                target_generation = row[0]
                try:
                    await self._pulse_cache_reset(user_id)
                except Exception:
                    retry_at = self.now() + 60_000

                    def postpone(
                        txn,
                        target_user_id=user_id,
                        target_retry_at=retry_at,
                    ):
                        txn.execute(
                            "UPDATE retentionbot_cache_reset_users SET due_ts=? "
                            "WHERE user_id=? AND generation>reset_generation",
                            (target_retry_at, target_user_id),
                        )

                    await self.api.run_db_interaction(
                        "retention_cache_reset_postpone", postpone
                    )
                    logger.exception(
                        "Не удалось сбросить клиентский EventCache",
                        extra={"event": "client_cache.reset_failed", "user_id": user_id},
                    )
                    continue

                completed = self.now()

                def acknowledge(
                    txn,
                    target_user_id=user_id,
                    acknowledged_generation=target_generation,
                    completed_ts=completed,
                ):
                    txn.execute(
                        "SELECT generation,reset_generation "
                        "FROM retentionbot_cache_reset_users WHERE user_id=?",
                        (target_user_id,),
                    )
                    latest = txn.fetchone()
                    if not latest:
                        return
                    generation, reset_generation = latest
                    reset_generation = max(reset_generation, acknowledged_generation)
                    due_ts = (
                        completed_ts + self.cache_reset_debounce_ms
                        if generation > reset_generation
                        else 0
                    )
                    txn.execute(
                        "UPDATE retentionbot_cache_reset_users "
                        "SET reset_generation=?,due_ts=?,last_reset_ts=? WHERE user_id=?",
                        (reset_generation, due_ts, completed_ts, target_user_id),
                    )

                await self.api.run_db_interaction(
                    "retention_cache_reset_acknowledge", acknowledge
                )
                logger.info(
                    "Сброшен persistent EventCache Element X",
                    extra={
                        "event": "client_cache.reset",
                        "user_id": user_id,
                        "generation": target_generation,
                    },
                )

    async def invalidate_endpoint(self, request):
        self.authenticate(request)
        data = body(request)
        event_id = data.get("event_id")
        room_id = data.get("room_id")
        if (
            not isinstance(event_id, str)
            or not event_id.startswith("$")
            or len(event_id) > 1024
            or not isinstance(room_id, str)
            or not room_id.startswith("!")
            or len(room_id) > 1024
        ):
            raise SynapseError(400, "Требуются event_id и room_id.", "BAD_INVALIDATION")

        # The invalidation must happen only after native retention has physically
        # removed the target. Query the events table directly: Synapse's event cache can
        # retain a previously loaded event briefly after the purge transaction.
        def still_persisted(txn):
            txn.execute("SELECT 1 FROM events WHERE event_id=?", (event_id,))
            return txn.fetchone() is not None

        if await self.api.run_db_interaction("retention_invalidation_purge_check", still_persisted):
            return 200, {
                "status": "deferred",
                "code": "EVENT_NOT_PURGED",
                "retry_at_ms": self.now() + 10_000,
            }

        await self.ensure_invalidation_schema()

        def bump(txn):
            txn.execute(
                "SELECT room_id,generation FROM retentionbot_event_invalidations "
                "WHERE event_id=?",
                (event_id,),
            )
            previous = txn.fetchone()
            if previous:
                if previous[0] != room_id:
                    raise ValueError("Invalidation room mismatch")
                return previous[1], False

            txn.execute(
                "SELECT generation FROM retentionbot_room_invalidations WHERE room_id=?",
                (room_id,),
            )
            row = txn.fetchone()
            generation = (row[0] if row else 0) + 1
            if row:
                txn.execute(
                    "UPDATE retentionbot_room_invalidations "
                    "SET generation=?,updated_ts=? WHERE room_id=?",
                    (generation, self.now(), room_id),
                )
            else:
                txn.execute(
                    "INSERT INTO retentionbot_room_invalidations(room_id,generation,updated_ts) "
                    "VALUES(?,?,?)",
                    (room_id, generation, self.now()),
                )
            txn.execute(
                "INSERT INTO retentionbot_event_invalidations(event_id,room_id,generation) "
                "VALUES(?,?,?)",
                (event_id, room_id, generation),
            )
            return generation, True

        generation, created = await self.api.run_db_interaction(
            "retention_invalidation_bump", bump
        )
        if created:
            logger.info(
                "Timeline комнаты помечен для повторной синхронизации",
                extra={"event": "timeline.invalidated", "room_id": room_id, "event_id": event_id},
            )
            # The retention generation is outside Synapse's normal Matrix streams.
            # Keep waking Sliding Sync for the legacy limited-timeline fallback.
            self.hs.get_notifier().on_new_event(
                StreamKeyType.ROOM,
                self.store.get_room_max_token(),
                rooms=[room_id],
            )

        cache_reset_queued = await self.queue_cache_reset(event_id, room_id)
        return 200, {
            "status": "done",
            "generation": generation,
            "cache_reset_queued": cache_reset_queued,
        }

    @staticmethod
    def _requester_device_id(requester) -> str:
        if requester.device_id:
            return requester.device_id
        token_id = getattr(requester, "access_token_id", None)
        return f"token:{token_id}" if token_id is not None else "unknown"

    async def get_unseen_invalidation_rooms(self, requester, room_ids) -> set[str]:
        if self.cache_reset_enabled:
            return set()
        await self.ensure_invalidation_schema()
        relevant = set(room_ids)
        if not relevant:
            return set()
        user_id = requester.user.to_string()
        device_id = self._requester_device_id(requester)

        def select(txn):
            txn.execute(
                """
                SELECT r.room_id
                FROM retentionbot_room_invalidations AS r
                LEFT JOIN retentionbot_client_invalidations AS c
                  ON c.room_id=r.room_id
                 AND c.user_id=?
                 AND c.device_id=?
                WHERE c.generation IS NULL OR c.generation < r.generation
                """,
                (user_id, device_id),
            )
            return [row[0] for row in txn.fetchall()]

        unseen = await self.api.run_db_interaction(
            "retention_invalidation_unseen_rooms", select
        )
        return {room_id for room_id in unseen if room_id in relevant}

    async def should_force_limited(self, requester, room_id: str) -> bool:
        if self.cache_reset_enabled:
            return False
        await self.ensure_invalidation_schema()
        user_id = requester.user.to_string()
        device_id = self._requester_device_id(requester)
        now = self.now()
        grace_ms = 300_000

        def consume(txn):
            txn.execute(
                "SELECT generation FROM retentionbot_room_invalidations WHERE room_id=?",
                (room_id,),
            )
            room = txn.fetchone()
            if not room:
                return False
            generation = room[0]
            txn.execute(
                "SELECT generation,force_until FROM retentionbot_client_invalidations "
                "WHERE user_id=? AND device_id=? AND room_id=?",
                (user_id, device_id, room_id),
            )
            seen = txn.fetchone()
            if not seen or seen[0] < generation:
                force_until = now + grace_ms
                if seen:
                    txn.execute(
                        "UPDATE retentionbot_client_invalidations "
                        "SET generation=?,force_until=? "
                        "WHERE user_id=? AND device_id=? AND room_id=?",
                        (generation, force_until, user_id, device_id, room_id),
                    )
                else:
                    txn.execute(
                        "INSERT INTO retentionbot_client_invalidations"
                        "(user_id,device_id,room_id,generation,force_until) "
                        "VALUES(?,?,?,?,?)",
                        (user_id, device_id, room_id, generation, force_until),
                    )
                return True
            return seen[1] > now

        return await self.api.run_db_interaction("retention_invalidation_consume", consume)

    async def command(self, request):
        requester = await self.api.get_user_by_req(request)
        return await self.execute_command(requester.user.to_string(), body(request))

    async def authorized_state(self, user, room, *, admin=False):
        state = await self.api.get_room_state(
            room, [("m.room.member", user), ("m.room.power_levels", ""), ("m.room.create", "")]
        )
        member = state.get(("m.room.member", user))
        if not member or member.content.get("membership") != "join":
            raise SynapseError(403, "Команда доступна участнику комнаты.", "NOT_JOINED")
        if admin:
            levels = state.get(("m.room.power_levels", ""))
            power = levels.content if levels else {}
            create = state.get(("m.room.create", ""))
            level = power.get("users", {}).get(user, power.get("users_default", 0))
            version = await self.store.get_room_version(room)
            if create and user in {create.sender, *create.content.get("additional_creators", [])}:
                if version.msc4289_creator_power_enabled:
                    level = float("inf")
                elif not levels:
                    level = 100
            if level < self.admin_level:
                raise SynapseError(403, "Настройку меняет администратор комнаты.", "NOT_ROOM_ADMIN")
        return state

    async def execute_command(self, user, data, *, trigger=None):
        if data.get("command") != "retention" or data.get("action") not in (
            "help",
            "status",
            "set",
            "off",
        ):
            raise SynapseError(400, "Допустимые действия: help, status, set, off.", "BAD_COMMAND")
        room = data.get("room_id")
        if not isinstance(room, str) or not room.startswith("!"):
            raise SynapseError(400, "Укажите room_id.", "BAD_ROOM_ID")
        action = data["action"]
        await self.authorized_state(user, room, admin=action in {"set", "off"})
        replay = False
        if action in {"set", "off"}:
            try:
                if action == "off":
                    if self.server_policy.default_max is not None:
                        raise ValueError("Серверный срок хранения действует для этой комнаты.")
                    content = {}
                else:
                    if trigger and "min_lifetime" not in data:
                        existing = await self.effective(room)
                        if existing.min_lifetime is not None:
                            data = {**data, "min_lifetime": existing.min_lifetime}
                    content = self.server_policy.requested(json.dumps(data))
                    maximum = content["max_lifetime"]
                    if (
                        self.lead_ms
                        and max(content.get("min_lifetime", 0), maximum - self.lead_ms) >= maximum
                    ):
                        raise ValueError("Нужен запас между min_lifetime и max_lifetime.")
            except (KeyError, ValueError) as error:
                raise SynapseError(400, str(error), "BAD_POLICY") from None
            replay = trigger and await self.commands.previously_applied(room, user, trigger)
            if not replay:
                if trigger:
                    content["org.retentionbot.command_event_id"] = trigger
                await self.api.create_and_send_event_into_room(
                    {
                        "type": "m.room.retention",
                        "state_key": "",
                        "room_id": room,
                        "sender": user,
                        "content": content,
                    }
                )
                logger.info(
                    "Изменён срок хранения комнаты",
                    extra={"event": "policy.changed", "room_id": room},
                )
        result = await self.policy_data(room)
        result.update(
            ok=True,
            message="Команда уже выполнена. Показана текущая политика."
            if replay
            else "Настройка сохранена."
            if action in {"set", "off"}
            else "Текущая политика хранения.",
            bot_membership_required=False,
            encrypted_chat_commands=bool(self.commands.bot_user),
            message_linked_media_cleanup=False,
        )
        if action == "help":
            result["actions"] = ["set", "status", "off", "help"]
        return 200, result
