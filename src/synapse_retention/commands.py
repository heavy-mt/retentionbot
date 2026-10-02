"""Delegated command API: the actor comes from a stored personal-chat event."""

from synapse.api.errors import SynapseError
from synapse.types import RoomAlias

from retentionbot.room_reference import reference


class BotCommands:
    def __init__(self, module, bot_user):
        self.module, self.bot_user = module, bot_user

    async def authenticate(self, request):
        user = (await self.module.api.get_user_by_req(request)).user.to_string()
        if not self.bot_user or user != self.bot_user:
            raise SynapseError(
                403, "Этот API доступен настроенному боту команд.", "NOT_COMMAND_BOT"
            )

    async def info(self, request):
        await self.authenticate(request)
        return 200, {"ok": True, "bot_user_id": self.bot_user}

    async def personal(self, room_id, invited=False):
        if not isinstance(room_id, str) or not room_id.startswith("!"):
            raise SynapseError(400, "Укажите личный чат с ботом.", "BAD_DM")
        state = await self.module.api.get_room_state(
            room_id, [("m.room.member", None), ("m.room.join_rules", "")]
        )
        active = {
            user: event.content.get("membership")
            for (kind, user), event in state.items()
            if kind == "m.room.member" and event.content.get("membership") in {"join", "invite"}
        }
        rule = state.get(("m.room.join_rules", ""))
        if (
            len(active) != 2
            or active.get(self.bot_user) != ("invite" if invited else "join")
            or (rule and rule.content.get("join_rule") == "public")
        ):
            raise SynapseError(403, "Напишите боту в отдельный личный чат.", "NOT_PERSONAL_CHAT")
        actor = next(user for user in active if user != self.bot_user)
        if active[actor] != "join" or not self.module.hs.is_mine_id(actor):
            raise SynapseError(
                403, "Нужен личный чат с пользователем этого сервера.", "BAD_DM_ACTOR"
            )
        return actor

    async def actor(self, data):
        actor = await self.personal(data.get("dm_room_id"))
        key = data.get("event_id")
        if not isinstance(key, str) or not key.startswith("$") or len(key) > 1024:
            raise SynapseError(400, "Укажите событие команды.", "BAD_EVENT_ID")
        event = await self.module.store.get_event(key, allow_none=True)
        if (
            event is None
            or event.room_id != data["dm_room_id"]
            or event.sender != actor
            or event.is_state()
            or event.type not in {"m.room.message", "m.room.encrypted"}
            or "redacted_because" in event.unsigned
        ):
            raise SynapseError(
                403, "Событие не является командой отправителя в личном чате.", "BAD_COMMAND_EVENT"
            )
        return actor

    async def invite(self, request):
        await self.authenticate(request)
        from .module import body

        await self.personal(body(request).get("dm_room_id"), invited=True)
        return 200, {"ok": True}

    async def context(self, request):
        await self.authenticate(request)
        from .module import body

        return 200, {"actor": await self.actor(body(request))}

    async def chat(self, request):
        await self.authenticate(request)
        from .module import body

        return 200, {"actor": await self.personal(body(request).get("dm_room_id"))}

    async def describe(self, room_id, actor):
        await self.module.authorized_state(actor, room_id, admin=True)
        state = await self.module.api.get_room_state(
            room_id, [("m.room.name", ""), ("m.room.canonical_alias", ""), ("m.room.member", None)]
        )
        name_event = state.get(("m.room.name", ""))
        alias_event = state.get(("m.room.canonical_alias", ""))
        name = name_event.content.get("name") if name_event else None
        alias = alias_event.content.get("alias") if alias_event else None
        name = name if isinstance(name, str) else None
        alias = alias if isinstance(alias, str) else None
        participants = [
            event.content["displayname"]
            if isinstance(event.content.get("displayname"), str) and event.content["displayname"]
            else user.split(":", 1)[0][1:]
            for (kind, user), event in state.items()
            if kind == "m.room.member"
            and user != actor
            and event.content.get("membership") in {"join", "invite"}
        ]
        label = name or alias or ", ".join(sorted(participants)[:5]) or room_id
        return {"room_id": room_id, "name": label[:200], "alias": alias}

    async def resolve(self, request):
        await self.authenticate(request)
        from .module import body

        data = body(request)
        actor = await self.actor(data)
        try:
            kind, value = reference(data.get("query"))
        except ValueError as error:
            raise SynapseError(400, str(error), "BAD_ROOM_REFERENCE") from None
        if kind == "alias":
            association = await self.module.hs.get_directory_handler().get_association(
                RoomAlias.from_string(value)
            )
            value = association["room_id"]
            kind = "id"
        if kind == "id":
            return 200, {"rooms": [await self.describe(value, actor)], "truncated": False}
        matches = []
        for room in sorted(await self.module.store.get_rooms_for_user(actor)):
            if room == data["dm_room_id"]:
                continue
            try:
                candidate = await self.describe(room, actor)
            except SynapseError as error:
                if error.code in {403, 404}:
                    continue
                raise
            if candidate["name"].casefold() == value.casefold():
                matches.append(candidate)
                if len(matches) > 20:
                    break
        return 200, {"rooms": matches[:20], "truncated": len(matches) > 20}

    async def command(self, request):
        await self.authenticate(request)
        from .module import body

        data = body(request)
        actor = await self.actor(data)
        command = data.get("command")
        if not isinstance(command, dict):
            raise SynapseError(400, "Требуется JSON команды.", "BAD_COMMAND")
        async with self.module.hs.get_worker_locks_handler().acquire_lock(
            "retention_command", data["event_id"]
        ):
            return await self.module.execute_command(actor, command, trigger=data["event_id"])

    async def previously_applied(self, room, actor, trigger):
        # The audit tag persists in ordinary Matrix state events. No private Synapse
        # tables or room memberships are created for command bookkeeping.
        escaped = trigger.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")

        def select(txn):
            txn.execute(
                "SELECT e.event_id FROM events e JOIN event_json j ON j.event_id=e.event_id "
                "WHERE e.room_id=? AND e.sender=? AND e.type='m.room.retention' "
                "AND j.json LIKE ? ESCAPE '\\'",
                (room, actor, "%" + escaped + "%"),
            )
            return [row[0] for row in txn.fetchall()]

        for key in await self.module.api.run_db_interaction("retention_command_replay", select):
            event = await self.module.store.get_event(key, allow_none=True)
            if event and event.content.get("org.retentionbot.command_event_id") == trigger:
                return True
        return False
