#!/usr/local/bin/python
"""Patch Synapse Sliding Sync for retention invalidation.

Retention generations are consumed only for explicit room subscriptions. Rooms
that merely appear in a Sliding Sync list (including background/list sync) must
not consume an invalidation, otherwise Element X can keep stale persistent
timeline chunks and never receive a reset when the room is opened later.
"""

from pathlib import Path

import synapse.handlers.sliding_sync

handler_path = Path(synapse.handlers.sliding_sync.__file__)
handler_text = handler_path.read_text()

handler_import = (
    "from synapse_retention.module import (\n"
    "    get_unseen_invalidation_rooms,\n"
    "    should_force_limited,\n"
    ")\n"
)
handler_import_marker = "from synapse.api.constants import "

handler_old = (
    "        relevant_rooms_to_send_map = interested_rooms.relevant_rooms_to_send_map\n"
)
handler_new = (
    "        relevant_rooms_to_send_map = dict(interested_rooms.relevant_rooms_to_send_map)\n"
    "        retention_subscription_rooms = set(\n"
    "            (sync_config.room_subscriptions or {}).keys()\n"
    "        )\n"
    "        force_retention_rooms = await get_unseen_invalidation_rooms(\n"
    "            sync_config.requester, retention_subscription_rooms\n"
    "        )\n"
    "        for room_id in force_retention_rooms:\n"
    "            room_sync_config = relevant_room_map.get(room_id)\n"
    "            if room_sync_config is not None and room_sync_config.timeline_limit > 0:\n"
    "                relevant_rooms_to_send_map[room_id] = room_sync_config\n"
)

room_result_old = (
    "            # Filter out empty room results during incremental sync\n"
    "            if room_sync_result or not from_token:\n"
    "                rooms[room_id] = room_sync_result\n"
)
room_result_new = (
    "            # Retention invalidation is consumed only by an explicit room\n"
    "            # subscription with a real timeline boundary. A list/background\n"
    "            # sync must leave the generation unseen for the later room open.\n"
    "            if room_id in force_retention_rooms and room_sync_result.prev_batch is not None:\n"
    "                if await should_force_limited(sync_config.requester, room_id):\n"
    "                    # matrix-rust-sdk skips the limited reset when every returned\n"
    "                    # event is already cached (all-duplicates fast path). Force an\n"
    "                    # empty gappy timeline instead: limited=true + prev_batch with\n"
    "                    # no events makes EventCache shrink the stale linked chunk.\n"
    "                    room_sync_result = attr.evolve(\n"
    "                        room_sync_result,\n"
    "                        timeline_events=[],\n"
    "                        bundled_aggregations=None,\n"
    "                        limited=True,\n"
    "                        num_live=0,\n"
    "                    )\n"
    "\n"
    "            # Filter out empty room results during incremental sync. Forced\n"
    "            # retention rooms must still be serialized so limited=true reaches\n"
    "            # matrix-rust-sdk and rebuilds its persistent linked chunk.\n"
    "            if room_sync_result or not from_token or room_id in force_retention_rooms:\n"
    "                rooms[room_id] = room_sync_result\n"
)

if handler_import not in handler_text:
    if handler_import_marker not in handler_text:
        raise SystemExit(f"Synapse handler import marker not found in {handler_path}")
    handler_text = handler_text.replace(
        handler_import_marker, handler_import + handler_import_marker, 1
    )

if handler_new not in handler_text:
    if handler_old not in handler_text:
        raise SystemExit(f"Synapse relevant-room marker not found in {handler_path}")
    handler_text = handler_text.replace(handler_old, handler_new, 1)

if room_result_new not in handler_text:
    if room_result_old not in handler_text:
        raise SystemExit(f"Synapse room-result marker not found in {handler_path}")
    handler_text = handler_text.replace(room_result_old, room_result_new, 1)

handler_path.write_text(handler_text)
print(f"patched {handler_path}")
