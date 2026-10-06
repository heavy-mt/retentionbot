#!/usr/local/bin/python
"""Patch Synapse Sliding Sync for retention invalidation.

The retention module records room purge generations. A room with an unseen
generation must be forced into the Sliding Sync response and serialized with
limited=true so matrix-rust-sdk rebuilds its linked chunk after native retention
has physically purged events.
"""

from pathlib import Path

import synapse.handlers.sliding_sync
import synapse.rest.client.sync

rest_path = Path(synapse.rest.client.sync.__file__)
handler_path = Path(synapse.handlers.sliding_sync.__file__)

rest_text = rest_path.read_text()
handler_text = handler_path.read_text()

# 1. Serializer hook: force limited=true for an unseen retention generation.
rest_import = "from synapse_retention.module import should_force_limited\n"
rest_import_marker = "from synapse.types import (\n"
rest_old = (
    '            if room_result.limited is not None:\n'
    '                serialized_rooms[room_id]["limited"] = room_result.limited\n'
)
rest_new = (
    '            force_limited = await should_force_limited(requester, room_id)\n'
    '            # Preserve Synapse omission semantics unless retention invalidation\n'
    '            # explicitly needs to force a limited timeline for this device.\n'
    '            if room_result.limited is not None or force_limited:\n'
    '                serialized_rooms[room_id]["limited"] = (\n'
    '                    bool(room_result.limited) or force_limited\n'
    '                )\n'
)

if rest_import not in rest_text:
    if rest_import_marker not in rest_text:
        raise SystemExit(f"Synapse REST import marker not found in {rest_path}")
    rest_text = rest_text.replace(rest_import_marker, rest_import + rest_import_marker, 1)

if rest_new not in rest_text:
    if rest_old not in rest_text:
        raise SystemExit(f"Synapse limited serializer marker not found in {rest_path}")
    rest_text = rest_text.replace(rest_old, rest_new, 1)

# 2. Handler hook: an unseen generation makes an otherwise-clean room dirty for
# this device, so get_room_sync_data() runs even without a Matrix stream event.
handler_import = "from synapse_retention.module import get_unseen_invalidation_rooms\n"
handler_import_marker = "from synapse.api.constants import "
handler_old = (
    "        relevant_rooms_to_send_map = interested_rooms.relevant_rooms_to_send_map\n"
)
handler_new = (
    "        relevant_rooms_to_send_map = dict(interested_rooms.relevant_rooms_to_send_map)\n"
    "        force_retention_rooms = await get_unseen_invalidation_rooms(\n"
    "            sync_config.requester, relevant_room_map.keys()\n"
    "        )\n"
    "        for room_id in force_retention_rooms:\n"
    "            relevant_rooms_to_send_map[room_id] = relevant_room_map[room_id]\n"
)
filter_old = (
    "            if room_sync_result or not from_token:\n"
    "                rooms[room_id] = room_sync_result\n"
)
filter_new = (
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

if filter_new not in handler_text:
    if filter_old not in handler_text:
        raise SystemExit(f"Synapse room-result filter marker not found in {handler_path}")
    handler_text = handler_text.replace(filter_old, filter_new, 1)

rest_path.write_text(rest_text)
handler_path.write_text(handler_text)
print(f"patched {rest_path}")
print(f"patched {handler_path}")
