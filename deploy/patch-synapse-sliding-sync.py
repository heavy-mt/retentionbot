#!/usr/local/bin/python
"""Patch the pinned Synapse Sliding Sync serializer for retention invalidation.

The retention module records a room purge generation. matrix-rust-sdk needs a
limited timeline response after that generation changes so its linked chunk can
be rebuilt without events that native Synapse retention has physically purged.
"""

from pathlib import Path

import synapse.rest.client.sync

path = Path(synapse.rest.client.sync.__file__)
text = path.read_text()

import_line = (
    "from synapse_retention.module import (\n"
    "    get_unseen_invalidation_rooms,\n"
    "    should_force_limited,\n"
    ")\n"
)
import_marker = "from synapse.types import (\n"
old = (
    '            if room_result.limited is not None:\n'
    '                serialized_rooms[room_id]["limited"] = room_result.limited\n'
)
new = (
    '            force_limited = await should_force_limited(requester, room_id)\n'
    '            # Preserve Synapse omission semantics unless retention invalidation\n'
    '            # explicitly needs to force a limited timeline for this device.\n'
    '            if room_result.limited is not None or force_limited:\n'
    '                serialized_rooms[room_id]["limited"] = (\n'
    '                    bool(room_result.limited) or force_limited\n'
    '                )\n'
)

if import_line not in text:
    legacy_import = "from synapse_retention.module import should_force_limited\n"
    if legacy_import in text:
        text = text.replace(legacy_import, import_line, 1)
    else:
        if import_marker not in text:
            raise SystemExit(f"Synapse import marker not found in {path}")
        text = text.replace(import_marker, import_line + import_marker, 1)

dirty_old = (
    "        relevant_rooms_to_send_map = interested_rooms.relevant_rooms_to_send_map\n"
)
dirty_new = (
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

if dirty_new not in text:
    if dirty_old not in text:
        raise SystemExit(f"Synapse relevant-room marker not found in {path}")
    text = text.replace(dirty_old, dirty_new, 1)

if filter_new not in text:
    if filter_old not in text:
        raise SystemExit(f"Synapse room-result filter marker not found in {path}")
    text = text.replace(filter_old, filter_new, 1)

if new not in text:
    if old not in text:
        raise SystemExit(f"Synapse limited serializer marker not found in {path}")
    text = text.replace(old, new, 1)

path.write_text(text)
print(f"patched {path}")
