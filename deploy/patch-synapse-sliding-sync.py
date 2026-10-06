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

import_line = "from synapse_retention.module import should_force_limited\n"
import_marker = "from synapse.types import (\n"
old = '                serialized_rooms[room_id]["limited"] = room_result.limited\n'
new = (
    '                serialized_rooms[room_id]["limited"] = (\n'
    '                    room_result.limited\n'
    '                    or await should_force_limited(requester, room_id)\n'
    '                )\n'
)

if import_line not in text:
    if import_marker not in text:
        raise SystemExit(f"Synapse import marker not found in {path}")
    text = text.replace(import_marker, import_line + import_marker, 1)

if new not in text:
    if old not in text:
        raise SystemExit(f"Synapse limited serializer marker not found in {path}")
    text = text.replace(old, new, 1)

path.write_text(text)
print(f"patched {path}")
