"""Coordinated deletion of completed per-event Synapse idempotency records."""

METADATA_WINDOW_MS = 7 * 86_400_000


def validate_receipts(receipts, now: int):
    if not isinstance(receipts, list) or not 1 <= len(receipts) <= 1000:
        raise ValueError("Expected 1..1000 completion receipts")
    for item in receipts:
        if not isinstance(item, dict):
            raise ValueError("Invalid receipt")
        for key, prefix in (("event_id", "$"), ("room_id", "!")):
            value = item.get(key)
            if not isinstance(value, str) or not value.startswith(prefix) or len(value) > 1024:
                raise ValueError("Invalid receipt identifier")
        generation, completed = item.get("generation"), item.get("completed_at")
        if type(generation) is not int or generation < 1:
            raise ValueError("Invalid receipt generation")
        if type(completed) is not int or not 0 <= completed < now - METADATA_WINDOW_MS:
            raise ValueError("Completion receipt is too recent")


def compact_receipts_txn(txn, receipts: list[dict], cache_reset_enabled: bool) -> bool:
    # Validate the entire batch before deleting anything. A lost response is
    # repaired by replay: absent rows are already acknowledged, not a new bump.
    for item in receipts:
        txn.execute(
            "SELECT room_id,generation FROM retentionbot_event_invalidations WHERE event_id=?",
            (item["event_id"],),
        )
        row = txn.fetchone()
        if row is None:
            continue
        if row[0] != item["room_id"] or row[1] != item["generation"]:
            return False
        txn.execute("SELECT 1 FROM events WHERE event_id=?", (item["event_id"],))
        if txn.fetchone() is not None:
            return False
        if cache_reset_enabled:
            txn.execute(
                "SELECT generation FROM retentionbot_cache_reset_rooms WHERE room_id=?",
                (item["room_id"],),
            )
            queued = txn.fetchone()
            if queued is None or queued[0] < item["generation"]:
                return False
    for item in receipts:
        txn.execute(
            "DELETE FROM retentionbot_event_invalidations "
            "WHERE event_id=? AND room_id=? AND generation=?",
            (item["event_id"], item["room_id"], item["generation"]),
        )
    return True
