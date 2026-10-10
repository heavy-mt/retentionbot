# Metadata compaction

The observer compacts terminal event metadata in bounded batches on each scheduling pass. The retention window is seven days. Both `done` and `missed` are eligible: `missed` with `EVENT_PURGED` means Synapse had already physically purged the original before redaction.

An event is deleted only when its completion timestamp is older than the cutoff and every associated invalidation is `done`, has a completion timestamp, and is also older than the cutoff. Pending invalidations, unknown invalidation completion times, pending events, and blocked events are preserved. Legacy completed events without invalidation records remain eligible. The foreign key cascades deletion to the associated `server.invalidations` row. Room and observer cursor metadata are retained.

This change does not clean Synapse's `retentionbot_event_invalidations` table. Those rows prevent repeated invalidation requests from advancing the room generation twice. Their lifecycle needs a separate design covering broker redelivery and observer recovery; do not truncate this table as routine maintenance.

Synapse's `stream_ordering_to_exterm` is a separate historical DAG cache. Fixing DAG amplification does not eliminate historical snapshots or change the native monthly cleanup window. Do not infer that a historical snapshot is dispensable merely because its original event has been purged.

Compaction makes freed database pages reusable; it does not guarantee that PostgreSQL relation files immediately shrink on disk.
