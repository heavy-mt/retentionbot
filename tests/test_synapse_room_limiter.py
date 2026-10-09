"""Regression tests for the send critical section, also run in both Synapse images."""

from __future__ import annotations

import ast
import asyncio
import importlib.util
import os
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

PATCH_PATH = Path(os.environ.get(
    "RETENTION_ROOM_LIMITER_PATCH",
    str(Path(__file__).resolve().parents[1] / "deploy/patch-synapse-room-limiter.py"),
))
spec = importlib.util.spec_from_file_location("room_limiter_patch", PATCH_PATH)
patch = importlib.util.module_from_spec(spec)
spec.loader.exec_module(patch)

# Minimal handler with the affected control flow, independent of Synapse dependencies.
BROKEN = '''class EventCreationHandler:
    async def create_and_send_nonmember_event(self, requester, event_dict, txn_id=None):
        room_id = event_dict["room_id"]
        async with self.limiter.queue(room_id):
            if txn_id:
                event = await self.get_event_from_transaction(requester, txn_id, room_id)
                if event:
                    assert event.internal_metadata.stream_ordering
                    return event, event.internal_metadata.stream_ordering
        async with self._worker_lock_handler.acquire_read_write_lock(
            NEW_EVENT_DURING_PURGE_LOCK_NAME, room_id, write=False
        ):
            return await self._create_and_send_nonmember_event_locked(
                requester=requester, event_dict=event_dict, txn_id=txn_id
            )
'''


def installed_source() -> str:
    spec = importlib.util.find_spec("synapse.handlers.message")
    return Path(spec.origin).read_text()


def critical_section(source: str):
    method = patch.find_method(ast.parse(source))
    # Execute the real final critical section, without unrelated auth/rate-limit setup.
    signature = ast.parse(
        "async def send(self, requester, event_dict, txn_id=None): pass"
    ).body[0]
    if patch.is_limiter(method.body[-1]):
        signature.body = method.body[-2:]
    else:
        signature.body = method.body[-3:]
    tree = ast.fix_missing_locations(ast.Module(body=[signature], type_ignores=[]))
    namespace = dict.fromkeys((
        "prev_event_ids", "state_event_ids", "outlier", "depth", "prev_state_events", "delay_id"
    ))
    namespace.update(
        NEW_EVENT_DURING_PURGE_LOCK_NAME="purge", ratelimit=False, ignore_shadow_ban=True
    )
    exec(compile(tree, "actual-send-critical-section", "exec"), namespace)
    return namespace["send"]


class Handler:
    def __init__(self):
        self.locks = {}
        self.held = set()
        self.sending = {}
        self.max_same_room = 0
        self.max_total = 0
        self.calls = 0
        self.cache_checks_locked = []
        self.purge_checks_locked = []
        self.cached = SimpleNamespace(internal_metadata=SimpleNamespace(stream_ordering=42))
        self.limiter = SimpleNamespace(queue=self.room_lock)
        self._worker_lock_handler = SimpleNamespace(acquire_read_write_lock=self.purge_lock)

    @asynccontextmanager
    async def room_lock(self, room):
        lock = self.locks.setdefault(room, asyncio.Lock())
        async with lock:
            self.held.add(room)
            try:
                yield
            finally:
                self.held.remove(room)

    @asynccontextmanager
    async def purge_lock(self, name, room, write):
        assert name == "purge" and write is False
        self.purge_checks_locked.append(room in self.held)
        yield

    async def get_event_from_transaction(self, requester, txn_id, room):
        self.cache_checks_locked.append(room in self.held)
        return self.cached if txn_id == "cached" else None

    async def _create_and_send_nonmember_event_locked(self, **kwargs):
        room = kwargs["event_dict"]["room_id"]
        self.sending[room] = self.sending.get(room, 0) + 1
        self.max_same_room = max(self.max_same_room, self.sending[room])
        self.max_total = max(self.max_total, sum(self.sending.values()))
        self.calls += 1
        try:
            await asyncio.sleep(0)
            return "sent", self.calls
        finally:
            self.sending[room] -= 1


class PatchTests(unittest.TestCase):
    def test_idempotent_and_preserves_send_block(self):
        fixed = patch.patch_source(BROKEN)
        self.assertNotEqual(fixed, BROKEN)
        self.assertEqual(patch.patch_source(fixed), fixed)
        before = patch.find_method(ast.parse(BROKEN)).body[-1]
        after = patch.find_method(ast.parse(fixed)).body[-1].body[-1]
        self.assertEqual(ast.dump(before), ast.dump(after))

    def test_rejects_unknown_lock_layout(self):
        with self.assertRaises(ValueError):
            patch.patch_source(BROKEN.replace("write=False", "write=True"))

    def test_installed_image_has_fix(self):
        if not os.getenv("RETENTION_VERIFY_INSTALLED_SYNAPSE"):
            self.skipTest("Enabled in Synapse image CI")
        source = installed_source()
        self.assertEqual(patch.patch_source(source), source)
        self.assertTrue(patch.is_limiter(patch.find_method(ast.parse(source)).body[-1]))


class ConcurrencyTests(unittest.IsolatedAsyncioTestCase):
    def source(self):
        if os.getenv("RETENTION_VERIFY_INSTALLED_SYNAPSE"):
            return installed_source()
        return patch.patch_source(BROKEN)

    async def test_broken_scope_reproduces_parallel_send(self):
        handler = Handler()
        send = critical_section(BROKEN)
        await asyncio.gather(*(send(handler, None, {"room_id": "a"}, str(i)) for i in range(64)))
        self.assertGreater(handler.max_same_room, 1)
        self.assertFalse(any(handler.purge_checks_locked))

    async def test_64_same_room_sends_are_serialized(self):
        handler = Handler()
        send = critical_section(self.source())
        await asyncio.gather(*(send(handler, None, {"room_id": "a"}, str(i)) for i in range(64)))
        self.assertEqual(handler.calls, 64)
        self.assertEqual(handler.max_same_room, 1)
        self.assertTrue(all(handler.cache_checks_locked))
        self.assertTrue(all(handler.purge_checks_locked))

    async def test_different_rooms_can_send_concurrently(self):
        handler = Handler()
        send = critical_section(self.source())
        await asyncio.gather(send(handler, None, {"room_id": "a"}),
                             send(handler, None, {"room_id": "b"}))
        self.assertEqual(handler.max_same_room, 1)
        self.assertEqual(handler.max_total, 2)

    async def test_cached_transaction_does_not_resend(self):
        handler = Handler()
        result = await critical_section(self.source())(
            handler, None, {"room_id": "a"}, "cached"
        )
        self.assertEqual(result, (handler.cached, 42))
        self.assertEqual(handler.calls, 0)
        self.assertEqual(handler.cache_checks_locked, [True])
        self.assertFalse(handler.held)


if __name__ == "__main__":
    unittest.main()
