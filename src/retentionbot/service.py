from __future__ import annotations

import asyncio
import logging

import aiohttp

from .api import ApiError, ServerApi
from .broker import Broker
from .config import Config
from .store import Store, now_ms

logger = logging.getLogger(__name__)


class Observer:
    def __init__(self, config: Config, store: Store, api: ServerApi, broker: Broker):
        self.config, self.store, self.api, self.broker = config, store, api, broker
        self.last_refresh = 0

    async def poll_once(self):
        cursor = self.store.get("cursor")
        if cursor is None:
            checkpoint = await self.api.feed(None, self.config.batch_size)
            self.store.bootstrap(checkpoint["cursor"], checkpoint["now_ms"])
            cursor = str(checkpoint["cursor"])
            logger.info("Начат учёт новых сообщений", extra={"event": "observer.started"})
        page = await self.api.feed(int(cursor), self.config.batch_size)
        rooms = {e["room_id"] for e in page["events"]}
        at = now_ms()
        refresh = at - self.last_refresh >= self.config.refresh_seconds * 1000
        if refresh:
            rooms.update(
                row["room_id"] for row in self.store.db.execute("SELECT room_id FROM rooms")
            )
        policies = {}
        for room in sorted(rooms):
            try:
                policies[room] = await self.api.policy(room)
            except ApiError as error:
                if error.status != 404:
                    raise
                # Admin purge of one entire room must not stall the global event feed.
                policies[room] = {
                    "policy_error": "ROOM_NOT_FOUND",
                    "redact_after_ms": None,
                    "max_lifetime": None,
                }
        self.store.ingest(page, policies)
        if refresh:
            self.last_refresh = at
        return page["caught_up"]

    async def schedule(self):
        at = now_ms()
        for event in self.store.due(at, self.config.batch_size):
            # Claim before publish; loss between the two is repaired by the lease.
            self.store.queued(event["event_id"], at)
            try:
                await self.broker.publish("redact", event["event_id"])
            except Exception:
                self.store.queued(event["event_id"], 0)
                raise

        for invalidation in self.store.due_invalidations(at, self.config.batch_size):
            self.store.invalidation_queued(invalidation["event_id"], at)
            try:
                await self.broker.publish("invalidate", invalidation["event_id"])
            except Exception:
                self.store.invalidation_queued(invalidation["event_id"], 0)
                raise

        self.store.compact(at - 7 * 86_400_000, self.config.batch_size)


async def run(config: Config):
    store = Store(config.database_url or config.data_dir / "server.db")
    broker = await Broker().connect(config.rabbitmq_url)
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
            observer = Observer(
                config, store, ServerApi(config.synapse_url, config.module_secret, session), broker
            )
            while True:
                try:
                    caught_up = await observer.poll_once()
                    await observer.schedule()
                except (TimeoutError, ApiError, aiohttp.ClientError) as error:
                    code = error.code if isinstance(error, ApiError) else type(error).__name__
                    logger.warning(
                        "Synapse временно недоступен",
                        extra={"code": code, "event": "observer.retry"},
                    )
                    caught_up = True
                await asyncio.sleep(config.poll_seconds if caught_up else 0)
    finally:
        await broker.close()
        store.close()
