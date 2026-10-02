from __future__ import annotations

import asyncio
import json
import logging

import aiohttp
import psycopg

from .api import ApiError, ServerApi
from .broker import Broker
from .config import Config
from .store import Store, now_ms

logger = logging.getLogger(__name__)


class Worker:
    def __init__(self, store: Store, api: ServerApi):
        self.store, self.api = store, api

    async def handle(self, event_id: str):
        event = self.store.event(event_id)
        if not event or event["status"] != "pending":
            return
        try:
            result = await self.api.redact(event_id)
            if result.get("status") == "deferred":
                at = max(now_ms() + 1000, result["retry_at_ms"])
                self.store.retry(event_id, at, result.get("code", "NOT_DUE"))
                return
            if result.get("status") not in {"done", "missed", "blocked"}:
                raise ApiError(502, "REDACTION_UNCONFIRMED")
            if result["status"] == "done" and not result.get("redaction_id"):
                raise ApiError(502, "REDACTION_UNCONFIRMED")
            self.store.finish(event_id, result, now_ms())
            logger.info(
                "Задание удаления обработано",
                extra={
                    "event": "message." + result["status"],
                    "event_id": event_id,
                    "room_id": event["room_id"],
                    "code": result.get("code"),
                },
            )
        except (TimeoutError, ApiError, aiohttp.ClientError) as error:
            code = error.code if isinstance(error, ApiError) else type(error).__name__
            delay = min(300_000, 1000 * 2 ** min(event["attempts"], 8))
            delay = max(delay, getattr(error, "retry_ms", 0))
            self.store.retry(event_id, now_ms() + delay, code)
            logger.warning(
                "Удаление будет повторено",
                extra={"event": "job.retry", "event_id": event_id, "code": code},
            )


async def run(config: Config):
    store = Store(config.database_url or config.data_dir / "server.db")
    broker = await Broker().connect(config.rabbitmq_url)
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
            worker = Worker(store, ServerApi(config.synapse_url, config.module_secret, session))
            async with broker.queue.iterator() as iterator:
                async for message in iterator:
                    try:
                        job = json.loads(message.body)
                        if (
                            not isinstance(job, dict)
                            or job.get("v") != 1
                            or job.get("kind") != "redact"
                            or not isinstance(job.get("key"), str)
                            or not job["key"].startswith("$")
                        ):
                            raise ValueError("Invalid job")
                    except (ValueError, UnicodeDecodeError):
                        await message.reject(requeue=False)
                        continue
                    try:
                        await worker.handle(job["key"])
                    except (psycopg.OperationalError, psycopg.InterfaceError):
                        await message.nack(requeue=True)
                        raise
                    except Exception as error:
                        logger.error(
                            "Ошибка обработки задания", extra={"code": type(error).__name__}
                        )
                        await message.nack(requeue=True)
                        await asyncio.sleep(1)
                    else:
                        await message.ack()
    finally:
        await broker.close()
        store.close()
