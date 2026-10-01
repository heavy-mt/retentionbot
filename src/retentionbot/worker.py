from __future__ import annotations

import asyncio
import hashlib
import json
import logging

import aiohttp
import psycopg

from .api import ApiError, GatewayApi, MatrixApi
from .broker import Broker
from .config import Config, secret
from .policy import ServerPolicy
from .store import Store, now_ms

log = logging.getLogger(__name__)


def transaction(event_id: str) -> str:
    return "retention-" + hashlib.sha256(event_id.encode()).hexdigest()


class Worker:
    def __init__(self, config: Config, store: Store, matrix: MatrixApi, gateway: GatewayApi):
        self.config, self.store, self.matrix, self.gateway = config, store, matrix, gateway

    async def handle(self, kind: str, key: str):
        at = now_ms()
        if kind == "redact":
            known = self.store.db.execute(
                "SELECT room_id,redacted FROM events WHERE event_id=?", (key,)
            ).fetchone()
            if known and known["redacted"]:
                self.store.release(kind, key)
                return
            settings = self.store.get("server_retention")
            if known and settings:
                # Re-read room state before acting on a possibly stale broker job.
                try:
                    policy = ServerPolicy(**json.loads(settings)).effective(
                        await self.matrix.retention(known["room_id"])
                    )
                    self.store.policy(
                        known["room_id"],
                        policy.min_lifetime,
                        policy.max_lifetime,
                        lead_ms=self.config.redaction_lead_ms,
                    )
                except (ApiError, aiohttp.ClientError, TimeoutError, ValueError):
                    self.store.retry(key, at + 60_000, "POLICY_REFRESH_FAILED")
                    return
            event = self.store.event_due(key, at)
            if not event:
                self.store.release(kind, key)
                return
            try:
                await self.matrix.redact(event["room_id"], key, transaction(key))
            except (ApiError, aiohttp.ClientError, TimeoutError) as error:
                wait = max(5000, min(300_000, 5000 * 2 ** min(event["attempts"], 6)))
                if isinstance(error, ApiError):
                    wait = max(wait, error.retry_ms)
                    if error.status in {401, 403, 404}:
                        wait = max(wait, 60_000)
                code = error.code if isinstance(error, ApiError) else "NETWORK_ERROR"
                self.store.retry(key, at + wait, code)
                log.warning(
                    "Redaction failed room=%s event=%s code=%s", event["room_id"], key, code
                )
                return
            self.store.mark_redacted(key, now_ms() + self.config.media_grace_seconds * 1000)
            log.info(
                "Содержимое сообщения удалено",
                extra={"event": "message.redacted", "room_id": event["room_id"], "event_id": key},
            )
        elif kind == "media":
            if not self.store.cleanup_ready(at) or not self.store.media_eligible(key, at):
                self.store.release(kind, key)
                return
            try:
                result = await self.gateway.delete_media(key)
                if result.get("deleted") is not True and result.get("protected") is not True:
                    raise ApiError(502, "MEDIA_DELETE_UNCONFIRMED")
            except (ApiError, aiohttp.ClientError, TimeoutError) as error:
                code = error.code if isinstance(error, ApiError) else "NETWORK_ERROR"
                self.store.media_retry(key, at + 60_000, code)
                log.warning("Media cleanup failed uri=%s code=%s", key, code)
                return
            self.store.media_done(key, protected=result.get("protected", False))
            log.info("Media cleanup uri=%s result=%s", key, result.get("reason", "deleted"))
        else:
            raise ValueError("Unknown job type")


async def run(config: Config):
    store = Store(config.database_url or config.data_dir / "retention.db")
    broker = await Broker().connect(config.rabbitmq_url)
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=45)) as session:
        worker = Worker(
            config,
            store,
            MatrixApi(config.homeserver, secret("BOT_ACCESS_TOKEN"), session),
            GatewayApi(config.gateway_url, config.gateway_secret, session),
        )
        try:
            async with broker.queue.iterator() as iterator:
                async for message in iterator:
                    store.set("worker_at", str(now_ms()))
                    try:
                        job = json.loads(message.body)
                        if job.get("v") != 1 or not isinstance(job.get("key"), str):
                            raise ValueError("Invalid job")
                        await worker.handle(job["kind"], job["key"])
                    except (ValueError, KeyError, TypeError, json.JSONDecodeError):
                        await message.reject(requeue=False)
                        log.error("Rejected malformed job")
                    except (psycopg.OperationalError, psycopg.InterfaceError):
                        await message.nack(requeue=True)
                        log.error("Database connection failed; restarting worker")
                        raise
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        await message.nack(requeue=True)
                        log.exception("Job processing failed")
                        await asyncio.sleep(1)
                    else:
                        await message.ack()
        finally:
            await broker.close()
            store.close()
