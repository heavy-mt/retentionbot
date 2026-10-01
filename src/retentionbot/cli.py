from __future__ import annotations

import argparse
import asyncio
import getpass
import json
import logging
import os
import signal
from pathlib import Path

from nio import AsyncClient, AsyncClientConfig, ErrorResponse

from . import gateway, service, worker
from .config import Config, base_url
from .jsonlog import JsonFormatter
from .store import Store, now_ms


async def login(args):
    """Create a real device. An admin impersonation token is unsuitable for E2EE."""
    directory = Path(args.output)
    directory.mkdir(parents=True, exist_ok=True)
    client = AsyncClient(
        base_url(args.homeserver), args.user, config=AsyncClientConfig(encryption_enabled=False)
    )
    try:
        response = await client.login(
            password=getpass.getpass("Пароль бота: "), device_name="Retentionbot"
        )
        if isinstance(response, ErrorResponse):
            raise RuntimeError(f"Login failed: {response.status_code}")
        for name, value in {
            "bot_access_token": response.access_token,
            "bot_device_id": response.device_id,
        }.items():
            path = directory / name
            descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "w") as output:
                output.write(value + "\n")
        print(
            json.dumps(
                {"ok": True, "message": "Сохранены bot_access_token и bot_device_id."},
                ensure_ascii=False,
            )
        )
    finally:
        await client.close()


async def supervise(coroutine):
    task = asyncio.create_task(coroutine)
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signum, task.cancel)
    try:
        await task
    except asyncio.CancelledError:
        pass


def main():
    parser = argparse.ArgumentParser(description="Matrix retention service")
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("observer", "worker", "gateway", "status", "health"):
        sub.add_parser(name)
    auth = sub.add_parser("login")
    auth.add_argument("--homeserver", required=True)
    auth.add_argument("--user", required=True)
    auth.add_argument("--output", default="secrets")
    args = parser.parse_args()
    os.umask(0o077)
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    for handler in logging.getLogger().handlers:
        handler.setFormatter(JsonFormatter())
    # Matrix-nio debug logging may contain events. AMQP logs may contain connection URLs.
    for name in ("nio", "aio_pika", "aiormq"):
        logging.getLogger(name).setLevel(logging.CRITICAL)
    if args.command == "login":
        asyncio.run(login(args))
        return
    try:
        config = Config.from_env()
    except Exception as error:
        logging.error("Configuration failed: %s; check .env and secret files", type(error).__name__)
        raise SystemExit(1) from None
    if args.command in {"status", "health"}:
        store = Store(config.database_url or config.data_dir / "retention.db")
        try:
            if args.command == "health":
                last_sync = int(store.get("last_sync_at") or "0")
                raise SystemExit(0 if now_ms() - last_sync < 90_000 else 1)
            counts = store.counts()
            counts.update(
                coverage_ok=store.get("coverage_ok") == "1",
                last_sync_at=store.get("last_sync_at"),
                rooms=[dict(row) for row in store.db.execute("SELECT * FROM rooms")],
            )
            print(json.dumps(counts, ensure_ascii=False))
        finally:
            store.close()
        return
    entry = {"observer": service.run, "worker": worker.run, "gateway": gateway.serve}[args.command]
    try:
        asyncio.run(supervise(entry(config)))
    except Exception as error:
        # Do not interpolate credential-bearing exception values.
        logging.error("Service startup failed: %s", type(error).__name__)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
