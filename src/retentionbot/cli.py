from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
from pathlib import Path

import aiohttp

from . import service, worker
from .api import PREFIX, ApiError, JsonApi
from .config import Config, base_url
from .jsonlog import JsonFormatter
from .store import Store, now_ms


async def command(args):
    body = json.loads(args.json)
    if not isinstance(body, dict):
        raise ValueError("Command must be a JSON object")
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        api = JsonApi(
            base_url(args.synapse_url), Path(args.token_file).read_text().strip(), session
        )
        result = await api.request("POST", PREFIX + "/command", json=body)
        print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))


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
    parser = argparse.ArgumentParser(description="Server-side Matrix retention service")
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("observer", "worker", "status", "health"):
        sub.add_parser(name)
    cmd = sub.add_parser("command")
    cmd.add_argument("--synapse-url", default=os.getenv("SYNAPSE_URL"))
    cmd.add_argument("--token-file", required=True)
    cmd.add_argument("--json", required=True)
    args = parser.parse_args()
    os.umask(0o077)
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    for handler in logging.getLogger().handlers:
        handler.setFormatter(JsonFormatter())
    for name in ("aio_pika", "aiormq"):
        logging.getLogger(name).setLevel(logging.CRITICAL)
    try:
        if args.command == "command":
            asyncio.run(command(args))
            return
        config = Config.from_env()
        if args.command in {"status", "health"}:
            store = Store(config.database_url or config.data_dir / "server.db")
            try:
                last_poll = int(store.get("last_poll_at") or 0)
                if args.command == "health":
                    raise SystemExit(0 if now_ms() - last_poll < 90_000 else 1)
                print(
                    json.dumps(
                        {
                            "counts": store.counts(),
                            "last_poll_at": last_poll,
                            "cursor": store.get("cursor"),
                            "since_ts": store.get("since_ts"),
                            "rooms": list(store.db.execute("SELECT * FROM rooms")),
                        },
                        ensure_ascii=False,
                        separators=(",", ":"),
                    )
                )
            finally:
                store.close()
            return
        entry = {"observer": service.run, "worker": worker.run}[args.command]
        asyncio.run(supervise(entry(config)))
    except ApiError as error:
        print(
            json.dumps(
                {
                    "ok": False,
                    "code": error.code,
                    "message": error.message or "Сервер отклонил запрос.",
                },
                ensure_ascii=False,
                separators=(",", ":"),
            )
        )
        raise SystemExit(1) from None
    except Exception as error:
        logging.error("Служба завершилась с ошибкой", extra={"code": type(error).__name__})
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
