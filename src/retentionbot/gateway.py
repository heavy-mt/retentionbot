from __future__ import annotations

import hmac
import logging
from dataclasses import asdict

import aiohttp
from aiohttp import web

from .api import ApiError, JsonApi, segment
from .config import Config, secret
from .media import split_mxc
from .policy import ServerPolicy
from .store import Store

log = logging.getLogger(__name__)


class AdminGateway:
    """Restricted interface; the Matrix observer and worker never receive the admin token."""

    def __init__(self, config: Config, api: JsonApi, store: Store):
        self.config, self.api, self.store = config, api, store

    async def retention_config(self, _request):
        if not self.config.retention_config_file:
            raise ApiError(503, "RETENTION_CONFIG_REQUIRED")
        policy = ServerPolicy.from_file(self.config.retention_config_file)
        return web.json_response(asdict(policy))

    async def rooms(self, _request):
        rooms, offset = [], "0"
        while True:
            page = await self.api.request(
                "GET",
                "/_synapse/admin/v1/rooms",
                params={
                    "from": offset,
                    "limit": "100",
                    "empty_rooms": "false",
                },
            )
            rooms.extend(page["rooms"])
            next_page = page.get("next_batch", page.get("next_token"))
            if next_page is None:
                break
            if str(next_page) == offset:
                raise ApiError(502, "PAGINATION_STALLED")
            offset = str(next_page)
        return web.json_response({"rooms": rooms})

    async def enroll(self, request):
        room_id = (await request.json()).get("room_id")
        if not isinstance(room_id, str) or not room_id.startswith("!") or len(room_id) > 1024:
            raise web.HTTPBadRequest()
        await self.api.request(
            "POST",
            f"/_synapse/admin/v1/rooms/{segment(room_id)}/make_room_admin",
            json={"user_id": self.config.user_id},
        )
        return web.json_response({"invited": True})

    async def delete_media(self, request):
        uri = (await request.json()).get("uri", "")
        try:
            server, media_id = split_mxc(uri)
        except (ValueError, TypeError):
            raise web.HTTPBadRequest() from None
        # This gateway deliberately cannot remove remote-origin files or old uploads.
        if server != self.config.server_name:
            return web.json_response({"protected": True, "reason": "remote_origin"})
        path = f"/_synapse/admin/v1/media/{segment(server)}/{segment(media_id)}"
        try:
            info = (await self.api.request("GET", path))["media_info"]
        except ApiError as error:
            if error.status == 404:
                return web.json_response({"deleted": True, "already_absent": True})
            raise
        cutoff = int(self.store.get("started_at"))
        created = info.get("created_ts")
        if not isinstance(created, int) or created < cutoff:
            return web.json_response({"protected": True, "reason": "preexisting_upload"})
        if info.get("safe_from_quarantine") or info.get("url_cache"):
            return web.json_response({"protected": True, "reason": "protected_upload"})
        # A 200 without a deleted ID is not treated as confirmed deletion.
        result = await self.api.request("DELETE", path, json={})
        if media_id not in result.get("deleted_media", []):
            raise ApiError(502, "MEDIA_DELETE_UNCONFIRMED")
        return web.json_response({"deleted": True})

    def app(self) -> web.Application:
        async def health(_):
            return web.json_response({"status": "ok"})

        @web.middleware
        async def authenticate(request, handler):
            if request.path == "/health":
                return await handler(request)
            provided = request.headers.get("Authorization", "")
            expected = "Bearer " + self.config.gateway_secret
            if not hmac.compare_digest(provided.encode(), expected.encode()):
                raise web.HTTPUnauthorized()
            try:
                return await handler(request)
            except ApiError as error:
                return web.json_response(
                    {"errcode": error.code, "retry_after_ms": error.retry_ms}, status=error.status
                )
            except (aiohttp.ClientError, TimeoutError):
                return web.json_response({"errcode": "UPSTREAM_UNAVAILABLE"}, status=502)

        app = web.Application(middlewares=[authenticate], client_max_size=8192)
        app.router.add_get("/health", health)
        app.router.add_get("/v1/rooms", self.rooms)
        app.router.add_get("/v1/retention-config", self.retention_config)
        app.router.add_post("/v1/enroll", self.enroll)
        app.router.add_post("/v1/delete-media", self.delete_media)
        return app


async def serve(config: Config):
    if not config.retention_config_file:
        raise ValueError("Set SYNAPSE_RETENTION_CONFIG_FILE to the actual Synapse config")
    ServerPolicy.from_file(config.retention_config_file)
    store = Store(config.database_url or config.data_dir / "gateway.db", namespace="gateway")
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=45)) as session:
        gateway = AdminGateway(
            config, JsonApi(config.homeserver, secret("SYNAPSE_ADMIN_TOKEN"), session), store
        )
        runner = web.AppRunner(gateway.app(), access_log=None)
        await runner.setup()
        await web.TCPSite(runner, "0.0.0.0", 8080).start()
        log.info("Admin gateway listening on port 8080")
        try:
            import asyncio

            await asyncio.Event().wait()
        finally:
            await runner.cleanup()
            store.close()
