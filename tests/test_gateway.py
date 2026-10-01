from unittest.mock import AsyncMock

import aiohttp
from aiohttp import web

from retentionbot.api import JsonApi
from retentionbot.gateway import AdminGateway
from retentionbot.store import now_ms


async def start_app(app):
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    return runner, f"http://127.0.0.1:{port}"


async def test_gateway_limits_admin_operations_and_protects_old_uploads(config, store):
    api = AsyncMock()
    cutoff = now_ms()
    store.set("started_at", str(cutoff))
    api.request.return_value = {"media_info": {"created_ts": cutoff - 1}}
    runner, base = await start_app(AdminGateway(config, api, store).app())
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                base + "/v1/delete-media", json={"uri": "mxc://example.org/x"}
            ) as r:
                assert r.status == 401
            client = JsonApi(base, config.gateway_secret, session)
            result = await client.request(
                "POST", "/v1/delete-media", json={"uri": "mxc://example.org/x"}
            )
            assert result == {"protected": True, "reason": "preexisting_upload"}
            api.request.assert_awaited_once()
            api.request.reset_mock()
            result = await client.request(
                "POST", "/v1/delete-media", json={"uri": "mxc://foreign.org/x"}
            )
            assert result["reason"] == "remote_origin"
            api.request.assert_not_called()
    finally:
        await runner.cleanup()


async def test_gateway_requires_physical_delete_confirmation(config, store):
    api = AsyncMock()
    api.request.side_effect = [
        {"media_info": {"created_ts": now_ms() + 1000}},
        {"deleted_media": ["x"]},
        {"media_info": {"created_ts": now_ms() + 1000}},
        {"deleted_media": []},
    ]
    runner, base = await start_app(AdminGateway(config, api, store).app())
    try:
        async with aiohttp.ClientSession() as session:
            client = JsonApi(base, config.gateway_secret, session)
            result = await client.request(
                "POST", "/v1/delete-media", json={"uri": "mxc://example.org/x"}
            )
            assert result["deleted"]
            import pytest

            from retentionbot.api import ApiError

            with pytest.raises(ApiError, match="MEDIA_DELETE_UNCONFIRMED"):
                await client.request(
                    "POST", "/v1/delete-media", json={"uri": "mxc://example.org/x"}
                )
    finally:
        await runner.cleanup()
