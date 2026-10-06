from __future__ import annotations

from urllib.parse import quote

import aiohttp

PREFIX = "/_synapse/retention/v1"


def segment(value: str) -> str:
    return quote(value, safe="")


class ApiError(Exception):
    def __init__(self, status: int, code: str, retry_ms: int = 0, message: str | None = None):
        self.status, self.code, self.retry_ms = status, code, retry_ms
        self.message = message
        super().__init__(f"HTTP {status} {code}")


class JsonApi:
    def __init__(self, base: str, token: str, session: aiohttp.ClientSession):
        self.base, self.token, self.session = base, token, session

    async def request(self, method: str, path: str, **kwargs) -> dict:
        async with self.session.request(
            method,
            self.base + path,
            headers={"Authorization": "Bearer " + self.token},
            allow_redirects=False,
            **kwargs,
        ) as response:
            try:
                body = await response.json()
            except (ValueError, aiohttp.ContentTypeError):
                body = None
            if not isinstance(body, dict):
                raise ApiError(
                    response.status if response.status >= 300 else 502, "UPSTREAM_INVALID_JSON"
                )
            if response.status >= 300:
                code = body.get("errcode", "HTTP_ERROR")
                if not isinstance(code, str) or not code.isupper() or len(code) > 80:
                    code = "HTTP_ERROR"
                raise ApiError(
                    response.status,
                    code,
                    int(body.get("retry_after_ms", 0)),
                    body.get("error") if isinstance(body.get("error"), str) else None,
                )
            return body


class ServerApi(JsonApi):
    async def feed(self, after: int | None, limit: int = 1000):
        params = {"limit": str(limit)}
        if after is not None:
            params["after"] = str(after)
        return await self.request("GET", PREFIX + "/internal/feed", params=params)

    async def policy(self, room_id: str):
        return await self.request("GET", PREFIX + "/internal/policy", params={"room_id": room_id})

    async def redact(self, event_id: str):
        return await self.request("POST", PREFIX + "/internal/redact", json={"event_id": event_id})

    async def invalidate(self, event_id: str, room_id: str):
        return await self.request(
            "POST",
            PREFIX + "/internal/invalidate",
            json={"event_id": event_id, "room_id": room_id},
        )
