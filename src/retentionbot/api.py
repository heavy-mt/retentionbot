from __future__ import annotations

from urllib.parse import quote

import aiohttp


def segment(value: str) -> str:
    return quote(value, safe="")


class ApiError(Exception):
    def __init__(self, status: int, code: str, retry_ms: int = 0):
        self.status, self.code, self.retry_ms = status, code, retry_ms
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
                if response.status < 300:
                    raise ApiError(502, "UPSTREAM_INVALID_JSON")
                body = {}
            if response.status >= 300:
                raise ApiError(
                    response.status,
                    str(body.get("errcode", "HTTP_ERROR")),
                    int(body.get("retry_after_ms", 0)),
                )
            return body


class MatrixApi(JsonApi):
    async def power_levels(self, room_id: str) -> dict:
        return await self.request(
            "GET", f"/_matrix/client/v3/rooms/{segment(room_id)}/state/m.room.power_levels"
        )

    async def member(self, room_id: str, user_id: str) -> dict:
        return await self.request(
            "GET",
            f"/_matrix/client/v3/rooms/{segment(room_id)}/state/m.room.member/{segment(user_id)}",
        )

    async def retention(self, room_id: str) -> dict:
        try:
            return await self.request(
                "GET", f"/_matrix/client/v3/rooms/{segment(room_id)}/state/m.room.retention"
            )
        except ApiError as error:
            if error.status == 404 and error.code == "M_NOT_FOUND":
                return {}
            raise

    async def set_retention(self, room_id: str, content: dict):
        return await self.request(
            "PUT",
            f"/_matrix/client/v3/rooms/{segment(room_id)}/state/m.room.retention",
            json=content,
        )

    async def redact(self, room_id: str, event_id: str, transaction: str):
        result = await self.request(
            "PUT",
            f"/_matrix/client/v3/rooms/{segment(room_id)}/redact/{segment(event_id)}/{transaction}",
            json={"reason": "Message retention policy"},
        )
        if not isinstance(result.get("event_id"), str) or not result["event_id"]:
            raise ApiError(502, "REDACTION_UNCONFIRMED")
        # Synapse can accept a redaction after purge while withholding it from clients.
        # Confirm that the newly created redaction is actually available to a room member.
        try:
            visible = await self.request(
                "GET",
                f"/_matrix/client/v3/rooms/{segment(room_id)}/event/{segment(result['event_id'])}",
            )
        except ApiError as error:
            if error.status == 404:
                raise ApiError(502, "REDACTION_NOT_VISIBLE") from None
            raise
        if visible.get("type") != "m.room.redaction":
            raise ApiError(502, "REDACTION_NOT_VISIBLE")
        return result


class GatewayApi(JsonApi):
    async def retention_config(self) -> dict:
        return await self.request("GET", "/v1/retention-config")

    async def rooms(self) -> list[dict]:
        return (await self.request("GET", "/v1/rooms"))["rooms"]

    async def enroll(self, room_id: str):
        return await self.request("POST", "/v1/enroll", json={"room_id": room_id})

    async def delete_media(self, uri: str):
        return await self.request("POST", "/v1/delete-media", json={"uri": uri})
