from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

PERIODS = {"1h": 3_600_000, "1d": 86_400_000, "7d": 604_800_000, "30d": 2_592_000_000}


def secret(name: str, required: bool = True) -> str:
    filename = os.getenv(name + "_FILE")
    value = Path(filename).read_text().strip() if filename else os.getenv(name, "").strip()
    if required and not value:
        raise ValueError(f"Set {name} or {name}_FILE")
    return value


def positive(name: str, default: int) -> int:
    value = int(os.getenv(name, str(default)))
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


def base_url(value: str) -> str:
    parts = urlsplit(value)
    if parts.scheme not in {"http", "https"} or not parts.netloc or parts.username:
        raise ValueError("Invalid homeserver/gateway URL")
    if parts.query or parts.fragment or parts.path not in {"", "/"}:
        raise ValueError("URL must be a server origin without path, query or fragment")
    return value.rstrip("/")


@dataclass(frozen=True)
class Config:
    homeserver: str
    server_name: str
    user_id: str
    gateway_url: str
    gateway_secret: str
    rabbitmq_url: str = "amqp://guest:guest@localhost/"
    data_dir: Path = Path("data")
    default_period: str = "7d"
    poll_seconds: int = 5
    discover_seconds: int = 60
    admin_power_level: int = 100
    media_grace_seconds: int = 60
    trust_devices: str = "tofu"

    @property
    def default_lifetime(self) -> int | None:
        return None if self.default_period == "off" else PERIODS[self.default_period]

    @classmethod
    def from_env(cls) -> Config:
        server_name = os.environ["MATRIX_SERVER_NAME"]
        user_id = os.environ["BOT_USER_ID"]
        if not user_id.startswith("@") or user_id.split(":", 1)[-1] != server_name:
            raise ValueError("BOT_USER_ID must be local to MATRIX_SERVER_NAME")
        period = os.getenv("DEFAULT_RETENTION", "7d")
        if period not in {*PERIODS, "off"}:
            raise ValueError("DEFAULT_RETENTION must be 1h, 1d, 7d, 30d or off")
        trust = os.getenv("DEVICE_TRUST", "tofu")
        if trust not in {"tofu", "verified"}:
            raise ValueError("DEVICE_TRUST must be tofu or verified")
        return cls(
            homeserver=base_url(os.environ["MATRIX_HOMESERVER"]),
            server_name=server_name,
            user_id=user_id,
            gateway_url=base_url(os.getenv("GATEWAY_URL", "http://admin-gateway:8080")),
            gateway_secret=secret("GATEWAY_SECRET"),
            rabbitmq_url=secret("RABBITMQ_URL"),
            data_dir=Path(os.getenv("DATA_DIR", "/data")),
            default_period=period,
            poll_seconds=positive("POLL_SECONDS", 5),
            discover_seconds=positive("DISCOVER_SECONDS", 60),
            admin_power_level=positive("ROOM_ADMIN_POWER_LEVEL", 100),
            media_grace_seconds=positive("MEDIA_GRACE_SECONDS", 60),
            trust_devices=trust,
        )
