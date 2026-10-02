from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit


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
        raise ValueError("Invalid Synapse URL")
    if parts.query or parts.fragment or parts.path not in {"", "/"}:
        raise ValueError("URL must be a server origin without path, query or fragment")
    return value.rstrip("/")


@dataclass(frozen=True)
class Config:
    synapse_url: str
    module_secret: str
    database_url: str = ""
    rabbitmq_url: str = "amqp://guest:guest@localhost/"
    data_dir: Path = Path("/data")
    poll_seconds: int = 1
    refresh_seconds: int = 60
    batch_size: int = 1000

    @classmethod
    def from_env(cls) -> Config:
        return cls(
            synapse_url=base_url(os.environ["SYNAPSE_URL"]),
            module_secret=secret("SYNAPSE_MODULE_SECRET"),
            database_url=secret("DATABASE_URL", required=False),
            rabbitmq_url=secret("RABBITMQ_URL"),
            data_dir=Path(os.getenv("DATA_DIR", "/data")),
            poll_seconds=positive("POLL_SECONDS", 1),
            refresh_seconds=positive("REFRESH_SECONDS", 60),
            batch_size=min(positive("BATCH_SIZE", 1000), 5000),
        )
