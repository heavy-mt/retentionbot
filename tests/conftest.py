import os
from pathlib import Path

import pytest

from retentionbot.config import Config

from .db import open_store


@pytest.fixture(
    autouse=True, params=["sqlite", "postgres"] if os.getenv("TEST_DATABASE_URL") else ["sqlite"]
)
def backend(request, monkeypatch):
    monkeypatch.setenv("TEST_STORE_BACKEND", request.param)


@pytest.fixture
def config(tmp_path):
    return Config(
        homeserver="http://localhost:8008",
        server_name="example.org",
        user_id="@retention:example.org",
        gateway_url="http://localhost:8080",
        gateway_secret="test-secret",
        data_dir=tmp_path,
        default_period="off",
        redaction_lead_ms=0,
    )


@pytest.fixture
def store(tmp_path: Path):
    db = open_store(tmp_path / "retention.db")
    db.enroll("!room:example.org", 1000, since_ts=0)
    yield db
    db.close()
