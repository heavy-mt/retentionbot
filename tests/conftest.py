from pathlib import Path

import pytest

from retentionbot.config import Config
from retentionbot.store import Store


@pytest.fixture
def config(tmp_path):
    return Config(
        homeserver="http://localhost:8008",
        server_name="example.org",
        user_id="@retention:example.org",
        gateway_url="http://localhost:8080",
        gateway_secret="test-secret",
        data_dir=tmp_path,
    )


@pytest.fixture
def store(tmp_path: Path):
    db = Store(tmp_path / "retention.db")
    db.enroll("!room:example.org", 1000, since_ts=0)
    yield db
    db.close()
