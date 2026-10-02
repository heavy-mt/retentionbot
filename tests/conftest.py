import os

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
    return Config(synapse_url="http://localhost:8008", module_secret="x" * 64, data_dir=tmp_path)


@pytest.fixture
def store(tmp_path):
    db = open_store(tmp_path / "server.db")
    db.bootstrap(0, 100)
    yield db
    db.close()
