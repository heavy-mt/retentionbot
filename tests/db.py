import hashlib
import json
import os
from pathlib import Path

from retentionbot.store import Store


def open_store(path: Path):
    if os.getenv("TEST_STORE_BACKEND") == "postgres":
        namespace = "test_" + hashlib.sha256(str(path).encode()).hexdigest()[:24]
        return Store(os.environ["TEST_DATABASE_URL"], namespace=namespace)
    return Store(path)


def snapshot(store):
    return json.dumps(
        {
            table: [dict(row) for row in store.db.execute(f"SELECT * FROM {table}")]
            for table in ("events", "media", "media_refs", "commands", "outbox")
        }
    )
