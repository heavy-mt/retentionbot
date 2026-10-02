import hashlib
import os

from retentionbot.store import Store


def open_store(path):
    if os.getenv("TEST_STORE_BACKEND") == "postgres":
        namespace = "test_" + hashlib.sha256(str(path).encode()).hexdigest()[:24]
        return Store(os.environ["TEST_DATABASE_URL"], namespace=namespace)
    return Store(path)
