#!/bin/bash
set -euo pipefail
export SYNAPSE_NETWORK=retention-smoke
export SYNAPSE_URL=http://synapse-smoke:8008
export DATABASE_PASSWORD=disposable_database_password
export RABBITMQ_PASSWORD=disposable_broker_password
mkdir -p secrets test-runtime/compose-synapse
for name in synapse_module_secret postgres_admin_password; do
    printf '%s\n' 'disposable-compose-smoke-secret-00000000000000' > "secrets/$name"
done
# Secrets are exposed only to assigned containers; the host directory is private.
chmod 700 secrets
chmod 444 secrets/*
cleanup() {
    docker compose logs --tail=30 observer worker || true
    docker compose down -v || true
    docker rm -f synapse-smoke >/dev/null 2>&1 || true
    docker network rm retention-smoke >/dev/null 2>&1 || true
}
trap cleanup EXIT
docker network create retention-smoke
docker run --rm --entrypoint /usr/local/bin/python \
    -v "$PWD/test-runtime/compose-synapse:/data" retention-synapse:1.161.0 \
    -m synapse.app.homeserver --server-name test.invalid \
    --config-path /data/homeserver.yaml --generate-config --report-stats=no
docker run --rm -i --entrypoint /usr/local/bin/python \
    -v "$PWD/test-runtime/compose-synapse:/data" retention-synapse:1.161.0 - <<'PY'
from pathlib import Path
import yaml
path = Path('/data/homeserver.yaml')
data = yaml.safe_load(path.read_text())
data.update(
    database={'name': 'sqlite3', 'args': {'database': '/data/synapse.db'}},
    signing_key_path='/data/test.invalid.signing.key',
    media_store_path='/data/media',
    trusted_key_servers=[],
    listeners=[{'port': 8008, 'tls': False, 'type': 'http', 'bind_addresses': ['0.0.0.0'],
                'resources': [{'names': ['client'], 'compress': False}]}],
    retention={'enabled': True, 'default_policy': {'max_lifetime': '7d'}},
    modules=[{'module': 'synapse_retention.module.RetentionModule',
              'config': {'secret_file': '/module-secret', 'cutoff_file': '/data/retention-cutoff'}}],
)
path.write_text(yaml.safe_dump(data))
PY
sudo chown -R 991:991 test-runtime/compose-synapse
docker run -d --name synapse-smoke --network retention-smoke \
    -e SYNAPSE_CONFIG_PATH=/data/homeserver.yaml \
    -v "$PWD/test-runtime/compose-synapse:/data" \
    -v "$PWD/secrets/synapse_module_secret:/module-secret:ro" retention-synapse:1.161.0
docker compose config --quiet
docker compose up -d --wait --wait-timeout 180
docker compose exec -T observer python - <<'PY'
import asyncio
import aiohttp
from retentionbot.api import ServerApi
from retentionbot.broker import Broker
from retentionbot.config import Config
from retentionbot.store import Store
config = Config.from_env()
store = Store(config.database_url)
assert store.get('cursor') is not None
assert store.db.execute('SELECT rolsuper FROM pg_roles WHERE rolname=current_user').fetchone()[0] is False
store.close()
async def check():
    broker = await Broker().connect(config.rabbitmq_url)
    assert broker.queue.name == 'retention.jobs'
    await broker.close()
    async with aiohttp.ClientSession() as session:
        page = await ServerApi(config.synapse_url, config.module_secret, session).feed(None)
        assert isinstance(page['cursor'], int)
asyncio.run(check())
print('Server module, PostgreSQL app role and RabbitMQ: OK')
PY
docker exec -i synapse-smoke /usr/local/bin/python - <<'PY'
import socket
try:
    socket.getaddrinfo('rabbitmq', 5672)
except socket.gaierror:
    print('Synapse cannot resolve the isolated RabbitMQ service: OK')
else:
    raise AssertionError('Synapse can resolve isolated RabbitMQ')
PY
