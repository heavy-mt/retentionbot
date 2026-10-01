#!/bin/bash
set -euo pipefail
export MATRIX_HOMESERVER=http://unreachable.invalid
export MATRIX_SERVER_NAME=test.invalid
export BOT_USER_ID=@retention:test.invalid
export DATABASE_PASSWORD=disposable_database_password
export RABBITMQ_PASSWORD=disposable_broker_password
export SYNAPSE_RETENTION_CONFIG_PATH="$PWD/deploy/synapse-retention.yaml.example"
mkdir -p secrets
for name in bot_access_token bot_device_id gateway_secret crypto_store_key synapse_admin_token postgres_admin_password; do
    printf '%s\n' 'disposable-compose-smoke-value' > "secrets/$name"
done
sudo chown 10001:10001 secrets/*
sudo chmod 400 secrets/*
trap 'docker compose down -v' EXIT
docker compose config --quiet
docker compose up -d --wait --wait-timeout 120 postgres rabbitmq admin-gateway worker
docker compose exec -T worker python - <<'PY'
import asyncio
from retentionbot.broker import Broker
from retentionbot.config import Config
from retentionbot.store import Store

config = Config.from_env()
store = Store(config.database_url)
store.set('smoke', 'ok')
assert store.get('smoke') == 'ok'
assert store.db.execute('SELECT rolsuper FROM pg_roles WHERE rolname=current_user').fetchone()[0] is False
store.close()
async def check():
    broker = await Broker().connect(config.rabbitmq_url)
    assert broker.queue.name == 'retention.jobs'
    await broker.close()
asyncio.run(check())
print('PostgreSQL app role and bot RabbitMQ access: OK')
PY
docker compose exec -T admin-gateway python - <<'PY'
import socket
import urllib.request
with urllib.request.urlopen('http://127.0.0.1:8080/health') as response:
    assert response.status == 200
try:
    socket.getaddrinfo('rabbitmq', 5672)
except socket.gaierror:
    print('Gateway cannot resolve the isolated RabbitMQ service: OK')
else:
    raise AssertionError('Gateway can resolve RabbitMQ outside its network')
PY
