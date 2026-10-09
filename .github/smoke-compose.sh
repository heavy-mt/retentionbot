#!/bin/bash
set -euo pipefail
export SYNAPSE_NETWORK=retention-smoke
export SYNAPSE_URL=http://synapse-smoke:8008
export DATABASE_PASSWORD=disposable_database_password
export RABBITMQ_PASSWORD=disposable_broker_password
export COMMAND_BOT_USER_ID=@retention:test.invalid
mkdir -p secrets test-runtime/compose-synapse
for name in synapse_module_secret postgres_admin_password; do
    printf '%s\n' 'disposable-compose-smoke-secret-00000000000000' > "secrets/$name"
done
# Secrets are exposed only to assigned containers; the host directory is private.
chmod 700 secrets
chmod 444 secrets/*
cleanup() {
    docker logs --tail=80 synapse-smoke || true
    docker compose --profile commands logs --tail=30 observer worker command-bot || true
    docker compose --profile commands down -v || true
    docker rm -f synapse-smoke >/dev/null 2>&1 || true
    docker network rm retention-smoke >/dev/null 2>&1 || true
}
trap cleanup EXIT
docker network create retention-smoke
docker run --rm --workdir /data --entrypoint /usr/local/bin/python \
    -v "$PWD/test-runtime/compose-synapse:/data" retention-synapse:1.162.0 \
    -m synapse.app.homeserver --server-name test.invalid \
    --config-path /data/homeserver.yaml --generate-config --report-stats=no
docker run --rm -i --entrypoint /usr/local/bin/python \
    -v "$PWD/test-runtime/compose-synapse:/data" retention-synapse:1.162.0 - <<'PY'
from pathlib import Path
import yaml
path = Path('/data/homeserver.yaml')
data = yaml.safe_load(path.read_text())
data.update(
    database={'name': 'sqlite3', 'args': {'database': '/data/synapse.db'}},
    signing_key_path='/data/test.invalid.signing.key',
    media_store_path='/data/media',
    trusted_key_servers=[],
    registration_shared_secret='disposable-compose-registration-secret',
    listeners=[{'port': 8008, 'tls': False, 'type': 'http', 'bind_addresses': ['0.0.0.0'],
                'resources': [{'names': ['client'], 'compress': False}]}],
    retention={'enabled': True, 'default_policy': {'max_lifetime': '7d'}},
    modules=[{'module': 'synapse_retention.module.RetentionModule',
              'config': {'secret_file': '/module-secret', 'cutoff_file': '/data/retention-cutoff',
                         'command_bot_user_id': '@retention:test.invalid'}}],
)
path.write_text(yaml.safe_dump(data))
PY
sudo chown -R 991:991 test-runtime/compose-synapse
docker run -d --name synapse-smoke --network retention-smoke \
    -e SYNAPSE_CONFIG_PATH=/data/homeserver.yaml \
    -v "$PWD/test-runtime/compose-synapse:/data" \
    -v "$PWD/secrets/synapse_module_secret:/module-secret:ro" retention-synapse:1.162.0
docker exec -i synapse-smoke /usr/local/bin/python - <<'PY'
import time
import urllib.error
import urllib.request
deadline = time.monotonic() + 60
while True:
    try:
        with urllib.request.urlopen('http://localhost:8008/health', timeout=2) as response:
            assert response.status == 200
        break
    except urllib.error.URLError:
        if time.monotonic() >= deadline:
            raise
        time.sleep(1)
PY
docker exec -i synapse-smoke /usr/local/bin/python - <<'PY'
import hashlib
import hmac
import json
import urllib.request
from pathlib import Path
base = 'http://localhost:8008/_synapse/admin/v1/register'
for name in ('retention', 'alice', 'bob'):
    with urllib.request.urlopen(base) as response:
        nonce = json.load(response)['nonce']
    password = 'disposable-compose-account-password'
    mac = hmac.new(b'disposable-compose-registration-secret',
                   '\0'.join([nonce, name, password, 'notadmin']).encode(), hashlib.sha1).hexdigest()
    request = urllib.request.Request(base, method='POST', headers={'Content-Type': 'application/json'},
        data=json.dumps({'nonce': nonce, 'username': name, 'password': password,
                         'admin': False, 'mac': mac}).encode())
    with urllib.request.urlopen(request) as response:
        account = json.load(response)
    if name == 'retention':
        Path('/data/command_bot_access_token').write_text(account['access_token'])
    else:
        Path('/data/smoke-' + name + '.json').write_text(json.dumps(account))
PY
docker cp synapse-smoke:/data/command_bot_access_token secrets/command_bot_access_token
chmod 444 secrets/command_bot_access_token
docker compose --profile commands config --quiet
docker compose --profile commands up -d --wait --wait-timeout 180
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
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
alice = json.loads(Path('/data/smoke-alice.json').read_text())
bob = json.loads(Path('/data/smoke-bob.json').read_text())
def request(user, method, path, data=None):
    req = urllib.request.Request('http://localhost:8008/_matrix/client/v3' + path, method=method,
        headers={'Authorization': 'Bearer ' + user['access_token'], 'Content-Type': 'application/json'},
        data=json.dumps(data).encode() if data is not None else None)
    with urllib.request.urlopen(req, timeout=5) as response:
        return json.load(response)
target = request(alice, 'POST', '/createRoom', {'preset': 'private_chat',
    'name': 'Compose retention test', 'invite': [bob['user_id']]})['room_id']
encoded = urllib.parse.quote(target, safe='')
request(bob, 'POST', '/join/' + encoded, {})
personal = request(alice, 'POST', '/createRoom', {'preset': 'private_chat',
    'is_direct': True, 'invite': ['@retention:test.invalid']})['room_id']
dm = urllib.parse.quote(personal, safe='')
deadline = time.monotonic() + 40
while '@retention:test.invalid' not in request(alice, 'GET', '/rooms/' + dm + '/joined_members')['joined']:
    assert time.monotonic() < deadline, 'Command bot did not accept personal invitation'
    time.sleep(1)
request(alice, 'PUT', '/rooms/' + dm + '/send/m.room.message/select',
        {'msgtype': 'm.text', 'body': 'https://matrix.to/#/' + target})
request(alice, 'PUT', '/rooms/' + dm + '/send/m.room.message/set',
        {'msgtype': 'm.text', 'body': '1d'})
while True:
    try:
        policy = request(alice, 'GET', '/rooms/' + encoded + '/state/m.room.retention')
        if policy.get('max_lifetime') == 86400000:
            break
    except urllib.error.HTTPError as error:
        if error.code != 404:
            raise
    assert time.monotonic() < deadline, 'Personal command did not change native retention'
    time.sleep(1)
assert set(request(alice, 'GET', '/rooms/' + encoded + '/joined_members')['joined']) == {
    alice['user_id'], bob['user_id']}
print('Docker personal command bot changed target retention without joining it: OK')
PY
docker compose exec -T command-bot python - <<'PY'
import os
import socket
assert 'SYNAPSE_MODULE_SECRET_FILE' not in os.environ and 'DATABASE_URL' not in os.environ
for host in ('rabbitmq', 'postgres'):
    try:
        socket.getaddrinfo(host, 5672)
    except socket.gaierror:
        pass
    else:
        raise AssertionError('Command bot can resolve isolated ' + host)
print('Command bot has no broker/database access or module master key: OK')
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

