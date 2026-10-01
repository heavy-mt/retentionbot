#!/bin/sh
set -eu
psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
    --set ON_ERROR_STOP=1 --set bot_password="$DATABASE_PASSWORD" <<'SQL'
CREATE ROLE retention LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD :'bot_password';
REVOKE ALL ON DATABASE retentionbot FROM PUBLIC;
GRANT CONNECT, CREATE, TEMPORARY ON DATABASE retentionbot TO retention;
SQL
