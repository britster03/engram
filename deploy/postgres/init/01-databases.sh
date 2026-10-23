#!/bin/sh
set -eu

# docker-entrypoint-initdb.d runs this once on an empty Postgres volume.
psql --username "$POSTGRES_USER" --dbname postgres \
  --set=engram_password="$ENGRAM_DB_PASSWORD" \
  --set=engram_dispatcher_password="$ENGRAM_DB_PASSWORD" \
  --set=engram_migrator_password="$ENGRAM_DB_PASSWORD" \
  --set=temporal_password="$ENGRAM_DB_PASSWORD" <<'SQL'
CREATE ROLE engram_app LOGIN PASSWORD :'engram_password';
CREATE ROLE engram_dispatcher LOGIN PASSWORD :'engram_dispatcher_password';
CREATE ROLE engram_migrator LOGIN PASSWORD :'engram_migrator_password';
CREATE ROLE temporal_app LOGIN PASSWORD :'temporal_password';
CREATE DATABASE engram OWNER engram_app;
CREATE DATABASE temporal OWNER temporal_app;
CREATE DATABASE temporal_visibility OWNER temporal_app;
REVOKE CONNECT ON DATABASE engram FROM PUBLIC;
REVOKE CONNECT ON DATABASE temporal FROM PUBLIC;
REVOKE CONNECT ON DATABASE temporal_visibility FROM PUBLIC;
GRANT CONNECT ON DATABASE engram TO engram_app, engram_dispatcher, engram_migrator;
GRANT CONNECT ON DATABASE temporal, temporal_visibility TO temporal_app;

\connect engram
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE, CREATE ON SCHEMA public TO engram_app;
GRANT USAGE ON SCHEMA public TO engram_dispatcher;
ALTER DEFAULT PRIVILEGES FOR ROLE engram_app IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO engram_app;
SQL
