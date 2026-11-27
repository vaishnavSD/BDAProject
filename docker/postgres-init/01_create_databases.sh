#!/bin/bash
# Runs automatically on first postgres container boot.
# Creates both databases needed by the stack.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE smart_inventory;
    CREATE DATABASE airflow_db;

    CREATE USER inventory WITH PASSWORD 'inventory';
    GRANT ALL PRIVILEGES ON DATABASE smart_inventory TO inventory;

    CREATE USER airflow WITH PASSWORD 'airflow';
    GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow;

    \echo 'Databases and users created.'
EOSQL

# PostgreSQL 15+ revoked CREATE on the public schema from PUBLIC by default.
# Grant it explicitly so the inventory user can create tables.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname smart_inventory <<-EOSQL
    GRANT CREATE ON SCHEMA public TO inventory;
    GRANT USAGE  ON SCHEMA public TO inventory;
    ALTER DATABASE smart_inventory OWNER TO inventory;
    \echo 'Schema permissions granted to inventory user.'
EOSQL

# Same for airflow_db
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname airflow_db <<-EOSQL
    GRANT CREATE ON SCHEMA public TO airflow;
    GRANT USAGE  ON SCHEMA public TO airflow;
    ALTER DATABASE airflow_db OWNER TO airflow;
    \echo 'Schema permissions granted to airflow user.'
EOSQL
