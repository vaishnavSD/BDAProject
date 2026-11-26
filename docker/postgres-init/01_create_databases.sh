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
