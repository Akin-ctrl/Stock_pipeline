#!/bin/bash
set -e

# This script creates multiple databases in PostgreSQL
# It runs automatically when the postgres container starts for the first time

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Create Airflow database if it doesn't exist
    SELECT 'CREATE DATABASE ${AIRFLOW_DB}'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${AIRFLOW_DB}')\gexec

    -- Grant privileges
    GRANT ALL PRIVILEGES ON DATABASE ${AIRFLOW_DB} TO ${POSTGRES_USER};

    -- Log success
    \echo 'Databases created successfully:'
    \echo '  - ${POSTGRES_DB} (application database)'
    \echo '  - ${AIRFLOW_DB} (airflow database)'
EOSQL
