#!/bin/bash
set -e

# Ensure the environment variables are available
: "${POSTGRES_DB?Variable POSTGRES_DB is not set}"
: "${POSTGRES_USER?Variable POSTGRES_USER is not set}"

# Use psql to run SQL commands against the postgres database
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" <<-EOSQL
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_database WHERE datname = '${POSTGRES_DB}') THEN
            CREATE DATABASE "${POSTGRES_DB}" WITH OWNER = '${POSTGRES_USER}' ENCODING = 'UTF8';
        END IF;
    END
    \$\$;
EOSQL

# Connect to the newly created or verified database to configure the schema and permissions
psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${POSTGRES_DB}" <<-EOSQL
    -- Create the schema if it doesn't exist
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pipeline') THEN
            CREATE SCHEMA pipeline AUTHORIZATION "${POSTGRES_USER}";
        END IF;
    END
    \$\$;

    -- Revoke all privileges from PUBLIC
    REVOKE ALL ON SCHEMA pipeline FROM PUBLIC;
    REVOKE ALL ON ALL TABLES IN SCHEMA pipeline FROM PUBLIC;

    -- Grant USAGE on the schema to the user
    GRANT USAGE ON SCHEMA pipeline TO "${POSTGRES_USER}";

    -- Grant SELECT, INSERT, UPDATE, DELETE on all current tables in the schema to the user
    GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA pipeline TO "${POSTGRES_USER}";

    -- Set default privileges for future tables in the schema for the user
    ALTER DEFAULT PRIVILEGES IN SCHEMA pipeline GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO "${POSTGRES_USER}";
EOSQL
