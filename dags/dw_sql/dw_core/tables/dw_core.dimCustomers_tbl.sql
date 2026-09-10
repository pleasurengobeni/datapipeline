-- idempotent DDL for dimCustomers
CREATE SCHEMA IF NOT EXISTS dw_core;

CREATE TABLE IF NOT EXISTS dw_core.dimCustomers (
    id BIGINT PRIMARY KEY,
    customer_key TEXT,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    active_bool BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now()
);
