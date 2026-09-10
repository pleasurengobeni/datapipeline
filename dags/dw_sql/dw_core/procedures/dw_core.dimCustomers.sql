-- Example procedure to load dw_core.dimCustomers
-- Replace with your ETL SQL: transforms, conforming keys, SCD logic etc.
CREATE OR REPLACE PROCEDURE dw_core.dimCustomer()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Example: a quick upsert placeholder; replace with real logic
    RAISE NOTICE 'Running load for dw_core.dimCustomers';
    -- Implementation details:
    -- 1) populate staging
    -- 2) dedupe & conform
    -- 3) merge into dw_core.dimCustomers
END;
$$;
