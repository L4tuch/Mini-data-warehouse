-- 01_create_schemas.sql
-- Purpose:
--   Creates two schemas in PostgreSQL:
--     - stg: staging area (raw data loaded from CSVs)
--     - dw : data warehouse layer (star schema: dimensions + facts)
-- Notes:
--   Run this script once when initializing the database.

CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS dw;