-- 01_create_schemas.sql
-- Purpose:
--   Creates three schemas in PostgreSQL:
--     - raw : landing area (raw CSV data, 1:1 with source + batch metadata)
--     - stg : staging area (cleaned, typed, deduplicated)
--     - dw  : data warehouse layer (star schema: dimensions + facts)
--
-- Notes:
--   Run this script once when initializing the databa
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS dw;