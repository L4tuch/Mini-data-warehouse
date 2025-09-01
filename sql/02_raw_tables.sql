-- 02_raw_tables.sql
-- Purpose:
--   Create RAW-layer tables (1:1 with CSVs + batch metadata).
--   No constraints, no indexes. All cleaning/typing happens later in STG.
-- 
-- Prerequisite:
--   Schema 'raw' is created in 01_create_schemas.sql

-- raw.customers
CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id     text,
    country         text,
    batch_id        bigint,
    ingestion_time  timestamptz
);

-- raw.products
CREATE TABLE IF NOT EXISTS raw.products (
    stock_code      text,
    description     text,
    unit_price      text,
    batch_id        bigint,
    ingestion_time  timestamptz
);

-- raw.orders
CREATE TABLE IF NOT EXISTS raw.orders (
    invoice_no      text,
    invoice_date    text,
    customer_id     text,
    batch_id        bigint,
    ingestion_time  timestamptz
);

-- raw.order_items
CREATE TABLE IF NOT EXISTS raw.order_items (
    invoice_no      text,
    stock_code      text,
    quantity        text,
    unit_price      text,
    batch_id        bigint,
    ingestion_time  timestamptz
);
