-- RAW tables (1:1 with CSV) + batch metadata.
-- No cleaning, no FKs; only technical columns: batch_id, ingestion_time.
-- Light indexes on (batch_id) / natural keys to speed up loads.

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
