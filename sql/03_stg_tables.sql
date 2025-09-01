-- STG tables: typed, cleaned, deduplicated with natural PKs.
-- One row per key (customer_id, stock_code, invoice_no, invoice_no+stock_code).
-- Keep negative unit_price in order_items (adjustments live here; filtered later in DW).
-- Customers: exactly 1 row per customer_id
CREATE TABLE IF NOT EXISTS stg.customers (
  customer_id  bigint PRIMARY KEY,
  country      text
);

-- Products: exactly 1 row per stock_code
CREATE TABLE IF NOT EXISTS stg.products (
  stock_code   text PRIMARY KEY,
  description  text,
  unit_price   numeric(12,4)
);

-- Orders: exactly 1 row per invoice_no
CREATE TABLE IF NOT EXISTS stg.orders (
  invoice_no   text PRIMARY KEY,
  invoice_date timestamptz,
  customer_id  bigint
);

-- Order items: composite PK = (invoice_no, stock_code)
-- NOTE: we DO NOT enforce unit_price >= 0 here (to keep adjustments).
CREATE TABLE IF NOT EXISTS stg.order_items (
  invoice_no   text,
  stock_code   text,
  quantity     integer CHECK (quantity > 0),
  unit_price   numeric(12,4),
  PRIMARY KEY (invoice_no, stock_code)
);

-- Helpful index for joins (optional)
CREATE INDEX IF NOT EXISTS ix_stg_orders_customer ON stg.orders(customer_id);

-- If an older version created a CHECK on unit_price, drop it now (idempotent).
ALTER TABLE stg.order_items
  DROP CONSTRAINT IF EXISTS order_items_unit_price_check;
