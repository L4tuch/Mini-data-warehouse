-- 02_stg_tables.sql
-- Purpose:
--   Define raw staging tables in schema "stg".
--   Each table mirrors the raw CSVs produced by extract.py (no FKs, no business logic).
-- Notes:
--   - Keep types simple; we'll clean/transform in DW layer.
--   - Load order: customers, products, orders, order_items.

-- Customers (CustomerID, Country)
CREATE TABLE IF NOT EXISTS stg.customers (
  customer_id TEXT,
  country     TEXT
);

-- Products (StockCode, Description)
CREATE TABLE IF NOT EXISTS stg.products (
  stock_code  TEXT,
  description TEXT
);

-- Orders (InvoiceNo, InvoiceDate, CustomerID, Country)
CREATE TABLE IF NOT EXISTS stg.orders (
  invoice_no    TEXT,
  invoice_date  TIMESTAMP,
  customer_id   TEXT,
  country       TEXT
);

-- Order items (InvoiceNo, StockCode, Quantity, UnitPrice)
CREATE TABLE IF NOT EXISTS stg.order_items (
  invoice_no  TEXT,
  stock_code  TEXT,
  quantity    INTEGER,
  unit_price  NUMERIC(12,4)
);