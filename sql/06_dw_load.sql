-- Load STG -> DW (latest batch).
-- Upsert SCD1 dims; build dim_date from non-null order dates only.
-- fact_sales: exclude negative price and code 'B'; fact_adjustments: keep negative/'B'.
-- Idempotent by invoice set for the current batch (delete+insert).

-- dim_date 
-- Build date range ONLY from rows that have a valid invoice_date
WITH bounds AS (
  SELECT
    DATE_TRUNC('day', MIN(invoice_date))::date AS min_d,
    DATE_TRUNC('day', MAX(invoice_date))::date AS max_d
  FROM stg.orders
  WHERE invoice_date IS NOT NULL
),
series AS (
  SELECT generate_series(min_d, max_d, '1 day'::interval)::date AS d
  FROM bounds
)
INSERT INTO dw.dim_date (date_key, full_date, year, month, day, month_name)
SELECT
  EXTRACT(YEAR FROM d)::int * 10000 + EXTRACT(MONTH FROM d)::int * 100 + EXTRACT(DAY FROM d)::int AS date_key,
  d,
  EXTRACT(YEAR FROM d)::int,
  EXTRACT(MONTH FROM d)::int,
  EXTRACT(DAY FROM d)::int,
  TO_CHAR(d, 'Mon')
FROM series
ON CONFLICT (full_date) DO NOTHING;

-- dim_customer (SCD1) 
INSERT INTO dw.dim_customer (customer_id, country)
SELECT c.customer_id, c.country
FROM stg.customers c
ON CONFLICT (customer_id)
DO UPDATE SET
  country    = EXCLUDED.country,
  updated_at = NOW();

--dim_product (SCD1) 
INSERT INTO dw.dim_product (stock_code, description, unit_price)
SELECT p.stock_code, p.description, p.unit_price
FROM stg.products p
ON CONFLICT (stock_code)
DO UPDATE SET
  description = EXCLUDED.description,
  unit_price  = EXCLUDED.unit_price,
  updated_at  = NOW();

-- helper: current_batch 
-- We will repeatedly compute latest RAW batch in small CTEs below.

-- delete facts for batch
WITH b AS (
  SELECT GREATEST(
           COALESCE((SELECT MAX(batch_id) FROM raw.customers), 0),
           COALESCE((SELECT MAX(batch_id) FROM raw.products), 0),
           COALESCE((SELECT MAX(batch_id) FROM raw.orders), 0),
           COALESCE((SELECT MAX(batch_id) FROM raw.order_items), 0)
         )::bigint AS current_batch
),
inv_keys AS (
  SELECT DISTINCT ro.invoice_no
  FROM raw.orders ro, b
  WHERE ro.batch_id = b.current_batch
    AND ro.invoice_no IS NOT NULL
)
DELETE FROM dw.fact_sales fs
USING inv_keys k
WHERE fs.invoice_no = k.invoice_no;

WITH b AS (
  SELECT GREATEST(
           COALESCE((SELECT MAX(batch_id) FROM raw.customers), 0),
           COALESCE((SELECT MAX(batch_id) FROM raw.products), 0),
           COALESCE((SELECT MAX(batch_id) FROM raw.orders), 0),
           COALESCE((SELECT MAX(batch_id) FROM raw.order_items), 0)
         )::bigint AS current_batch
),
inv_keys AS (
  SELECT DISTINCT ro.invoice_no
  FROM raw.orders ro, b
  WHERE ro.batch_id = b.current_batch
    AND ro.invoice_no IS NOT NULL
)
DELETE FROM dw.fact_adjustments fa
USING inv_keys k
WHERE fa.invoice_no = k.invoice_no;

--  fact_sales 
WITH b AS (
  SELECT GREATEST(
           COALESCE((SELECT MAX(batch_id) FROM raw.customers), 0),
           COALESCE((SELECT MAX(batch_id) FROM raw.products), 0),
           COALESCE((SELECT MAX(batch_id) FROM raw.orders), 0),
           COALESCE((SELECT MAX(batch_id) FROM raw.order_items), 0)
         )::bigint AS current_batch
),
src AS (
  SELECT
    oi.invoice_no,
    oi.stock_code,
    o.invoice_date::date AS d,
    o.customer_id,
    oi.quantity,
    oi.unit_price
  FROM stg.order_items oi
  JOIN stg.orders o
    ON o.invoice_no = oi.invoice_no
   AND o.invoice_date IS NOT NULL                      -- <<< ensure non-null date
  JOIN raw.orders ro
    ON ro.invoice_no = o.invoice_no, b
  WHERE ro.batch_id = b.current_batch
),
rn AS (
  SELECT
    s.*,
    ROW_NUMBER() OVER (PARTITION BY invoice_no ORDER BY stock_code) AS line_no
  FROM src s
  WHERE s.unit_price >= 0 AND s.stock_code <> 'B'                   -- sales only
)
INSERT INTO dw.fact_sales (
  invoice_no, line_no, customer_sk, product_sk, date_key,
  quantity, unit_price, amount, batch_id
)
SELECT
  r.invoice_no,
  r.line_no,
  dc.customer_sk,
  dp.product_sk,
  (EXTRACT(YEAR FROM r.d)::int * 10000
   + EXTRACT(MONTH FROM r.d)::int * 100
   + EXTRACT(DAY FROM r.d)::int) AS date_key,
  r.quantity,
  r.unit_price,
  (r.quantity * r.unit_price) AS amount,
  b.current_batch
FROM rn r
JOIN dw.dim_customer dc ON dc.customer_id = r.customer_id
JOIN dw.dim_product  dp ON dp.stock_code  = r.stock_code, b;

-- fact_adjustments 
WITH b AS (
  SELECT GREATEST(
           COALESCE((SELECT MAX(batch_id) FROM raw.customers), 0),
           COALESCE((SELECT MAX(batch_id) FROM raw.products), 0),
           COALESCE((SELECT MAX(batch_id) FROM raw.orders), 0),
           COALESCE((SELECT MAX(batch_id) FROM raw.order_items), 0)
         )::bigint AS current_batch
),
src AS (
  SELECT
    oi.invoice_no,
    oi.stock_code,
    o.invoice_date::date AS d,
    o.customer_id,
    oi.quantity,
    oi.unit_price
  FROM stg.order_items oi
  JOIN stg.orders o
    ON o.invoice_no = oi.invoice_no
   AND o.invoice_date IS NOT NULL                      -- <<< ensure non-null date
  JOIN raw.orders ro
    ON ro.invoice_no = o.invoice_no, b
  WHERE ro.batch_id = b.current_batch
),
rn AS (
  SELECT
    s.*,
    ROW_NUMBER() OVER (PARTITION BY invoice_no ORDER BY stock_code) AS line_no
  FROM src s
  WHERE (s.unit_price < 0) OR (s.stock_code = 'B')                   -- adjustments only
)
INSERT INTO dw.fact_adjustments (
  invoice_no, line_no, customer_sk, product_sk, date_key,
  quantity, unit_price, amount, batch_id
)
SELECT
  r.invoice_no,
  r.line_no,
  dc.customer_sk,
  dp.product_sk,
  (EXTRACT(YEAR FROM r.d)::int * 10000
   + EXTRACT(MONTH FROM r.d)::int * 100
   + EXTRACT(DAY FROM r.d)::int) AS date_key,
  r.quantity,
  r.unit_price,
  (r.quantity * r.unit_price) AS amount,
  b.current_batch
FROM rn r
JOIN dw.dim_customer dc ON dc.customer_id = r.customer_id
JOIN dw.dim_product  dp ON dp.stock_code  = r.stock_code, b;
