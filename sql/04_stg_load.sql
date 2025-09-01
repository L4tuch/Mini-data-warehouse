-- Load RAW -> STG for the latest batch (self-contained).
-- Customers/products: keep 1 row per key (latest by ingestion_time).
-- Orders: parse 'DD/MM/YYYY HH24:MI' (Kaggle) and ISO; ignore rows without a date.
-- Idempotent pattern: delete keys from STG that appear in this batch, then insert cleaned rows.


--CUSTOMERS 

-- Delete STG rows whose keys appear in RAW for the latest batch
WITH b AS (
  SELECT GREATEST(
           COALESCE((SELECT MAX(batch_id) FROM raw.customers), 0),
           COALESCE((SELECT MAX(batch_id) FROM raw.products), 0),
           COALESCE((SELECT MAX(batch_id) FROM raw.orders), 0),
           COALESCE((SELECT MAX(batch_id) FROM raw.order_items), 0)
         )::bigint AS current_batch
),
keys AS (
  SELECT DISTINCT (NULLIF(TRIM(rc.customer_id), ''))::numeric::bigint AS customer_id
  FROM raw.customers rc, b
  WHERE rc.batch_id = b.current_batch
    AND NULLIF(TRIM(rc.customer_id), '') IS NOT NULL
)
DELETE FROM stg.customers s
USING keys r
WHERE s.customer_id = r.customer_id;

-- Insert exactly 1 row per customer_id (choose the latest by ingestion_time)
WITH b AS (
  SELECT GREATEST(
           COALESCE((SELECT MAX(batch_id) FROM raw.customers), 0),
           COALESCE((SELECT MAX(batch_id) FROM raw.products), 0),
           COALESCE((SELECT MAX(batch_id) FROM raw.orders), 0),
           COALESCE((SELECT MAX(batch_id) FROM raw.order_items), 0)
         )::bigint AS current_batch
),
ranked AS (
  SELECT
    (NULLIF(TRIM(rc.customer_id), ''))::numeric::bigint AS customer_id,
    NULLIF(TRIM(rc.country), '')                        AS country,
    ROW_NUMBER() OVER (
      PARTITION BY (NULLIF(TRIM(rc.customer_id), ''))::numeric::bigint
      ORDER BY rc.ingestion_time DESC NULLS LAST
    ) AS rn
  FROM raw.customers rc, b
  WHERE rc.batch_id = b.current_batch
    AND NULLIF(TRIM(rc.customer_id), '') IS NOT NULL
)
INSERT INTO stg.customers (customer_id, country)
SELECT customer_id, country
FROM ranked
WHERE rn = 1;

--PRODUCTS

-- Delete STG rows whose keys appear in RAW for the latest batch
WITH b AS (
  SELECT GREATEST(
           COALESCE((SELECT MAX(batch_id) FROM raw.customers), 0),
           COALESCE((SELECT MAX(batch_id) FROM raw.products), 0),
           COALESCE((SELECT MAX(batch_id) FROM raw.orders), 0),
           COALESCE((SELECT MAX(batch_id) FROM raw.order_items), 0)
         )::bigint AS current_batch
),
keys AS (
  SELECT DISTINCT NULLIF(TRIM(rp.stock_code), '') AS stock_code
  FROM raw.products rp, b
  WHERE rp.batch_id = b.current_batch
    AND NULLIF(TRIM(rp.stock_code), '') IS NOT NULL
)
DELETE FROM stg.products s
USING keys r
WHERE s.stock_code = r.stock_code;

-- Insert exactly 1 row per stock_code (latest by ingestion_time)
WITH b AS (
  SELECT GREATEST(
           COALESCE((SELECT MAX(batch_id) FROM raw.customers), 0),
           COALESCE((SELECT MAX(batch_id) FROM raw.products), 0),
           COALESCE((SELECT MAX(batch_id) FROM raw.orders), 0),
           COALESCE((SELECT MAX(batch_id) FROM raw.order_items), 0)
         )::bigint AS current_batch
),
ranked AS (
  SELECT
    NULLIF(TRIM(rp.stock_code), '')                  AS stock_code,
    NULLIF(TRIM(rp.description), '')                 AS description,
    (NULLIF(TRIM(rp.unit_price), ''))::numeric(12,4) AS unit_price,
    ROW_NUMBER() OVER (
      PARTITION BY NULLIF(TRIM(rp.stock_code), '')
      ORDER BY rp.ingestion_time DESC NULLS LAST
    ) AS rn
  FROM raw.products rp, b
  WHERE rp.batch_id = b.current_batch
    AND NULLIF(TRIM(rp.stock_code), '') IS NOT NULL
)
INSERT INTO stg.products (stock_code, description, unit_price)
SELECT stock_code, description, unit_price
FROM ranked
WHERE rn = 1;

--ORDERS

-- Delete STG rows whose keys appear in RAW for the latest batch
WITH b AS (
  SELECT GREATEST(
           COALESCE((SELECT MAX(batch_id) FROM raw.customers), 0),
           COALESCE((SELECT MAX(batch_id) FROM raw.products), 0),
           COALESCE((SELECT MAX(batch_id) FROM raw.orders), 0),
           COALESCE((SELECT MAX(batch_id) FROM raw.order_items), 0)
         )::bigint AS current_batch
),
keys AS (
  SELECT DISTINCT NULLIF(TRIM(ro.invoice_no), '') AS invoice_no
  FROM raw.orders ro, b
  WHERE ro.batch_id = b.current_batch
    AND NULLIF(TRIM(ro.invoice_no), '') IS NOT NULL
)
DELETE FROM stg.orders s
USING keys r
WHERE s.invoice_no = r.invoice_no;

-- Insert cleaned orders (timestamp parsing for two common formats)
WITH b AS (
  SELECT GREATEST(
           COALESCE((SELECT MAX(batch_id) FROM raw.customers), 0),
           COALESCE((SELECT MAX(batch_id) FROM raw.products), 0),
           COALESCE((SELECT MAX(batch_id) FROM raw.orders), 0),
           COALESCE((SELECT MAX(batch_id) FROM raw.order_items), 0)
         )::bigint AS current_batch
)
INSERT INTO stg.orders (invoice_no, invoice_date, customer_id)
SELECT DISTINCT
  NULLIF(TRIM(ro.invoice_no), '') AS invoice_no,
  CASE
    WHEN NULLIF(TRIM(ro.invoice_date), '') IS NULL THEN NULL
    WHEN POSITION('/' IN ro.invoice_date) > 0
      THEN to_timestamp(NULLIF(TRIM(ro.invoice_date), ''), 'DD/MM/YYYY HH24:MI')
    ELSE to_timestamp(NULLIF(TRIM(ro.invoice_date), ''), 'YYYY-MM-DD HH24:MI:SS')
  END                                                   AS invoice_date,
  (NULLIF(TRIM(ro.customer_id), ''))::numeric::bigint   AS customer_id
FROM raw.orders ro, b
WHERE ro.batch_id = b.current_batch
  AND NULLIF(TRIM(ro.invoice_no), '') IS NOT NULL;

--  ORDER ITEMS

-- Delete STG rows whose keys appear in RAW for the latest batch
WITH b AS (
  SELECT GREATEST(
           COALESCE((SELECT MAX(batch_id) FROM raw.customers), 0),
           COALESCE((SELECT MAX(batch_id) FROM raw.products), 0),
           COALESCE((SELECT MAX(batch_id) FROM raw.orders), 0),
           COALESCE((SELECT MAX(batch_id) FROM raw.order_items), 0)
         )::bigint AS current_batch
),
keys AS (
  SELECT DISTINCT
    NULLIF(TRIM(roi.invoice_no), '') AS invoice_no,
    NULLIF(TRIM(roi.stock_code), '') AS stock_code
  FROM raw.order_items roi, b
  WHERE roi.batch_id = b.current_batch
    AND NULLIF(TRIM(roi.invoice_no), '') IS NOT NULL
    AND NULLIF(TRIM(roi.stock_code), '') IS NOT NULL
)
DELETE FROM stg.order_items s
USING keys r
WHERE s.invoice_no = r.invoice_no
  AND s.stock_code = r.stock_code;

-- Insert deduplicated line items (we keep negative unit_price for adjustments)
WITH b AS (
  SELECT GREATEST(
           COALESCE((SELECT MAX(batch_id) FROM raw.customers), 0),
           COALESCE((SELECT MAX(batch_id) FROM raw.products), 0),
           COALESCE((SELECT MAX(batch_id) FROM raw.orders), 0),
           COALESCE((SELECT MAX(batch_id) FROM raw.order_items), 0)
         )::bigint AS current_batch
)
INSERT INTO stg.order_items (invoice_no, stock_code, quantity, unit_price)
SELECT DISTINCT
  NULLIF(TRIM(roi.invoice_no), '')               AS invoice_no,
  NULLIF(TRIM(roi.stock_code), '')               AS stock_code,
  (NULLIF(TRIM(roi.quantity), ''))::int          AS quantity,
  (NULLIF(TRIM(roi.unit_price), ''))::numeric(12,4) AS unit_price
FROM raw.order_items roi, b
WHERE roi.batch_id = b.current_batch
  AND NULLIF(TRIM(roi.invoice_no), '') IS NOT NULL
  AND NULLIF(TRIM(roi.stock_code), '') IS NOT NULL
  AND (NULLIF(TRIM(roi.quantity), ''))::int > 0;
