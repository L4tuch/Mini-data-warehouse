-- Analytical views:
-- v_monthly_revenue (sales), v_aov_by_month (sales), v_top_products_30d (sales, last 30d).

-- Monthly revenue (sales only)
CREATE OR REPLACE VIEW dw.v_monthly_revenue AS
SELECT
  d.year,
  d.month,
  TO_CHAR(d.full_date, 'YYYY-MM') AS month_label,
  SUM(fs.amount) AS revenue
FROM dw.fact_sales fs
JOIN dw.dim_date d ON d.date_key = fs.date_key
GROUP BY d.year, d.month, month_label
ORDER BY d.year, d.month;

-- Average Order Value (AOV) by month (sales only)
CREATE OR REPLACE VIEW dw.v_aov_by_month AS
WITH per_invoice AS (
  SELECT fs.invoice_no, SUM(fs.amount) AS invoice_amount,
         MIN(d.full_date) AS order_date
  FROM dw.fact_sales fs
  JOIN dw.dim_date d ON d.date_key = fs.date_key
  GROUP BY fs.invoice_no
)
SELECT
  EXTRACT(YEAR FROM order_date)::int AS year,
  EXTRACT(MONTH FROM order_date)::int AS month,
  TO_CHAR(order_date, 'YYYY-MM') AS month_label,
  AVG(invoice_amount) AS aov
FROM per_invoice
GROUP BY year, month, month_label
ORDER BY year, month;

-- Top products by revenue (last 30 days)
CREATE OR REPLACE VIEW dw.v_top_products_30d AS
WITH last_day AS (
  SELECT MAX(full_date) AS max_d FROM dw.dim_date
),
cut AS (
  SELECT (SELECT max_d FROM last_day) - INTERVAL '30 days' AS d_from
)
SELECT
  dp.stock_code,
  dp.description,
  SUM(fs.amount) AS revenue_30d,
  SUM(fs.quantity) AS qty_30d
FROM dw.fact_sales fs
JOIN dw.dim_product dp ON dp.product_sk = fs.product_sk
JOIN dw.dim_date d     ON d.date_key   = fs.date_key,
     cut
WHERE d.full_date > cut.d_from
GROUP BY dp.stock_code, dp.description
ORDER BY revenue_30d DESC
LIMIT 20;
