-- Hard data-quality assertions; raise on failure.
-- Checks: non-empty dims, price sign rules per fact, amount = qty*price, valid date_key FKs.

DO $$
DECLARE
  v_cnt INT;
BEGIN
  -- dims must not be empty
  SELECT COUNT(*) INTO v_cnt FROM dw.dim_customer; IF v_cnt = 0 THEN RAISE EXCEPTION 'dim_customer is empty'; END IF;
  SELECT COUNT(*) INTO v_cnt FROM dw.dim_product;  IF v_cnt = 0 THEN RAISE EXCEPTION 'dim_product is empty';  END IF;
  SELECT COUNT(*) INTO v_cnt FROM dw.dim_date;     IF v_cnt = 0 THEN RAISE EXCEPTION 'dim_date is empty';     END IF;

  -- fact_sales: no negative prices
  SELECT COUNT(*) INTO v_cnt FROM dw.fact_sales WHERE unit_price < 0;
  IF v_cnt > 0 THEN RAISE EXCEPTION 'fact_sales contains negative unit_price'; END IF;

  -- fact_adjustments: should contain only adjustments (negative prices OR special code 'B')
  -- (We don’t have stock_code in facts; we enforce sign-only here)
  SELECT COUNT(*) INTO v_cnt FROM dw.fact_adjustments WHERE unit_price >= 0;
  IF v_cnt > 0 THEN RAISE EXCEPTION 'fact_adjustments contains non-negative prices'; END IF;

  -- amount must equal quantity * unit_price in both facts
  SELECT COUNT(*) INTO v_cnt FROM dw.fact_sales WHERE amount <> quantity * unit_price;
  IF v_cnt > 0 THEN RAISE EXCEPTION 'fact_sales amount mismatch'; END IF;

  SELECT COUNT(*) INTO v_cnt FROM dw.fact_adjustments WHERE amount <> quantity * unit_price;
  IF v_cnt > 0 THEN RAISE EXCEPTION 'fact_adjustments amount mismatch'; END IF;

  -- date_key must exist in dim_date (FK already enforces, but double-check)
  SELECT COUNT(*) INTO v_cnt
  FROM dw.fact_sales fs LEFT JOIN dw.dim_date d ON d.date_key = fs.date_key
  WHERE d.date_key IS NULL;
  IF v_cnt > 0 THEN RAISE EXCEPTION 'fact_sales has invalid date_key'; END IF;

  SELECT COUNT(*) INTO v_cnt
  FROM dw.fact_adjustments fa LEFT JOIN dw.dim_date d ON d.date_key = fa.date_key
  WHERE d.date_key IS NULL;
  IF v_cnt > 0 THEN RAISE EXCEPTION 'fact_adjustments has invalid date_key'; END IF;

END $$;
