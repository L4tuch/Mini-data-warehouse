-- DW star schema: SCD1 dimensions + two facts.
-- dim_customer, dim_product, dim_date; fact_sales (>=0 price), fact_adjustments (negative/“B”).
-- Analytics indexes on date/customer/product FKs.

-- Dimension: Customer (SCD1)
CREATE TABLE IF NOT EXISTS dw.dim_customer (
  customer_sk  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  customer_id  BIGINT UNIQUE,   -- natural key from STG
  country      TEXT,
  created_at   TIMESTAMPTZ DEFAULT NOW(),
  updated_at   TIMESTAMPTZ DEFAULT NOW()
);

-- Dimension: Product (SCD1)
CREATE TABLE IF NOT EXISTS dw.dim_product (
  product_sk   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  stock_code   TEXT UNIQUE,     -- natural key from STG
  description  TEXT,
  unit_price   NUMERIC(12,4),
  created_at   TIMESTAMPTZ DEFAULT NOW(),
  updated_at   TIMESTAMPTZ DEFAULT NOW()
);

-- Dimension: Date
CREATE TABLE IF NOT EXISTS dw.dim_date (
  date_key     INT PRIMARY KEY, -- YYYYMMDD
  full_date    DATE NOT NULL UNIQUE,
  year         INT  NOT NULL,
  month        INT  NOT NULL,
  day          INT  NOT NULL,
  month_name   TEXT NOT NULL
);

-- Fact: Sales (only positive prices)
CREATE TABLE IF NOT EXISTS dw.fact_sales (
  sales_sk     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  invoice_no   TEXT NOT NULL,
  line_no      INT  NOT NULL,
  customer_sk  BIGINT NOT NULL REFERENCES dw.dim_customer(customer_sk),
  product_sk   BIGINT NOT NULL REFERENCES dw.dim_product(product_sk),
  date_key     INT    NOT NULL REFERENCES dw.dim_date(date_key),
  quantity     INT    NOT NULL CHECK (quantity > 0),
  unit_price   NUMERIC(12,4) NOT NULL CHECK (unit_price >= 0),
  amount       NUMERIC(14,4) NOT NULL,
  batch_id     BIGINT,
  loaded_at    TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE (invoice_no, line_no)  -- one line per invoice/line
);

-- Fact: Adjustments (negative prices / special codes)
CREATE TABLE IF NOT EXISTS dw.fact_adjustments (
  adj_sk       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  invoice_no   TEXT NOT NULL,
  line_no      INT  NOT NULL,
  customer_sk  BIGINT NOT NULL REFERENCES dw.dim_customer(customer_sk),
  product_sk   BIGINT NOT NULL REFERENCES dw.dim_product(product_sk),
  date_key     INT    NOT NULL REFERENCES dw.dim_date(date_key),
  quantity     INT    NOT NULL CHECK (quantity > 0),
  unit_price   NUMERIC(12,4) NOT NULL, -- may be negative
  amount       NUMERIC(14,4) NOT NULL, -- may be negative
  batch_id     BIGINT,
  loaded_at    TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE (invoice_no, line_no)
);

-- Helpful indexes for analytics
CREATE INDEX IF NOT EXISTS ix_fact_sales_date ON dw.fact_sales(date_key);
CREATE INDEX IF NOT EXISTS ix_fact_sales_customer ON dw.fact_sales(customer_sk);
CREATE INDEX IF NOT EXISTS ix_fact_sales_product ON dw.fact_sales(product_sk);

CREATE INDEX IF NOT EXISTS ix_fact_adj_date ON dw.fact_adjustments(date_key);
CREATE INDEX IF NOT EXISTS ix_fact_adj_customer ON dw.fact_adjustments(customer_sk);
CREATE INDEX IF NOT EXISTS ix_fact_adj_product ON dw.fact_adjustments(product_sk);
