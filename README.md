# Mini Data Warehouse (Python + PostgreSQL)

**Goal:** build a realistic, reproducible ETL/ELT pipeline with clear layers **RAW → STG → DW**, batch-based loading, data quality checks, and KPI views.

---

---
## 📜 Licensing & Data Use

- **Dataset:** “E-Commerce Data (carrie1)” on Kaggle. All rights in the data remain with the original author/uploader.
- **License status:** The dataset is marked as **Unknown** on Kaggle. Therefore, this repository does **not** include any original data files. Users who wish to run the project must download the data directly from Kaggle and accept the dataset’s terms.
- **Repository contents:** Code only (ETL/ELT, SQL, views). No CSVs or database dumps are distributed here.
- **Purpose:** Educational and portfolio use. The project demonstrates a data-engineering workflow (RAW → STG → DW) locally on your machine.
- **Aggregated outputs:** The README may show high-level, illustrative KPIs (e.g., monthly revenue, AOV, top products). These are aggregate statistics intended **not** to reveal or enable reconstruction of any substantial portion of the dataset.
- **Attribution:** Please credit the dataset author (“carrie1”) and link to the dataset page when referring to the source data.
- **If you are the rightsholder:** Contact me if you believe any part of this repository should be adjusted or removed to better respect your rights.


## 📂 Data Source

- **E-Commerce Data (carrie1)** — Kaggle
- Period: UK online store transactions (Dec 2010 – Dec 2011)
- **License note:** Kaggle marks this dataset as **Unknown** → the raw CSV is **not included** in the repo.

**Run locally:**

1. Download `data.csv` from Kaggle.
2. Put it at `data/external/data.csv`.
3. Used **strictly for education/portfolio**.

---

## 🔧 Design Decisions

- **Layers**
  - **RAW:** landing zone, 1:1 with source files + `batch_id` & `ingestion_time`. No cleaning, no FKs.
  - **STG:** typed, cleaned, deduplicated; **natural PKs** (`customer_id`, `stock_code`, `invoice_no`, `(invoice_no, stock_code)`).
  - **DW:** star schema with SCD1 dims and **two facts**:
    - `dw.fact_sales` — normal sales (non-negative prices).
    - `dw.fact_adjustments` — accounting adjustments/credit notes (e.g., _Adjust bad debt_), where prices may be negative.
- **Idempotency:** for the **latest batch** the loaders perform **delete+insert** by keys. Re-running the same batch yields the same result.
- **Dataset specifics handled**
  - Some `customer_id` values appear with different `country` in RAW → STG keeps **exactly one row per customer** (the latest by `ingestion_time`).
  - Negative `unit_price` occurs for adjustments (e.g., special code `'B'`) → kept in STG, modeled in `fact_adjustments` in DW.
  - Some orders miss a date → DW only loads rows with a valid date (so `date_key` is never NULL).

---

## 📁 Repository Layout

    mini-data-warehouse/
    ├── config/
    │   ├── .env.example           # DB connection template
    │   └── .env                   # local secrets (ignored by git)
    ├── data/
    │   ├── external/              # Kaggle input (ignored by git)
    │   └── raw/                   # split CSVs from extract (ignored by git)
    ├── sql/
    │   ├── 01_create_schemas.sql
    │   ├── 02_raw_tables.sql
    │   ├── 03_stg_tables.sql
    │   ├── 04_stg_load.sql
    │   ├── 05_dw_tables.sql
    │   ├── 06_dw_load.sql
    │   ├── 07_quality_checks.sql
    │   └── 08_views_kpis.sql
    ├── src/
    │   ├── extract.py             # split data.csv → 4 CSVs + batch metadata
    │   ├── load_to_raw.py         # COPY into raw.*
    │   ├── load_to_stg.py         # runs 04_stg_load.sql
    │   ├── run_sql.py             # runs 05..08
    │   └── main.py                # end-to-end: extract → raw → stg → dw → checks → views
    ├── README.md
    ├── requirements.txt
    └── .gitignore

---

## 📦 Prerequisites

- Python **3.10+**
- PostgreSQL **14–16** (tested locally)
- `pip` (virtualenv recommended)

---

## ⚙️ Configuration

Copy and edit your local environment file:

    cp config/.env.example config/.env

Example `config/.env`:

    DB_HOST=127.0.0.1
    DB_PORT=5432
    DB_NAME=mini_dwh
    DB_USER=postgres
    DB_PASSWORD=your_password

Ensure a local Postgres instance exists and database `mini_dwh` is created.

---

## 🚀 How to Run

### A) Full pipeline (from CSV to KPI views)

    python src/main.py

Steps:

1. `extract.py` → writes `data/raw/*.csv` (with `batch_id`, `ingestion_time`)
2. `load_to_raw.py` → loads CSVs into `raw.*` via `COPY`
3. `03_stg_tables.sql` → STG DDL (idempotent)
4. `04_stg_load.sql` → RAW → STG (cleaning, typing, dedup, auto “latest batch”)
5. `05_dw_tables.sql` → DW DDL (dims & facts)
6. `06_dw_load.sql` → STG → DW (SCD1 upserts, `dim_date`, facts)
7. `07_quality_checks.sql` → hard assertions
8. `08_views_kpis.sql` → analytical views

### B) Only the DW layer (when STG is ready)

    python src/run_sql.py

---

## ✅ Data Quality Checks (examples)

- `dw.dim_*` not empty.
- `dw.fact_sales` — **no** negative `unit_price`.
- `dw.fact_adjustments` — **only** negative `unit_price` (and/or special codes).
- `amount = quantity * unit_price` in both facts.
- Valid `date_key` present in `dw.dim_date`.

`sql/07_quality_checks.sql` raises **exceptions** on violations.

---

## 📈 KPI Views (queries + sample outputs)

> Sample values below are **illustrative**. Your numbers depend on the data loaded.

### 1) Monthly revenue (sales only)

    SELECT * FROM dw.v_monthly_revenue LIMIT 12;

| year | month | month_label | revenue    |
| ---: | ----: | ----------- | ---------- |
| 2010 |    12 | 2010-12     | 230,450.75 |
| 2011 |    01 | 2011-01     | 178,320.10 |
| 2011 |    02 | 2011-02     | 165,912.55 |
| 2011 |    03 | 2011-03     | 192,480.30 |
| 2011 |    04 | 2011-04     | 201,115.90 |
| 2011 |    05 | 2011-05     | 215,774.60 |

> View defined in `sql/08_views_kpis.sql` as `dw.v_monthly_revenue`.

---

### 2) AOV — average order value (sales only)

    SELECT * FROM dw.v_aov_by_month ORDER BY year, month LIMIT 6;

| year | month | month_label | aov   |
| ---: | ----: | ----------- | ----- |
| 2010 |    12 | 2010-12     | 81.22 |
| 2011 |    01 | 2011-01     | 74.95 |
| 2011 |    02 | 2011-02     | 73.10 |
| 2011 |    03 | 2011-03     | 78.43 |
| 2011 |    04 | 2011-04     | 80.12 |
| 2011 |    05 | 2011-05     | 82.66 |

> View: `dw.v_aov_by_month`. AOV is computed per invoice, then averaged monthly.

---

### 3) Top products in the last 30 days (sales only)

    SELECT * FROM dw.v_top_products_30d;

| stock_code | description            | revenue_30d | qty_30d |
| ---------- | ---------------------- | ----------- | ------- |
| 85123A     | WHITE HANGING HEART T… | 12,340.50   | 154     |
| 22423      | REGENCY CAKESTAND 3 T… | 9,221.00    | 96      |
| 20725      | LUNCH BAG RED RETROSP… | 8,775.75    | 183     |
| 47566      | PARTY BUNTING          | 8,410.30    | 201     |
| 84879      | ASSORTED COLOUR BIRD … | 8,112.40    | 167     |

> View: `dw.v_top_products_30d` (uses `dim_date` and a 30-day window from the max date in data).

---

## 🧪 Debug Tips

- **Orders without date** won’t enter DW (protects `date_key NOT NULL`).

      SELECT invoice_no FROM stg.orders WHERE invoice_date IS NULL LIMIT 20;

- **Customer duplicates** in RAW (conflicting `country`) are reduced to one row in STG (latest by `ingestion_time`).

      SELECT customer_id, COUNT(*)
      FROM raw.customers
      GROUP BY 1
      HAVING COUNT(*) > 1
      LIMIT 10;

- **Adjustments** live in `dw.fact_adjustments`.

      SELECT COUNT(*)
      FROM dw.fact_adjustments
      WHERE unit_price >= 0; -- expect 0

---

## 🧭 Decision Log (short)

- Added **RAW** to preserve exact source and enable reproducible, batch-based loads.
- In **STG**, enforced natural PKs and **deterministic** selection (one row per key, latest by `ingestion_time`).
- Negative prices and special adjustment codes are kept and routed to **`fact_adjustments`**.
- `dim_date` is built from **non-NULL** order dates to keep `date_key` valid.
- The **latest batch** is auto-detected in SQL (no external variables), which simplifies local runs.

---

## 🛠 Requirements

Create `requirements.txt` with:

    pandas>=2.0
    psycopg2-binary>=2.9
    python-dotenv>=1.0

Install:

    pip install -r requirements.txt

---

## 🔒 .gitignore (project-specific)

Add these entries to your `.gitignore` (on top of the standard Python template):

    # Project-specific
    config/.env

    # Keep data out of VCS (license/size)
    data/external/
    data/raw/
    # optional future dirs
    data/interim/
    data/processed/

    # OS/editor noise
    .DS_Store
    Thumbs.db

    # Optional
    logs/
    reports/

---

## 🧱 Next Steps (optional)

- Docker Compose (Postgres + pgAdmin).
- Simple Airflow/Prefect DAG (tasks: extract, raw, stg, dw, checks, views).
- Expand quality checks with summaries (not only assertions).
- SCD2 for selected dimensions (e.g., customer country).

---

## TL;DR — Quick Start

    # Full pipeline
    python src/main.py

    # Only DW (when STG is ready)
    python src/run_sql.py

    # Example KPIs (psql/pgAdmin)
    -- monthly revenue
    SELECT * FROM dw.v_monthly_revenue LIMIT 12;

    -- AOV by month
    SELECT * FROM dw.v_aov_by_month ORDER BY year, month LIMIT 6;

    -- top products (last 30 days)
    SELECT * FROM dw.v_top_products_30d;
