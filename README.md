# Mini Data Warehouse project (Python + PostgreSQL)

This project is a **mini data warehouse** built with Python and PostgreSQL.  
The goal is to practice building a reproducible ETL pipeline:  

- ingesting raw data from an external dataset,  
- landing it into a **RAW** schema (1:1 with source, + batch metadata),  
- transforming it into a **STG** schema (typed, deduplicated, cleaned),  
- modeling a **DW** layer in a star schema (dimensions + fact),  
- exposing analytical views for basic business KPIs.  

---


## 📖 Design Decisions

Originally this project started with only two layers (`stg` and `dw`).  
After the first commits I decided to redesign the architecture and introduce a **RAW layer**.  

**Why?**
- RAW provides a 1:1 copy of the source data (append-only, no constraints, no type casting).  
- It makes the pipeline **reproducible**: each load can be tracked by `batch_id` and `ingestion_time`.  
- It supports **idempotency**: STG/DW can be safely rebuilt from RAW without re-downloading external data.  
- This design reflects how real-world Data Engineering projects are structured (`raw → stg → dw`).  

The old two-layer approach is still visible in early commits, but the final version uses **three layers**:  



## 📂 Data Source

This project uses the **E-Commerce Data** dataset available on Kaggle:  
[E-Commerce Data (carrie1)](https://www.kaggle.com/datasets/carrie1/ecommerce-data)

The dataset contains transactional records from a UK-based online store (Dec 2010 – Dec 2011).  
⚠️ The dataset license is marked as **Unknown** on Kaggle.  

For this reason, the raw CSV file is **not included in this repository**.  
To run the project, please download it manually from Kaggle and place it in the `data/external/` folder as `data.csv`.  
The dataset is used here **strictly for educational and portfolio purposes**.

---


---

## 📂 Data Source

This project uses the **E-Commerce Data** dataset available on Kaggle:  
[E-Commerce Data (carrie1)](https://www.kaggle.com/datasets/carrie1/ecommerce-data)

The dataset contains transactional records from a UK-based online store (Dec 2010 – Dec 2011).  
⚠️ The dataset license is marked as **Unknown** on Kaggle.  

For this reason, the raw CSV file is **not included in this repository**.  
To run the project, please download it manually from Kaggle and place it in the `data/external/` folder as `data.csv`.  
The dataset is used here **strictly for educational and portfolio purposes**.

---

## 🎯 Project Goals
- Build an end-to-end ETL pipeline with Python & SQL  
- Practice multi-layer modeling: `raw → stg → dw`  
- Implement a star schema (dimensions + fact)  
- Add batch metadata (`batch_id`, `ingestion_time`) for reproducibility  
- Create analytical views (monthly sales, AOV, customer segmentation)  
- Showcase Data Engineering best practices in a small project

---

## 📌 Current Progress
- **`extract.py`** → reads `data.csv`, splits into 4 normalized CSVs (`customers.csv`, `products.csv`, `orders.csv`, `order_items`) with batch metadata  
- **`01_create_schemas.sql`** → creates PostgreSQL schemas (`raw`, `stg`, `dw`)  
- **`02_raw_tables.sql`** → defines RAW tables for CSV ingestion  
- **`load_to_raw.py`** → loads CSVs from `data/raw/` into `raw.*` tables in PostgreSQL  

---

## 🗄️ Planned SQL Scripts
- `03_stg_tables.sql` → create staging tables with proper types, PKs, deduplication  
- `04_dw_tables.sql` → create star schema (dimensions + fact_sales)  
- `05_transforms.sql` → transformations from STG → DW (incl. SCD handling)  
- `06_quality_checks.sql` → basic data quality tests (completeness, uniqueness, referential integrity, business rules)  
- `07_views_kpis.sql` → create analytical views for KPIs  

---

## 🔧 Configuration

Database credentials are stored in environment variables.  
To configure:

1. Copy `config/.env.example` → `config/.env`  
2. Fill in your local PostgreSQL credentials (host, port, db, user, password)  
3. `.env` is ignored by Git for security  

The project uses [`python-dotenv`](https://pypi.org/project/python-dotenv/) to load these variables into Python.

---

## 🐍 Python Scripts (current & planned)

Python scripts orchestrate the ETL pipeline:

- **extract.py** → splits raw dataset into 4 normalized CSVs with batch metadata  
- **load_to_raw.py** → loads CSVs into `raw.*` tables  
- **run_sql.py** *(planned)* → helper to execute SQL scripts in order  
- **main.py** *(planned)* → orchestrates the whole pipeline:
  1. extract  
  2. load to RAW  
  3. create STG tables & load  
  4. create DW tables  
  5. run transformations  
  6. run data quality checks  
  7. create KPI views  

---

## ⚙️ Requirements

Main dependencies are listed in `requirements.txt`:



