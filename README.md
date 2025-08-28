# Mini Data Warehouse project (Python + PostgreSQL)

This project is a mini data warehouse built with Python and PostgreSQL.  
The goal is to practice building an ETL pipeline: from a single external dataset, split into normalized tables (`customers`, `products`, `orders`, `order_items`), loaded into PostgreSQL staging, transformed into a star schema, and queried for basic business KPIs.

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

##  Project Goals
- Build an end-to-end ETL pipeline with Python & SQL  
- Load and transform data using PostgreSQL  
- Model a star schema (dimensions + fact)  
- Create analytical views (monthly sales, AOV, customer segmentation)  
- Showcase a reproducible Data Engineering project

---

##  Current Progress
- `extract.py` reads `data.csv` and splits it into 4 raw CSV files:  
  `customers.csv`, `products.csv`, `orders.csv`, `order_items.csv`  
- Raw files are stored in `data/raw/` and ready for loading to staging


## 🗄️ Database Setup (current state)

SQL scripts live in the `sql/` folder.

- **01_create_schemas.sql** — creates two PostgreSQL schemas:
  - `stg` → staging area (raw data loaded from CSVs)
  - `dw`  → data warehouse layer (star schema: dimensions + facts)

> Next scripts to be added:
> 02_stg_tables.sql, 03_dw_tables.sql, 04_transforms.sql, 05_quality_checks.sql, 06_views_kpis.sql

---

