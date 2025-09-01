
"""
Fast-load CSVs from data/raw/ into raw.* tables using COPY.
Append-only by batch_id; requires DB creds from config/.env.


Steps:
1. Read database credentials from config/.env
2. Connect to PostgreSQL
3. For each CSV in data/raw/, load into raw.* table
4. Commit transaction
"""

import psycopg2
from dotenv import load_dotenv
from pathlib import Path
import os

# ── Load env variables ─────────────────────────────────────────────
load_dotenv("config/.env")

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASSWORD")

# ── Connect to PostgreSQL ─────────────────────────────────────────
conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASS
)
cur = conn.cursor()

# ── Map CSV files to RAW tables ───────────────────────────────────
files_to_tables = {
    "customers.csv": "raw.customers",
    "products.csv": "raw.products",
    "orders.csv": "raw.orders",
    "order_items.csv": "raw.order_items"
}

raw_dir = Path("./data/raw")

# ── Load each CSV into the target table ───────────────────────────
for file, table in files_to_tables.items():
    file_path = raw_dir / file
    print(f"▶Loading {file_path} → {table}")

    with open(file_path, "r", encoding="utf-8") as f:
        cur.copy_expert(f"COPY {table} FROM STDIN WITH CSV HEADER", f)

# ── Commit and close ──────────────────────────────────────────────
conn.commit()
cur.close()
conn.close()


