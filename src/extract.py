"""
Split Kaggle data.csv into 4 CSVs (customers/products/orders/order_items).
Adds batch_id + ingestion_time, normalizes column names (snake_case), light dedupe, basic filters.
Writes to data/raw/; encoding handled; no DB work here.
"""

from pathlib import Path
from datetime import datetime
import pandas as pd

# ── Paths ─────────────────────────────────────────────────────────────
in_path = Path("./data/external/data.csv")
out_dir = Path("./data/raw")
out_dir.mkdir(parents=True, exist_ok=True)

# ── Batch metadata ─────────────────────────────────────────────────────
batch_id = int(datetime.now().strftime("%Y%m%d%H%M%S"))
ingestion_time = datetime.now().isoformat(timespec="seconds")

# ── Read CSV with robust encoding handling ─────────────────────────────
try:
    df = pd.read_csv(in_path, encoding="utf-8")
except UnicodeDecodeError:
    print("UTF-8 failed, retrying with latin1 encoding...")
    df = pd.read_csv(in_path, encoding="latin1")

# ── Normalize column names to snake_case ───────────────────────────────
df = df.rename(columns={
    "InvoiceNo": "invoice_no",
    "StockCode": "stock_code",
    "Description": "description",
    "Quantity": "quantity",
    "InvoiceDate": "invoice_date",
    "UnitPrice": "unit_price",
    "CustomerID": "customer_id",
    "Country": "country"
})

# ── Type conversions / light memory optimization ───────────────────────
df["description"] = df["description"].astype("category")
df["country"] = df["country"].astype("category")
df["quantity"] = df["quantity"].astype("int16")
df["unit_price"] = df["unit_price"].astype("float32")
df["invoice_date"] = pd.to_datetime(
    df["invoice_date"], dayfirst=True, errors="coerce")

# ── Build normalized raw tables ────────────────────────────────────────
customers = (
    df[["customer_id", "country"]]
    .dropna(subset=["customer_id"])
    .drop_duplicates()
)

products = (
    df[["stock_code", "description", "unit_price"]]
    .dropna(subset=["stock_code"])
    .drop_duplicates(subset=["stock_code"])
)

orders = (
    df[["invoice_no", "invoice_date", "customer_id"]]
    .dropna(subset=["invoice_no", "customer_id"])
    .drop_duplicates(subset=["invoice_no"])
)

order_items = (
    df[["invoice_no", "stock_code", "quantity", "unit_price"]]
    .dropna(subset=["invoice_no", "stock_code"])
    .query("quantity > 0")
    .drop_duplicates(subset=["invoice_no", "stock_code"])
)

# ── Attach batch metadata ──────────────────────────────────────────────
for t in (customers, products, orders, order_items):
    t["batch_id"] = batch_id
    t["ingestion_time"] = ingestion_time

# ── Save CSVs (UTF-8 for portability) ─────────────────────────────────
customers.to_csv(out_dir / "customers.csv", index=False, encoding="utf-8")
products.to_csv(out_dir / "products.csv", index=False, encoding="utf-8")
orders.to_csv(out_dir / "orders.csv", index=False, encoding="utf-8")
order_items.to_csv(out_dir / "order_items.csv", index=False, encoding="utf-8")

print(
    f"Extract finished - batch_id={batch_id} | "
    f"customers={len(customers)}, products={len(products)}, "
    f"orders={len(orders)}, order_items={len(order_items)}"
)
