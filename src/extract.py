""" 
data/raw/customers.csv: CustomerID, Country

data/raw/products.csv: stock_code, description, unit_price

data/raw/orders.csv: invoice_no, invoice_date, CustomerID

data/raw/order_items.csv: invoice_no, stock_code, quantity, unit_price

"""

import pandas as pd
from pathlib import Path


# Define input and output paths
path = Path("./data/external/data.csv")
out_path = Path("./data/raw/")

# Load the external dataset
df = pd.read_csv(path)


# Type conversions / memory optimizations
df['Description'] = df['Description'].astype('category')
df['Quantity'] = df['Quantity'].astype('int16')
df["InvoiceDate"] = pd.to_datetime(
df["InvoiceDate"], dayfirst=True, errors="coerce")
df['UnitPrice'] = df['UnitPrice'].astype('float32')
df['Country'] = df['Country'].astype('category')


# Split into normalized tables, delete null, delete duplicates, filter data in quantity

customers = (df[["CustomerID", "Country"]]
             .dropna(subset=["CustomerID"])
             .drop_duplicates())

products = (df[["StockCode", "Description", "UnitPrice"]]
            .dropna(subset=["StockCode"])
            .drop_duplicates(subset=["StockCode"]))

orders = (df[["InvoiceNo", "InvoiceDate", "CustomerID"]]
          .dropna(subset=["InvoiceNo", "CustomerID"])
          .drop_duplicates(subset=["InvoiceNo"]))

order_items = (df[["InvoiceNo", "StockCode", "Quantity", "UnitPrice"]]
               .dropna(subset=["InvoiceNo", "StockCode"])
               .query("Quantity > 0"))

# Save tables to CSV (UTF-8 for portability)
customers.to_csv(out_path/"customers.csv", index=False, encoding='UTF-8')
products.to_csv(out_path/"products.csv", index=False, encoding='UTF-8')
orders.to_csv(out_path/"orders.csv", index=False, encoding='UTF-8')
order_items.to_csv(out_path/"order_items.csv", index=False, encoding='UTF-8')
