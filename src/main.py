"""
End-to-end pipeline runner:
extract -> load_to_raw -> STG DDL -> RAW->STG -> DW DDL -> STG->DW -> checks -> views.
Targets the latest batch automatically; keeps adjustments for a separate fact.
"""

import os
import subprocess
import psycopg2
from dotenv import load_dotenv
from pathlib import Path

SQL_ORDER = [
    "sql/03_stg_tables.sql",
    "sql/04_stg_load.sql",
    "sql/05_dw_tables.sql",
    "sql/06_dw_load.sql",
    "sql/07_quality_checks.sql",
    "sql/08_views_kpis.sql",
]

def run_py(cmd: list[str]):
    print(f"▶ Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)

def run_sql_file(cur, path: str):
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    cur.execute(sql)

def main():
    load_dotenv("config/.env")

    # 1) Extract
    run_py(["python", "src/extract.py"])

    # 2) Load to RAW
    run_py(["python", "src/load_to_raw.py"])

    # 3..8) SQL phases (STG, DW, checks, views)
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )
    try:
        with conn:
            with conn.cursor() as cur:
                for p in SQL_ORDER:
                    print(f"▶ Running {p}")
                    run_sql_file(cur, p)
                    conn.commit()
        print("Pipeline completed.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
