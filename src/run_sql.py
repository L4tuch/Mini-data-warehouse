"""
Run DW SQL steps in order (05..08), committing between files; stop on first error.
Use when STG is ready and you only want to (re)build/load DW + checks + views.
"""

import os
import psycopg2
from dotenv import load_dotenv
from pathlib import Path

SQL_FILES = [
    "sql/05_dw_tables.sql",
    "sql/06_dw_load.sql",
    "sql/07_quality_checks.sql",
    "sql/08_views_kpis.sql",
]

def run_sql_file(cur, path: str):
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    cur.execute(sql)

def main():
    load_dotenv("config/.env")
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
                for p in SQL_FILES:
                    print(f"▶ Running {p}")
                    run_sql_file(cur, p)
                    conn.commit()
        print("✅ All DW SQL scripts executed.")
    except Exception as e:
        print("SQL execution failed:")
        print(str(e))
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
