"""
Execute sql/04_stg_load.sql in a single transaction.
Moves latest batch RAW -> STG with casting, cleaning, dedupe.
Minimal logging; reads DB creds from config/.env.
"""

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv("config/.env")

def main():
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
                with open("sql/04_stg_load.sql", "r", encoding="utf-8") as f:
                    sql = f.read()
                cur.execute(sql)
        print("STG load completed.")
    except Exception as e:
        # Minimal, readable error message
        print("STG load failed:")
        print(str(e))
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
