import psycopg2
from dotenv import load_dotenv
import os
import pandas as pd



#Load environment variables from .env file
load_dotenv("./config/.env")

#Read variables from environment
PGDATABASE = os.getenv("PGDATABASE")
PGUSER = os.getenv("PGUSER")
PGPASSWORD = os.getenv("PGPASSWORD")
PGHOST = os.getenv("PGHOST")   
PGPORT = os.getenv("PGPORT")


#Establish connection to PostgreSQL
conn = psycopg2.connect(
    database=PGDATABASE,
    user=PGUSER,
    password=PGPASSWORD,
    host=PGHOST,
    port=PGPORT
)

print("Connected:", conn)

query = conn.cursor()

