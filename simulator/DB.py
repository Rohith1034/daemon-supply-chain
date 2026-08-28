# DB.py
import psycopg2
DB_CONFIG = {
    "host": "localhost",
    "port": 55432,
    "database": "supply_chain",
    "user": "supplychain_app",
    "password": "Cr7@1034"
}

def get_connection():
    return psycopg2.connect(**DB_CONFIG)