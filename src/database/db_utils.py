import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from pathlib import Path
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

#  Update with your local PostgreSQL credentials
DB_CONFIG = {
    "dbname": "sensor_db",       # Database name
    "user": "postgres",          # Default postgres user
    "password": "AdminPass123!", 
    "host": "localhost",         # Database host
    "port": 5432                # Default Postgres port
}

def get_connection():
    """
    Establish a connection to PostgreSQL using psycopg2.
    """
    return psycopg2.connect(**DB_CONFIG)

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=30))
def safe_execute_values(cur, sql, rows, template=None):
    """
    Execute a batch insert with retry logic.
    Retries if DB is unavailable temporarily.
    """
    execute_values(cur, sql, rows, template=template)
    logger.info(f"Inserted {len(rows)} rows (retry-safe).")


