import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from pathlib import Path

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


