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

# def insert_raw_data(file_path: Path):
#     """
#     Insert transformed sensor data into raw_data table.
#     """
#     df = pd.read_csv(file_path)
#     df["file_name"] = Path(file_path).name

#     cols = ["ts", "device", "co", "humidity", "light", "lpg", "motion", "smoke", "temp", "file_name"]
#     records = df[cols].to_records(index=False).tolist()

#     query = """
#         INSERT INTO raw_data
#         (ts, device_id, co, humidity, light, lpg, motion, smoke, temp, file_name)
#         VALUES %s
#     """

#     with get_connection() as conn:
#         with conn.cursor() as cur:
#             execute_values(cur, query, records)
#         conn.commit()

# def insert_aggregates(file_path: Path):
#     """
#     Insert aggregated metrics into aggregates table.
#     """
#     df = pd.read_csv(file_path)
#     records = df.to_records(index=False).tolist()

#     query = """
#         INSERT INTO aggregates
#         (file_name, processed_at, device_id,
#          temp_min, temp_max, temp_mean, temp_std,
#          humidity_min, humidity_max, humidity_mean, humidity_std,
#          co_min, co_max, co_mean, co_std,
#          lpg_min, lpg_max, lpg_mean, lpg_std,
#          smoke_min, smoke_max, smoke_mean, smoke_std)
#         VALUES %s
#     """

#     with get_connection() as conn:
#         with conn.cursor() as cur:
#             execute_values(cur, query, records)
#         conn.commit()
