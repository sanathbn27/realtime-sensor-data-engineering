import pandas as pd
from pathlib import Path
from psycopg2.extras import execute_values
from src.database.db_utils import get_connection

# A more robust way to define the aggregated data directory
# This assumes the 'aggregated_data' folder is a sibling of the 'src' folder
# which is a common project structure.
AGG_DIR = Path(__file__).resolve().parent.parent.parent / "aggregated_data"

def load_aggregated_file(csv_path: Path):
    """Loads a single aggregated data CSV file into the database."""
    try:
        df = pd.read_csv(csv_path)

        # Correcting the first two column names to match the database schema
        # The CSV columns are `file_name`, `processed_at` but the database
        # columns are `file_name` and `processed_at`. This renames them.
        df = df.rename(columns={df.columns[0]: "file_name", df.columns[1]: "processed_at"})

        # Ensure processed_at is a proper datetime object
        df["processed_at"] = pd.to_datetime(df["processed_at"], errors="coerce")

        # Ensure NaN values are converted to None for database insertion
        df = df.where(pd.notnull(df), None)

        # The columns to insert into the database
        # This list must match the columns in your database table exactly
        cols = list(df.columns)
        
        # Prepare rows as tuples for bulk insertion
        
        rows = [tuple(row) for row in df[cols].values.tolist()]

        conn = get_connection()
        cur = conn.cursor()

        # Construct the SQL INSERT statement with dynamic columns
        sql = f"""
            INSERT INTO analytics.aggregated_sensor_data ({", ".join(cols)})
            VALUES %s
        """
        
        # Use execute_values for efficient bulk insertion
        execute_values(cur, sql, rows)

        conn.commit()
        print(f"[SUCCESS] Inserted {len(rows)} aggregate rows from {csv_path}")

    except Exception as e:
        conn.rollback()
        print(f"[ERROR] Failed to insert data from {csv_path}: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    if not AGG_DIR.is_dir():
        print(f"[WARNING] Directory not found: {AGG_DIR}. Please check the path.")
    
    csv_files = list(AGG_DIR.glob("*.csv"))
    if not csv_files:
        print(f"[WARNING] No CSV files found in directory: {AGG_DIR}")

    for csv_file in csv_files:
        load_aggregated_file(csv_file)
