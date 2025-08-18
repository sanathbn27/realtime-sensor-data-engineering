# src/database/load_raw_data.py

import pandas as pd
from pathlib import Path
from psycopg2.extras import execute_values
from src.database.db_utils import get_connection, safe_execute_values

# project root = two levels up from this file (src/database/)
BASE_DIR = Path(__file__).resolve().parents[2]
INCOMING_DIR = BASE_DIR / "incoming"

BOOL_MAP = {"TRUE": True, "FALSE": False, "1": True, "0": False}

def _to_bool(val):
    if pd.isna(val):
        return None
    s = str(val).strip().upper()
    return BOOL_MAP.get(s, None)

def load_raw_file(file_path: Path):
    print(f"[INFO] Loading raw data from {file_path}")

    # Read CSV; treat these as missing
    df = pd.read_csv(file_path, na_values=["N/A", "", "null", "NULL"])

    # Ensure expected columns exist
    expected = ["ts", "device", "co", "humidity", "light", "lpg", "motion", "smoke", "temp"]
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in {file_path.name}: {missing}")

    # Attach file lineage
    df["file_name"] = file_path.name

    # Parse epoch seconds as numeric (keep as numbers, do NOT convert to datetime here)
    df["ts_epoch"] = pd.to_numeric(df["ts"], errors="coerce")

    # Numeric columns (keep raw — no rounding)
    for col in ["co", "humidity", "lpg", "smoke", "temp"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Booleans
    df["light"] = df["light"].apply(_to_bool)
    df["motion"] = df["motion"].apply(_to_bool)

    # Prepare rows;to convert epoch to plain Python int for TO_TIMESTAMP(%s)
    rows = []
    skipped_no_ts = 0
    for _, r in df.iterrows():
        ts_val = r["ts_epoch"]
        if pd.isna(ts_val):
            
            skipped_no_ts += 1
            continue
        try:
            # handle scientific notation / floats safely
            ts_int = int(float(ts_val))
        except Exception:
            skipped_no_ts += 1
            continue

        rows.append((
            ts_int,                # TO_TIMESTAMP(%s)
            r["device"],           # device_id
            r["co"],
            r["humidity"],
            r["light"],
            r["lpg"],
            r["motion"],
            r["smoke"],
            r["temp"],
            r["file_name"]
        ))

    if not rows:
        print(f"[WARN] No rows to insert from {file_path} (invalid/missing ts?). Skipped: {skipped_no_ts}")
        return

    insert_sql = """
        INSERT INTO raw.raw_sensor_data
        (ts, device_id, co, humidity, light, lpg, motion, smoke, temp, file_name)
        VALUES %s
    """
    # Use a template so ts is converted inside Postgres
    tpl = "(TO_TIMESTAMP(%s), %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    with get_connection() as conn:
        with conn.cursor() as cur:
            safe_execute_values(cur, insert_sql, rows, template=tpl)
        conn.commit()

    print(f"[SUCCESS] Inserted {len(rows)} rows from {file_path}. Skipped (no/invalid ts): {skipped_no_ts}")

if __name__ == "__main__":
    any_loaded = False
    for csv_file in INCOMING_DIR.glob("*.csv"):
        any_loaded = True
        try:
            load_raw_file(csv_file)
        except Exception as e:
            print(f"[ERROR] Failed to load {csv_file.name}: {e}")
    if not any_loaded:
        print(f"[INFO] No CSV files found in {INCOMING_DIR}")
