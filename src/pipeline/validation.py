import pandas as pd
import os
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent

LOGS_DIR = BASE_DIR/ "logs"
QUARANTINE_DIR = BASE_DIR/ "quarantine"
ARCHIVE_DIR = BASE_DIR/ "archive"
ERROR_LOG_FILE = os.path.join(LOGS_DIR, "error_log.csv")

for folder in [LOGS_DIR, QUARANTINE_DIR, ARCHIVE_DIR]:
    os.makedirs(folder, exist_ok=True)

def log_error(file_name, row_idx, column, value, message):
    """Append error details to CSV log."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(ERROR_LOG_FILE, "a") as f:
        f.write(f"{timestamp},{file_name},{row_idx},{column},{value},{message}\n")

def validate_file(file_path):
    """Validates each row; saves valid/invalid rows separately."""
    file_name = os.path.basename(file_path)
    df = pd.read_csv(file_path)

    valid_rows = []
    invalid_rows = []

    for idx, row in df.iterrows():
        row_valid = True

        # Null checks
        if pd.isnull(row["device"]):
            log_error(file_name, idx, "device", None, "Null device ID")
            row_valid = False
        if pd.isnull(row["ts"]):
            log_error(file_name, idx, "ts", None, "Null timestamp")
            row_valid = False
        if pd.isnull(row["temp"]):
            log_error(file_name, idx, "temp", None, "Null temperature")
            row_valid = False
        if pd.isnull(row["humidity"]):
            log_error(file_name, idx, "humidity", None, "Null humidity")
            row_valid = False
        if pd.isnull(row["light"]):
            log_error(file_name, idx, "light", None, "Null light status")
            row_valid = False   
        if pd.isnull(row["motion"]):
            log_error(file_name, idx, "motion", None, "Null motion status")
            row_valid = False
        if pd.isnull(row["co"]):
            log_error(file_name, idx, "co", None, "Null CO level")
            row_valid = False

        # Type + range checks
        # Temperature
        try:
            temp = float(row["temp"])
            if temp < -50 or temp > 50:
                log_error(file_name, idx, "temp", row["temp"], "Temperature out of range")
                row_valid = False
        except ValueError:
            log_error(file_name, idx, "temp", row["temp"], "Non-numeric temperature")
            row_valid = False

        # Humidity
        try:
            humidity = float(row["humidity"])
            if humidity < 0 or humidity > 100:
                log_error(file_name, idx, "humidity", row["humidity"], "Humidity out of range")
                row_valid = False
        except ValueError:
            log_error(file_name, idx, "humidity", row["humidity"], "Non-numeric humidity")
            row_valid = False

        # Boolean checks
        for bool_col in ["light", "motion"]:
            if str(row[bool_col]).upper() not in ["TRUE", "FALSE", "0", "1"]:
                log_error(file_name, idx, bool_col, row[bool_col], "Invalid boolean value")
                row_valid = False

        # Add to appropriate list
        if row_valid:
            valid_rows.append(row)
        else:
            invalid_rows.append(row)

    # Save valid and invalid rows
    if valid_rows:
        pd.DataFrame(valid_rows).to_csv(
            os.path.join(ARCHIVE_DIR, file_name), index=False
        )
    if invalid_rows:
        pd.DataFrame(invalid_rows).to_csv(
            os.path.join(QUARANTINE_DIR, file_name.replace(".csv", "_errors.csv")), index=False
        )

    return len(invalid_rows) == 0  # True if all rows valid
