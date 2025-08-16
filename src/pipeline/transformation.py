import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent
ARCHIVE_DIR = BASE_DIR / "archive"
TRANSFORMED_DIR = BASE_DIR / "transformed"
TRANSFORMED_DIR.mkdir(exist_ok=True)

def transform_file(file_path):
    print(f"Transforming file: {file_path}")
    df = pd.read_csv(file_path)
    print(f"[DEBUG] Rows read: {len(df)}")

    # 1. Convert UNIX timestamp to datetime
    if "ts" in df.columns:
        df["ts"] = pd.to_datetime(df["ts"], unit="s", errors="coerce")

    # 2. Round numeric sensor values
    numeric_cols = ["humidity", "temp"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").round(2)
    

    numeric_cols = ["co", "lpg", "smoke"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = pd.to_numeric(df[col], errors="coerce").round(3)

    # 3. Standardize boolean columns
    bool_cols = ["light", "motion"]
    for col in bool_cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.upper()
                .map({"TRUE": True, "FALSE": False, "1": True, "0": False})
            )

    # 4. Column ordering
    desired_order = ["ts", "device", "co", "humidity", "light", "lpg", "motion", "smoke", "temp"]
    df = df[[c for c in desired_order if c in df.columns]]

    # 5. Save transformed file
    transformed_path = TRANSFORMED_DIR / Path(file_path).name
    df.to_csv(transformed_path, index=False)
    return transformed_path

if __name__ == "__main__":
    # Batch process all archive files
    for file in ARCHIVE_DIR.glob("*.csv"):
        new_path = transform_file(file)
        print(f"Transformed file saved to {new_path}")

