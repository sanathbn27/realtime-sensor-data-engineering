import pandas as pd
from pathlib import Path
from datetime import datetime
import numpy as np
from loguru import logger

BASE_DIR = Path(__file__).resolve().parent.parent.parent
TRANSFORMED_DIR = BASE_DIR / "transformed"
AGGREGATES_DIR = BASE_DIR / "aggregates"
AGGREGATES_DIR.mkdir(exist_ok=True)

def aggregate_file(file_path):
    """
    Calculates and stores aggregated metrics for each unique device in a file.
    
    Args:
        file_path (Path): The path to the transformed CSV file.
    
    Returns:
        Path: The path to the output aggregates file.
    """
    df = pd.read_csv(file_path)

    # Assuming the device ID column is the second one, as per the image
    # Note: It's better to use a specific column name if available
    device_col = df.columns[1]
    
    # Define the numeric columns for aggregation
    numeric_cols = ["temp", "humidity", "co", "lpg", "smoke"]
    
    # Check if the device column exists and if it's the right one
    if not isinstance(device_col, str) or len(df[device_col].unique()) == len(df):
        raise ValueError(f"Could not identify a suitable device column. Found: {device_col}")

    # Group the DataFrame by the device column and calculate statistics
    agg_df = df.groupby(device_col)[numeric_cols].agg([
        'min', 'max', 'mean', 'std'
    ])
    
    # Flatten the multi-level column names for easier saving
    agg_df.columns = ['_'.join(col).strip() for col in agg_df.columns.values]

    #round off numeric columns to 4 decimal places
    agg_df = agg_df.round(4)
    
    # Add metadata columns
    agg_df.reset_index(inplace=True)
    agg_df.rename(columns={device_col: 'device_id'}, inplace=True)
    agg_df['file_name'] = Path(file_path).name
    agg_df['processed_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Rearrange columns to put metadata at the front
    cols = ['file_name', 'processed_at', 'device_id'] + [col for col in agg_df.columns if col not in ['file_name', 'processed_at', 'device_id']]
    agg_df = agg_df[cols]

    # Save to CSV (append if exists)
    output_file = AGGREGATES_DIR / "aggregates_by_device.csv"
    
    # Append to the existing file or create a new one
    if output_file.exists():
        # Check if the header matches before appending
        existing_df = pd.read_csv(output_file, nrows=0)
        if list(existing_df.columns) == list(agg_df.columns):
            agg_df.to_csv(output_file, mode="a", header=False, index=False)
        else:
            logger.error("Header mismatch in existing aggregates file. Skipping append.")
    else:
        agg_df.to_csv(output_file, index=False)
    
    return output_file

if __name__ == "__main__":
    for file in TRANSFORMED_DIR.glob("*.csv"):
        try:
            out = aggregate_file(file)
            print(f"Aggregates by device saved to {out}")
        except Exception as e:
            print(f"Failed to aggregate file {file}: {e}")
