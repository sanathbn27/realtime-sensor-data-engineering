import pandas as pd
from pathlib import Path
from datetime import datetime
import numpy as np
from loguru import logger

BASE_DIR = Path(__file__).resolve().parent.parent.parent
TRANSFORMED_DIR = BASE_DIR / "transformed_data"
AGGREGATES_DIR = BASE_DIR / "aggregated_data"
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
    
    # Flatten the multi-level column names to save
    agg_df.columns = ['_'.join(col).strip() for col in agg_df.columns.values]

    #round off numeric columns to 4 decimal places
    agg_df = agg_df.round(4)
    
    # Add metadata columns
    agg_df.reset_index(inplace=True)
    agg_df.rename(columns={device_col: 'device_id'}, inplace=True)
    output_file_name = file_path.name
    output_file = AGGREGATES_DIR / output_file_name
    agg_df['file_name'] = output_file.name
    agg_df['processed_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Rearrange columns to put metadata at the front
    cols = ['file_name', 'processed_at', 'device_id'] + [col for col in agg_df.columns if col not in ['file_name', 'processed_at', 'device_id']]
    agg_df = agg_df[cols]

    # Save to a new CSV file
    agg_df.to_csv(output_file, index=False)
    
    return output_file

if __name__ == "__main__":
    for file in TRANSFORMED_DIR.glob("*.csv"):
        try:
            out = aggregate_file(file)
            print(f"Aggregates by device saved to {out}")
        except Exception as e:
            print(f"Failed to aggregate file {file}: {e}")
