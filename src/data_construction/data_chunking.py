import pandas as pd
import os

# paths
raw_file = "data/iot_telemetry_data.csv"
output_dir = "data"
os.makedirs(output_dir, exist_ok=True)

# read dataset
df = pd.read_csv(raw_file)

# how many rows per file
chunk_size = 50000

for i in range(0, len(df), chunk_size):
    chunk = df.iloc[i:i+chunk_size]
    file_name = f"sensor_data_part_{i//chunk_size + 1}.csv"
    chunk.to_csv(os.path.join(output_dir, file_name), index=False)

print("Splitting complete!")
