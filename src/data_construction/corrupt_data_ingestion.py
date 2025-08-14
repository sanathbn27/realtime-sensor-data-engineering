import pandas as pd
import os
import random
import numpy as np

# folder with your split files
input_dir = "data"
output_dir = "data_corrupted"
os.makedirs(output_dir, exist_ok=True)

# probability of injecting each type of error
MISSING_DEVICE_PROB = 0.01     # 1% missing device IDs
TYPE_ERROR_PROB = 0.01         # 1% values replaced with "N/A"
OUT_OF_RANGE_PROB = 0.01       # 1% values set outside range

for file in os.listdir(input_dir):
    if file.endswith(".csv"):
        file_path = os.path.join(input_dir, file)
        df = pd.read_csv(file_path)

        # Inject missing device IDs
        missing_idx = df.sample(frac=MISSING_DEVICE_PROB).index
        df.loc[missing_idx, 'device'] = None
        

        # Inject type errors into numeric columns
        numeric_columns = ['co', 'humidity', 'light', 'lpg', 'smoke', 'temp']
        for col in numeric_columns:
            type_err_idx = df.sample(frac=TYPE_ERROR_PROB).index
            df[col] = df[col].astype(object)
            df.loc[type_err_idx, col] = "N/A"

        # Inject out-of-range temperature & humidity
        temp_out_idx = df.sample(frac=OUT_OF_RANGE_PROB).index
        df.loc[temp_out_idx, 'temp'] = np.random.choice([150, -120])  # extreme temps

        humidity_out_idx = df.sample(frac=OUT_OF_RANGE_PROB).index
        df.loc[humidity_out_idx, 'humidity'] = np.random.choice([250, -50])  # extreme humidity

        # Save corrupted file
        output_path = os.path.join(output_dir, file)
        df.to_csv(output_path, index=False)

print(f"Bad data injection complete! Corrupted files saved to '{output_dir}'")
