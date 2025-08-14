# Real-Time Sensor Pipeline

Goal: Monitor `data/` for incoming CSVs every 5–10s, validate + quarantine bad rows, 
transform + aggregate and store raw + aggregates to PostgreSQL. Later provide MySQL schema.


## Dataset Setup
1. Download the Environmental Sensor Data (132K samples) from Kaggle:
   https://www.kaggle.com/datasets/garystafford/environmental-sensor-data-132k
2. Place the downloaded file into `raw_dataset/`.
3. Run `src/data/data_chunking.py` to create smaller CSV files in `data/`.
4. Run `src/data/corrupt_data_ingestion.py` to generate `data_corrupted/` with simulated bad data.

