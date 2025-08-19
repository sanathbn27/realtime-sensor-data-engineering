# Real-Time Sensor Pipeline

Goal: Monitor `data/` for incoming CSVs every 5–10s, validate + quarantine bad rows, 
transform + aggregate and store raw + aggregates to PostgreSQL.


## Dataset Setup

To simulate the incoming sensor data, we will use a pre-existing dataset and process it into smaller, manageable chunks.

1.  **Download Dataset:** Download the ["Environmental Sensor Data (132K samples)"](https://www.kaggle.com/datasets/garystafford/environmental-sensor-data-132k?resource=download) from Kaggle and place the `.csv` file into a new folder named `raw_dataset/`.
2.  **Create Chunks:** Run the included data chunking script to split the large file into smaller, individual CSVs. These will be placed in the `data/` folder.
    ```bash
    python src/data_construction/data_chunking.py
    ```
3.  **Simulate Bad Data:** Run the following script to create a parallel directory, `data_corrupted/`, with files that contain simulated errors (e.g., missing values, type errors, out-of-range readings). This is used for testing the validation component of the pipeline.
    ```bash
    python src/data_construction/corrupt_data_ingestion.py
    ```

---

## PostgreSQL Database Setup

The pipeline uses PostgreSQL to store and manage the processed sensor data. You must have a running PostgreSQL instance to use this pipeline.

### Installation & Configuration

1. **Install PostgreSQL:** Download and install PostgreSQL on your machine from the [official](https://www.enterprisedb.com/downloads/postgres-postgresql-downloads) website.
2. **Create Database:** Use psql or preferred tool (e.g., PgAdmin) to create a new database for this project.
```sql
CREATE DATABASE sensor_db;
```
3. **Database Connection:** Ensure you have configured your database connection details (host, database name, user, password) in db_utils.py to allow the pipeline to connect.

### Database Schema 
We will create a two new schema and tables to store our data: one for the raw sensor data and another for the aggregated metrics.

1. **Create Schema**
First, connect to your new database and create a schema named raw and analytics. This helps organize and isolate our project's data.
```sql
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;
```


2. **Create Tables**
Next, create the two tables within the raw and analytics schema.

* **raw.raw_sensor_data** Table.
This table stores the raw sensor data coming to the incoming folder.

```sql
CREATE TABLE IF NOT EXISTS raw.raw_sensor_data (
    id          SERIAL PRIMARY KEY,
    ts          TIMESTAMP NOT NULL,
    device_id   VARCHAR(50) NOT NULL,
    co          NUMERIC(10,4),
    humidity    NUMERIC(5,2),
    light       BOOLEAN,
    lpg         NUMERIC(10,4),
    motion      BOOLEAN,
    smoke       NUMERIC(10,4),
    temp        NUMERIC(5,2),
    file_name   VARCHAR(255),
    inserted_at TIMESTAMP DEFAULT NOW()
);
```

* **analytics.aggregated_sensor_data** Table.
This table stores the aggregated metrics, linked to the original file.
```sql
CREATE TABLE IF NOT EXISTS analytics.aggregated_sensor_data (
    id              SERIAL PRIMARY KEY,
    file_name       VARCHAR(255),
    processed_at    TIMESTAMP NOT NULL,
    device_id       VARCHAR(50) NOT NULL,
    temp_min        NUMERIC(5,2),
    temp_max        NUMERIC(5,2),
    temp_mean       NUMERIC(5,2),
    temp_std        NUMERIC(5,2),
    humidity_min    NUMERIC(5,2),
    humidity_max    NUMERIC(5,2),
    humidity_mean   NUMERIC(5,2),
    humidity_std    NUMERIC(5,2),
    co_min          NUMERIC(10,4),
    co_max          NUMERIC(10,4),
    co_mean         NUMERIC(10,4),
    co_std          NUMERIC(10,4),
    lpg_min         NUMERIC(10,4),
    lpg_max         NUMERIC(10,4),
    lpg_mean        NUMERIC(10,4),
    lpg_std         NUMERIC(10,4),
    smoke_min       NUMERIC(10,4),
    smoke_max       NUMERIC(10,4),
    smoke_mean      NUMERIC(10,4),
    smoke_std       NUMERIC(10,4),
    inserted_at     TIMESTAMP DEFAULT NOW()
);
```

* Creating helpful indexes for both the tables
```sql
CREATE INDEX IF NOT EXISTS idx_raw_device_ts ON raw.raw_sensor_data(device_id, ts);
CREATE INDEX IF NOT EXISTS idx_raw_file_name  ON raw.raw_sensor_data(file_name);
CREATE INDEX IF NOT EXISTS idx_raw_device ON raw.raw_sensor_data(device_id);
CREATE INDEX IF NOT EXISTS idx_agg_device_name ON analytics.aggregated_sensor_data(device_id, file_name);
CREATE INDEX IF NOT EXISTS idx_agg_processed_at ON analytics.aggregated_sensor_data(processed_at);
```


## Real-Time Monitoring & Validation

The core of this pipeline is a real-time monitoring system built using `watchdog`. It automatically detects and processes new files as soon as they are placed in the `incoming/` folder.

### How It Works

* **File Watcher (`src/pipeline/watcher.py`):** This script uses the `watchdog` library to continuously monitor the `incoming/` folder. It is configured to trigger a processing function whenever a new CSV file is created or an existing one is modified. 
* **Validation (`src/pipeline/validation.py`):** The validation module reads each incoming CSV file and performs a series of quality checks on every row. It verifies:
    * **Null Values:** Ensures critical columns like `device` and `ts` are not empty.
    * **Range Checks:** Validates that `Temperature` values are within a realistic range (e.g., between -50°C and 50°C) and `Humidity` values are between 0% and 100%.
    * **Boolean Checks:** Confirms that boolean-type columns like `light` and `motion` contain valid values.

### Archiving & Quarantine

* **Valid Data:** Rows that pass all validation checks are saved to a directory named `archive/`.
* **Invalid Data:** Rows that fail validation are moved to a `quarantine/` folder with an `_errors.csv` suffix, allowing for later inspection and recovery.

### Logging

The pipeline uses `loguru` for real-time logging. It records:
* Processed files and their validation status.
* Detailed errors, including the timestamp, file name, row index, column, value, and a specific error message.

A rotating log file (`logs/pipeline.log`) is maintained to prevent the log from growing indefinitely.

## Data Transformation

Once validated, the data is transformed into a clean, standardized format before being stored. The (`src/pipeline/transformation.py`) module handles this process by:

* **Standardizing Timestamps:** The raw UNIX timestamp (`ts`) is converted into a readable datetime format (e.g., `YYYY-MM-DD HH:MM:SS.ffffff`). This is crucial for accurate time-series analysis.

* **Rounding Numeric Values:** Sensor readings for `humidity`, `temp`, `co`, `lpg`, and `smoke` are rounded to a specific number of decimal places to ensure consistent precision across the dataset.

* **Standardizing Boolean Values:** The `light` and `motion` columns are converted to a consistent boolean format (`True`/`False`).

## Analytical Processing and Aggregation

After transformation, the pipeline calculates aggregation metrics to provide immediate insights and prepare the data for downstream analysis. The (`src/pipeline/aggregation.py`) module computes key metrics, such as:

* **min, max, mean, standard_deviation for `temp`, `humidity`, `co`, `lpg` and `smoke`** per device respective to the incoming file.

These aggregated metrics are then stored in the database alongside the raw sensor data, providing a dual-layered approach to analysis.


---

## Data Loading
Once the new file has been found in the incoming folder and aggregated_data, the pipeline loads it directly into the PostgreSQL database.

* **Loading Raw Data:** The raw sensor data is loaded into the raw.raw_sensor_data table.

* **Loading Aggregated Data:** The newly created aggregated file is loaded into the analytics.aggregated_sensor_data table. This step is designed to efficiently handle new files as they are created.

## Running the Real-Time Pipeline

1. **Create the Conda Environment:**
   ```bash
   conda create --name sensor_dataengineering python=3.11 -y
   ```

2.  **Activate Environment:** Ensure you have activated your development environment, such as a Conda environment.
    ```bash
    conda activate sensor_dataengineering
    ```
3. **Install Dependencies:**
   ```bash
   pip install -r requirements.txt
   ```
4.  **Start the Watcher:** Run the watcher script. This will start the continuous monitoring process.
    ```bash
    python src/pipeline/watcher.py
    ```
5.  **Ingest Data:** Place your CSV files (from either the `data/` or `data_corrupted/` folders) into the `incoming/` folder. The pipeline will automatically begin processing them.

The pipeline will now automatically:
* Validate the incoming files.
* Log any errors encountered.
* Quarantine all invalid rows for later analysis.
* Archive all valid rows.
* Transform the data into standard form
* Aggregate the data for analysis.
* Load both raw and aggregated data into the PostgreSQL database.


