# Real-Time Sensor Pipeline

Goal: Monitor `data/` for incoming CSVs every 5–10s, validate + quarantine bad rows, 
transform + aggregate and store raw + aggregates to PostgreSQL. Later provide MySQL schema.


## Dataset Setup

To simulate the incoming sensor data, we will use a pre-existing dataset and process it into smaller, manageable chunks.

1.  **Download Dataset:** Download the "Environmental Sensor Data (132K samples)" from Kaggle and place the `.csv` file into a new folder named `raw_dataset/`.
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

1. **Install PostgreSQL:** Download and install PostgreSQL on your machine from the official website.
2. **Create Database:** Use psql or preferred tool (e.g., PgAdmin) to create a new database for this project.
```sql
CREATE DATABASE sensor_db;
```
3. **Database Connection:** Ensure you have configured your database connection details (host, database name, user, password) in db_utils.py to allow the pipeline to connect.

### Database Schema 
We will create a new schema and two tables to store our data: one for the raw sensor data and another for the aggregated metrics.

1. **Create Schema**
First, connect to your new database and create a schema named raw and analytics. This helps organize and isolate our project's data.
```sql
CREATE SCHEMA raw;
CREATE SCHEMA analytics;
```


2. **Create Tables**
Next, create the two tables within the raw and analytics schema.

raw.raw_sensor_data Table
This table stores the raw sensor data coming to the incoming folder.

```sql
CREATE TABLE analytics.raw_sensor_data (
    id SERIAL PRIMARY KEY,
    file_name VARCHAR(255) NOT NULL,
    device_id VARCHAR(255) NOT NULL,
    ts TIMESTAMP NOT NULL,
    co_ppm NUMERIC,
    humidity_percent NUMERIC,
    lpg_ppm NUMERIC,
    smoke_ppm NUMERIC,
    temp_celsius NUMERIC,
    light BOOLEAN,
    motion BOOLEAN,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

analytics.aggregated_sensor_data Table
This table stores the aggregated metrics, linked to the original file.
```sql
CREATE TABLE analytics.aggregated_sensor_data (
    id SERIAL PRIMARY KEY,
    file_name VARCHAR(255) NOT NULL,
    processed_at TIMESTAMP NOT NULL,
    device_id VARCHAR(255) NOT NULL,
    co_min NUMERIC,
    co_max NUMERIC,
    co_mean NUMERIC,
    co_std NUMERIC,
    humidity_min NUMERIC,
    humidity_max NUMERIC,
    humidity_mean NUMERIC,
    humidity_std NUMERIC,
    lpg_min NUMERIC,
    lpg_max NUMERIC,
    lpg_mean NUMERIC,
    lpg_std NUMERIC,
    smoke_min NUMERIC,
    smoke_max NUMERIC,
    smoke_mean NUMERIC,
    smoke_std NUMERIC,
    temp_min NUMERIC,
    temp_max NUMERIC,
    temp_mean NUMERIC,
    temp_std NUMERIC,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
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

Once validated, the data is transformed into a clean, standardized format before being stored. The `transformation.py` module handles this process by:

* **Standardizing Timestamps:** The raw UNIX timestamp (`ts`) is converted into a readable datetime format (e.g., `YYYY-MM-DD HH:MM:SS.ffffff`). This is crucial for accurate time-series analysis.

* **Rounding Numeric Values:** Sensor readings for `humidity`, `temp`, `co`, `lpg`, and `smoke` are rounded to a specific number of decimal places to ensure consistent precision across the dataset.

* **Standardizing Boolean Values:** The `light` and `motion` columns are converted to a consistent boolean format (`True`/`False`).

## Analytical Processing and Aggregation

After transformation, the pipeline calculates aggregation metrics to provide immediate insights and prepare the data for downstream analysis. The `aggregation.py` module computes key metrics, such as:

* **Average `temp` and `humidity`** per device over a specific time window.

* **Maximum `co` and `smoke`** levels per device to identify potential alarms.

* **Count of `motion` and `light`** events to track activity.

These aggregated metrics are then stored in the database alongside the raw sensor data, providing a dual-layered approach to analysis.


---

## Data Loading
Once the new file has been found in the incoming folder and aggregated_data, the pipeline loads it directly into the PostgreSQL database.

* **Loading Raw Data:** The raw sensor data is loaded into the raw.raw_sensor_data table.

* **Loading Aggregated Data:** The newly created aggregated file is loaded into the analytics.aggregated_sensor_data table. This step is designed to efficiently handle new files as they are created.

## Running the Real-Time Pipeline

1.  **Activate Environment:** Ensure you have activated your development environment, such as a Conda environment.
    ```bash
    conda activate sensor_dataengineering
    ```
2.  **Start the Watcher:** Run the watcher script. This will start the continuous monitoring process.
    ```bash
    python src/pipeline/watcher.py
    ```
3.  **Ingest Data:** Place your CSV files (from either the `data/` or `data_corrupted/` folders) into the `incoming/` folder. The pipeline will automatically begin processing them.

The pipeline will now automatically:
* Validate the incoming files.
* Log any errors encountered.
* Quarantine all invalid rows for later analysis.
* Archive all valid rows.
* Transform the data into standard form
* Aggregate the data for analysis.
* Load both raw and aggregated data into the PostgreSQL database.


