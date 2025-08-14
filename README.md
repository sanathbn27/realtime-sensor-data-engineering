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

---

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
* Archive all valid rows.
* Quarantine all invalid rows for later analysis.

