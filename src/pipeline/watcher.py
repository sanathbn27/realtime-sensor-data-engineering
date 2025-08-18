import time
import os
import sys
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from loguru import logger
import threading
import shutil

from src.pipeline.validation import validate_file
from src.pipeline.transformation import transform_file 
from src.pipeline.aggregation import aggregate_file

from ..database.load_raw_data import load_raw_file
from ..database.load_aggregated_data import load_aggregated_file

BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(BASE_DIR))
INCOMING_DIR = BASE_DIR /"incoming"
LOGS_DIR = BASE_DIR / "logs"

os.makedirs(INCOMING_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

logger.add(LOGS_DIR /"pipeline.log", rotation="1 MB")

PROCESSED_FILES = set()
file_lock = threading.Lock()

class IncomingHandler(FileSystemEventHandler):
    def on_created(self, event):
        self.process(event)

    # def on_modified(self, event):
    #     self.process(event)

    def process(self, event):
        # A unique identifier for the file to prevent re-processing.
        file_id = os.path.basename(event.src_path)

        # Check if the file has already been processed in this session.
         # Use the lock to ensure only one thread checks the set at a time
        with file_lock:
            if file_id in PROCESSED_FILES:
                logger.info(f"Skipping already processed file: {event.src_path}")
                return
            
            # Add the file to the processed set immediately
            PROCESSED_FILES.add(file_id)
        
        if not event.is_directory and event.src_path.endswith(".csv"):
            time.sleep(0.5)  # Allow time for file to be fully written
            
            try:
                logger.info(f"New or modified file detected: {event.src_path}")

                #step 1: Validate the file
                all_valid = validate_file(event.src_path)
                logger.debug(f"Files currently in archive: {list((BASE_DIR / 'archive').glob('*.csv'))}")

                if all_valid:
                    logger.info(f"All rows in {event.src_path} valid")
                else:
                    logger.warning(f"Some rows in {event.src_path} failed validation")

                # Step 2: Transformation
                archive_path = BASE_DIR / "archive" / Path(event.src_path).name

                    # # Wait up to 3 seconds for the archive file to appear
                    # for _ in range(6):  # 6 x 0.5s = 3 seconds max wait
                    #     if archive_path.exists():
                    #         break
                    #     time.sleep(0.5)
                logger.debug(f"Looking for archive file: {archive_path}")

                max_wait_time_seconds = 5
                wait_start = time.time()
                while not archive_path.exists() and (time.time() - wait_start) < max_wait_time_seconds:
                    logger.info(f"Waiting for archive file to appear: {archive_path}")
                    time.sleep(0.5)

                if archive_path.exists():
                    transformed_path = transform_file(archive_path)
                    logger.info(f"File transformed and saved to {transformed_path}")
                else:
                    logger.warning(f"Archive file not found for transformation: {archive_path}")

                # Step 3: Aggregation

                transformed_path = BASE_DIR / "transformed_data" / Path(event.src_path).name

                logger.debug(f"Looking for transformed file: {transformed_path}")
                max_wait_time_seconds = 5
                wait_start = time.time()
                while not transformed_path.exists() and (time.time() - wait_start) < max_wait_time_seconds:
                    logger.info(f"Waiting for transformed file to appear: {transformed_path}")
                    time.sleep(0.5)

                if transformed_path.exists():
                    aggregated_file = aggregate_file(transformed_path)
                    logger.info(f"Aggregated metrics saved to {aggregated_file}")
                    
                else:   
                    logger.warning(f"Transformed file not found for aggregation: {transformed_path}")

                # Step 4: Load raw data and aggregated data into the database
                try:
                    logger.info(f"Inserting RAW data into DB from {event.src_path}")
                    load_raw_file(Path(event.src_path))  # raw data from INCOMING
                except Exception as e:
                    logger.error(f"Failed to insert RAW data: {e}")
                    shutil.move(event.src_path, BASE_DIR / "failed" / Path(event.src_path).name)
                    return

                try:
                    logger.info(f"Inserting AGGREGATED data into DB from {aggregated_file}")
                    load_aggregated_file(Path(aggregated_file))
                except Exception as e:
                    logger.error(f"Failed to insert AGGREGATED data: {e}")
                    shutil.move(aggregated_file, BASE_DIR / "failed" / Path(aggregated_file).name)

                
            except PermissionError as e:
                logger.error(f"Permission error while reading {event.src_path}: {e}")
            except Exception as e:
                logger.error(f"Unexpected error while processing {event.src_path}: {e}")

if __name__ == "__main__":
    event_handler = IncomingHandler()
    observer = Observer()
    observer.schedule(event_handler, INCOMING_DIR, recursive=False)
    observer.start()
    logger.info("Started monitoring incoming folder...")

    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
