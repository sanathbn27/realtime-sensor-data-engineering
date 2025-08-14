import time
import os
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from loguru import logger
from validation import validate_file

BASE_DIR = Path(__file__).resolve().parent.parent.parent

INCOMING_DIR = BASE_DIR /"incoming"
LOGS_DIR = BASE_DIR / "logs"

os.makedirs(INCOMING_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

logger.add(LOGS_DIR /"pipeline.log", rotation="1 MB")

class IncomingHandler(FileSystemEventHandler):
    def on_created(self, event):
        self.process(event)

    def on_modified(self, event):
        self.process(event)

    def process(self, event):
        if not event.is_directory and event.src_path.endswith(".csv"):
            time.sleep(0.5)  # Allow time for file to be fully written
            try:
                logger.info(f"New or modified file detected: {event.src_path}")
                all_valid = validate_file(event.src_path)
                if all_valid:
                    logger.info(f"All rows in {event.src_path} valid")
                else:
                    logger.warning(f"Some rows in {event.src_path} failed validation")
            except PermissionError as e:
                logger.error(f"Permission error while reading {event.src_path}: {e}")

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
