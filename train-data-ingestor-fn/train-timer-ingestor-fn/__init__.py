import azure.functions as func
import logging
from Function.function import ingest_all_data


def main(mytimer: func.TimerRequest) -> None:
    logging.info("Timer Trigger received.")

    try:
        ingest_all_data()
        logging.info(" Timer ingestion succeeded.")

    except Exception as e:
        logging.error(f" Timer ingestion failed: {e}")
