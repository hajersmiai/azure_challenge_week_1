import logging
import azure.functions as func
from Function.function import ingest_all_data

def main(req: func.HttpRequest) -> func.HttpResponse:
    print("STEP A: Function triggered (HTTP)")
    logging.info("HTTP Trigger received.")

    try:
        print("STEP B: Calling ingest_all_data...")
        ingest_all_data()
        print("STEP C: Finished ingest_all_data")
        logging.info("HTTP ingestion succeeded.")
        return func.HttpResponse("HTTP ingestion successful", status_code=200)

    except Exception as e:
        logging.error(f"HTTP ingestion failed: {e}")
        return func.HttpResponse(f"HTTP ingestion failed: {e}", status_code=500)
