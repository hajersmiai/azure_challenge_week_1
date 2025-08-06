import logging
import azure.functions as func
from function import ingest_all_data

def main(mytimer: func.TimerRequest = None, req: func.HttpRequest = None) -> func.HttpResponse:
    logging.info("Azure Function triggered...")
    try:
        ingest_all_data()
        logging.info(" Data ingestion completed successfully.")
    except Exception as e:
        logging.error(f" Error during data ingestion: {e}")
    if req:
        return func.HttpResponse("Ingestion done via HTTP trigger.", status_code=200)

    return None