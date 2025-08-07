import azure.functions as func
import logging
from Function.function import ingest_all_data

app = func.FunctionApp()

@app.schedule(schedule="0 */15 * * * *", arg_name="mytimer", run_on_startup=False, use_monitor=True)
def main(mytimer: func.TimerRequest) -> None:
    logging.info("Timer Trigger received.")

    try:
        ingest_all_data()
        logging.info(" Timer ingestion succeeded.")

    except Exception as e:
        logging.error(f" Timer ingestion failed: {e}")
