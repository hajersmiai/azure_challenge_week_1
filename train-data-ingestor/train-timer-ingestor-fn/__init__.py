import azure.functions as func
import logging
from Function.function import ingest_all_data

# Creation of FunctionApp object
app = func.FunctionApp()

# Declaration of timer function
@app.schedule(
    schedule="0 */5 * * * *",  # every 5 minutes
    arg_name="mytimer",
    run_on_startup=True,
    use_monitor=False
)
def train_timer_ingestor(mytimer: func.TimerRequest) -> None:
    logging.info(f"Timer Trigger received at {mytimer.schedule_status.last}")
    
    try:
        ingest_all_data()
        logging.info(" Data ingestion completed.")
    except Exception as e:
        logging.error(f" Data ingestion failed: {e}")
