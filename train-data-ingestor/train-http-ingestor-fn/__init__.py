import logging
import azure.functions as func

def main(req: func.HttpRequest) -> func.HttpResponse:
    # Configuration du logging pour Azure
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.info("========== FUNCTION START ==========")

    try:
        # Test 1: Verify Python fonctionne
        logger.info("TEST 1: Python fonctionne")
        message = "Python OK"

        # Test 2: Verify l'import
        logger.info("TEST 2: Tentative d'import des modules")
        try:
            import sqlalchemy
            logger.info("SQLAlchemy imported with success")
            message += " | SQLAlchemy OK"
        except ImportError as e:
            logger.error(f"Error import SQLAlchemy: {e}")
            message += " | SQLAlchemy ERROR"

        try:
            import pymssql
            logger.info("pymssql imported with success")
            message += " | pymssql OK"
        except ImportError as e:
            logger.error(f"Error import pymssql: {e}")
            message += " | pymssql ERROR"

        try:
            import requests
            logger.info("requests imported with success")
            message += " | requests OK"
        except ImportError as e:
            logger.error(f"Error import requests: {e}")
            message += " | requests ERROR"

        # Test 3: Import of our custom modules
        logger.info("TEST 3: Import of our custom modules")
        try:
            from Function.train_data_repository import TrainDataRepository
            logger.info("TrainDataRepository imported")
            message += " | TrainDataRepository OK"
        except Exception as e:
            logger.error(f"Erreur import TrainDataRepository: {e}")
            message += f" | TrainDataRepository ERREUR: {str(e)}"

        try:
            from Function.iRail_API import IRailAPI
            logger.info("IRailAPI importé")
            message += " | IRailAPI OK"
        except Exception as e:
            logger.error(f"Error import IRailAPI: {e}")
            message += f" | IRailAPI ERROR: {str(e)}"

        try:
            from Function.DirectInsertor import DirectInsertor
            logger.info("DirectInsertor importé")
            message += " | DirectInsertor OK"
        except Exception as e:
            logger.error(f"Error import DirectInsertor: {e}")
            message += f" | DirectInsertor ERROR: {str(e)}"

        # Test 4: Connexion basique
        logger.info("TEST 4: Test of basic connection")
        try:
            from sqlalchemy import create_engine, text
            conn_str = "mssql+pymssql://sqladmin:Th021008....@train-sql-serve-hajer.database.windows.net:1433/train-data-db"
            engine = create_engine(conn_str, echo=False)

            # Test simple de connexion
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1 as test"))
                test_result = result.fetchone()
                if test_result:
                    logger.info("Connection BDD successful!")
                    message += " | BDD Connection OK"
                else:
                    logger.error("Connection BDD failed")
                    message += " | BDD Connection ERROR"

        except Exception as e:
            logger.error(f"Error connection BDD: {e}")
            message += f" | BDD ERROR: {str(e)}"

        # Appel à la fonction d'insertion directe
        from Function.function import ingest_all_data
        try:
            logger.info("TEST 5: Appel à run_direct_insertion")
            inserter = DirectInsertor(server="train-sql-serve-hajer.database.windows.net",
                        database="train-data-db",
                        uid="sqladmin",
                        pwd="Th021008...."
                    )
            inserter.ingest_all_data()
            ##ingest_all_data()
            message += " | Direct insertion OK"
        except Exception as e:
            logger.error(f"Error in DirectInsertor.run_direct_insertion: {e}")
            message += f" | DirectInsertor.run_direct_insertion ERROR: {str(e)}"

        logger.info("========== TESTS DONE ==========")
        logger.info(f"Result: {message}")

        return func.HttpResponse(
            f"Tests de diagnostic done: {message}",
            status_code=200
        )

    except Exception as e:
        error_msg = f"FATAL ERROR: {str(e)}"
        logger.error(error_msg)
        return func.HttpResponse(error_msg, status_code=500)
