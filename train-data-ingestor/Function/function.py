"""
function.py
---------------------

This script demonstrates how to populate our Azure SQL database with
live train data from the iRail API using the reusable classes defined
in `iRail_API.py`, `train_data_repository.py` and
`train_data_ingestor.py`.  It connects to an Azure SQL Database,
optionally loads all stations into the Station dimension, and then
ingests live departures for one or more stations.

The script is designed to be run from our local environment or an
Azure Function.  We will need to supply valid connection details for
our Azure SQL server.
"""

from Function.iRail_API import IRailAPI
from Function.train_data_repository import TrainDataRepository
from Function.train_data_ingestor import TrainDataIngestor
from datetime import datetime
import os
import logging

def populate_stations(api: IRailAPI, repo: TrainDataRepository) -> None:
    """Insert all iRail stations into the Station dimension.

    Args:
        api: An instance of IRailAPI for fetching station data.
        repo: An instance of TrainDataRepository for writing to SQL.
    """
    stations = api.get_stations()
    for st in stations.values():
        repo.insert_station(
            station_code=st["id"],
            name=st["name"],
            standard_name=st.get("standardname"),
            latitude=float(st["locationY"]) if st.get("locationY") else None,
            longitude=float(st["locationX"]) if st.get("locationX") else None,
            iri_url=st.get("@id"),
        )


def populate_liveboards(api: IRailAPI, repo: TrainDataRepository, stations: list[str]) -> None:
    """Ingest live departures for a list of station names or IDs.

    Args:
        api: IRailAPI instance.
        repo: TrainDataRepository instance.
        stations: Iterable of station names or IDs to query.
    """
    ingestor = TrainDataIngestor(api, repo)
    for station_code in stations:
        print(f"Ingesting liveboard for {station_code}...")
        try:
            ingestor.ingest_liveboard(station_code)
        except Exception as ex:
            # Catch and log exceptions to avoid stopping the entire run.
            print(f"Failed to ingest {station_code}: {ex}")


def populate_compositions(api: IRailAPI, repo: TrainDataRepository, train_ids: list[str]) -> None:
    """Ingest composition units for a list of trains.

    Args:
        api: IRailAPI instance.
        repo: TrainDataRepository instance.
        train_ids: List of iRail vehicle IDs to ingest compositions for.
    """
    ingestor = TrainDataIngestor(api, repo)
    for vid in train_ids:
        print(f"Ingesting composition for train {vid}...")
        try:
            ingestor.ingest_composition_for_train(vid)
        except Exception as ex:
            print(f"Failed to ingest composition for {vid}: {ex}")


def populate_disturbances(api: IRailAPI, repo: TrainDataRepository) -> None:
    """Ingest current disturbances into the database.

    Fetches disturbances from the API and inserts them into the Disturbance table.
    """
    ingestor = TrainDataIngestor(api, repo)
    print("Ingesting current disturbances...")
    try:
        ingestor.ingest_disturbances()
    except Exception as ex:
        print(f"Failed to ingest disturbances: {ex}")


def ingest_all_data():
    print("STEP 1: Start ingestion process")
    start_time = datetime.now()
    stations_processed = 0
    trains_processed = 0
    # Hardcoded credentials (TEMP TEST ONLY)
    server = "train-sql-serve-hajer.database.windows.net"
    database = "train-data-db"
    uid = "sqladmin"
    pwd = "Th021008...."

    print(f"STEP 2: Using credentials: server={server}, database={database}, uid={uid}, pwd={'***' if pwd else None}")

    try:
        print("STEP 3: Connecting to SQL")
        repo = TrainDataRepository(server=server, database=database, uid=uid, pwd=pwd)

        print("STEP 4: Creating iRail API client")
        api = IRailAPI(lang="en")

        print("STEP 5: Fetching station list")
        stations_dict = api.get_stations()
        stations_to_ingest = list(stations_dict.keys())
        print(f"STEP 6: Fetched {len(stations_to_ingest)} stations")

        ingestor = TrainDataIngestor(api, repo)

        print("STEP 7: Ingesting liveboards...")
        for station_code in stations_to_ingest:
            print(f"    → Ingesting liveboard for: {station_code}")
            ingestor.ingest_liveboard(station_code)
            stations_processed += 1
            
        print("STEP 8: Gathering train IDs from departures")
        train_ids_set = set()
        for station_code in stations_to_ingest:
            board = api.get_liveboard(station=station_code)
            departures = board.get("departures", {}).get("departure", [])
            for dep in departures:
                vehicle = dep.get("vehicle")
                if vehicle:
                    train_ids_set.add(vehicle)

        train_ids_to_ingest = list(train_ids_set)
        print(f"STEP 9: Found {len(train_ids_to_ingest)} train IDs")

        print("STEP 10: Ingesting compositions")
        for vid in train_ids_to_ingest:
            print(f"    → Ingesting composition for train {vid}")
            ingestor.ingest_composition_for_train(vid)

        print("STEP 11: Ingesting disturbances...")
        ingestor.ingest_disturbances()

        print("STEP 12: Ingestion finished ")

        end_time = datetime.now()
        duration = end_time - start_time
        print(f"STEP 12: Ingestion finished in {duration}")
        print(f"Statistics: {stations_processed} stations, {trains_processed} trains")
    except Exception as e:
        print(f"ERROR during ingestion: {e}")
        raise
