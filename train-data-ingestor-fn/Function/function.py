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

from iRail_API import IRailAPI
from train_data_repository import TrainDataRepository
from train_data_ingestor import TrainDataIngestor
from datetime import datetime


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


def ingest_all_data() -> None:
    api = IRailAPI(lang="en")
    repo = TrainDataRepository(
        server="train-sql-serve-hajer.database.windows.net",
        database="train-data-db",
        uid="sqladmin",
        pwd="Th021008....",
    )

    # Optionally load all stations once
    # populate_stations(api, repo)

    # Build a list of station codes to ingest
    stations_dict = api.get_stations()
    stations_to_ingest = list(stations_dict.keys())

    # Ingest departures for each station
    populate_liveboards(api, repo, stations_to_ingest)

    # Collect unique train vehicle IDs from liveboard data
    train_ids_set = set()
    for station_code in stations_to_ingest:
        try:
            board = api.get_liveboard(station=station_code)
            departures = board.get("departures", {}).get("departure", [])
            for dep in departures:
                vehicle = dep.get("vehicle")  # e.g. 'BE.NMBS.IC3033'
                if vehicle:
                    train_ids_set.add(vehicle)
        except Exception as ex:
            print(f"Failed to fetch liveboard for {station_code}: {ex}")

    train_ids_to_ingest = list(train_ids_set)

    # Ingest composition data for all discovered trains
    populate_compositions(api, repo, train_ids_to_ingest)

    # Ingest current disturbances
    populate_disturbances(api, repo)

