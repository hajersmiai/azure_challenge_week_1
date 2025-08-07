"""TrainDataIngestor module.

This module defines a `TrainDataIngestor` class that orchestrates data
ingestion from the iRail API into an Azure SQL data warehouse.  It
accepts instances of `IRailAPI` (for fetching data) and
`TrainDataRepository` (for writing data) and provides a method
to ingest liveboard data for a given station.  The ingestor
parses each departure record, inserts dimension records into Station,
Train and Date tables if necessary, computes the actual departure
datetime from the scheduled time and delay, and writes a new
movement fact to the TrainMovements table.

Note: Arrival information is not available from the liveboard
endpoint; therefore, the arrival station is set to the immediate
destination station in the departure record, and the arrival date
and times are set equal to the departure date/time.  For more
accurate arrival data, consider enriching this method with calls to
IRailAPI.get_vehicle() or IRailAPI.get_connections().
"""

from datetime import datetime, timedelta
from typing import Optional

from Function.iRail_API import IRailAPI
from Function.train_data_repository import TrainDataRepository


class TrainDataIngestor:
    """Orchestrates ingestion of iRail liveboard data into Azure SQL.

    This class encapsulates the logic for fetching data from the
    iRail API and inserting it into the star‑schema tables via a
    TrainDataRepository.  It currently supports ingesting liveboard
    departure data; future methods could be added to ingest
    compositions, disturbances, feedback, etc.
    """

    def __init__(self, api: IRailAPI, repo: TrainDataRepository) -> None:
        self.api = api
        self.repo = repo

    def ingest_liveboard(self, station_code: str, *, arrdep: str = "departure") -> None:
        """
        Ingest the liveboard departures (or arrivals) for a given station.

        Args:
            station_code: Name or ID of the station to query.
            arrdep: 'departure' or 'arrival'.  Defaults to 'departure'.

        This method will:
            1. Fetch liveboard data from the iRail API.
            2. Insert the departure station into the Station dimension if needed.
            3. For each train departure:
                a. Insert the arrival station into the Station dimension.
                b. Insert the train into the Train dimension.
                c. Insert the departure date/time into the Date dimension.
                d. Insert a fact row into the TrainMovements table.

        Arrival information is not available from the liveboard response; the
        arrival date/time fields are populated with the same values as the
        departure date/time.  To enrich arrival data, call
        IRailAPI.get_vehicle() or IRailAPI.get_connections().
        """
        data = self.api.get_liveboard(station=station_code, arrdep=arrdep)
        # Top-level stationinfo describes the queried station (departure station)
        dep_station = data["stationinfo"]["id"]
        # Insert or retrieve the departure station into the dimension table.
        # The insert_station method returns the numeric StationID but we
        # ignore it here because get_station_key in insert_train_movement will
        # determine the correct surrogate key.  We call insert_station to
        # ensure latitude/longitude and names are stored in the dimension.
        self.repo.insert_station(
            station_code=dep_station,
            name=data["stationinfo"].get("name"),
            standard_name=data["stationinfo"].get("standardname"),
            latitude=float(data["stationinfo"].get("locationY")) if data["stationinfo"].get("locationY") is not None else None,
            longitude=float(data["stationinfo"].get("locationX")) if data["stationinfo"].get("locationX") is not None else None,
            iri_url=data["stationinfo"].get("@id"),
        )
        departures = data.get("departures", {}).get("departure", [])
        for dep in departures:
            # Determine arrival station (immediate next stop)
            arr_station = dep.get("stationinfo", {}).get("id")
            arr_standard_name = dep.get("stationinfo", {}).get("standardname")
            arr_name = dep.get("stationinfo", {}).get("name")
            arr_lat = dep.get("stationinfo", {}).get("locationY")
            arr_lng = dep.get("stationinfo", {}).get("locationX")
            arr_uri = dep.get("stationinfo", {}).get("@id")
            # Insert or retrieve arrival station
            self.repo.insert_station(
                station_code=arr_station,
                name=arr_name,
                standard_name=arr_standard_name,
                latitude=float(arr_lat) if arr_lat is not None else None,
                longitude=float(arr_lng) if arr_lng is not None else None,
                iri_url=arr_uri,
            )
            # Insert or retrieve train dimension
            train_info = self.api.get_train_info(dep["vehicle"])
            self.repo.insert_train(
                train_number=train_info["trainNumber"],
                train_type=train_info.get("trainType"),
                operator=train_info["operator"],
                vehicle_shortname=train_info["trainId"],
            )
            # Parse scheduled departure timestamp and delay
            sched_dep = datetime.fromtimestamp(int(dep["time"]))
            delay_sec = int(dep.get("delay", 0))
            actual_dep = sched_dep + timedelta(seconds=delay_sec)
            delay_min = delay_sec // 60 if delay_sec else None
            # Insert date dimension for departure
            dep_date_id = self.repo.insert_date(
                full_date=sched_dep.date(),
                day=sched_dep.day,
                month=sched_dep.month,
                year=sched_dep.year,
                hour=sched_dep.hour,
                minute=sched_dep.minute,
                second=sched_dep.second,
            )
            # Set arrival date/time equal to departure (no info available)
            arr_date_id: Optional[int] = dep_date_id
            sched_arr: Optional[datetime] = None
            actual_arr: Optional[datetime] = None
            # Insert the fact row using numeric surrogate keys.  Provide both
            # textual codes and names so the repository can derive or create
            # the appropriate dimension records before inserting the fact.
            self.repo.insert_train_movement(
                train_code=train_info["trainId"],
                train_number=train_info["trainNumber"],
                train_type=train_info.get("trainType"),
                operator=train_info["operator"],
                departure_station_code=dep_station,
                departure_station_name=data["stationinfo"].get("name"),
                arrival_station_code=arr_station,
                arrival_station_name=arr_name,
                departure_date_id=dep_date_id,
                arrival_date_id=arr_date_id,
                scheduled_departure=sched_dep,
                actual_departure=actual_dep,
                scheduled_arrival=sched_arr,
                actual_arrival=actual_arr,
                delay_minutes=delay_min,
                platform=str(dep.get("platform", "")) if dep.get("platform") is not None else None,
            )

    def ingest_composition_for_train(self, vehicle_id: str, *, date: Optional[str] = None) -> None:
        """
        Ingest composition units for a specific train.

        Args:
            vehicle_id: The iRail vehicle ID (e.g. 'BE.NMBS.IC3033').
            date: Optional date (ddmmyy) for the composition.  If not provided,
                  the current date is used by the API.

        This method fetches the composition for the given vehicle, parses the
        units, and writes each unit into the TrainCompositionUnit table.

        The train associated with the composition is looked up (or inserted)
        in the Train dimension to obtain its numeric TrainID.  That numeric
        key is then stored in the TrainCompositionUnit table to maintain
        referential integrity.
        """
        # First fetch composition data from the API
        comp = self.api.get_composition(vehicle_id, date=date)
        # Extract unit-level dictionaries from the composition
        units = IRailAPI.extract_composition_units(comp)
        if not units:
            return
        # Determine train information and ensure the train dimension exists
        try:
            train_info = self.api.get_train_info(vehicle_id)
        except Exception:
            # If we cannot determine train info (e.g. vehicle_id unknown),
            # default to using the vehicle_id as both shortname and number.
            train_info = {
                "trainId": vehicle_id.split(".")[-1],
                "trainNumber": vehicle_id.split(".")[-1],
                "trainType": None,
                "operator": vehicle_id.split(".")[1] if "." in vehicle_id else "",
            }
        # Insert the train into the dimension (if not exists) and retrieve the numeric key
        self.repo.insert_train(
            train_number=train_info["trainNumber"],
            train_type=train_info.get("trainType"),
            operator=train_info["operator"],
            vehicle_shortname=train_info["trainId"],
        )
        train_id_num = self.repo.get_train_key(
            vehicle_shortname=train_info["trainId"],
            train_number=train_info["trainNumber"],
            train_type=train_info.get("trainType"),
            operator=train_info["operator"],
        )
        # Now insert each unit of this train composition.  Segment origin and destination
        # identifiers are stored as returned by the API (station codes).  The train ID
        # used is the numeric surrogate key retrieved above.
        for unit in units:
            self.repo.insert_composition_unit(
                train_id=train_id_num,
                segment_origin_id=unit.get("segmentOriginId"),
                segment_destination_id=unit.get("segmentDestinationId"),
                unit_id=unit.get("unitId"),
                parent_type=unit.get("parent_type"),
                sub_type=unit.get("sub_type"),
                orientation=unit.get("orientation"),
                has_toilets=unit.get("hasToilets"),
                has_tables=unit.get("hasTables"),
                has_second_class_outlets=unit.get("hasSecondClassOutlets"),
                has_first_class_outlets=unit.get("hasFirstClassOutlets"),
                has_heating=unit.get("hasHeating"),
                has_airco=unit.get("hasAirco"),
                material_number=unit.get("materialNumber"),
                traction_type=unit.get("tractionType"),
                can_pass_to_next_unit=unit.get("canPassToNextUnit"),
                standing_places_second_class=unit.get("standingPlacesSecondClass"),
                standing_places_first_class=unit.get("standingPlacesFirstClass"),
                seats_second_class=unit.get("seatsSecondClass"),
                seats_first_class=unit.get("seatsFirstClass"),
                length_in_meter=unit.get("lengthInMeter"),
                has_semi_automatic_interior_doors=unit.get("hasSemiAutomaticInteriorDoors"),
                has_luggage_section=unit.get("hasLuggageSection"),
                material_sub_type_name=unit.get("materialSubTypeName"),
                traction_position=unit.get("tractionPosition"),
                has_prm_section=unit.get("hasPrmSection"),
                has_priority_places=unit.get("hasPriorityPlaces"),
                has_bike_section=unit.get("hasBikeSection"),
            )

    def ingest_disturbances(self) -> None:
        """
        Ingest current disturbances into the Disturbance table.

        This method fetches the list of disturbances from the API, parses
        the timestamp into a Python datetime, and inserts each record
        into the Disturbance table via the repository.
        """
        disturbances = self.api.get_disturbances()
        print(f"[INFO] {len(disturbances)} disturbance(s) fetched from API.")

        for idx, d in enumerate(disturbances):
            print(f"\n[INFO] Processing disturbance {idx + 1}/{len(disturbances)}: ID = {d.get('id')}")

            ts = d.get("timestamp")
            timestamp_dt = None
            if ts is not None:
                if isinstance(ts, datetime):
                    timestamp_dt = ts
                else:
                    try:
                        timestamp_dt = datetime.fromtimestamp(int(ts))
                    except Exception as e:
                        print(f"[WARN] Failed to parse timestamp '{ts}' → {e}")

            try:
                self.repo.insert_disturbance(
                    disturbance_id=d.get("id"),
                    title=d.get("title"),
                    description=d.get("description"),
                    type=d.get("type"),
                    timestamp=timestamp_dt,
                    link=d.get("link"),
                    attachment=d.get("attachment"),
                )
                print(f"[SUCCESS] Disturbance ID {d.get('id')} inserted.")
            except Exception as e:
                print(f"[ERROR] Failed to insert disturbance ID {d.get('id')}: {e}")
