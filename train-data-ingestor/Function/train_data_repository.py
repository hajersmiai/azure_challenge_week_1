from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from datetime import datetime
from typing import Optional



class TrainDataRepository:
    """
    A simple data access layer for storing train-related data into an Azure SQL
    database.  This class encapsulates the connection and provides helper
    methods for inserting records into the dimension and fact tables defined
    in the star schema (Train, Station, DateDimension, TrainMovements, and
    TrainFeedback).

    Example usage:

        repo = TrainDataRepository(
            server="my-server.database.windows.net",
            database="train-data-db",
            uid="sqladmin",
            pwd="StrongPassword123!"
        )
        # Insert a station if it doesn't already exist
        repo.insert_station(
            station_id="BE.NMBS.008821006",
            name="Antwerp-Central",
            standard_name="Antwerpen-Centraal",
            latitude=51.2172,
            longitude=4.421101,
            irail_id="BE.NMBS.008821006",
            iri_url="http://irail.be/stations/NMBS/008821006",
        )
        # Insert a train
        repo.insert_train(
            train_id="IC3033",
            train_number="3033",
            train_type="IC",
            operator="NMBS"
        )
        # Insert a date record
        dt = datetime(2025, 8, 5, 10, 30, 0)
        repo.insert_date(
            full_date=dt.date(),
            day=dt.day,
            month=dt.month,
            year=dt.year,
            hour=dt.hour,
            minute=dt.minute,
            second=dt.second,
        )
        # Finally insert a train movement
        repo.insert_train_movement(
            train_id="IC3033",
            station_id="BE.NMBS.008821006",
            date_id=20250805103000,
            departure_time=dt,
            arrival_time=None,
            delay_seconds=60,
            platform="4"
        )

    Note: All inserts use an `IF NOT EXISTS` pattern to avoid duplicate
    dimension records. The fact table does not perform deduplication; it
    always inserts a new movement event.
    """
    def __init__(self, server: str, database: str, uid: str, pwd: str) -> None:
        # Create SQLAlchemy engine with pymssql (pure Python)
        conn_str = f"mssql+pymssql://{uid}:{pwd}@{server}:1433/{database}"
        self.engine: Engine = create_engine(conn_str, echo=False, future=True)
    # ------------------------------------------------------------------
    # Helper methods to fetch or create numeric keys
    # ------------------------------------------------------------------
    def get_train_key(
        self,
        *,
        vehicle_shortname: str,
        train_number: str,
        train_type: Optional[str],
        operator: str,
        ) -> int:
        """
        Retrieve the numeric TrainID from the Train dimension table using a combination
        of attributes. Insert it if it does not exist.
        """
        select_sql = text("""
            SELECT TrainID
            FROM Train
            WHERE TrainNumber = train_number AND Operator = operator
        """)

        with self.engine.begin() as conn:
            result = conn.execute(select_sql, {
                "train_number": train_number,
                "operator": operator,
            }).fetchone()

            if result:
                return result.TrainID

            insert_sql = text("""
                INSERT INTO Train (TrainNumber, TrainType, Operator, id)
                VALUES (train_number, train_type, operator, vehicle_shortname);

                SELECT CAST(SCOPE_IDENTITY() AS INT) AS TrainID;
            """)

            result = conn.execute(insert_sql, {
                "train_number": train_number,
                "train_type": train_type,
                "operator": operator,
                "vehicle_shortname": vehicle_shortname
            }).fetchone()

        return result.TrainID
    # ------------------------------------------------------------------

    def get_station_key(
    self,
    *,
    station_code: str,
    station_name: str,
    latitude: Optional[float],
    longitude: Optional[float],
    iri_url: Optional[str],
    standard_name: Optional[str] = None,
    ) -> int:
        """
        Retrieve the numeric StationID from the Station dimension table using station code or name.
        Insert if not found.
        """
        with self.engine.begin() as conn:
        # Try by station code
            result = conn.execute(text("""
                SELECT StationID FROM Station WHERE id = station_code
            """)).fetchone()

            if result:
                return result.StationID

            # Try by station name (fallback)
            result = conn.execute(text("""
                SELECT StationID FROM Station WHERE StationName = station_name
            """), {
                "station_name": station_name
            }).fetchone()

            if result:
                return result.StationID

            # Insert new station
            insert_sql = text("""
                INSERT INTO Station (
                    StationName, standard_name, latitude, longitude,
                    id, iri_url
                ) VALUES (
                    station_name, standard_name, latitude, longitude,
                    station_code, iri_url
                );

                SELECT CAST(SCOPE_IDENTITY() AS INT) AS StationID;
            """)

            result = conn.execute(insert_sql, {
                "station_name": station_name,
                "standard_name": standard_name,
                "latitude": latitude,
                "longitude": longitude,
                "station_code": station_code,
                "iri_url": iri_url,
            }).fetchone()

        return result.StationID


    # ------------------------------------------------------------------
    # Dimension table inserts
    # ------------------------------------------------------------------


    def insert_station(
    self,
    *,
    station_code: str,
    name: str,
    standard_name: Optional[str],
    latitude: Optional[float],
    longitude: Optional[float],
    iri_url: Optional[str],
    ) -> int:
        """
        Insert a station into the Station dimension if it does not already exist.
        Returns the numeric StationID.
        """

        # Step 1: Check if the station already exists
        select_sql = text("""
            SELECT StationID
            FROM Station
            WHERE id = station_code
        """)

        with self.engine.begin() as conn:
            result = conn.execute(select_sql, {
                "station_code": station_code
            }).fetchone()

            if result:
                return result.StationID

            # Step 2: Insert the station and return StationID
            insert_sql = text("""
                INSERT INTO Station (
                    StationName, standard_name, latitude, longitude,
                    id, iri_url
                )
                VALUES (
                    name, standard_name, latitude, longitude,
                    station_code, iri_url
                );

                SELECT CAST(SCOPE_IDENTITY() AS INT) AS StationID;
            """)

            result = conn.execute(insert_sql, {
                "name": name,
                "standard_name": standard_name,
                "latitude": latitude,
                "longitude": longitude,
                "station_code": station_code,
                "iri_url": iri_url,
            }).fetchone()

        return result.StationID

    # ------------------------------------------------------------------

    def insert_train(
        self,
        *,
        train_number: str,
        train_type: Optional[str],
        operator: str,
        vehicle_shortname: Optional[str] = None,
        ) -> int:
        """
        Insert a train into the Train dimension if it does not already exist.
        Returns the numeric TrainID.
        """

        # Step 1: Check if train already exists
        select_sql = text("""
            SELECT TrainID
            FROM Train
            WHERE TrainNumber = train_number AND Operator = operator
            """)

        with self.engine.begin() as conn:
            result = conn.execute(select_sql, {
                "train_number": train_number,
                "operator": operator,
                 }).fetchone()

            if result:
                return result.TrainID

            # Step 2: Insert the new train and return generated ID
            insert_sql = text("""
                INSERT INTO Train (TrainNumber, TrainType, Operator, id)
                VALUES (train_number, train_type, operator, vehicle_shortname);

                SELECT CAST(SCOPE_IDENTITY() AS INT) AS TrainID;
                """)

            result = conn.execute(insert_sql, {
                "train_number": train_number,
                "train_type": train_type,
                "operator": operator,
                "vehicle_shortname": vehicle_shortname,
                }).fetchone()

        return result.TrainID
    #-------------------------------------------------------------------------------------------------------------

    def insert_date(
        self,
        *,
        full_date: datetime.date,
        day: int,
        month: int,
        year: int,
        hour: int,
        minute: int,
        second: int,
        ) -> int:
        """
        Insert a record into the DateDimension table if it does not already exist.
        Returns the deterministic date_id (YYYYMMDDHHMMSS).
        """
        # Compute a deterministic date_id
        date_id = (
            year * 10000000000
            + month * 100000000
            + day * 1000000
            + hour * 10000
            + minute * 100
            + second
            )

        insert_sql = text("""
            IF NOT EXISTS (
                SELECT 1 FROM DateDimension WHERE DateID = date_id
            )
            BEGIN
                INSERT INTO DateDimension (
                    DateID, FullDate, Year, Month, Day, Hour, Minute, Second
                )
                VALUES (
                    date_id, full_date, year, month, day, hour, minute, second
                )
            END
            """)

        with self.engine.begin() as conn:
            conn.execute(insert_sql, {
                "date_id": date_id,
                "full_date": full_date,
                "year": year,
                "month": month,
                "day": day,
                "hour": hour,
                "minute": minute,
                "second": second,
            })

        return date_id

        # ------------------------------------------------------------------
        # Fact table insert
        # ------------------------------------------------------------------


    def insert_train_movement(
        self,
        *,
        train_code: str,
        train_number: str,
        train_type: Optional[str],
        operator: str,
        departure_station_code: str,
        departure_station_name: str,
        arrival_station_code: str,
        arrival_station_name: str,
        departure_date_id: int,
        arrival_date_id: Optional[int],
        scheduled_departure: datetime,
        actual_departure: datetime,
        scheduled_arrival: Optional[datetime],
        actual_arrival: Optional[datetime],
        delay_minutes: Optional[int],
        platform: Optional[str],
        ) -> None:
        """
        Insert a record into the TrainMovements fact table using numeric keys.
        """

        # Step 1: Fetch or create foreign keys
        train_id_num = self.get_train_key(
            vehicle_shortname=train_code,
            train_number=train_number,
            train_type=train_type,
            operator=operator,
        )

        dep_station_id_num = self.get_station_key(
            station_code=departure_station_code,
            station_name=departure_station_name,
            latitude=None,
            longitude=None,
            iri_url=None,
            standard_name=None,
        )

        arr_station_id_num = self.get_station_key(
            station_code=arrival_station_code,
            station_name=arrival_station_name,
            latitude=None,
            longitude=None,
            iri_url=None,
            standard_name=None,
        )

        # Step 2: Insert into TrainMovements using SQLAlchemy
        insert_sql = text("""
            INSERT INTO TrainMovements (
                TrainID,
                DepartureStationID,
                ArrivalStationID,
                DepartureDateID,
                ArrivalDateID,
                ScheduledDepartureTime,
                ActualDepartureTime,
                ScheduledArrivalTime,
                ActualArrivalTime,
                DelayMinutes,
                Platform
                ) VALUES (
                train_id,
                dep_station_id,
                arr_station_id,
                departure_date_id,
                arrival_date_id,
                scheduled_departure,
                actual_departure,
                scheduled_arrival,
                actual_arrival,
                delay_minutes,
                platform
                )
            """)

        with self.engine.begin() as conn:
            conn.execute(insert_sql, {
                "train_id": train_id_num,
                "dep_station_id": dep_station_id_num,
                "arr_station_id": arr_station_id_num,
                "departure_date_id": departure_date_id,
                "arrival_date_id": arrival_date_id,
                "scheduled_departure": scheduled_departure,
                "actual_departure": actual_departure,
                "scheduled_arrival": scheduled_arrival,
                "actual_arrival": actual_arrival,
                "delay_minutes": delay_minutes,
                "platform": platform,
                })

        # ------------------------------------------------------------------
        # Feedback table insert
        # ------------------------------------------------------------------


    def insert_feedback(
        self,
        *,
        connection_url: str,
        station_url: str,
        feedback_date: datetime.date,
        vehicle_url: str,
        occupancy_term: str,
        ) -> None:
        """
        Insert feedback data into the TrainFeedback table. Stores crowding reports for later analysis.
        """
        insert_sql = text("""
            INSERT INTO TrainFeedback (
                connectionUrl, stationUrl, feedbackDate,
                vehicleUrl, occupancy
            ) VALUES (
                connection_url, station_url, feedback_date,
                vehicle_url, occupancy_term
            )
            """)

        with self.engine.begin() as conn:
            conn.execute(insert_sql, {
                "connection_url": connection_url,
                "station_url": station_url,
                "feedback_date": feedback_date,
                "vehicle_url": vehicle_url,
                "occupancy_term": occupancy_term,
                })

        # ------------------------------------------------------------------
        # Train composition insert
        # ------------------------------------------------------------------
    def insert_composition_unit(
        self,
        *,
        train_id: str,
        segment_origin_id: Optional[str],
        segment_destination_id: Optional[str],
        unit_id: str,
        parent_type: Optional[str],
        sub_type: Optional[str],
        orientation: Optional[str],
        has_toilets: Optional[bool],
        has_tables: Optional[bool],
        has_second_class_outlets: Optional[bool],
        has_first_class_outlets: Optional[bool],
        has_heating: Optional[bool],
        has_airco: Optional[bool],
        material_number: Optional[str],
        traction_type: Optional[str],
        can_pass_to_next_unit: Optional[bool],
        standing_places_second_class: Optional[int],
        standing_places_first_class: Optional[int],
        seats_second_class: Optional[int],
        seats_first_class: Optional[int],
        length_in_meter: Optional[int],
        has_semi_automatic_interior_doors: Optional[bool],
        has_luggage_section: Optional[bool],
        material_sub_type_name: Optional[str],
        traction_position: Optional[int],
        has_prm_section: Optional[bool],
        has_priority_places: Optional[bool],
        has_bike_section: Optional[bool],
        ) -> None:
        insert_sql = text("""
            INSERT INTO TrainCompositionUnit (
                TrainID, SegmentOriginId, SegmentDestinationId,
                UnitId, ParentType, SubType, Orientation,
                HasToilets, HasTables, HasSecondClassOutlets, HasFirstClassOutlets,
                HasHeating, HasAirco, MaterialNumber, TractionType,
                CanPassToNextUnit, StandingPlacesSecondClass, StandingPlacesFirstClass,
                SeatsSecondClass, SeatsFirstClass, LengthInMeter,
                HasSemiAutomaticInteriorDoors, HasLuggageSection,
                MaterialSubTypeName, TractionPosition,
                HasPrmSection, HasPriorityPlaces, HasBikeSection
                ) VALUES (
                train_id, segment_origin_id, segment_destination_id,
                unit_id, parent_type, sub_type, orientation,
                has_toilets, has_tables, has_second_class_outlets, has_first_class_outlets,
                has_heating, has_airco, material_number, traction_type,
                can_pass_to_next_unit, standing_places_second_class, standing_places_first_class,
                seats_second_class, seats_first_class, length_in_meter,
                has_semi_automatic_interior_doors, has_luggage_section,
                material_sub_type_name, traction_position,
                has_prm_section, has_priority_places, has_bike_section
                )
                """)

        with self.engine.begin() as conn:
            conn.execute(insert_sql, {
                "train_id": train_id,
                "segment_origin_id": segment_origin_id,
                "segment_destination_id": segment_destination_id,
                "unit_id": unit_id,
                "parent_type": parent_type,
                "sub_type": sub_type,
                "orientation": orientation,
                "has_toilets": has_toilets,
                "has_tables": has_tables,
                "has_second_class_outlets": has_second_class_outlets,
                "has_first_class_outlets": has_first_class_outlets,
                "has_heating": has_heating,
                "has_airco": has_airco,
                "material_number": material_number,
                "traction_type": traction_type,
                "can_pass_to_next_unit": can_pass_to_next_unit,
                "standing_places_second_class": standing_places_second_class,
                "standing_places_first_class": standing_places_first_class,
                "seats_second_class": seats_second_class,
                "seats_first_class": seats_first_class,
                "length_in_meter": length_in_meter,
                "has_semi_automatic_interior_doors": has_semi_automatic_interior_doors,
                "has_luggage_section": has_luggage_section,
                "material_sub_type_name": material_sub_type_name,
                "traction_position": traction_position,
                "has_prm_section": has_prm_section,
                "has_priority_places": has_priority_places,
                "has_bike_section": has_bike_section,
                })
    # ------------------------------------------------------------------
    # Disturbance insert
    # ------------------------------------------------------------------
    def insert_disturbance(
        self,
        *,
        disturbance_id: str,
        title: Optional[str],
        description: Optional[str],
        type: Optional[str],
        timestamp: datetime,
        link: Optional[str],
        attachment: Optional[str],
        ) -> None:
        """
        Insert a disturbance record into the Disturbance table.
        If the disturbance_id already exists, skip insertion.
        Args:
            disturbance_id: Unique identifier for the disturbance (string).
            title: Short title of the disturbance.
            description: Detailed description.
            type: Type of disturbance (e.g. 'disturbance', 'planned').
            timestamp: Timestamp when the disturbance was reported.
            link: URL for more information.
            attachment: URL to an attachment (PDF, etc.), if available.
        """
        try:
            insert_sql = text("""
                    IF NOT EXISTS (
                        SELECT 1 FROM Disturbance WHERE DisturbanceId = :id
                    )
                    BEGIN
                        INSERT INTO Disturbance (
                            DisturbanceId, Title, Description, Type,
                            Timestamp, Link, Attachment
                        ) VALUES (
                            id, title, description, type, timestamp, link, attachment
                        )
                    END
                """)

            with self.engine.begin() as conn:
                conn.execute(insert_sql, {
                    "id": disturbance_id,
                    "title": title,
                    "description": description,
                    "type": type,
                    "timestamp": timestamp,
                    "link": link,
                    "attachment": attachment,
                })

        except Exception as e:
            if "PRIMARY KEY" in str(e):
                print(f"[INFO] Disturbance '{disturbance_id}' already exists — skipping.")
            else:
                raise
