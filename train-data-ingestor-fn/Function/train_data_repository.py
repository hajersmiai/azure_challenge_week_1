import pyodbc
from datetime import datetime
from typing import Optional, Dict, Any


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
        # Build the connection string for Azure SQL Database
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={uid};"
            f"PWD={pwd}"
        )
        self.conn = pyodbc.connect(conn_str)
        self.cursor = self.conn.cursor()

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
        Retrieve the numeric TrainID from the Train dimension table using a
        combination of train attributes.  If a matching record does not
        exist, a new row is inserted and the generated TrainID is returned.

        Args:
            vehicle_shortname: The short name of the train (e.g. 'IC3033').
            train_number: The numeric part of the train code (e.g. '3033').
            train_type: The train type/category (e.g. 'IC', 'S', etc.).
            operator: The operator of the train (e.g. 'NMBS').

        Returns:
            The integer TrainID corresponding to the supplied train.
        """
        # Attempt to locate an existing train by its number and operator.  If
        # you choose to store vehicle_shortname in your Train table, include
        # it in the WHERE clause for more specificity.
        self.cursor.execute(
            "SELECT TrainID FROM Train WHERE TrainNumber = ? AND Operator = ?",
            train_number,
            operator,
        )
        row = self.cursor.fetchone()
        if row:
            return row.TrainID
        # No existing train found; insert a new record.  The TrainID column
        # should be defined as an IDENTITY in your table schema.  We omit
        # vehicle_shortname here because the canonical star-schema design
        # stores only TrainNumber, TrainType and Operator; adjust the
        # INSERT statement if you have a VehicleShortName column.
        self.cursor.execute(
            """
            INSERT INTO Train (TrainNumber, TrainType, Operator,id)
            VALUES (?, ?, ?, ?)
            """,
            train_number,
            train_type,
            operator,
            vehicle_shortname
        )
        self.conn.commit()
        # Retrieve the last generated identity value for the Train table.  In
        # SQL Server, @@IDENTITY returns the last identity value inserted in
        # the current session.  For better isolation, you could use
        # SCOPE_IDENTITY().  Fetchone().TrainID will contain the numeric key.
        row = self.cursor.execute("SELECT CAST(@@IDENTITY AS INT) AS TrainID").fetchone()
        return row.TrainID

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
        Retrieve the numeric StationID from the Station dimension table using
        the station's unique code or name.  If a matching record does not
        exist, a new row is inserted and the generated StationID is returned.

        Args:
            station_code: The iRail station code (e.g. 'BE.NMBS.008821006').
            station_name: The display name of the station.
            latitude: Latitude coordinate (can be None if unknown).
            longitude: Longitude coordinate (can be None if unknown).
            iri_url: The canonical URI for the station.
            standard_name: The standardized station name, if available.

        Returns:
            The integer StationID corresponding to the supplied station.
        """
        # Attempt to locate an existing station by its iRail code.  If your
        # schema stores irail_id in a separate column, adjust the WHERE clause
        # accordingly.  We first try matching on irail_id and fall back to
        # StationName in case the code is missing.
        self.cursor.execute(
            "SELECT StationID FROM Station WHERE id = ?",
            station_code,
        )
        row = self.cursor.fetchone()
        if row:
            return row.StationID
        # Attempt fallback match on station_name if id didn't match
        self.cursor.execute(
            "SELECT StatinID FROM Station WHERE StationName = ?",
            station_name,
        )
        row = self.cursor.fetchone()
        if row:
            return row.StationID
        # No existing station found; insert a new record.  StationID should be
        # defined as an IDENTITY in your schema.  We supply all known
        # attributes; unknown values remain NULL.
        self.cursor.execute(
            """
            INSERT INTO Station (
                StationName, standard_name, latitude, longitude,
                id, iri_url
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            station_name,
            standard_name,
            latitude,
            longitude,
            station_code,
            iri_url,
            
        )
        self.conn.commit()
        # Retrieve the last generated identity value for Station
        row = self.cursor.execute("SELECT CAST(@@IDENTITY AS INT) AS StationID").fetchone()
        return row.StationID

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
        Returns the numeric StationID.  The table is expected to use an
        IDENTITY column for StationID.

        Args:
            station_code: Unique iRail identifier for the station (e.g. 'BE.NMBS.008821006').
            name: Display name of the station (e.g. 'Antwerp-Central').
            standard_name: Standardized station name, if available.
            latitude: Latitude coordinate (locationY).
            longitude: Longitude coordinate (locationX).
            iri_url: Canonical URI for the station in the iRail ontology, if available.

        Returns:
            The numeric StationID of the inserted or existing station.
        """
        # Check if a station with this irail_id already exists
        self.cursor.execute(
            "SELECT StationID FROM Station WHERE id = ?",
            station_code,
        )
        row = self.cursor.fetchone()
        if row:
            return row.StationID
        # No existing station; insert a new row (StationID will auto-increment)
        self.cursor.execute(
            """
            INSERT INTO Station (
                StationName, standard_name, latitude, longitude,
                id, iri_url
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            name,
            standard_name,
            latitude,
            longitude,
            station_code,
            iri_url,
        )
        self.conn.commit()
        row = self.cursor.execute(
            "SELECT CAST(@@IDENTITY AS INT) AS StationID"
        ).fetchone()
        return row.StationID

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
        Returns the numeric TrainID.  The table is expected to use an
        IDENTITY column for TrainID.

        Args:
            train_number: Numeric part of the train code (e.g. '3033').
            train_type: Train category (e.g. 'IC', 'S', etc.).
            operator: Operating company (e.g. 'NMBS').
            vehicle_shortname: Short name of the train (e.g. 'IC3033'), optional.

        Returns:
            The numeric TrainID of the inserted or existing train.
        """
        # Find an existing train by number and operator (add train_type if desired)
        self.cursor.execute(
            "SELECT TrainID FROM Train WHERE TrainNumber = ? AND Operator = ?",
            train_number,
            operator,
        )
        row = self.cursor.fetchone()
        if row:
            return row.TrainID
        # No existing train; insert a new row (TrainID auto-increment)
        self.cursor.execute(
            """
            INSERT INTO Train (TrainNumber, TrainType, Operator,id)
            VALUES (?, ?, ?, ?)
            """,
            train_number,
            train_type,
            operator,
            vehicle_shortname
        )
        self.conn.commit()
        row = self.cursor.execute(
            "SELECT CAST(@@IDENTITY AS INT) AS TrainID"
        ).fetchone()
        return row.TrainID

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
        Insert a record into the DateDimension table if it does not already
        exist.  Returns the date_id used for the insert.  The date_id is
        generated deterministically based on the date and time components to
        ensure uniqueness.  For example, a record for 5 August 2025 at 10:30:00
        yields 20250805103000.

        Args:
            full_date: Python date object (only date, no time).
            day, month, year, hour, minute, second: Individual date/time components.

        Returns:
            The integer date_id that was inserted or found.
        """
        # Compute a deterministic date_id: YYYYMMDDHHMMSS
        date_id = (
            year * 10000000000
            + month * 100000000
            + day * 1000000
            + hour * 10000
            + minute * 100
            + second
        )
        # Insert if not exists
        self.cursor.execute(
            """
            IF NOT EXISTS (
                SELECT 1 FROM DateDimension WHERE DateID = ?
            )
            INSERT INTO DateDimension (
                DateID, FullDate, Year, Month, Day, Hour, Minute, Second
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            date_id,
            date_id,
            full_date,
            year,
            month,
            day,
            hour,
            minute,
            second,
        )
        self.conn.commit()
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
        Insert a record into the TrainMovements fact table using numeric keys
        for trains and stations.  This method will retrieve or create the
        corresponding dimension keys based on the provided textual codes and
        names, then insert the fact row.  No deduplication is performed.

        Args:
            train_code: Short code of the train (e.g. 'IC3033').
            train_number: Numeric part of the train code (e.g. '3033').
            train_type: Category of the train (e.g. 'IC', 'S', etc.).
            operator: Train operator (e.g. 'NMBS').
            departure_station_code: iRail code of the departure station.
            departure_station_name: Display name of the departure station.
            arrival_station_code: iRail code of the arrival station.
            arrival_station_name: Display name of the arrival station.
            departure_date_id: Date/time surrogate key for the departure.
            arrival_date_id: Date/time surrogate key for the arrival (may be None).
            scheduled_departure: Scheduled departure datetime.
            actual_departure: Actual departure datetime.
            scheduled_arrival: Scheduled arrival datetime (if available).
            actual_arrival: Actual arrival datetime (if available).
            delay_minutes: Delay in minutes, if known.
            platform: Platform designation as a string, if known.
        """
        # Fetch or create the numeric TrainID based on the train attributes
        train_id_num = self.get_train_key(
            vehicle_shortname=train_code,
            train_number=train_number,
            train_type=train_type,
            operator=operator,
        )

        # Fetch or create the numeric StationID for departure and arrival
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

        # Insert the fact row using numeric keys
        self.cursor.execute(
            """
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
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            train_id_num,
            dep_station_id_num,
            arr_station_id_num,
            departure_date_id,
            arrival_date_id,
            scheduled_departure,
            actual_departure,
            scheduled_arrival,
            actual_arrival,
            delay_minutes,
            platform,
        )
        self.conn.commit()

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
        Insert feedback data into the TrainFeedback table.  Stores crowding
        reports for later analysis.

        Args:
            connection_url: The iRail semantic connection URL.
            station_url: The iRail semantic station URL.
            feedback_date: Date of the feedback (a Python date object).
            vehicle_url: The iRail semantic vehicle URL.
            occupancy_term: Occupancy term (one of the URIs from iRail, e.g.
                            'http://api.irail.be/terms/low').
        """
        self.cursor.execute(
            """
            INSERT INTO TrainFeedback (
                connectionUrl, stationUrl, feedbackDate,
                vehicleUrl, occupancy
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            connection_url,
            station_url,
            feedback_date,
            vehicle_url,
            occupancy_term,
        )
        self.conn.commit()

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
        """
        Insert a single unit of a train composition into the TrainCompositionUnit table.

        Args:
            train_id: Identifier of the train (foreign key to Train).
            segment_origin_id: Station ID where this segment originates (nullable).
            segment_destination_id: Station ID where this segment ends (nullable).
            unit_id: Identifier of the unit/carriage within the composition.
            parent_type: Parent type of the material (e.g. 'AM08M').
            sub_type: Sub type of the material (e.g. 'c', 'a').
            orientation: Orientation of the unit (e.g. 'LEFT', 'RIGHT').
            has_toilets: True if the unit has toilets.
            has_tables: True if the unit has tables.
            has_second_class_outlets: True if second class outlets are available.
            has_first_class_outlets: True if first class outlets are available.
            has_heating: True if heating is available.
            has_airco: True if air conditioning is available.
            material_number: Identifier number of the unit.
            traction_type: Type of traction (e.g. 'AM/MR').
            can_pass_to_next_unit: True if passengers can pass to the next unit.
            standing_places_second_class: Number of standing places in second class.
            standing_places_first_class: Number of standing places in first class.
            seats_second_class: Number of seats in second class.
            seats_first_class: Number of seats in first class.
            length_in_meter: Length of the unit in meters.
            has_semi_automatic_interior_doors: True if semi‑automatic interior doors exist.
            has_luggage_section: True if a luggage section is present.
            material_sub_type_name: Sub type name (e.g. 'AM08M_c').
            traction_position: Position of traction (integer).
            has_prm_section: True if the unit has a PRM section.
            has_priority_places: True if the unit has priority places.
            has_bike_section: True if the unit has a bike section.
        """
        self.cursor.execute(
            """
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
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            train_id,
            segment_origin_id,
            segment_destination_id,
            unit_id,
            parent_type,
            sub_type,
            orientation,
            has_toilets,
            has_tables,
            has_second_class_outlets,
            has_first_class_outlets,
            has_heating,
            has_airco,
            material_number,
            traction_type,
            can_pass_to_next_unit,
            standing_places_second_class,
            standing_places_first_class,
            seats_second_class,
            seats_first_class,
            length_in_meter,
            has_semi_automatic_interior_doors,
            has_luggage_section,
            material_sub_type_name,
            traction_position,
            has_prm_section,
            has_priority_places,
            has_bike_section,
        )
        self.conn.commit()

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

        Args:
            disturbance_id: Unique identifier for the disturbance (string).
            title: Short title of the disturbance.
            description: Detailed description.
            type: Type of disturbance (e.g. 'disturbance', 'planned').
            timestamp: Timestamp when the disturbance was reported.
            link: URL for more information.
            attachment: URL to an attachment (PDF, etc.), if available.
        """
        self.cursor.execute(
            """
            INSERT INTO Disturbance (
                DisturbanceId, Title, Description, Type,
                Timestamp, Link, Attachment
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            disturbance_id,
            title,
            description,
            type,
            timestamp,
            link,
            attachment,
        )
        self.conn.commit()

