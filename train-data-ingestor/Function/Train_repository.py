from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from datetime import datetime
from typing import Optional
from Function.iRail_API import IRailAPI


class TrainDataRepository:
    def __init__(self, server: str, database: str, uid: str, pwd: str) -> None:
        conn_str = f"mssql+pymssql://{uid}:{pwd}@{server}:1433/{database}"
        self.engine: Engine = create_engine(conn_str, echo=False, future=True)
        self.api = IRailAPI(lang="en")

    def insert_train_departures(self, station_name: str):
        """
        Fetch and insert all live departures for a given station into TrainDepartures table.
        """
        print(f" Fetching departures for: {station_name}")

        stations_dict = self.api.get_stations()
        station_info = next((s for s in stations_dict.values() if s["name"] == station_name), None)
        if not station_info:
            print(f" Station not found: {station_name}")
            return

        try:
            liveboard = self.api.get_liveboard(station=station_name)
        except Exception as e:
            print(f" Failed to fetch liveboard for {station_name}: {e}")
            return

        inserted = 0
        for dep in liveboard.get("departures", {}).get("departure", []):
            vehicle_raw = dep.get("vehicle", "")
            if not vehicle_raw:
                continue

            vehicle = vehicle_raw.split(".")[-1]
            train_type = ''.join(c for c in vehicle if c.isalpha())
            train_number = ''.join(c for c in vehicle if c.isdigit())

            try:
                departure_time = datetime.fromtimestamp(int(dep.get("time", 0)))
            except:
                continue

            delay_seconds = int(dep.get("delay", 0))
            canceled = 1 if dep.get("canceled") == "1" else 0
            platform = dep.get("platform")

            values = {
                "stationId": station_info.get("id"),
                "stationName": station_info.get("name"),
                "standardStationName": station_info.get("standardname"),
                "longitude": float(station_info.get("locationX", 0)),
                "latitude": float(station_info.get("locationY", 0)),
                "iriUrl": station_info.get("@id"),
                "vehicle": vehicle,
                "trainType": train_type,
                "trainNumber": train_number,
                "platform": platform,
                "time": departure_time,
                "delaySeconds": delay_seconds,
                "canceled": canceled
            }

            insert_query = text("""
                INSERT INTO TrainDepartures (
                    stationId, stationName, standardStationName, longitude, latitude, iriUrl,
                    vehicle, trainType, trainNumber, platform, time, delaySeconds, canceled
                ) VALUES (
                    :stationId, :stationName, :standardStationName, :longitude, :latitude, :iriUrl,
                    :vehicle, :trainType, :trainNumber, :platform, :time, :delaySeconds, :canceled
                )
            """)

            try:
                with self.engine.begin() as conn:
                    conn.execute(insert_query, values)
                    inserted += 1
            except Exception as e:
                print(f" Insert failed for train {vehicle}: {e}")

        print(f" {inserted} departures inserted for station: {station_name}")

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
        Insert a disturbance into the Disturbance table (skip if already exists).
        """
        insert_sql = text("""
            IF NOT EXISTS (
                SELECT 1 FROM Disturbance WHERE DisturbanceId = :id
            )
            BEGIN
                INSERT INTO Disturbance (
                    DisturbanceId, Title, Description, Type,
                    Timestamp, Link, Attachment
                ) VALUES (
                    :id, :title, :description, :type, :timestamp, :link, :attachment
                )
            END
        """)

        try:
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
            print(f"Disturbance '{disturbance_id}' inserted.")
        except Exception as e:
            print(f" Insert disturbance failed: {e}")

    def insert_train_connections(
        self,
        *,
        departure_station: str,
        arrival_station: str,
        departure_time: datetime,
        arrival_time: datetime,
        duration: str,
        train_vehicle: str,
        number_of_vias: int
    ) -> None:
        """
        Insert a train connection into TrainConnections table.
        """
        insert_sql = text("""
            INSERT INTO TrainConnections (
                DepartureStation, ArrivalStation, DepartureTime,
                ArrivalTime, Duration, TrainVehicle, NumberOfVias
            ) VALUES (
                :departure_station, :arrival_station, :departure_time,
                :arrival_time, :duration, :train_vehicle, :number_of_vias
            )
        """)

        try:
            with self.engine.begin() as conn:
                conn.execute(insert_sql, {
                    "departure_station": departure_station,
                    "arrival_station": arrival_station,
                    "departure_time": departure_time,
                    "arrival_time": arrival_time,
                    "duration": duration,
                    "train_vehicle": train_vehicle,
                    "number_of_vias": number_of_vias
                })
            print(f" Connection {departure_station} → {arrival_station} inserted.")
        except Exception as e:
            print(f"Insert connection failed: {e}")
