"""
Direct insertor without any control
Objective: Quickly fill the database to identify issues
"""
from Function.iRail_API import IRailAPI 
from sqlalchemy import create_engine, text
import requests
from datetime import datetime, timedelta
import logging

class DirectInsertor:
    def __init__(self, server, database, uid, pwd):
        conn_str = f"mssql+pymssql://{uid}:{pwd}@{server}:1433/{database}"
        self.engine = create_engine(conn_str, echo=False)
        
    def insert_stations_direct(self):
        """Direct insertion of all iRail stations"""
        print("=== DIRECT INSERTION OF STATIONS ===")

        try:
            # Fetching stations from the API
            url = "https://api.irail.be/stations/?format=json&lang=en"
            response = requests.get(url)
            data = response.json()
            
            stations = data.get("stations", {}).get("station", [])
            print(f"Found {len(stations)} stations")

            # Batch direct insertion
            insert_sql = text("""
                INSERT INTO Station (StationName, Latitude, Longitude, standard_name, id, iri_url)
                VALUES (name, lat, lng, standard_name, station_id, iri_url)
            """)
       
            
            with self.engine.begin() as conn:
                inserted_count = 0
                for station in stations:
                    try:
                        conn.execute(insert_sql, {
                            "name": station.get("name", "Unknown"),
                            "lat": float(station.get("locationY")) if station.get("locationY") else None,
                            "lng": float(station.get("locationX")) if station.get("locationX") else None,
                            "station_id": station.get("id", f"UNKNOWN_{inserted_count}"),
                            "standard_name": station.get("standardname"),
                            "iri_url": station.get("@id", "")
                        })
                        inserted_count += 1
                        
                        if inserted_count % 50 == 0:
                            print(f"Inséré {inserted_count} stations...")
                            
                    except Exception as e:
                        print(f"Error station {station.get('name')}: {e}")
                        continue

                print(f" {inserted_count} stations inserted")
                return inserted_count
                
        except Exception as e:
            print(f" Error inserting stations: {e}")
            return 0

    def insert_sample_liveboards(self, max_stations=None):
        """Direct insertion of liveboards for a few stations"""
        print("=== DIRECT INSERTION OF LIVEBOARDS ===")
        api = IRailAPI(lang="en")
        # Stations principales belges
        sample_stations = api.get_stations()
        
        total_trains = 0
        total_movements = 0
        
        for station_name in sample_stations:
            try:
                print(f"Processing of {station_name}...")
                trains, movements = self._process_liveboard_direct(station_name)
                total_trains += trains
                total_movements += movements
                
            except Exception as e:
                print(f"Error for {station_name}: {e}")
                continue

        print(f" {total_trains} trains and {total_movements} movements inserted")
        return total_trains, total_movements

    def _process_liveboard_direct(self, station_name):
        """ Process a liveboard for a specific station and insert data directly into the database."""
        url = f"https://api.irail.be/liveboard/?station={station_name}&format=json&lang=en"
        response = requests.get(url)
        data = response.json()
        
        # Récupérer les informations de la station de départ
        station_info = data.get("stationinfo", {})
        dep_station_id = station_info.get("id")
        dep_station_name = station_info.get("name")
        
        departures = data.get("departures", {}).get("departure", [])
        if not isinstance(departures, list):
            departures = [departures]
        
        trains_inserted = 0
        movements_inserted = 0
        
        with self.engine.begin() as conn:
            for dep in departures:
                try:
                    # 1. Insérer le train directement
                    vehicle_id = dep.get("vehicle", "UNKNOWN")
                    train_parts = vehicle_id.split(".")
                    
                    if len(train_parts) >= 3:
                        operator = train_parts[1]
                        train_code = train_parts[2]
                        train_type = "".join(c for c in train_code if c.isalpha())
                        train_number = "".join(c for c in train_code if c.isdigit())
                    else:
                        operator = "UNKNOWN"
                        train_code = vehicle_id
                        train_type = "UNKNOWN"
                        train_number = vehicle_id
                    
                    # Insertion train
                    train_sql = text("""
                        INSERT INTO Train (TrainNumber, TrainType, Operator, id)
                        VALUES (number, type, operator, vehicle_id)
                    """)
                    
                    conn.execute(train_sql, {
                        "number": train_number,
                        "type": train_type,
                        "operator": operator,
                        "vehicle_id": train_code
                    })
                    trains_inserted += 1
                    
                    # 2. Insérer la station d'arrivée
                    arr_info = dep.get("stationinfo", {})
                    arr_station_id = arr_info.get("id", "UNKNOWN_ARR")
                    arr_station_name = arr_info.get("name", "Unknown Destination")
                    
                    station_sql = text("""
                        INSERT INTO Station (StationName, Latitude, Longitude, id, iri_url)
                        VALUES (name, lat, lng, station_id, uri)
                    """)
                    
                    conn.execute(station_sql, {
                        "name": arr_station_name,
                        "lat": float(arr_info.get("locationY")) if arr_info.get("locationY") else None,
                        "lng": float(arr_info.get("locationX")) if arr_info.get("locationX") else None,
                        "station_id": arr_station_id,
                        "uri": arr_info.get("@id", "")
                    })
                    
                    # 3. Insérer les dates
                    sched_time = datetime.fromtimestamp(int(dep.get("time", 0)))
                    delay_sec = int(dep.get("delay", 0))
                    actual_time = sched_time + timedelta(seconds=delay_sec)
                    
                    # Date ID simple: YYYYMMDDHHMM
                    date_id = int(sched_time.strftime("%Y%m%d%H%M"))
                    
                    date_sql = text("""
                        INSERT INTO DateDimension (DateID, FullDate, Day, Month, Year, Hour, Minute, Second)
                        VALUES (date_id, full_date, day, month, year, hour, minute, second)
                    """)
                    
                    conn.execute(date_sql, {
                        "date_id": date_id,
                        "full_date": sched_time.date(),
                        "day": sched_time.day,
                        "month": sched_time.month,
                        "year": sched_time.year,
                        "hour": sched_time.hour,
                        "minute": sched_time.minute,
                        "second": sched_time.second
                    })
                    
                    # 4. Insérer le mouvement (avec des IDs fixes temporaires)
                    movement_sql = text("""
                        INSERT INTO TrainMovements (
                            TrainID, DepartureStationID, ArrivalStationID,
                            DepartureDateID, ArrivalDateID, 
                            ScheduledDepartureTime, ActualDepartureTime,
                            DelayMinutes, Platform
                        ) VALUES (
                            1, 1, 2, date_id, date_id,
                            sched_time, actual_time, delay_min, platform
                        )
                    """)
                    
                    conn.execute(movement_sql, {
                        "date_id": date_id,
                        "sched_time": sched_time,
                        "actual_time": actual_time,
                        "delay_min": delay_sec // 60 if delay_sec else 0,
                        "platform": dep.get("platform", "")
                    })
                    movements_inserted += 1
                    
                except Exception as e:
                    print(f"Erreur insertion départ: {e}")
                    continue
        
        return trains_inserted, movements_inserted

    def insert_disturbances_direct(self):
        """insert disturbances directly into the database"""
        print("=== Insert Disturbances ===")

        try:
            url = "https://api.irail.be/disturbances/?format=json&lang=en"
            response = requests.get(url)
            data = response.json()
            
            disturbances = data.get("disturbance", [])
            if not isinstance(disturbances, list):
                disturbances = [disturbances]
            
            insert_sql = text("""
                INSERT INTO Disturbance (Title, Description, Type, Timestamp, Link, Attachment)
                VALUES (title, desc, type, timestamp, link, attachment)
            """)
            
            with self.engine.begin() as conn:
                inserted_count = 0
                for d in disturbances:
                    try:
                        ts = d.get("timestamp")
                        dt = datetime.fromtimestamp(int(ts)) if ts else datetime.now()
                        
                        conn.execute(insert_sql, {
                            "title": d.get("title", "No title"),
                            "desc": d.get("description", ""),
                            "type": d.get("type", "unknown"),
                            "timestamp": dt,
                            "link": d.get("link", ""),
                            "attachment": d.get("attachment", "")
                        })
                        inserted_count += 1
                        
                    except Exception as e:
                        print(f"Erreur perturbation: {e}")
                        continue

                print(f"{inserted_count} disturbances inserted")
                return inserted_count
                
        except Exception as e:
            print(f" Error insertion disturbances: {e}")
            return 0

    def cleanup_duplicates(self):
        """Cleaning duplicates"""
        print("=== Cleaning Duplicates ===")

        cleanup_queries = [
            # Stations dupliquées par id
            """
            WITH DuplicateStations AS (
                SELECT id, MIN(StationID) as KeepID
                FROM Station
                GROUP BY id
                HAVING COUNT(*) > 1
            )
            DELETE s FROM Station s
            INNER JOIN DuplicateStations d ON s.id = d.id
            WHERE s.StationID != d.KeepID
            """,
            
            # Trains dupliqués par numéro et opérateur
            """
            WITH DuplicateTrains AS (
                SELECT TrainNumber, Operator, MIN(TrainID) as KeepID
                FROM Train
                GROUP BY TrainNumber, Operator
                HAVING COUNT(*) > 1
            )
            DELETE t FROM Train t
            INNER JOIN DuplicateTrains d ON t.TrainNumber = d.TrainNumber AND t.Operator = d.Operator
            WHERE t.TrainID != d.KeepID
            """,
            
            # Dates dupliquées
            """
            WITH DuplicateDates AS (
                SELECT DateID, MIN(DateID) as KeepID
                FROM DateDimension
                GROUP BY DateID
                HAVING COUNT(*) > 1
            )
            DELETE dd FROM DateDimension dd
            INNER JOIN DuplicateDates d ON dd.DateID = d.DateID
            WHERE dd.DateID != d.KeepID
            """
        ]
        
        with self.engine.begin() as conn:
            for i, query in enumerate(cleanup_queries, 1):
                try:
                    result = conn.execute(text(query))
                    print(f"Cleaning {i}/3 done")
                except Exception as e:
                    print(f"Error cleaning {i}: {e}")

    def run_direct_insertion(self):
        """Main function for direct insertion"""
        print("========== DIRECT INSERTION STARTED ==========")

        insertor = DirectInsertor(
            server="train-sql-serve-hajer.database.windows.net",
            database="train-data-db",
            uid="sqladmin",
            pwd="Th021008...."
        )
        
        try:
            # 1. Stations
            stations_count = insertor.insert_stations_direct()
            
            # 2. Liveboards (trains + mouvements)
            trains_count, movements_count = insertor.insert_sample_liveboards(max_stations=3)
            
            # 3. Perturbations
            disturbances_count = insertor.insert_disturbances_direct()
            
            # 4. Cleaning
            insertor.cleanup_duplicates()
            
            print("========== RESULTS ==========")
            print(f"Stations: {stations_count}")
            print(f"Trains: {trains_count}")
            print(f"Movements: {movements_count}")
            print(f"Disturbances: {disturbances_count}")
            print("Cleaning done")

            return f"Insertion successful: {stations_count} stations, {trains_count} trains, {movements_count} movements"

        except Exception as e:
            error_msg = f"ERREUR INSERTION: {e}"
            print(error_msg)
            raise

# Function for Azure Function
    def ingest_all_data(self):
        """Entry point for Azure Function"""
        return self.run_direct_insertion()