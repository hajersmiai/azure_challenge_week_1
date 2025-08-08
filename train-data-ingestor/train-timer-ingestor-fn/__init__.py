import azure.functions as func
import logging
import json
import requests
import pandas as pd
import pyodbc
import os
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from concurrent.futures import ThreadPoolExecutor
import time

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

# Configuration for automatic data collection
MAJOR_STATIONS = [
    'Brussels-Central', 'Brussels-North', 'Brussels-South', 'Brussels-Midi',
    'Antwerp-Central', 'Ghent-Sint-Pieters', 'Charleroi-South', 'Liege-Guillemins',
    'Leuven', 'Mechelen', 'Bruges', 'Ostend', 'Hasselt', 'Namur', 'Mons',
    'La-Louviere-South', 'Kortrijk', 'Sint-Niklaas', 'Tournai', 'Aalst'
]

POPULAR_ROUTES = [
    ('Brussels-Central', 'Antwerp-Central'),
    ('Brussels-Central', 'Ghent-Sint-Pieters'),
    ('Brussels-Central', 'Liege-Guillemins'),
    ('Brussels-Central', 'Charleroi-South'),
    ('Brussels-Central', 'Leuven'),
    ('Antwerp-Central', 'Ghent-Sint-Pieters'),
    ('Antwerp-Central', 'Brussels-South'),
    ('Ghent-Sint-Pieters', 'Bruges'),
    ('Liege-Guillemins', 'Namur'),
    ('Brussels-North', 'Brussels-South')
]

@app.route(route="auto_collect_all", methods=["GET", "POST"])
def auto_collect_all(req: func.HttpRequest) -> func.HttpResponse:
    """
    Automatically collect all liveboard and connections data for major stations
    """
    logging.info('Starting automatic data collection for all stations and routes.')
    
    try:
        # Get collection mode from parameters
        mode = req.params.get('mode', 'full')  # full, liveboard_only, connections_only
        
        results = {
            "liveboard_records": 0,
            "connection_records": 0,
            "errors": [],
            "timestamp": datetime.now().isoformat()
        }
        
        # Collect liveboard data for all major stations
        if mode in ['full', 'liveboard_only']:
            liveboard_results = collect_all_liveboards()
            results["liveboard_records"] = liveboard_results["total_records"]
            results["errors"].extend(liveboard_results["errors"])
        
        # Collect connections data for popular routes
        if mode in ['full', 'connections_only']:
            connections_results = collect_all_connections()
            results["connection_records"] = connections_results["total_records"]
            results["errors"].extend(connections_results["errors"])
        
        return func.HttpResponse(
            json.dumps({
                "status": "success",
                "message": f"Automatic collection completed",
                "results": results
            }),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        logging.error(f"Error in automatic collection: {str(e)}")
        return func.HttpResponse(
            json.dumps({
                "status": "error",
                "message": str(e),
                "timestamp": datetime.now().isoformat()
            }),
            status_code=500,
            mimetype="application/json"
        )

@app.timer_trigger(schedule="0 */5 * * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def timer_collect_liveboards(myTimer: func.TimerRequest) -> None:
    """
    Timer function that runs every 5 minutes to collect liveboard data
    """
    logging.info('Timer trigger function ran for liveboard collection.')
    
    try:
        results = collect_all_liveboards()
        logging.info(f"Timer collection completed: {results['total_records']} records inserted")
        
        if results["errors"]:
            logging.warning(f"Collection completed with {len(results['errors'])} errors")
            
    except Exception as e:
        logging.error(f"Error in timer collection: {str(e)}")

@app.timer_trigger(schedule="0 */15 * * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False)
def timer_collect_connections(myTimer: func.TimerRequest) -> None:
    """
    Timer function that runs every 15 minutes to collect connections data
    """
    logging.info('Timer trigger function ran for connections collection.')
    
    try:
        results = collect_all_connections()
        logging.info(f"Timer connections collection completed: {results['total_records']} records inserted")
        
        if results["errors"]:
            logging.warning(f"Connections collection completed with {len(results['errors'])} errors")
            
    except Exception as e:
        logging.error(f"Error in timer connections collection: {str(e)}")

@app.route(route="get_all_stations", methods=["GET"])
def get_all_stations(req: func.HttpRequest) -> func.HttpResponse:
    """
    Fetch and store all available stations from iRail API
    """
    logging.info('Fetching all available stations.')
    
    try:
        stations_data = fetch_all_stations()
        
        if stations_data:
            records_inserted = store_stations_data(stations_data)
            
            return func.HttpResponse(
                json.dumps({
                    "status": "success",
                    "message": "Successfully fetched and stored all stations",
                    "stations_count": records_inserted,
                    "timestamp": datetime.now().isoformat()
                }),
                status_code=200,
                mimetype="application/json"
            )
        else:
            return func.HttpResponse(
                "Failed to fetch stations data",
                status_code=500
            )
            
    except Exception as e:
        logging.error(f"Error fetching stations: {str(e)}")
        return func.HttpResponse(
            json.dumps({
                "status": "error",
                "message": str(e),
                "timestamp": datetime.now().isoformat()
            }),
            status_code=500,
            mimetype="application/json"
        )

def collect_all_liveboards() -> Dict[str, Any]:
    """
    Collect liveboard data for all major stations concurrently
    """
    results = {
        "total_records": 0,
        "successful_stations": [],
        "errors": []
    }
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_station = {
            executor.submit(fetch_and_store_single_liveboard, station): station 
            for station in MAJOR_STATIONS
        }
        
        for future in future_to_station:
            station = future_to_station[future]
            try:
                records_count = future.result(timeout=60)
                results["total_records"] += records_count
                results["successful_stations"].append(station)
                logging.info(f"Collected {records_count} records for {station}")
                
            except Exception as e:
                error_msg = f"Error collecting data for {station}: {str(e)}"
                logging.error(error_msg)
                results["errors"].append(error_msg)
    
    return results

def collect_all_connections() -> Dict[str, Any]:
    """
    Collect connections data for all popular routes concurrently
    """
    results = {
        "total_records": 0,
        "successful_routes": [],
        "errors": []
    }
    
    with ThreadPoolExecutor(max_workers=8) as executor:
        future_to_route = {
            executor.submit(fetch_and_store_single_connection, from_station, to_station): (from_station, to_station)
            for from_station, to_station in POPULAR_ROUTES
        }
        
        for future in future_to_route:
            route = future_to_route[future]
            try:
                records_count = future.result(timeout=60)
                results["total_records"] += records_count
                results["successful_routes"].append(route)
                logging.info(f"Collected {records_count} connection records for {route[0]} -> {route[1]}")
                
            except Exception as e:
                error_msg = f"Error collecting connections for {route}: {str(e)}"
                logging.error(error_msg)
                results["errors"].append(error_msg)
    
    return results

def fetch_and_store_single_liveboard(station: str) -> int:
    """
    Fetch and store liveboard data for a single station
    """
    try:
        liveboard_data = fetch_irail_liveboard(station)
        if liveboard_data:
            return process_and_store_liveboard(liveboard_data)
        return 0
    except Exception as e:
        logging.error(f"Error processing liveboard for {station}: {str(e)}")
        raise

def fetch_and_store_single_connection(from_station: str, to_station: str) -> int:
    """
    Fetch and store connections data for a single route
    """
    try:
        connections_data = fetch_irail_connections(from_station, to_station)
        if connections_data:
            return process_and_store_connections(connections_data)
        return 0
    except Exception as e:
        logging.error(f"Error processing connections for {from_station} -> {to_station}: {str(e)}")
        raise

def fetch_all_stations() -> Optional[Dict[Any, Any]]:
    """
    Fetch all available stations from iRail API
    """
    try:
        url = "https://api.irail.be/stations/"
        params = {
            'format': 'json',
            'lang': 'en'
        }
        
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        return response.json()
        
    except requests.RequestException as e:
        logging.error(f"Error fetching stations data: {str(e)}")
        return None

def store_stations_data(data: Dict[Any, Any]) -> int:
    """
    Store stations data in Azure SQL Database
    """
    try:
        stations = data.get('station', [])
        
        if not stations:
            logging.warning("No stations found in data")
            return 0
        
        # Normalize data
        normalized_data = []
        for station in stations:
            record = {
                'station_id': station.get('id', ''),
                'station_name': station.get('name', ''),
                'country_code': station.get('locationX', ''),  # Longitude
                'latitude': station.get('locationY', ''),     # Latitude
                'longitude': station.get('locationX', ''),
                'updated_at': datetime.now()
            }
            normalized_data.append(record)
        
        df = pd.DataFrame(normalized_data)
        
        # Store in database
        conn = get_sql_connection()
        cursor = conn.cursor()
        
        # Create table if not exists
        create_stations_table(cursor)
        
        # Clear existing data and insert new
        cursor.execute("DELETE FROM stations_data")
        
        records_inserted = 0
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO stations_data 
                (station_id, station_name, latitude, longitude, updated_at)
                VALUES (?, ?, ?, ?, ?)
            """, 
            row['station_id'], row['station_name'], 
            row['latitude'], row['longitude'], row['updated_at'])
            records_inserted += 1
        
        conn.commit()
        conn.close()
        
        logging.info(f"Successfully inserted {records_inserted} station records")
        return records_inserted
        
    except Exception as e:
        logging.error(f"Error storing stations data: {str(e)}")
        raise

def create_stations_table(cursor):
    """
    Create stations_data table if it doesn't exist
    """
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='stations_data' AND xtype='U')
        CREATE TABLE stations_data (
            id INT IDENTITY(1,1) PRIMARY KEY,
            station_id NVARCHAR(100),
            station_name NVARCHAR(255),
            latitude NVARCHAR(50),
            longitude NVARCHAR(50),
            updated_at DATETIME2,
            UNIQUE(station_id)
        )
    """)

# Enhanced versions of existing functions with better error handling and logging

def fetch_irail_liveboard(station: str, format_type: str = 'json', lang: str = 'en') -> Optional[Dict[Any, Any]]:
    """
    Fetch liveboard data from iRail API with retry logic
    """
    max_retries = 3
    retry_delay = 1
    
    for attempt in range(max_retries):
        try:
            url = "https://api.irail.be/liveboard/"
            params = {
                'station': station,
                'format': format_type,
                'lang': lang
            }
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            return response.json()
            
        except requests.RequestException as e:
            if attempt < max_retries - 1:
                logging.warning(f"Attempt {attempt + 1} failed for {station}, retrying in {retry_delay}s: {str(e)}")
                time.sleep(retry_delay)
                retry_delay *= 2
            else:
                logging.error(f"All attempts failed for liveboard {station}: {str(e)}")
                return None

def fetch_irail_connections(from_station: str, to_station: str, format_type: str = 'json', lang: str = 'en') -> Optional[Dict[Any, Any]]:
    """
    Fetch connections data from iRail API with retry logic
    """
    max_retries = 3
    retry_delay = 1
    
    for attempt in range(max_retries):
        try:
            url = "https://api.irail.be/connections/"
            params = {
                'from': from_station,
                'to': to_station,
                'format': format_type,
                'lang': lang
            }
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            return response.json()
            
        except requests.RequestException as e:
            if attempt < max_retries - 1:
                logging.warning(f"Attempt {attempt + 1} failed for {from_station}->{to_station}, retrying in {retry_delay}s: {str(e)}")
                time.sleep(retry_delay)
                retry_delay *= 2
            else:
                logging.error(f"All attempts failed for connections {from_station}->{to_station}: {str(e)}")
                return None

def get_sql_connection():
    """
    Create connection to Azure SQL Database using environment variables
    """
    server = os.environ.get('SQL_SERVER')
    database = os.environ.get('SQL_DATABASE')
    username = os.environ.get('SQL_USERNAME')
    password = os.environ.get('SQL_PASSWORD')
    
    if not all([server, database, username, password]):
        raise ValueError("Missing required SQL connection environment variables")
    
    connection_string = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    
    return pyodbc.connect(connection_string)

def process_and_store_liveboard(data: Dict[Any, Any]) -> int:
    """
    Process liveboard data and store in Azure SQL Database with enhanced error handling
    """
    try:
        departures = data.get('departures', {}).get('departure', [])
        
        if not departures:
            logging.warning("No departures found in liveboard data")
            return 0
        
        normalized_data = []
        
        for departure in departures:
            record = {
                'station_name': data.get('stationinfo', {}).get('name', ''),
                'platform': departure.get('platform', ''),
                'time': departure.get('time', ''),
                'delay': departure.get('delay', 0),
                'canceled': departure.get('canceled', 0),
                'vehicle_id': departure.get('vehicle', ''),
                'direction': departure.get('station', ''),
                'fetched_at': datetime.now()
            }
            normalized_data.append(record)
        
        df = pd.DataFrame(normalized_data)
        
        conn = get_sql_connection()
        cursor = conn.cursor()
        
        create_liveboard_table(cursor)
        
        records_inserted = 0
        for _, row in df.iterrows():
            try:
                cursor.execute("""
                    INSERT INTO liveboard_data 
                    (station_name, platform, time, delay, canceled, vehicle_id, direction, fetched_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, 
                row['station_name'], row['platform'], row['time'], 
                row['delay'], row['canceled'], row['vehicle_id'], 
                row['direction'], row['fetched_at'])
                records_inserted += 1
            except Exception as insert_error:
                logging.warning(f"Failed to insert liveboard record: {str(insert_error)}")
                continue
        
        conn.commit()
        conn.close()
        
        logging.info(f"Successfully inserted {records_inserted}/{len(df)} liveboard records")
        return records_inserted
        
    except Exception as e:
        logging.error(f"Error processing liveboard data: {str(e)}")
        raise

def process_and_store_connections(data: Dict[Any, Any]) -> int:
    """
    Process connections data and store in Azure SQL Database with enhanced error handling
    """
    try:
        connections = data.get('connection', [])
        
        if not connections:
            logging.warning("No connections found in data")
            return 0
        
        normalized_data = []
        
        for connection in connections:
            departure = connection.get('departure', {})
            arrival = connection.get('arrival', {})
            
            record = {
                'departure_station': departure.get('station', ''),
                'departure_time': departure.get('time', ''),
                'departure_platform': departure.get('platform', ''),
                'departure_delay': departure.get('delay', 0),
                'arrival_station': arrival.get('station', ''),
                'arrival_time': arrival.get('time', ''),
                'arrival_platform': arrival.get('platform', ''),
                'arrival_delay': arrival.get('delay', 0),
                'duration': connection.get('duration', ''),
                'fetched_at': datetime.now()
            }
            normalized_data.append(record)
        
        df = pd.DataFrame(normalized_data)
        
        conn = get_sql_connection()
        cursor = conn.cursor()
        
        create_connections_table(cursor)
        
        records_inserted = 0
        for _, row in df.iterrows():
            try:
                cursor.execute("""
                    INSERT INTO connections_data 
                    (departure_station, departure_time, departure_platform, departure_delay,
                     arrival_station, arrival_time, arrival_platform, arrival_delay, duration, fetched_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, 
                row['departure_station'], row['departure_time'], row['departure_platform'], row['departure_delay'],
                row['arrival_station'], row['arrival_time'], row['arrival_platform'], row['arrival_delay'],
                row['duration'], row['fetched_at'])
                records_inserted += 1
            except Exception as insert_error:
                logging.warning(f"Failed to insert connection record: {str(insert_error)}")
                continue
        
        conn.commit()
        conn.close()
        
        logging.info(f"Successfully inserted {records_inserted}/{len(df)} connection records")
        return records_inserted
        
    except Exception as e:
        logging.error(f"Error processing connections data: {str(e)}")
        raise

def create_liveboard_table(cursor):
    """
    Create liveboard_data table if it doesn't exist
    """
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='liveboard_data' AND xtype='U')
        CREATE TABLE liveboard_data (
            id INT IDENTITY(1,1) PRIMARY KEY,
            station_name NVARCHAR(255),
            platform NVARCHAR(50),
            time NVARCHAR(50),
            delay INT,
            canceled INT,
            vehicle_id NVARCHAR(100),
            direction NVARCHAR(255),
            fetched_at DATETIME2,
            INDEX IX_liveboard_station_time (station_name, fetched_at),
            INDEX IX_liveboard_fetched_at (fetched_at)
        )
    """)

def create_connections_table(cursor):
    """
    Create connections_data table if it doesn't exist
    """
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='connections_data' AND xtype='U')
        CREATE TABLE connections_data (
            id INT IDENTITY(1,1) PRIMARY KEY,
            departure_station NVARCHAR(255),
            departure_time NVARCHAR(50),
            departure_platform NVARCHAR(50),
            departure_delay INT,
            arrival_station NVARCHAR(255),
            arrival_time NVARCHAR(50),
            arrival_platform NVARCHAR(50),
            arrival_delay INT,
            duration NVARCHAR(50),
            fetched_at DATETIME2,
            INDEX IX_connections_route_time (departure_station, arrival_station, fetched_at),
            INDEX IX_connections_fetched_at (fetched_at)
        )
    """)

# Original functions preserved for backward compatibility
@app.route(route="irail_liveboard", methods=["GET", "POST"])
def irail_liveboard(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function to fetch iRail liveboard data and store in Azure SQL Database
    """
    logging.info('Python HTTP trigger function processed a request.')
    
    try:
        station = req.params.get('station', 'Brussels-Central')
        format_type = req.params.get('format', 'json')
        lang = req.params.get('lang', 'en')
        
        liveboard_data = fetch_irail_liveboard(station, format_type, lang)
        
        if not liveboard_data:
            return func.HttpResponse(
                "Failed to fetch data from iRail API",
                status_code=500
            )
        
        records_inserted = process_and_store_liveboard(liveboard_data)
        
        return func.HttpResponse(
            json.dumps({
                "status": "success",
                "message": f"Successfully processed liveboard data for {station}",
                "records_inserted": records_inserted,
                "timestamp": datetime.now().isoformat()
            }),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        logging.error(f"Error processing request: {str(e)}")
        return func.HttpResponse(
            json.dumps({
                "status": "error",
                "message": str(e),
                "timestamp": datetime.now().isoformat()
            }),
            status_code=500,
            mimetype="application/json"
        )

@app.route(route="irail_connections", methods=["GET", "POST"])
def irail_connections(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function to fetch iRail connections data and store in Azure SQL Database
    """
    logging.info('Processing connections request.')
    
    try:
        from_station = req.params.get('from', 'Brussels-Central')
        to_station = req.params.get('to', 'Antwerp-Central')
        format_type = req.params.get('format', 'json')
        lang = req.params.get('lang', 'en')
        
        connections_data = fetch_irail_connections(from_station, to_station, format_type, lang)
        
        if not connections_data:
            return func.HttpResponse(
                "Failed to fetch connections data from iRail API",
                status_code=500
            )
        
        records_inserted = process_and_store_connections(connections_data)
        
        return func.HttpResponse(
            json.dumps({
                "status": "success",
                "message": f"Successfully processed connections from {from_station} to {to_station}",
                "records_inserted": records_inserted,
                "timestamp": datetime.now().isoformat()
            }),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        logging.error(f"Error processing connections request: {str(e)}")
        return func.HttpResponse(
            json.dumps({
                "status": "error",
                "message": str(e),
                "timestamp": datetime.now().isoformat()
            }),
            status_code=500,
            mimetype="application/json"
        )