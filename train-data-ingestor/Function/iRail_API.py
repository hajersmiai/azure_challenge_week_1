# iRail_API.py
import requests
from datetime import datetime
from typing import Dict, List, Any, Optional
import json as _json

class IRailAPI:
    BASE_URL = "https://api.irail.be"

    def __init__(self, lang: str = "en") -> None:
        self.lang = lang

    def _get(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Internal helper to GET and parse JSON."""
        url = f"{self.BASE_URL}/{endpoint}"
        response = requests.get(url, params=params)
        response.raise_for_status()
        if response.status_code == 401:
            print (f"erreur {endpoint}")
            raise ValueError(f"Expected JSON response, got {response.headers.get('Content-Type')}")
        return response.json()

    # -- Stations ------------------------------------------------------------
    def get_stations(self) -> Dict[str, Dict[str, Any]]:
        """Return all stations keyed by station ID."""
        data = self._get("stations", {"format": "json", "lang": self.lang})
        stations = data.get("stations", {}).get("station", [])
        return {st["id"]: st for st in stations}

    @staticmethod
    def extract_station_info(station_json: Any) -> Dict[str, Any]:
        """Extract id, URI and coordinates from a station response."""
       
        data = _json.loads(station_json) if isinstance(station_json, str) else station_json
        station = data.get("station", {})
        return {
            "station_id": station.get("id"),
            "iri_url": station.get("@id"),
            "longitude": station.get("locationX"),
            "latitude": station.get("locationY"),
            "standard_name": station.get("standardname"),
            "name": station.get("name"),
        }

    # -- Connections ---------------------------------------------------------
    def get_connections(
        self,
        from_station: str,
        to_station: str,
        *,
        date: Optional[str] = None,
        time: Optional[str] = None,
        timesel: str = "departure",
        fmt: str = "json",
    ) -> List[Dict[str, Any]]:
        """Retrieve train connections between two stations."""
        params = {
            "from": from_station,
            "to": to_station,
            "format": fmt,
            "timesel": timesel,
        }
        if date:
            params["date"] = date
        if time:
            params["time"] = time
        data = self._get("connections", params)
        return data.get("connection", [])

    @staticmethod
    def get_date_parts(timestamp: Any) -> tuple:
        """Convert a Unix timestamp into (date, day, month, year, hour, minute, second)."""
        dt = datetime.fromtimestamp(int(timestamp))
        return dt.date(), dt.day, dt.month, dt.year, dt.hour, dt.minute, dt.second

    # -- Vehicles and trains --------------------------------------------------
    def get_vehicle(self, vehicle_id: str, *, date: Optional[str] = None,
                    fmt: str = "json", alerts: bool = False) -> Dict[str, Any]:
        """Return detailed information about a single train (vehicle)."""
        if not vehicle_id:
            raise ValueError("vehicle_id is required")
        params = {
            "id": vehicle_id,
            "format": fmt,
            "lang": self.lang,
            "alerts": str(alerts).lower(),
        }
        if date:
            params["date"] = date
        return self._get("vehicle", params)

    def get_train_info(self, vehicle_id: str, *, date: Optional[str] = None,
                       alerts: bool = False) -> Dict[str, Any]:
        """Parse a train’s ID, number, type and operator from its vehicle data."""
        data = self.get_vehicle(vehicle_id, date=date, fmt="json", alerts=alerts)
        full_name = data["vehicleinfo"]["name"]        # e.g. 'BE.NMBS.IC3033'
        parts = full_name.split(".")
        if len(parts) < 3:
            raise ValueError(f"Unexpected vehicle name format: {full_name}")
        operator = parts[1]
        train_code = parts[2]
        train_type = "".join(c for c in train_code if c.isalpha())
        train_number = "".join(c for c in train_code if c.isdigit())
        train_id = data["vehicleinfo"].get("shortname", train_code)
        return {
            "trainId": train_id,
            "trainNumber": train_number,
            "trainType": train_type,
            "operator": operator,
        }

    def get_vehicle_stops(self, vehicle_id: str, *, date: Optional[str] = None,
                          alerts: bool = False) -> List[Dict[str, Any]]:
        """Return only the list of stops from a vehicle response."""
        data = self.get_vehicle(vehicle_id, date=date, fmt="json", alerts=alerts)
        return data.get("stops", {}).get("stop", [])

    # -- Liveboard -----------------------------------------------------------
    def get_liveboard(self, *, station: Optional[str] = None, station_id: Optional[str] = None,
                      date: Optional[str] = None, time: Optional[str] = None,
                      arrdep: str = "departure", fmt: str = "json",
                      alerts: bool = False) -> Dict[str, Any]:
        """Retrieve the liveboard for departures or arrivals at a station."""
        if station and station_id:
            raise ValueError("Provide either station or station_id, not both")
        if not station and not station_id:
            raise ValueError("station or station_id must be provided")
        params: Dict[str, Any] = {
            "format": fmt,
            "lang": self.lang,
            "arrdep": arrdep,
            "alerts": str(alerts).lower(),
        }
        if station:
            params["station"] = station
        else:
            params["id"] = station_id
        if date:
            params["date"] = date
        if time:
            params["time"] = time
        response = requests.get(f"{self.BASE_URL}/liveboard", params=params)
        response.raise_for_status()
        return response.json() if fmt == "json" else response.text

    
    # -- Composition ---------------------------------------------------------
    def get_composition(self, vehicle_id: str, *, date: Optional[str] = None,
                        fmt: str = "json") -> Dict[str, Any]:
        """Retrieve composition details for a train (list of carriages and features)."""
        params = {
            "id": vehicle_id,
            "format": fmt,
            "lang": self.lang,
        }
        if date:
            params["date"] = date
        return self._get("composition", params)

    @staticmethod
    def extract_composition_units(composition_json: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Flatten the composition response into a list of carriage/unit dictionaries."""
        units_list: List[Dict[str, Any]] = []
        segments = composition_json.get("composition", {}).get("segments", {}).get("segment", [])
        if not isinstance(segments, list):
            segments = [segments]
        def parse_bool(value: Any) -> bool:
            return str(value) == "1"
        for seg in segments:
            origin = seg.get("origin", {})
            dest = seg.get("destination", {})
            units = seg.get("composition", {}).get("units", {}).get("unit", [])
            if not isinstance(units, list):
                units = [units]
            for u in units:
                units_list.append({
                    "vehicleId": composition_json.get("vehicle"),
                    "segmentOriginId": origin.get("id"),
                    "segmentDestinationId": dest.get("id"),
                    "segmentOriginName": origin.get("name"),
                    "segmentDestinationName": dest.get("name"),
                    "unitId": u.get("id"),
                    "parent_type": u.get("materialType", {}).get("parent_type"),
                    "sub_type": u.get("materialType", {}).get("sub_type"),
                    "orientation": u.get("materialType", {}).get("orientation"),
                    "hasToilets": parse_bool(u.get("hasToilets", "0")),
                    "hasTables": parse_bool(u.get("hasTables", "0")),
                    "hasSecondClassOutlets": parse_bool(u.get("hasSecondClassOutlets", "0")),
                    "hasFirstClassOutlets": parse_bool(u.get("hasFirstClassOutlets", "0")),
                    "hasHeating": parse_bool(u.get("hasHeating", "0")),
                    "hasAirco": parse_bool(u.get("hasAirco", "0")),
                    "materialNumber": u.get("materialNumber"),
                    "tractionType": u.get("tractionType"),
                    "canPassToNextUnit": parse_bool(u.get("canPassToNextUnit", "0")),
                    "standingPlacesSecondClass": int(u.get("standingPlacesSecondClass", "0")),
                    "standingPlacesFirstClass": int(u.get("standingPlacesFirstClass", "0")),
                    "seatsCoupeSecondClass": int(u.get("seatsCoupeSecondClass", "0")),
                    "seatsCoupeFirstClass": int(u.get("seatsCoupeFirstClass", "0")),
                    "seatsSecondClass": int(u.get("seatsSecondClass", "0")),
                    "seatsFirstClass": int(u.get("seatsFirstClass", "0")),
                    "lengthInMeter": int(u.get("lengthInMeter", "0")),
                    "hasSemiAutomaticInteriorDoors": parse_bool(u.get("hasSemiAutomaticInteriorDoors", "0")),
                    "hasLuggageSection": parse_bool(u.get("hasLuggageSection", "0")),
                    "materialSubTypeName": u.get("materialSubTypeName"),
                    "tractionPosition": int(u.get("tractionPosition", "0")),
                    "hasPrmSection": parse_bool(u.get("hasPrmSection", "0")),
                    "hasPriorityPlaces": parse_bool(u.get("hasPriorityPlaces", "0")),
                    "hasBikeSection": parse_bool(u.get("hasBikeSection", "0")),
                })
        return units_list

    # -- Disturbances --------------------------------------------------------
    def get_disturbances(self, *, fmt: str = "json",
                         lineBreakCharacter: str = "") -> List[Dict[str, Any]]:
        """Return current service disturbances and planned works."""
        params = {
            "format": fmt,
            "lang": self.lang,
            "lineBreakCharacter": lineBreakCharacter,
        }
        data = self._get("disturbances", params)
        disturbances = data.get("disturbance", [])
        parsed: List[Dict[str, Any]] = []
        for d in disturbances:
            ts = d.get("timestamp")
            dt = datetime.fromtimestamp(int(ts)) if ts else None
            parsed.append({
                "id": d.get("id"),
                "title": d.get("title"),
                "description": d.get("description"),
                "link": d.get("link"),
                "type": d.get("type"),
                "timestamp": dt,
                "attachment": d.get("attachment"),
            })
        return parsed
