from pydantic import BaseModel
from typing import List, Optional


class LocationCandidate(BaseModel):
    city: str
    country: str
    latitude: float
    longitude: float
    confidence: float = 0.0
    climate: str = ""
    landscape: str = ""
    description: str = ""


class AnalysisResult(BaseModel):
    locations: List[LocationCandidate]


class OriginInfo(BaseModel):
    city: Optional[str] = None
    country: Optional[str] = None
    country_code: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    status: str = "ok"


class FlightDestinationResult(BaseModel):
    price: str
    date: str = "N/A"
    observed: str = "N/A"
    duration: Optional[str] = None
    stops: str = "N/A"
    lat: float = 0
    lon: float = 0
    hotel_price: Optional[str] = None


class FlightSearchResponse(BaseModel):
    origin: dict
    results: dict