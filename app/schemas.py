from pydantic import BaseModel
from typing import List, Optional

class PlaceInput(BaseModel):
    name: str
    formatted_address: str
    latitude: float
    longitude: float
    matched_visual_terms: List[str]
    types: List[str]
    rating: Optional[float] = None
    reasons: Optional[List[str]] = []