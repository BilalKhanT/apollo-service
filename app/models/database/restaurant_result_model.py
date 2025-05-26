from beanie import Document
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field
from datetime import datetime


class RestaurantSummaryData(BaseModel):
    country: str
    language: str
    cities_requested: List[str]
    scrape_date: str
    total_cities: int
    cities_processed: int
    total_restaurants: int
    restaurants_processed: int
    total_deals: int
    execution_time_seconds: float
    status: str
    cities: List[str]
    restaurants_by_city: Dict[str, int] 
    deals_by_city: Dict[str, int] 


class RestaurantResult(Document):
    task_id: str = Field(..., unique=True, index=True)
    cities_requested: List[str]
    cities_processed: int = Field(default=0)
    restaurants_processed: int = Field(default=0)
    deals_processed: int = Field(default=0)
    total_cities: int = Field(default=0)
    total_restaurants: int = Field(default=0)
    output_directory: Optional[str] = None
    execution_time_seconds: float = Field(default=0.0)
    status: str = Field(default="running")
    error: Optional[str] = None
    database_summary: Optional[RestaurantSummaryData] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Settings:
        name = "restaurant_results"
        
    def update_timestamp(self):
        self.updated_at = datetime.utcnow()