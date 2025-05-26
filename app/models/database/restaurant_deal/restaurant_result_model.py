from beanie import Document
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime


class RestaurantData(BaseModel):
    name: str
    location: str
    city: str
    cuisine_type: Optional[str] = None
    rating: Optional[float] = None
    deals: List[Dict[str, Any]] = Field(default_factory=list)
    contact_info: Optional[Dict[str, str]] = None
    scraped_at: datetime = Field(default_factory=datetime.utcnow)


class DealResult(Document):
    task_id: str = Field(..., unique=True, index=True)
    cities_requested: List[str] = Field(description="Cities that were requested for scraping")
    cities_processed: int = Field(description="Number of cities successfully processed")
    restaurants_processed: int = Field(description="Total restaurants found and processed")
    deals_processed: int = Field(description="Total deals found and processed")
    execution_time_seconds: float = Field(description="Total time taken to complete the task")
    restaurants_data: List[RestaurantData] = Field(default_factory=list, description="Complete restaurant data")
    summary_by_city: Dict[str, Dict[str, int]] = Field(default_factory=dict, description="Summary statistics by city")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    completed_at: datetime = Field(default_factory=datetime.utcnow)
    
    class Settings:
        name = "restaurant_results"
        indexes = [
            "task_id",
            "created_at",
            "cities_requested"
        ]
    
    def get_summary(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "cities_requested": self.cities_requested,
            "cities_processed": self.cities_processed,
            "restaurants_processed": self.restaurants_processed,
            "deals_processed": self.deals_processed,
            "execution_time_seconds": self.execution_time_seconds,
            "summary_by_city": self.summary_by_city,
            "created_at": self.created_at,
            "completed_at": self.completed_at,
            "total_cities": len(self.cities_requested)
        }