from typing import List, Dict
from pydantic import BaseModel, Field, validator
from datetime import datetime
from app.models.base import BaseResponse


class RestaurantScrapingRequest(BaseModel):
    cities: List[str] = Field(
        description="List of cities to scrape restaurants from",
        example=["Karachi", "Lahore", "Islamabad"],
        min_items=1
    )
    
    @validator('cities')
    def validate_cities(cls, v):
        if not v:
            raise ValueError('At least one city must be provided')
        
        # Remove duplicates and empty strings
        cleaned_cities = list(set(city.strip() for city in v if city.strip()))
        if not cleaned_cities:
            raise ValueError('At least one valid city must be provided')
        
        return cleaned_cities
    
    class Config:
        json_schema_extra = {
            "example": {
                "cities": ["Karachi", "Lahore", "Islamabad"]
            }
        }


class RestaurantScrapingResponse(BaseResponse):
    task_id: str = Field(description="Unique task identifier for tracking")
    cities_requested: List[str] = Field(description="Cities that were requested for scraping")
    
    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "message": "Restaurant scraping started for 3 cities",
                "timestamp": "2025-05-26T10:00:00.000Z",
                "task_id": "b84751e8-2447-4d2c-bfc9-17f4c3d6d838",
                "cities_requested": ["Karachi", "Lahore", "Islamabad"]
            }
        }


class RestaurantResultSummary(BaseModel):
    task_id: str = Field(description="Task identifier")
    cities_requested: List[str] = Field(description="Cities that were requested")
    cities_processed: int = Field(description="Number of cities successfully processed")
    restaurants_processed: int = Field(description="Total restaurants found")
    deals_processed: int = Field(description="Total deals found")
    execution_time_seconds: float = Field(description="Time taken to complete")
    summary_by_city: Dict[str, Dict[str, int]] = Field(description="Statistics by city")
    created_at: datetime = Field(description="When the task was started")
    completed_at: datetime = Field(description="When the task was completed")
    total_cities: int = Field(description="Total number of cities requested")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
        json_schema_extra = {
            "example": {
                "task_id": "b84751e8-2447-4d2c-bfc9-17f4c3d6d838",
                "cities_requested": ["Karachi", "Lahore", "Islamabad"],
                "cities_processed": 3,
                "restaurants_processed": 36,
                "deals_processed": 119,
                "execution_time_seconds": 40.17,
                "summary_by_city": {
                    "Karachi": {"restaurants": 15, "deals": 52},
                    "Lahore": {"restaurants": 12, "deals": 38},
                    "Islamabad": {"restaurants": 9, "deals": 29}
                },
                "created_at": "2025-05-26T14:58:44.857956",
                "completed_at": "2025-05-26T14:59:25.236544",
                "total_cities": 3
            }
        }


class RestaurantResultsResponse(BaseResponse):
    data: List[RestaurantResultSummary] = Field(description="List of restaurant scraping results")
    total_count: int = Field(description="Total number of results available")
    page: int = Field(description="Current page number")
    page_size: int = Field(description="Number of results per page")
    has_more: bool = Field(description="Whether there are more results available")
    
    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "message": "Retrieved 2 restaurant results",
                "timestamp": "2025-05-26T10:00:00.000Z",
                "data": [
                    {
                        "task_id": "b84751e8-2447-4d2c-bfc9-17f4c3d6d838",
                        "cities_requested": ["Karachi", "Lahore", "Islamabad"],
                        "cities_processed": 3,
                        "restaurants_processed": 36,
                        "deals_processed": 119,
                        "execution_time_seconds": 40.17
                    }
                ],
                "total_count": 5,
                "page": 1,
                "page_size": 50,
                "has_more": False
            }
        }


class RestaurantStopResponse(BaseResponse):
    task_id: str = Field(description="ID of the task that was stopped")
    was_running: bool = Field(description="Whether the task was actually running when stopped")
    cleanup_completed: bool = Field(description="Whether cleanup was completed successfully")
    
    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "message": "Restaurant scraping task stopped successfully",
                "timestamp": "2025-05-26T10:00:00.000Z",
                "task_id": "b84751e8-2447-4d2c-bfc9-17f4c3d6d838",
                "was_running": True,
                "cleanup_completed": True
            }
        }