from typing import Optional, List
from pydantic import BaseModel, Field, validator
from datetime import datetime
from app.models.base import DataResponse, TaskStatus

class RestaurantRequest(BaseModel):
    cities: List[str] = Field(
        description="List of cities to scrape restaurant deals from",
        example=["Karachi", "Lahore", "Islamabad"],
        min_items=1
    )
    
    @validator('cities')
    def validate_cities(cls, v):
        if not v or len(v) == 0:
            raise ValueError('At least one city must be provided')
        
        cleaned_cities = [city.strip() for city in v if city and city.strip()]
        
        if not cleaned_cities:
            raise ValueError('At least one valid city must be provided')
            
        return cleaned_cities
    
    class Config:
        schema_extra = {
            "example": {
                "cities": ["Karachi", "Lahore", "Islamabad", "Faisalabad"]
            }
        }

class RestaurantProgress(BaseModel):
    current_city: Optional[str] = Field(default=None, description="Currently processing city", example="Karachi")
    current_restaurant: Optional[str] = Field(default=None, description="Currently processing restaurant", example="McDonald's")
    cities_processed: int = Field(default=0, description="Number of cities processed", example=5)
    restaurants_processed: int = Field(default=0, description="Number of restaurants processed", example=45)
    deals_processed: int = Field(default=0, description="Number of deals processed", example=123)
    total_cities: int = Field(default=0, description="Total number of cities", example=20)
    total_restaurants: int = Field(default=0, description="Total number of restaurants", example=200)
    
    class Config:
        schema_extra = {
            "example": {
                "current_city": "Karachi",
                "current_restaurant": "McDonald's",
                "cities_processed": 5,
                "restaurants_processed": 45,
                "deals_processed": 123,
                "total_cities": 20,
                "total_restaurants": 200
            }
        }

class RestaurantSummary(BaseModel):
    cities_requested: List[str] = Field(description="List of cities that were requested to be scraped")
    cities_processed: int = Field(description="Number of cities processed", example=4)
    restaurants_processed: int = Field(description="Number of restaurants processed", example=200)
    deals_processed: int = Field(description="Number of deals processed", example=1500)
    total_cities: int = Field(description="Total cities found", example=4)
    total_restaurants: int = Field(description="Total restaurants found", example=200)
    output_directory: str = Field(description="Output directory path")
    execution_time_seconds: float = Field(description="Total execution time", example=450.2)
    
    class Config:
        schema_extra = {
            "example": {
                "cities_requested": ["Karachi", "Lahore", "Islamabad", "Faisalabad"],
                "cities_processed": 4,
                "restaurants_processed": 200,
                "deals_processed": 1500,
                "total_cities": 4,
                "total_restaurants": 200,
                "output_directory": "/apollo_data/restaurant_deals/restaurant_20250127-153045",
                "execution_time_seconds": 450.2
            }
        }

class RestaurantStatus(BaseModel):
    id: str = Field(description="Unique task identifier", example="789e0123-e89b-12d3-a456-426614174000")
    status: TaskStatus = Field(description="Current task status", example=TaskStatus.RUNNING)
    progress: float = Field(
        default=0.0, 
        description="Progress percentage (0-100)", 
        example=75.5,
        ge=0.0,
        le=100.0
    )
    cities_requested: List[str] = Field(description="List of cities being scraped", example=["Karachi", "Lahore"])
    cities_processed: int = Field(default=0, description="Number of cities processed", example=1)
    restaurants_processed: int = Field(default=0, description="Number of restaurants processed", example=45)
    deals_processed: int = Field(default=0, description="Number of deals processed", example=123)
    error: Optional[str] = Field(default=None, description="Error message if task failed")
    progress_details: Optional[RestaurantProgress] = Field(default=None, description="Detailed progress information")
    created_at: Optional[datetime] = Field(default=None, description="Task creation timestamp")
    updated_at: Optional[datetime] = Field(default=None, description="Last update timestamp")
    execution_time_seconds: Optional[float] = Field(default=None, description="Current execution time", example=320.5)
    
    class Config:
        use_enum_values = True
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
        schema_extra = {
            "example": {
                "id": "789e0123-e89b-12d3-a456-426614174000",
                "status": "running",
                "progress": 75.5,
                "cities_requested": ["Karachi", "Lahore", "Islamabad"],
                "cities_processed": 1,
                "restaurants_processed": 45,
                "deals_processed": 123,
                "error": None,
                "progress_details": {
                    "current_city": "Karachi",
                    "current_restaurant": "McDonald's",
                    "cities_processed": 1,
                    "restaurants_processed": 45,
                    "deals_processed": 123,
                    "total_cities": 3,
                    "total_restaurants": 200
                },
                "created_at": "2025-01-27T16:00:00.000Z",
                "updated_at": "2025-01-27T16:05:30.000Z",
                "execution_time_seconds": 320.5
            }
        }

class RestaurantResponse(DataResponse):
    data: RestaurantStatus = Field(description="Restaurant scraper task status information")
    
    class Config:
        schema_extra = {
            "example": {
                "success": True,
                "message": "Restaurant scraping task started successfully",
                "timestamp": "2025-01-27T16:00:00.000Z",
                "data": {
                    "id": "789e0123-e89b-12d3-a456-426614174000",
                    "status": "created",
                    "progress": 0.0,
                    "cities_requested": ["Karachi", "Lahore", "Islamabad"],
                    "cities_processed": 0,
                    "restaurants_processed": 0,
                    "deals_processed": 0,
                    "error": None,
                    "created_at": "2025-01-27T16:00:00.000Z",
                    "updated_at": "2025-01-27T16:00:00.000Z",
                    "execution_time_seconds": 0.0
                }
            }
        }

class RestaurantStopResponse(BaseModel):
    success: bool = Field(description="Whether the stop operation was successful")
    message: str = Field(description="Status message")
    task_id: str = Field(description="ID of the stopped task")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Response timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
        schema_extra = {
            "example": {
                "success": True,
                "message": "Restaurant scraper stopped gracefully",
                "task_id": "789e0123-e89b-12d3-a456-426614174000",
                "timestamp": "2025-01-27T16:15:30.000Z"
            }
        }