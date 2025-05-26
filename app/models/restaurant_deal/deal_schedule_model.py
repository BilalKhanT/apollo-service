from pydantic import BaseModel, Field, validator
from typing import Optional, List
from datetime import time, datetime
from enum import Enum
from app.models.base import BaseResponse

class DayOfWeek(str, Enum):
    MONDAY = "monday"
    TUESDAY = "tuesday"
    WEDNESDAY = "wednesday"
    THURSDAY = "thursday"
    FRIDAY = "friday"
    SATURDAY = "saturday"
    SUNDAY = "sunday"

class ScheduleStatus(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    PAUSED = "paused"

class DealScheduleRequest(BaseModel):
    cities: List[str] = Field(..., description="Cities to scrape deals from", example=["Karachi", "Lahore", "Islamabad"])
    schedule_name: Optional[str] = Field(None, description="Optional name for the schedule", example="Daily Deal Scraping")
    day_of_week: DayOfWeek = Field(..., description="Day of the week to run the deal scraping", example=DayOfWeek.MONDAY)
    time_of_day: str = Field(..., description="Time of day to run the scraping (HH:MM format)", example="14:30")
    
    @validator('time_of_day')
    def validate_time_format(cls, v):
        try:
            time.fromisoformat(v)
            return v
        except ValueError:
            raise ValueError('time_of_day must be in HH:MM format (e.g., "14:30")')
    
    @validator('cities')
    def validate_cities(cls, v):
        if v is None:
            return []
        cleaned = list(set(city.strip() for city in v if city.strip()))
        if not cleaned:
            raise ValueError("At least one valid city is required")
        return cleaned
    
    class Config:
        use_enum_values = True
        schema_extra = {
            "example": {
                "cities": ["Karachi", "Lahore", "Islamabad"],
                "schedule_name": "Daily Deal Scraping",
                "day_of_week": "monday",
                "time_of_day": "14:30"
            }
        }

class DealScheduleResponse(BaseModel):
    id: str = Field(..., description="Schedule ID", example="789e0123-e89b-12d3-a456-426614174000")
    cities: List[str] = Field(description="Cities being scraped", example=["Karachi", "Lahore", "Islamabad"])
    schedule_name: Optional[str] = Field(default=None, description="Schedule name", example="Daily Deal Scraping")
    day_of_week: DayOfWeek = Field(description="Day of the week", example=DayOfWeek.MONDAY)
    time_of_day: str = Field(description="Time of day", example="14:30")
    status: ScheduleStatus = Field(description="Current schedule status", example=ScheduleStatus.ACTIVE)
    created_at: datetime = Field(description="Creation timestamp")
    updated_at: datetime = Field(description="Last update timestamp")
    last_run_at: Optional[datetime] = Field(default=None, description="Last execution timestamp")
    next_run_at: Optional[datetime] = Field(default=None, description="Next scheduled execution")
    total_runs: int = Field(default=0, description="Total number of executions", example=15)
    successful_runs: int = Field(default=0, description="Number of successful executions", example=12)
    failed_runs: int = Field(default=0, description="Number of failed executions", example=3)
    last_task_id: Optional[str] = Field(default=None, description="Last created task ID")
    last_error: Optional[str] = Field(default=None, description="Last error message")
    
    class Config:
        use_enum_values = True
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
        schema_extra = {
            "example": {
                "id": "789e0123-e89b-12d3-a456-426614174000",
                "cities": ["Karachi", "Lahore", "Islamabad"],
                "schedule_name": "Daily Deal Scraping",
                "day_of_week": "monday",
                "time_of_day": "14:30",
                "status": "active",
                "created_at": "2025-01-27T10:00:00.000Z",
                "updated_at": "2025-01-27T10:00:00.000Z",
                "last_run_at": "2025-01-27T14:30:00.000Z",
                "next_run_at": "2025-02-03T14:30:00.000Z",
                "total_runs": 15,
                "successful_runs": 12,
                "failed_runs": 3,
                "last_task_id": "123e4567-e89b-12d3-a456-426614174000",
                "last_error": None
            }
        }

class DealScheduleUpdateRequest(BaseModel):
    cities: Optional[List[str]] = Field(default=None, description="Updated cities list")
    schedule_name: Optional[str] = Field(default=None, description="Updated schedule name")
    day_of_week: Optional[DayOfWeek] = Field(default=None, description="Updated day of week")
    time_of_day: Optional[str] = Field(default=None, description="Updated time of day")
    status: Optional[ScheduleStatus] = Field(default=None, description="Updated status")
    
    @validator('time_of_day')
    def validate_time_format(cls, v):
        if v is not None:
            try:
                time.fromisoformat(v)
                return v
            except ValueError:
                raise ValueError('time_of_day must be in HH:MM format (e.g., "14:30")')
        return v
    
    @validator('cities')
    def validate_cities(cls, v):
        if v is None:
            return []
        cleaned = list(set(city.strip() for city in v if city.strip()))
        if not cleaned:
            raise ValueError("At least one valid city is required")
        return cleaned
    
    class Config:
        use_enum_values = True
        schema_extra = {
            "example": {
                "cities": ["Karachi", "Lahore", "Islamabad", "Faisalabad"],
                "schedule_name": "Updated Daily Deal Scraping",
                "day_of_week": "tuesday",
                "time_of_day": "15:00",
                "status": "paused"
            }
        }

class DealScheduleListResponse(BaseResponse):
    schedules: List[DealScheduleResponse] = Field(description="List of deal schedules")
    total_count: int = Field(description="Total number of schedules", example=5)
    
    class Config:
        schema_extra = {
            "example": {
                "success": True,
                "message": "Deal schedules retrieved successfully",
                "timestamp": "2025-01-27T16:00:00.000Z",
                "schedules": [
                    {
                        "id": "789e0123-e89b-12d3-a456-426614174000",
                        "cities": ["Karachi", "Lahore", "Islamabad"],
                        "schedule_name": "Daily Deal Scraping",
                        "day_of_week": "monday",
                        "time_of_day": "14:30",
                        "status": "active",
                        "total_runs": 15,
                        "successful_runs": 12,
                        "failed_runs": 3
                    }
                ],
                "total_count": 5
            }
        }

class DealScheduleStatusResponse(BaseModel):
    id: str = Field(description="Schedule ID", example="789e0123-e89b-12d3-a456-426614174000")
    status: ScheduleStatus = Field(description="Current status", example=ScheduleStatus.ACTIVE)
    next_run_at: Optional[datetime] = Field(default=None, description="Next execution time")
    last_run_at: Optional[datetime] = Field(default=None, description="Last execution time")
    total_runs: int = Field(description="Total executions", example=15)
    successful_runs: int = Field(description="Successful executions", example=12)
    failed_runs: int = Field(description="Failed executions", example=3)
    last_task_id: Optional[str] = Field(default=None, description="Last task ID")
    last_error: Optional[str] = Field(default=None, description="Last error message")
    cities_count: int = Field(description="Number of cities in schedule", example=3)
    
    class Config:
        use_enum_values = True
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
        schema_extra = {
            "example": {
                "id": "789e0123-e89b-12d3-a456-426614174000",
                "status": "active",
                "next_run_at": "2025-02-03T14:30:00.000Z",
                "last_run_at": "2025-01-27T14:30:00.000Z",
                "total_runs": 15,
                "successful_runs": 12,
                "failed_runs": 3,
                "last_task_id": "123e4567-e89b-12d3-a456-426614174000",
                "last_error": None,
                "cities_count": 3
            }
        }

class DealScheduleActionResponse(BaseResponse):
    schedule_id: str = Field(description="Schedule ID that was acted upon", example="789e0123-e89b-12d3-a456-426614174000")
    action: str = Field(description="Action that was performed", example="paused")
    
    class Config:
        schema_extra = {
            "example": {
                "success": True,
                "message": "Deal schedule paused successfully",
                "timestamp": "2025-01-27T16:00:00.000Z",
                "schedule_id": "789e0123-e89b-12d3-a456-426614174000",
                "action": "paused"
            }
        }