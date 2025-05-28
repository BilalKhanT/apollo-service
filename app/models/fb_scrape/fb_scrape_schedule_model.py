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

class FBScheduleRequest(BaseModel):
    keywords: List[str] = Field(
        ..., 
        description="List of keywords to filter Facebook posts", 
        example=["offer", "discount", "deal"], 
        min_items=1
    )
    days: int = Field(
        ..., 
        description="Number of days to look back for posts", 
        example=30, 
        ge=1, 
        le=365
    )
    schedule_name: Optional[str] = Field(
        None, 
        description="Optional name for the schedule", 
        example="Daily FB Deal Scraping"
    )
    day_of_week: DayOfWeek = Field(
        ..., 
        description="Day of the week to run the Facebook scraping", 
        example=DayOfWeek.MONDAY
    )
    time_of_day: str = Field(
        ..., 
        description="Time of day to run the scraping (HH:MM format)", 
        example="14:30"
    )

    @validator('time_of_day')
    def validate_time_format(cls, v):
        try:
            time.fromisoformat(v)
            return v
        except ValueError:
            raise ValueError('time_of_day must be in HH:MM format (e.g., "14:30")')

    @validator('keywords')
    def validate_keywords(cls, v):
        if not v:
            raise ValueError('At least one keyword must be provided')
        cleaned_keywords = []
        for keyword in v:
            if keyword and keyword.strip():
                cleaned_keywords.append(keyword.strip())
        if not cleaned_keywords:
            raise ValueError('At least one valid keyword must be provided')
        return cleaned_keywords

    @validator('days')
    def validate_days(cls, v):
        if v < 1:
            raise ValueError('Days must be at least 1')
        if v > 365:
            raise ValueError('Days cannot exceed 365')
        return v

    class Config:
        use_enum_values = True
        schema_extra = {
            "example": {
                "keywords": ["offer", "discount", "deal"],
                "days": 30,
                "schedule_name": "Daily FB Deal Scraping",
                "day_of_week": "monday",
                "time_of_day": "14:30"
            }
        }

class FBScheduleResponse(BaseModel):
    id: str = Field(..., description="Schedule ID", example="fb789e0123-e89b-12d3-a456-426614174000")
    keywords: List[str] = Field(description="Keywords being searched", example=["offer", "discount", "deal"])
    days: int = Field(description="Number of days to look back", example=30)
    schedule_name: Optional[str] = Field(default=None, description="Schedule name", example="Daily FB Deal Scraping")
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
                "id": "fb789e0123-e89b-12d3-a456-426614174000",
                "keywords": ["offer", "discount", "deal"],
                "days": 30,
                "schedule_name": "Daily FB Deal Scraping",
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
                "last_task_id": "fb123e4567-e89b-12d3-a456-426614174000",
                "last_error": None
            }
        }

class FBScheduleUpdateRequest(BaseModel):
    keywords: Optional[List[str]] = Field(
        default=None, 
        description="Updated keywords list",
        example=["offer", "discount", "deal", "promotion"]
    )
    days: Optional[int] = Field(
        default=None, 
        description="Updated number of days to look back", 
        example=45,
        ge=1, 
        le=365
    )
    schedule_name: Optional[str] = Field(
        default=None, 
        description="Updated schedule name",
        example="Updated FB Deal Scraping"
    )
    day_of_week: Optional[DayOfWeek] = Field(
        default=None, 
        description="Updated day of week",
        example=DayOfWeek.TUESDAY
    )
    time_of_day: Optional[str] = Field(
        default=None, 
        description="Updated time of day",
        example="15:00"
    )
    status: Optional[ScheduleStatus] = Field(
        default=None, 
        description="Updated status",
        example=ScheduleStatus.PAUSED
    )
    
    @validator('time_of_day')
    def validate_time_format(cls, v):
        if v is not None:
            try:
                time.fromisoformat(v)
                return v
            except ValueError:
                raise ValueError('time_of_day must be in HH:MM format (e.g., "14:30")')
        return v
    
    @validator('keywords')
    def validate_keywords(cls, v):
        if v is not None:
            if not v:
                raise ValueError('At least one keyword must be provided')
            cleaned_keywords = []
            for keyword in v:
                if keyword and keyword.strip():
                    cleaned_keywords.append(keyword.strip())
            if not cleaned_keywords:
                raise ValueError('At least one valid keyword must be provided')
            return cleaned_keywords
        return v

    @validator('days')
    def validate_days(cls, v):
        if v is not None:
            if v < 1:
                raise ValueError('Days must be at least 1')
            if v > 365:
                raise ValueError('Days cannot exceed 365')
        return v
    
    class Config:
        use_enum_values = True
        schema_extra = {
            "example": {
                "keywords": ["offer", "discount", "deal", "promotion"],
                "days": 45,
                "schedule_name": "Updated FB Deal Scraping",
                "day_of_week": "tuesday",
                "time_of_day": "15:00",
                "status": "paused"
            }
        }

class FBScheduleListResponse(BaseResponse):
    schedules: List[FBScheduleResponse] = Field(description="List of Facebook schedules")
    total_count: int = Field(description="Total number of schedules", example=5)
    
    class Config:
        schema_extra = {
            "example": {
                "success": True,
                "message": "Facebook schedules retrieved successfully",
                "timestamp": "2025-01-27T16:00:00.000Z",
                "schedules": [
                    {
                        "id": "fb789e0123-e89b-12d3-a456-426614174000",
                        "keywords": ["offer", "discount", "deal"],
                        "days": 30,
                        "schedule_name": "Daily FB Deal Scraping",
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

class FBScheduleStatusResponse(BaseModel):
    id: str = Field(description="Schedule ID", example="fb789e0123-e89b-12d3-a456-426614174000")
    status: ScheduleStatus = Field(description="Current status", example=ScheduleStatus.ACTIVE)
    next_run_at: Optional[datetime] = Field(default=None, description="Next execution time")
    last_run_at: Optional[datetime] = Field(default=None, description="Last execution time")
    total_runs: int = Field(description="Total executions", example=15)
    successful_runs: int = Field(description="Successful executions", example=12)
    failed_runs: int = Field(description="Failed executions", example=3)
    last_task_id: Optional[str] = Field(default=None, description="Last task ID")
    last_error: Optional[str] = Field(default=None, description="Last error message")
    keywords_count: int = Field(description="Number of keywords in schedule", example=3)
    days: int = Field(description="Number of days to look back", example=30)
    
    class Config:
        use_enum_values = True
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
        schema_extra = {
            "example": {
                "id": "fb789e0123-e89b-12d3-a456-426614174000",
                "status": "active",
                "next_run_at": "2025-02-03T14:30:00.000Z",
                "last_run_at": "2025-01-27T14:30:00.000Z",
                "total_runs": 15,
                "successful_runs": 12,
                "failed_runs": 3,
                "last_task_id": "fb123e4567-e89b-12d3-a456-426614174000",
                "last_error": None,
                "keywords_count": 3,
                "days": 30
            }
        }

class FBScheduleActionResponse(BaseResponse):
    schedule_id: str = Field(
        description="Schedule ID that was acted upon", 
        example="fb789e0123-e89b-12d3-a456-426614174000"
    )
    action: str = Field(
        description="Action that was performed", 
        example="paused"
    )
    
    class Config:
        schema_extra = {
            "example": {
                "success": True,
                "message": "Facebook schedule paused successfully",
                "timestamp": "2025-01-27T16:00:00.000Z",
                "schedule_id": "fb789e0123-e89b-12d3-a456-426614174000",
                "action": "paused"
            }
        }