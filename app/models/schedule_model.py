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

class CrawlScheduleRequest(BaseModel):
    base_url: str = Field(..., description="Base URL to crawl", example="https://example.com")
    schedule_name: Optional[str] = Field(None, description="Optional name for the schedule", example="Daily Example Crawl")
    day_of_week: DayOfWeek = Field(..., description="Day of the week to run the crawl", example=DayOfWeek.MONDAY)
    time_of_day: str = Field(..., description="Time of day to run the crawl (HH:MM format)", example="14:30")
    max_links_to_scrape: Optional[int] = Field(None, description="Maximum number of links to scrape", example=1000)
    max_pages_to_scrape: Optional[int] = Field(None, description="Maximum number of pages to scrape", example=500)
    depth_limit: Optional[int] = Field(None, description="Maximum depth to crawl", example=3)
    domain_restriction: bool = Field(True, description="Whether to restrict crawling to the base domain", example=True)
    scrape_pdfs_and_xls: bool = Field(True, description="Whether to scrape PDFs and XLS files", example=True)
    
    @validator('time_of_day')
    def validate_time_format(cls, v):
        try:
            time.fromisoformat(v)
            return v
        except ValueError:
            raise ValueError('time_of_day must be in HH:MM format (e.g., "14:30")')
    
    @validator('base_url')
    def validate_base_url(cls, v):
        if not v.startswith(('http://', 'https://')):
            raise ValueError('base_url must start with http:// or https://')
        return v
    
    class Config:
        use_enum_values = True
        schema_extra = {
            "example": {
                "base_url": "https://example.com",
                "schedule_name": "Daily Example Crawl",
                "day_of_week": "monday",
                "time_of_day": "14:30",
                "max_links_to_scrape": 1000,
                "max_pages_to_scrape": 500,
                "depth_limit": 3,
                "domain_restriction": True,
                "scrape_pdfs_and_xls": True
            }
        }

class CrawlScheduleResponse(BaseModel):
    id: str = Field(..., description="Schedule ID", example="789e0123-e89b-12d3-a456-426614174000")
    base_url: str = Field(description="Base URL being crawled", example="https://example.com")
    schedule_name: Optional[str] = Field(default=None, description="Schedule name", example="Daily Example Crawl")
    day_of_week: DayOfWeek = Field(description="Day of the week", example=DayOfWeek.MONDAY)
    time_of_day: str = Field(description="Time of day", example="14:30")
    max_links_to_scrape: Optional[int] = Field(default=None, description="Max links limit", example=1000)
    max_pages_to_scrape: Optional[int] = Field(default=None, description="Max pages limit", example=500)
    depth_limit: Optional[int] = Field(default=None, description="Depth limit", example=3)
    domain_restriction: bool = Field(default=True, description="Domain restriction", example=True)
    scrape_pdfs_and_xls: bool = Field(default=True, description="Scrape PDFs and XLS", example=True)
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
                "base_url": "https://example.com",
                "schedule_name": "Daily Example Crawl",
                "day_of_week": "monday",
                "time_of_day": "14:30",
                "max_links_to_scrape": 1000,
                "max_pages_to_scrape": 500,
                "depth_limit": 3,
                "domain_restriction": True,
                "scrape_pdfs_and_xls": True,
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

class CrawlScheduleUpdateRequest(BaseModel):
    schedule_name: Optional[str] = Field(default=None, description="Updated schedule name")
    day_of_week: Optional[DayOfWeek] = Field(default=None, description="Updated day of week")
    time_of_day: Optional[str] = Field(default=None, description="Updated time of day")
    max_links_to_scrape: Optional[int] = Field(default=None, description="Updated max links limit")
    max_pages_to_scrape: Optional[int] = Field(default=None, description="Updated max pages limit")
    depth_limit: Optional[int] = Field(default=None, description="Updated depth limit")
    domain_restriction: Optional[bool] = Field(default=None, description="Updated domain restriction")
    scrape_pdfs_and_xls: Optional[bool] = Field(default=None, description="Updated PDF/XLS scraping")
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
    
    class Config:
        use_enum_values = True
        schema_extra = {
            "example": {
                "schedule_name": "Updated Daily Crawl",
                "day_of_week": "tuesday",
                "time_of_day": "15:00",
                "status": "paused"
            }
        }

class ScheduleListResponse(BaseResponse):
    schedules: List[CrawlScheduleResponse] = Field(description="List of schedules")
    total_count: int = Field(description="Total number of schedules", example=5)
    
    class Config:
        schema_extra = {
            "example": {
                "success": True,
                "message": "Schedules retrieved successfully",
                "timestamp": "2025-01-27T16:00:00.000Z",
                "schedules": [
                    {
                        "id": "789e0123-e89b-12d3-a456-426614174000",
                        "base_url": "https://example.com",
                        "schedule_name": "Daily Example Crawl",
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

class ScheduleStatusResponse(BaseModel):
    id: str = Field(description="Schedule ID", example="789e0123-e89b-12d3-a456-426614174000")
    status: ScheduleStatus = Field(description="Current status", example=ScheduleStatus.ACTIVE)
    next_run_at: Optional[datetime] = Field(default=None, description="Next execution time")
    last_run_at: Optional[datetime] = Field(default=None, description="Last execution time")
    total_runs: int = Field(description="Total executions", example=15)
    successful_runs: int = Field(description="Successful executions", example=12)
    failed_runs: int = Field(description="Failed executions", example=3)
    last_task_id: Optional[str] = Field(default=None, description="Last task ID")
    last_error: Optional[str] = Field(default=None, description="Last error message")
    
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
                "last_error": None
            }
        }

class ScheduleActionResponse(BaseResponse):
    schedule_id: str = Field(description="Schedule ID that was acted upon", example="789e0123-e89b-12d3-a456-426614174000")
    action: str = Field(description="Action that was performed", example="paused")
    
    class Config:
        schema_extra = {
            "example": {
                "success": True,
                "message": "Schedule paused successfully",
                "timestamp": "2025-01-27T16:00:00.000Z",
                "schedule_id": "789e0123-e89b-12d3-a456-426614174000",
                "action": "paused"
            }
        }