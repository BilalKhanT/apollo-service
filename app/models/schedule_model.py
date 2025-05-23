from pydantic import BaseModel, Field, validator
from typing import Optional, List
from datetime import time, datetime
from enum import Enum


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
    base_url: str = Field(..., description="Base URL to crawl")
    schedule_name: Optional[str] = Field(None, description="Optional name for the schedule")
    day_of_week: DayOfWeek = Field(..., description="Day of the week to run the crawl")
    time_of_day: str = Field(..., description="Time of day to run the crawl (HH:MM format)")
    max_links_to_scrape: Optional[int] = Field(None, description="Maximum number of links to scrape")
    max_pages_to_scrape: Optional[int] = Field(None, description="Maximum number of pages to scrape")
    depth_limit: Optional[int] = Field(None, description="Maximum depth to crawl")
    domain_restriction: bool = Field(True, description="Whether to restrict crawling to the base domain")
    scrape_pdfs_and_xls: bool = Field(True, description="Whether to scrape PDFs and XLS files")
    
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


class CrawlScheduleResponse(BaseModel):
    id: str = Field(..., description="Schedule ID")
    base_url: str
    schedule_name: Optional[str] = None
    day_of_week: DayOfWeek
    time_of_day: str
    max_links_to_scrape: Optional[int] = None
    max_pages_to_scrape: Optional[int] = None
    depth_limit: Optional[int] = None
    domain_restriction: bool = True
    scrape_pdfs_and_xls: bool = True
    status: ScheduleStatus
    created_at: datetime
    updated_at: datetime
    last_run_at: Optional[datetime] = None
    next_run_at: Optional[datetime] = None
    total_runs: int = 0
    successful_runs: int = 0
    failed_runs: int = 0
    last_task_id: Optional[str] = None
    last_error: Optional[str] = None


class CrawlScheduleUpdateRequest(BaseModel):
    schedule_name: Optional[str] = None
    day_of_week: Optional[DayOfWeek] = None
    time_of_day: Optional[str] = None
    max_links_to_scrape: Optional[int] = None
    max_pages_to_scrape: Optional[int] = None
    depth_limit: Optional[int] = None
    domain_restriction: Optional[bool] = None
    scrape_pdfs_and_xls: Optional[bool] = None
    status: Optional[ScheduleStatus] = None
    
    @validator('time_of_day')
    def validate_time_format(cls, v):
        if v is not None:
            try:
                time.fromisoformat(v)
                return v
            except ValueError:
                raise ValueError('time_of_day must be in HH:MM format (e.g., "14:30")')
        return v


class ScheduleListResponse(BaseModel):
    schedules: List[CrawlScheduleResponse]
    total_count: int


class ScheduleStatusResponse(BaseModel):
    id: str
    status: ScheduleStatus
    next_run_at: Optional[datetime]
    last_run_at: Optional[datetime]
    total_runs: int
    successful_runs: int
    failed_runs: int
    last_task_id: Optional[str] = None
    last_error: Optional[str] = None