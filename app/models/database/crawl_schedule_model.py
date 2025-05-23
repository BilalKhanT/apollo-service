from beanie import Document
from typing import Optional
from pydantic import Field, validator
from datetime import datetime, time
from enum import Enum


class ScheduleStatus(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    PAUSED = "paused"


class DayOfWeek(str, Enum):
    MONDAY = "monday"
    TUESDAY = "tuesday"
    WEDNESDAY = "wednesday"
    THURSDAY = "thursday"
    FRIDAY = "friday"
    SATURDAY = "saturday"
    SUNDAY = "sunday"


class CrawlSchedule(Document):
    base_url: str = Field(..., index=True)
    schedule_name: Optional[str] = None
    day_of_week: DayOfWeek = Field(..., description="Day of the week to run the crawl")
    time_of_day: str = Field(..., description="Time of day to run the crawl (HH:MM format)")
    max_links_to_scrape: Optional[int] = None
    max_pages_to_scrape: Optional[int] = None
    depth_limit: Optional[int] = None
    domain_restriction: bool = True
    scrape_pdfs_and_xls: bool = True
    status: ScheduleStatus = Field(default=ScheduleStatus.ACTIVE)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    last_run_at: Optional[datetime] = None
    next_run_at: Optional[datetime] = None
    total_runs: int = Field(default=0)
    successful_runs: int = Field(default=0)
    failed_runs: int = Field(default=0)
    last_task_id: Optional[str] = None
    last_error: Optional[str] = None
    
    @validator('time_of_day')
    def validate_time_format(cls, v):
        if isinstance(v, str):
            try:
                time.fromisoformat(v)
                return v
            except ValueError:
                raise ValueError('time_of_day must be in HH:MM format (e.g., "14:30")')
        return v
    
    class Settings:
        name = "crawl_schedules"
        indexes = [
            "base_url",
            "status",
            "next_run_at",
            [("base_url", 1), ("status", 1)] 
        ]
    
    def update_timestamp(self):
        self.updated_at = datetime.utcnow()
    
    def get_time_object(self) -> time:
        return time.fromisoformat(self.time_of_day)
    
    def calculate_next_run(self) -> datetime:
        from datetime import datetime, timedelta

        now = datetime.utcnow().replace(second=0, microsecond=0)
        day_mapping = {
            DayOfWeek.MONDAY: 0,
            DayOfWeek.TUESDAY: 1,
            DayOfWeek.WEDNESDAY: 2,
            DayOfWeek.THURSDAY: 3,
            DayOfWeek.FRIDAY: 4,
            DayOfWeek.SATURDAY: 5,
            DayOfWeek.SUNDAY: 6
        }
        target_weekday = day_mapping[self.day_of_week]
        current_weekday = now.weekday()
        target_time = self.get_time_object()
        days_ahead = target_weekday - current_weekday

        if days_ahead == 0:
            scheduled_time_today = datetime.combine(now.date(), target_time)
            if scheduled_time_today >= now:
                return scheduled_time_today
            else:
                days_ahead = 7
        elif days_ahead < 0:
            days_ahead += 7

        next_run_date = now + timedelta(days=days_ahead)
        next_run = datetime.combine(next_run_date.date(), target_time)
        return next_run
    
    def mark_run_started(self, task_id: str):
        self.last_run_at = datetime.utcnow()
        self.last_task_id = task_id
        self.total_runs += 1
        self.next_run_at = self.calculate_next_run()
        self.update_timestamp()
    
    def mark_run_completed(self, success: bool, error: Optional[str] = None):
        if success:
            self.successful_runs += 1
            self.last_error = None
        else:
            self.failed_runs += 1
            self.last_error = error
        
        self.update_timestamp()