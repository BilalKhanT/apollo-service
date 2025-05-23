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
    
    # Crawl configuration
    max_links_to_scrape: Optional[int] = None
    max_pages_to_scrape: Optional[int] = None
    depth_limit: Optional[int] = None
    domain_restriction: bool = True
    scrape_pdfs_and_xls: bool = True
    
    # Schedule metadata
    status: ScheduleStatus = Field(default=ScheduleStatus.ACTIVE)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    last_run_at: Optional[datetime] = None
    next_run_at: Optional[datetime] = None
    
    # Execution tracking
    total_runs: int = Field(default=0)
    successful_runs: int = Field(default=0)
    failed_runs: int = Field(default=0)
    last_task_id: Optional[str] = None
    last_error: Optional[str] = None
    
    @validator('time_of_day')
    def validate_time_format(cls, v):
        if isinstance(v, str):
            try:
                # Validate the time format
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
            [("base_url", 1), ("status", 1)]  # Compound index
        ]
    
    def update_timestamp(self):
        self.updated_at = datetime.utcnow()
    
    def get_time_object(self) -> time:
        """Convert time_of_day string to time object"""
        return time.fromisoformat(self.time_of_day)
    
    def calculate_next_run(self) -> datetime:
        """Calculate the next run time based on day_of_week and time_of_day"""
        from datetime import datetime, timedelta
        
        now = datetime.utcnow()
        
        # Map day names to weekday numbers (Monday=0, Sunday=6)
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
        
        # Get the time object
        target_time = self.get_time_object()
        
        # Calculate days until next occurrence
        days_ahead = target_weekday - current_weekday
        
        # If the target day is today, check if the time has already passed
        if days_ahead == 0:
            scheduled_time = datetime.combine(now.date(), target_time)
            if scheduled_time <= now:
                days_ahead = 7  # Schedule for next week
        elif days_ahead < 0:
            days_ahead += 7  # Schedule for next week
        
        next_run = now + timedelta(days=days_ahead)
        next_run = datetime.combine(next_run.date(), target_time)
        
        return next_run
    
    def mark_run_started(self, task_id: str):
        """Mark that a scheduled run has started"""
        self.last_run_at = datetime.utcnow()
        self.last_task_id = task_id
        self.total_runs += 1
        self.next_run_at = self.calculate_next_run()
        self.update_timestamp()
    
    def mark_run_completed(self, success: bool, error: Optional[str] = None):
        """Mark that a scheduled run has completed"""
        if success:
            self.successful_runs += 1
            self.last_error = None
        else:
            self.failed_runs += 1
            self.last_error = error
        
        self.update_timestamp()