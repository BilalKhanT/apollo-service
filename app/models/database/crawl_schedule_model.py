from beanie import Document
from typing import Optional
from pydantic import Field, validator
from datetime import datetime, time
import pytz
from enum import Enum
import logging

logger = logging.getLogger(__name__)

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
    time_of_day: str = Field(..., description="Time of day to run the crawl (HH:MM format in Asia/Karachi timezone)")
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
        """
        Calculate the next run time in UTC, treating time_of_day as Asia/Karachi time.
        """
        from datetime import datetime, timedelta
        
        # Asia/Karachi timezone
        karachi_tz = pytz.timezone('Asia/Karachi')
        
        # Get current time in Karachi timezone
        now_utc = datetime.utcnow()
        now_karachi = now_utc.replace(tzinfo=pytz.UTC).astimezone(karachi_tz)
        
        logger.info(f"DEBUG: Current UTC time: {now_utc}")
        logger.info(f"DEBUG: Current Karachi time: {now_karachi}")
        
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
        current_weekday = now_karachi.weekday()
        target_time = self.get_time_object()
        
        logger.info(f"DEBUG: Target weekday: {target_weekday} ({self.day_of_week})")
        logger.info(f"DEBUG: Current weekday: {current_weekday}")
        logger.info(f"DEBUG: Target time: {target_time} (Karachi time)")
        
        days_ahead = target_weekday - current_weekday

        if days_ahead == 0:
            # Same day - check if time has passed
            scheduled_time_karachi = datetime.combine(now_karachi.date(), target_time)
            scheduled_time_karachi = karachi_tz.localize(scheduled_time_karachi)
            
            logger.info(f"DEBUG: Scheduled time Karachi: {scheduled_time_karachi}")
            
            if scheduled_time_karachi > now_karachi:
                # Time hasn't passed yet today
                next_run_utc = scheduled_time_karachi.astimezone(pytz.UTC).replace(tzinfo=None)
                logger.info(f"DEBUG: Next run today at: {next_run_utc} UTC ({scheduled_time_karachi} Karachi)")
                return next_run_utc
            else:
                # Time has passed, schedule for next week
                days_ahead = 7
                logger.info(f"DEBUG: Time passed today, scheduling for next week")
        elif days_ahead < 0:
            days_ahead += 7
            logger.info(f"DEBUG: Adjusted days ahead: {days_ahead}")

        # Calculate next run date in Karachi timezone
        next_run_date_karachi = now_karachi + timedelta(days=days_ahead)
        next_run_karachi = datetime.combine(next_run_date_karachi.date(), target_time)
        next_run_karachi = karachi_tz.localize(next_run_karachi)
        
        # Convert to UTC for storage
        next_run_utc = next_run_karachi.astimezone(pytz.UTC).replace(tzinfo=None)
        
        logger.info(f"DEBUG: Next run calculated: {next_run_utc} UTC ({next_run_karachi} Karachi)")
        return next_run_utc
    
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