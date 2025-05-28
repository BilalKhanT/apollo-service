from beanie import Document
from typing import Optional
from pydantic import Field, validator
from datetime import datetime, time, timedelta
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
    
    def calculate_next_run(self, force_next_week: bool = False) -> datetime:
        karachi_tz = pytz.timezone('Asia/Karachi')
        now_utc = datetime.utcnow()
        now_karachi = now_utc.replace(tzinfo=pytz.UTC).astimezone(karachi_tz)
        
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
        days_ahead = target_weekday - current_weekday

        if days_ahead == 0 and not force_next_week:
            scheduled_time_karachi = datetime.combine(now_karachi.date(), target_time)
            scheduled_time_karachi = karachi_tz.localize(scheduled_time_karachi)
            
            time_buffer = timedelta(minutes=1)
            buffer_time = now_karachi + time_buffer
            
            if scheduled_time_karachi > buffer_time:
                next_run_utc = scheduled_time_karachi.astimezone(pytz.UTC).replace(tzinfo=None)
                logger.debug(f"DEBUG: Next run today at: {next_run_utc} UTC ({scheduled_time_karachi} Karachi)")
                return next_run_utc
            else:
                days_ahead = 7
                logger.debug(f"DEBUG: Time passed, scheduling for next week")
        elif days_ahead <= 0 or force_next_week:
            days_ahead = 7 + (days_ahead if days_ahead < 0 else 0)
            logger.debug(f"DEBUG: Adjusted days ahead: {days_ahead}")

        next_run_date_karachi = now_karachi + timedelta(days=days_ahead)
        next_run_karachi = datetime.combine(next_run_date_karachi.date(), target_time)
        next_run_karachi = karachi_tz.localize(next_run_karachi)
        next_run_utc = next_run_karachi.astimezone(pytz.UTC).replace(tzinfo=None)
        
        logger.debug(f"DEBUG: Next run calculated: {next_run_utc} UTC ({next_run_karachi} Karachi)")
        return next_run_utc
    
    def mark_run_started(self, task_id: str):
        self.last_run_at = datetime.utcnow()
        self.last_task_id = task_id
        self.total_runs += 1
        self.next_run_at = self.calculate_next_run(force_next_week=True)
        self.update_timestamp()
        
        logger.info(f"Marked run started for schedule {self.id}. Next run display: {self.next_run_at} UTC")
    
    def mark_run_completed(self, success: bool, error: Optional[str] = None):
        if success:
            self.successful_runs += 1
            self.last_error = None
        else:
            self.failed_runs += 1
            self.last_error = error
        
        self.update_timestamp()
    
    async def sync_with_scheduler(self):
        try:
            from app.services.schedule_service import scheduler_service, ScheduleType
            
            if self.status == ScheduleStatus.ACTIVE:
                await scheduler_service.update_crawl_schedule(self)
            else:
                await scheduler_service.remove_schedule(str(self.id), ScheduleType.CRAWL)
                
        except Exception as e:
            logger.error(f"Error syncing schedule {self.id} with scheduler: {str(e)}")
    
    async def save(self, *args, **kwargs):
        result = await super().save(*args, **kwargs)
        await self.sync_with_scheduler()
        
        return result
    
    async def delete(self, *args, **kwargs):
        try:
            from app.services.schedule_service import scheduler_service, ScheduleType
            await scheduler_service.remove_schedule(str(self.id), ScheduleType.CRAWL)
        except Exception as e:
            logger.error(f"Error removing schedule {self.id} from scheduler: {str(e)}")
        
        return await super().delete(*args, **kwargs)