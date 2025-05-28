from beanie import Document
from typing import List, Optional
from pydantic import Field, validator
from datetime import datetime, time, timedelta
from enum import Enum
import pytz
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


class FacebookScrapeSchedule(Document):
    keywords: List[str] = Field(..., description="Keywords to filter Facebook posts")
    days: int = Field(..., description="Number of days to look back for posts", ge=1, le=365)
    schedule_name: Optional[str] = None
    day_of_week: DayOfWeek = Field(..., description="Day of the week to run the scrape")
    time_of_day: str = Field(..., description="Time of day (HH:MM, Asia/Karachi)")
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

    @validator('keywords')
    def validate_keywords(cls, v):
        if not v:
            raise ValueError('At least one keyword must be provided')
        
        # Clean and deduplicate keywords
        cleaned_keywords = []
        for keyword in v:
            if keyword and keyword.strip():
                cleaned_keywords.append(keyword.strip())
        
        # Remove duplicates while preserving order
        seen = set()
        unique_keywords = []
        for keyword in cleaned_keywords:
            if keyword.lower() not in seen:
                seen.add(keyword.lower())
                unique_keywords.append(keyword)
        
        if not unique_keywords:
            raise ValueError("At least one valid keyword must be provided")
        
        return unique_keywords

    @validator('days')
    def validate_days(cls, v):
        if v < 1:
            raise ValueError('Days must be at least 1')
        if v > 365:
            raise ValueError('Days cannot exceed 365')
        return v

    class Settings:
        name = "facebook_scrape_schedules"
        indexes = ["keywords", "status", "next_run_at", "days"]

    def update_timestamp(self):
        self.updated_at = datetime.utcnow()

    def get_time_object(self) -> time:
        return time.fromisoformat(self.time_of_day)

    def calculate_next_run(self, force_next_week: bool = False) -> datetime:
        karachi_tz = pytz.timezone('Asia/Karachi')
        now_utc = datetime.utcnow()
        now_karachi = now_utc.replace(tzinfo=pytz.UTC).astimezone(karachi_tz)

        # Map day of week to weekday number (Monday=0)
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

        # Calculate days until target day
        days_ahead = target_weekday - current_weekday
        
        # Handle same day scheduling
        if days_ahead == 0 and not force_next_week:
            # Check if we can still run today
            scheduled_time = karachi_tz.localize(datetime.combine(now_karachi.date(), target_time))
            buffer_time = now_karachi + timedelta(minutes=1)  # 1 minute buffer
            
            if scheduled_time > buffer_time:
                # Can still run today
                return scheduled_time.astimezone(pytz.UTC).replace(tzinfo=None)
            else:
                # Too late for today, schedule for next week
                days_ahead = 7
        elif days_ahead <= 0 or force_next_week:
            # Schedule for next occurrence
            days_ahead = 7 + (days_ahead if days_ahead < 0 else 0)

        # Calculate next run date
        next_date = now_karachi + timedelta(days=days_ahead)
        next_run_karachi = karachi_tz.localize(datetime.combine(next_date.date(), target_time))
        
        # Convert to UTC and remove timezone info for MongoDB storage
        return next_run_karachi.astimezone(pytz.UTC).replace(tzinfo=None)

    def mark_run_started(self, task_id: str):
        self.last_run_at = datetime.utcnow()
        self.last_task_id = task_id
        self.total_runs += 1
        self.next_run_at = self.calculate_next_run(force_next_week=True)
        self.update_timestamp()
        
        logger.info(f"Marked run started for Facebook schedule {self.id}. Next run: {self.next_run_at}")

    def mark_run_completed(self, success: bool, error: Optional[str] = None):
        if success:
            self.successful_runs += 1
            self.last_error = None
            logger.info(f"Facebook schedule {self.id} run completed successfully")
        else:
            self.failed_runs += 1
            self.last_error = error
            logger.warning(f"Facebook schedule {self.id} run failed: {error}")
        
        self.update_timestamp()
    
    async def sync_with_scheduler(self):
        try:
            from app.services.schedule_service import scheduler_service, ScheduleType
            
            if self.status == ScheduleStatus.ACTIVE:
                # Add or update the schedule in the scheduler
                await scheduler_service.update_facebook_schedule(self)
                logger.debug(f"Synced active Facebook schedule {self.id} with scheduler")
            else:
                # Remove the schedule from the scheduler if it's not active
                await scheduler_service.remove_schedule(str(self.id), ScheduleType.FACEBOOK_SCRAPE)
                logger.debug(f"Removed inactive Facebook schedule {self.id} from scheduler")
                
        except Exception as e:
            logger.error(f"Error syncing Facebook schedule {self.id} with scheduler: {str(e)}")
    
    async def save(self, *args, **kwargs):
        # Update timestamp before saving
        self.update_timestamp()
        
        # Save to database
        result = await super().save(*args, **kwargs)
        
        # Sync with scheduler after successful save
        await self.sync_with_scheduler()
        
        return result
    
    async def delete(self, *args, **kwargs):
        try:
            from app.services.schedule_service import scheduler_service, ScheduleType
            await scheduler_service.remove_schedule(str(self.id), ScheduleType.FACEBOOK_SCRAPE)
            logger.info(f"Removed Facebook schedule {self.id} from scheduler before deletion")
        except Exception as e:
            logger.error(f"Error removing Facebook schedule {self.id} from scheduler: {str(e)}")
        
        # Delete from database
        return await super().delete(*args, **kwargs)

    def get_summary_dict(self) -> dict:
        return {
            "id": str(self.id),
            "keywords": self.keywords,
            "days": self.days,
            "schedule_name": self.schedule_name,
            "day_of_week": self.day_of_week.value,
            "time_of_day": self.time_of_day,
            "status": self.status.value,
            "keywords_count": len(self.keywords),
            "total_runs": self.total_runs,
            "successful_runs": self.successful_runs,
            "failed_runs": self.failed_runs,
            "next_run_at": self.next_run_at.isoformat() if self.next_run_at else None,
            "last_run_at": self.last_run_at.isoformat() if self.last_run_at else None,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat()
        }

    def is_due_for_execution(self, buffer_minutes: int = 5) -> bool:
        if self.status != ScheduleStatus.ACTIVE:
            return False
        
        if not self.next_run_at:
            return False
        
        now_utc = datetime.utcnow()
        due_time = self.next_run_at - timedelta(minutes=buffer_minutes)
        
        return now_utc >= due_time

    def get_keywords_display(self) -> str:
        if not self.keywords:
            return "No keywords"
        
        if len(self.keywords) <= 3:
            return ", ".join(self.keywords)
        else:
            return f"{', '.join(self.keywords[:3])} and {len(self.keywords) - 3} more"

    def get_success_rate(self) -> float:
        if self.total_runs == 0:
            return 0.0
        
        return (self.successful_runs / self.total_runs) * 100.0

    def __str__(self):
        return f"FacebookScrapeSchedule(id={self.id}, keywords={self.get_keywords_display()}, day={self.day_of_week.value}, time={self.time_of_day}, status={self.status.value})"

    def __repr__(self):
        return f"FacebookScrapeSchedule(id={self.id}, keywords={self.keywords}, days={self.days}, schedule_name='{self.schedule_name}', day_of_week={self.day_of_week.value}, time_of_day={self.time_of_day}, status={self.status.value})"