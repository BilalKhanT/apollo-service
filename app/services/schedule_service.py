import asyncio
import logging
from datetime import datetime, time
from typing import Dict, Any, List, Optional, Union
from dataclasses import dataclass
from enum import Enum

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR, EVENT_JOB_MISSED
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.asyncio import AsyncIOExecutor
import pytz

from app.models.database.apollo_scraper.crawl_schedule_model import CrawlSchedule, ScheduleStatus as CrawlScheduleStatus
from app.models.database.restaurant_deal.deal_schedule_model import DealScrapeSchedule, ScheduleStatus as DealScheduleStatus
from app.utils.task_manager import task_manager

logger = logging.getLogger(__name__)


class ScheduleType(str, Enum):
    CRAWL = "crawl"
    DEAL_SCRAPE = "deal_scrape"


@dataclass
class ScheduleJobData:
    """Data structure to hold schedule job information"""
    schedule_id: str
    schedule_type: ScheduleType
    schedule_name: Optional[str]
    parameters: Dict[str, Any]
    timezone: str = "Asia/Karachi"


class SchedulerService:
    
    def __init__(self):
        self.scheduler: Optional[AsyncIOScheduler] = None
        self.running = False
        self.karachi_tz = pytz.timezone('Asia/Karachi')
        
        # Job stores and executors configuration
        jobstores = {
            'default': MemoryJobStore()
        }
        executors = {
            'default': AsyncIOExecutor()
        }
        job_defaults = {
            'coalesce': True,  # Combine multiple missed executions into one
            'max_instances': 1,  # Prevent overlapping executions
            'misfire_grace_time': 60  # Allow 1 minute grace period for missed jobs
        }
        
        # Initialize scheduler
        self.scheduler = AsyncIOScheduler(
            jobstores=jobstores,
            executors=executors,
            job_defaults=job_defaults,
            timezone=self.karachi_tz
        )
        
        # Set up event listeners
        self.scheduler.add_listener(
            self._job_executed_listener,
            EVENT_JOB_EXECUTED
        )
        self.scheduler.add_listener(
            self._job_error_listener,
            EVENT_JOB_ERROR
        )
        self.scheduler.add_listener(
            self._job_missed_listener,
            EVENT_JOB_MISSED
        )
        
        logger.info("UnifiedSchedulerService initialized with APScheduler")
    
    async def start(self):
        """Start the scheduler service"""
        if self.running:
            logger.warning("UnifiedSchedulerService is already running")
            return
        
        try:
            self.scheduler.start()
            self.running = True
            logger.info("UnifiedSchedulerService started successfully")
            
            # Synchronize existing schedules from database
            await self._sync_schedules_from_database()
            
        except Exception as e:
            logger.error(f"Failed to start UnifiedSchedulerService: {str(e)}")
            raise
    
    async def stop(self):
        """Stop the scheduler service"""
        if not self.running:
            return
        
        try:
            self.scheduler.shutdown(wait=False)
            self.running = False
            logger.info("UnifiedSchedulerService stopped successfully")
            
        except Exception as e:
            logger.error(f"Error stopping UnifiedSchedulerService: {str(e)}")
    
    async def _sync_schedules_from_database(self):
        """Synchronize existing schedules from database to APScheduler"""
        try:
            logger.info("Synchronizing schedules from database...")
            
            # Sync crawl schedules
            crawl_schedules = await CrawlSchedule.find(
                CrawlSchedule.status == CrawlScheduleStatus.ACTIVE
            ).to_list()
            
            for schedule in crawl_schedules:
                await self._add_crawl_schedule_job(schedule)
            
            # Sync deal scrape schedules
            deal_schedules = await DealScrapeSchedule.find(
                DealScrapeSchedule.status == DealScheduleStatus.ACTIVE
            ).to_list()
            
            for schedule in deal_schedules:
                await self._add_deal_schedule_job(schedule)
            
            logger.info(f"Synchronized {len(crawl_schedules)} crawl schedules and {len(deal_schedules)} deal schedules")
            
        except Exception as e:
            logger.error(f"Error synchronizing schedules from database: {str(e)}")
            raise
    
    async def _add_crawl_schedule_job(self, schedule: CrawlSchedule):
        """Add a crawl schedule job to APScheduler"""
        try:
            job_id = f"crawl_{schedule.id}"
            
            # Parse time
            schedule_time = time.fromisoformat(schedule.time_of_day)
            
            # Create cron trigger
            # Map day_of_week to cron day (APScheduler uses 0=Monday, 6=Sunday)
            day_mapping = {
                'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3,
                'friday': 4, 'saturday': 5, 'sunday': 6
            }
            cron_day = day_mapping.get(schedule.day_of_week.lower())
            
            trigger = CronTrigger(
                day_of_week=cron_day,
                hour=schedule_time.hour,
                minute=schedule_time.minute,
                timezone=self.karachi_tz
            )
            
            # Create job data
            job_data = ScheduleJobData(
                schedule_id=str(schedule.id),
                schedule_type=ScheduleType.CRAWL,
                schedule_name=schedule.schedule_name,
                parameters={
                    'base_url': schedule.base_url,
                    'max_links_to_scrape': schedule.max_links_to_scrape,
                    'max_pages_to_scrape': schedule.max_pages_to_scrape,
                    'depth_limit': schedule.depth_limit,
                    'domain_restriction': schedule.domain_restriction,
                    'scrape_pdfs_and_xls': schedule.scrape_pdfs_and_xls
                }
            )
            
            # Add job to scheduler
            self.scheduler.add_job(
                func=self._execute_schedule_job,
                trigger=trigger,
                args=[job_data],
                id=job_id,
                name=f"Crawl: {schedule.base_url}",
                replace_existing=True
            )
            
            logger.info(f"Added crawl schedule job {job_id} for {schedule.base_url}")
            
        except Exception as e:
            logger.error(f"Error adding crawl schedule job for {schedule.id}: {str(e)}")
            raise
    
    async def _add_deal_schedule_job(self, schedule: DealScrapeSchedule):
        """Add a deal scrape schedule job to APScheduler"""
        try:
            job_id = f"deal_{schedule.id}"
            
            # Parse time
            schedule_time = time.fromisoformat(schedule.time_of_day)
            
            # Create cron trigger
            day_mapping = {
                'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3,
                'friday': 4, 'saturday': 5, 'sunday': 6
            }
            cron_day = day_mapping.get(schedule.day_of_week.lower())
            
            trigger = CronTrigger(
                day_of_week=cron_day,
                hour=schedule_time.hour,
                minute=schedule_time.minute,
                timezone=self.karachi_tz
            )
            
            # Create job data
            job_data = ScheduleJobData(
                schedule_id=str(schedule.id),
                schedule_type=ScheduleType.DEAL_SCRAPE,
                schedule_name=schedule.schedule_name,
                parameters={
                    'cities': schedule.cities
                }
            )
            
            # Add job to scheduler
            self.scheduler.add_job(
                func=self._execute_schedule_job,
                trigger=trigger,
                args=[job_data],
                id=job_id,
                name=f"Deal Scrape: {', '.join(schedule.cities)}",
                replace_existing=True
            )
            
            logger.info(f"Added deal scrape schedule job {job_id} for cities: {', '.join(schedule.cities)}")
            
        except Exception as e:
            logger.error(f"Error adding deal scrape schedule job for {schedule.id}: {str(e)}")
            raise
    
    async def _execute_schedule_job(self, job_data: ScheduleJobData):
        """Execute a scheduled job"""
        try:
            logger.info(f"Executing scheduled job: {job_data.schedule_type.value} - {job_data.schedule_id}")
            
            if job_data.schedule_type == ScheduleType.CRAWL:
                await self._execute_crawl_schedule(job_data)
            elif job_data.schedule_type == ScheduleType.DEAL_SCRAPE:
                await self._execute_deal_schedule(job_data)
            else:
                logger.error(f"Unknown schedule type: {job_data.schedule_type}")
                
        except Exception as e:
            logger.error(f"Error executing scheduled job {job_data.schedule_id}: {str(e)}")
            raise
    
    async def _execute_crawl_schedule(self, job_data: ScheduleJobData):
        """Execute a crawl schedule"""
        schedule_id = job_data.schedule_id
        
        try:
            # Get the schedule from database
            schedule = await CrawlSchedule.get(schedule_id)
            if not schedule:
                logger.error(f"Crawl schedule {schedule_id} not found in database")
                return
            
            # Check if schedule is still active
            if schedule.status != CrawlScheduleStatus.ACTIVE:
                logger.info(f"Crawl schedule {schedule_id} is no longer active, skipping")
                return
            
            # Check if URL is already being crawled
            if await self._is_url_being_crawled(schedule.base_url):
                logger.warning(f"URL {schedule.base_url} is already being crawled, skipping scheduled run")
                await self._mark_schedule_skipped(schedule, "URL already being crawled")
                return
            
            # Create task
            task_id = task_manager.create_task(
                task_type="crawl",
                params={
                    "base_url": schedule.base_url,
                    "max_links_to_scrape": schedule.max_links_to_scrape,
                    "max_pages_to_scrape": schedule.max_pages_to_scrape,
                    "depth_limit": schedule.depth_limit,
                    "domain_restriction": schedule.domain_restriction,
                    "scrape_pdfs_and_xls": schedule.scrape_pdfs_and_xls,
                    "stop_scraper": False,
                    "scheduled": True,
                    "schedule_id": schedule_id
                }
            )
            
            # Mark schedule as started
            schedule.mark_run_started(task_id)
            await schedule.save()
            
            logger.info(f"Started scheduled crawl task {task_id} for {schedule.base_url}")
            
            # Execute crawl in background
            asyncio.create_task(self._run_scheduled_crawl(schedule, task_id))
            
        except Exception as e:
            logger.error(f"Error executing crawl schedule {schedule_id}: {str(e)}")
            # Mark schedule as failed
            try:
                schedule = await CrawlSchedule.get(schedule_id)
                if schedule:
                    schedule.mark_run_completed(success=False, error=str(e))
                    await schedule.save()
            except:
                pass
            raise
    
    async def _execute_deal_schedule(self, job_data: ScheduleJobData):
        """Execute a deal scrape schedule"""
        schedule_id = job_data.schedule_id
        
        try:
            # Get the schedule from database
            schedule = await DealScrapeSchedule.get(schedule_id)
            if not schedule:
                logger.error(f"Deal schedule {schedule_id} not found in database")
                return
            
            # Check if schedule is still active
            if schedule.status != DealScheduleStatus.ACTIVE:
                logger.info(f"Deal schedule {schedule_id} is no longer active, skipping")
                return
            
            # Create task
            from app.controllers.restaurant_deal.deal_scrape_controller import DealScrapeController
            task_info = await DealScrapeController.start_deal_scraping(cities=schedule.cities)
            task_id = task_info["task_id"]
            
            # Mark schedule as started
            schedule.mark_run_started(task_id)
            await schedule.save()
            
            logger.info(f"Started scheduled deal scrape task {task_id} for cities: {', '.join(schedule.cities)}")
            
            # Execute deal scraping in background
            asyncio.create_task(self._run_scheduled_deal_scrape(schedule, task_id))
            
        except Exception as e:
            logger.error(f"Error executing deal schedule {schedule_id}: {str(e)}")
            # Mark schedule as failed
            try:
                schedule = await DealScrapeSchedule.get(schedule_id)
                if schedule:
                    schedule.mark_run_completed(success=False, error=str(e))
                    await schedule.save()
            except:
                pass
            raise
    
    async def _run_scheduled_crawl(self, schedule: CrawlSchedule, task_id: str):
        """Run a scheduled crawl task"""
        try:
            from app.utils.orchestrator import orchestrator
            
            result = await orchestrator.run_crawl(
                task_id=task_id,
                base_url=schedule.base_url,
                max_links_to_scrape=schedule.max_links_to_scrape,
                max_pages_to_scrape=schedule.max_pages_to_scrape,
                depth_limit=schedule.depth_limit,
                domain_restriction=schedule.domain_restriction,
                scrape_pdfs_and_xls=schedule.scrape_pdfs_and_xls,
                stop_scraper=False
            )
            
            # Mark schedule as completed
            final_status = result.get("status")
            success = final_status == "completed"
            error_msg = result.get("error") if not success else None
            
            schedule.mark_run_completed(success=success, error=error_msg)
            await schedule.save()
            
            if success:
                logger.info(f"Scheduled crawl completed successfully for {schedule.base_url} (task: {task_id})")
            else:
                logger.warning(f"Scheduled crawl failed for {schedule.base_url} (task: {task_id}): {error_msg}")
                
        except Exception as e:
            error_msg = f"Error running scheduled crawl: {str(e)}"
            logger.error(error_msg)
            
            schedule.mark_run_completed(success=False, error=error_msg)
            await schedule.save()
    
    async def _run_scheduled_deal_scrape(self, schedule: DealScrapeSchedule, task_id: str):
        """Run a scheduled deal scrape task"""
        try:
            from app.utils.orchestrator import orchestrator
            
            result = await orchestrator.run_deal_scraping(
                task_id=task_id,
                cities=schedule.cities
            )
            
            # Mark schedule as completed
            final_status = result.get("status")
            success = final_status == "completed"
            error_msg = result.get("error") if not success else None
            
            schedule.mark_run_completed(success=success, error=error_msg)
            await schedule.save()
            
            if success:
                logger.info(f"Scheduled deal scrape completed successfully for cities {', '.join(schedule.cities)} (task: {task_id})")
            else:
                logger.warning(f"Scheduled deal scrape failed for cities {', '.join(schedule.cities)} (task: {task_id}): {error_msg}")
                
        except Exception as e:
            error_msg = f"Error running scheduled deal scrape: {str(e)}"
            logger.error(error_msg)
            
            schedule.mark_run_completed(success=False, error=error_msg)
            await schedule.save()
    
    async def _is_url_being_crawled(self, base_url: str) -> bool:
        """Check if a URL is currently being crawled"""
        try:
            running_tasks = task_manager.list_tasks(task_type="crawl", status="running")
            
            for task in running_tasks:
                task_params = task.get("params", {})
                if task_params.get("base_url") == base_url:
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking if URL is being crawled: {str(e)}")
            return False
    
    async def _mark_schedule_skipped(self, schedule: Union[CrawlSchedule, DealScrapeSchedule], reason: str):
        """Mark a schedule execution as skipped"""
        try:
            # We don't increment total_runs for skipped executions
            # Just log the skip reason
            logger.info(f"Schedule {schedule.id} skipped: {reason}")
            
            # Optionally, you could add a last_skip_reason field to the models
            # and set it here for tracking purposes
            
        except Exception as e:
            logger.error(f"Error marking schedule as skipped: {str(e)}")
    
    def _job_executed_listener(self, event):
        """Handle job execution events"""
        logger.info(f"Job {event.job_id} executed successfully")
    
    def _job_error_listener(self, event):
        """Handle job error events"""
        logger.error(f"Job {event.job_id} failed with exception: {event.exception}")
    
    def _job_missed_listener(self, event):
        """Handle missed job events"""
        logger.warning(f"Job {event.job_id} missed its scheduled execution time")
    
    # Schedule Management Methods
    
    async def add_crawl_schedule(self, schedule: CrawlSchedule):
        """Add a new crawl schedule to the scheduler"""
        if self.running:
            await self._add_crawl_schedule_job(schedule)
    
    async def add_deal_schedule(self, schedule: DealScrapeSchedule):
        """Add a new deal scrape schedule to the scheduler"""
        if self.running:
            await self._add_deal_schedule_job(schedule)
    
    async def remove_schedule(self, schedule_id: str, schedule_type: ScheduleType):
        """Remove a schedule from the scheduler"""
        try:
            job_id = f"{schedule_type.value}_{schedule_id}"
            
            if self.scheduler.get_job(job_id):
                self.scheduler.remove_job(job_id)
                logger.info(f"Removed schedule job {job_id}")
            else:
                logger.warning(f"Job {job_id} not found in scheduler")
                
        except Exception as e:
            logger.error(f"Error removing schedule {schedule_id}: {str(e)}")
    
    async def update_crawl_schedule(self, schedule: CrawlSchedule):
        """Update an existing crawl schedule"""
        try:
            # Remove old job
            await self.remove_schedule(str(schedule.id), ScheduleType.CRAWL)
            
            # Add updated job if schedule is active
            if schedule.status == CrawlScheduleStatus.ACTIVE:
                await self._add_crawl_schedule_job(schedule)
                
        except Exception as e:
            logger.error(f"Error updating crawl schedule {schedule.id}: {str(e)}")
    
    async def update_deal_schedule(self, schedule: DealScrapeSchedule):
        """Update an existing deal scrape schedule"""
        try:
            # Remove old job
            await self.remove_schedule(str(schedule.id), ScheduleType.DEAL_SCRAPE)
            
            # Add updated job if schedule is active
            if schedule.status == DealScheduleStatus.ACTIVE:
                await self._add_deal_schedule_job(schedule)
                
        except Exception as e:
            logger.error(f"Error updating deal schedule {schedule.id}: {str(e)}")
    
    def get_status(self) -> Dict[str, Any]:
        """Get the current status of the scheduler service"""
        try:
            jobs = self.scheduler.get_jobs() if self.running else []
            job_info = []
            
            for job in jobs:
                next_run = job.next_run_time
                job_info.append({
                    'id': job.id,
                    'name': job.name,
                    'next_run': next_run.isoformat() if next_run else None,
                    'next_run_karachi': next_run.astimezone(self.karachi_tz).strftime('%Y-%m-%d %H:%M:%S') if next_run else None
                })
            
            return {
                "service_name": "unified_scheduler_service",
                "running": self.running,
                "scheduler_state": self.scheduler.state if self.scheduler else "not_initialized",
                "total_jobs": len(jobs),
                "jobs": job_info,
                "timezone": str(self.karachi_tz),
                "current_time_karachi": datetime.now(self.karachi_tz).strftime('%Y-%m-%d %H:%M:%S')
            }
            
        except Exception as e:
            logger.error(f"Error getting scheduler status: {str(e)}")
            return {
                "service_name": "unified_scheduler_service",
                "running": self.running,
                "error": str(e)
            }
    
    async def get_next_scheduled_jobs(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get the next scheduled jobs"""
        try:
            if not self.running:
                return []
            
            jobs = self.scheduler.get_jobs()
            
            # Sort by next run time
            jobs.sort(key=lambda x: x.next_run_time or datetime.max.replace(tzinfo=self.karachi_tz))
            
            next_jobs = []
            for job in jobs[:limit]:
                if job.next_run_time:
                    next_jobs.append({
                        'id': job.id,
                        'name': job.name,
                        'next_run_time': job.next_run_time,
                        'next_run_karachi': job.next_run_time.astimezone(self.karachi_tz).strftime('%Y-%m-%d %H:%M:%S'),
                        'trigger': str(job.trigger)
                    })
            
            return next_jobs
            
        except Exception as e:
            logger.error(f"Error getting next scheduled jobs: {str(e)}")
            return []


scheduler_service = SchedulerService()