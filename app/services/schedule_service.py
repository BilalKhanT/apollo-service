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
from app.models.database.fb_scrape.fb_schedule_model import FacebookScrapeSchedule, ScheduleStatus as FacebookScheduleStatus
from app.utils.task_manager import task_manager

logger = logging.getLogger(__name__)


class ScheduleType(str, Enum):
    CRAWL = "crawl"
    DEAL_SCRAPE = "deal_scrape"
    FACEBOOK_SCRAPE = "facebook_scrape"  


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
        
        jobstores = {
            'default': MemoryJobStore()
        }
        executors = {
            'default': AsyncIOExecutor()
        }
        job_defaults = {
            'coalesce': True,  
            'max_instances': 1,  
            'misfire_grace_time': 60  
        }
        
        self.scheduler = AsyncIOScheduler(
            jobstores=jobstores,
            executors=executors,
            job_defaults=job_defaults,
            timezone=self.karachi_tz
        )
        
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
        
        logger.info("SchedulerService initialized")
    
    async def start(self):
        if self.running:
            logger.warning("SchedulerService is already running")
            return
        
        try:
            self.scheduler.start()
            self.running = True
            logger.info("SchedulerService started successfully")
            await self._sync_schedules_from_database()
            
        except Exception as e:
            logger.error(f"Failed to start SchedulerService: {str(e)}")
            raise
    
    async def stop(self):
        if not self.running:
            return
        
        try:
            self.scheduler.shutdown(wait=False)
            self.running = False
            logger.info("SchedulerService stopped successfully")
            
        except Exception as e:
            logger.error(f"Error stopping SchedulerService: {str(e)}")
    
    async def _sync_schedules_from_database(self):
        try:
            logger.info("Synchronizing schedules from database...")
            
            crawl_schedules = await CrawlSchedule.find(
                CrawlSchedule.status == CrawlScheduleStatus.ACTIVE
            ).to_list()
            
            for schedule in crawl_schedules:
                await self._add_crawl_schedule_job(schedule)
            
            deal_schedules = await DealScrapeSchedule.find(
                DealScrapeSchedule.status == DealScheduleStatus.ACTIVE
            ).to_list()
            
            for schedule in deal_schedules:
                await self._add_deal_schedule_job(schedule)
            
            facebook_schedules = await FacebookScrapeSchedule.find(
                FacebookScrapeSchedule.status == FacebookScheduleStatus.ACTIVE
            ).to_list()
            
            for schedule in facebook_schedules:
                await self._add_facebook_schedule_job(schedule)
            
            logger.info(f"Synchronized {len(crawl_schedules)} crawl schedules, {len(deal_schedules)} deal schedules, and {len(facebook_schedules)} Facebook schedules")
            
        except Exception as e:
            logger.error(f"Error synchronizing schedules from database: {str(e)}")
            raise
    
    async def _add_crawl_schedule_job(self, schedule: CrawlSchedule):
        try:
            job_id = f"crawl_{schedule.id}"
            schedule_time = time.fromisoformat(schedule.time_of_day)
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
        try:
            job_id = f"deal_{schedule.id}"
            schedule_time = time.fromisoformat(schedule.time_of_day)
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

            job_data = ScheduleJobData(
                schedule_id=str(schedule.id),
                schedule_type=ScheduleType.DEAL_SCRAPE,
                schedule_name=schedule.schedule_name,
                parameters={
                    'cities': schedule.cities
                }
            )

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
    
    async def _add_facebook_schedule_job(self, schedule: FacebookScrapeSchedule):
        try:
            job_id = f"facebook_{schedule.id}"
            schedule_time = time.fromisoformat(schedule.time_of_day)
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
            
            job_data = ScheduleJobData(
                schedule_id=str(schedule.id),
                schedule_type=ScheduleType.FACEBOOK_SCRAPE,
                schedule_name=schedule.schedule_name,
                parameters={
                    'keywords': schedule.keywords,
                    'days': schedule.days,
                    'access_token': "EAANwmjYfSZAMBO9my96Ipmky8pZCHEkDOu5eXZAaHc7ge2LZCZCsZBz7yoj7O5mfZCHlTLVey0RbZBIUgQTpkqH7goqwQLTw0kWAw4GMaiOh36qIh3jYDX6KYfOqMBVjZBChSlCLNmljS4dswIB9sZCNvQZCXZC3xlMJ9FLDLUyT0dzd9XQBG5nHv4FPSW5hkr8Kt9eO",
                    'page_id': "185182871519466"
                }
            )
            
            self.scheduler.add_job(
                func=self._execute_schedule_job,
                trigger=trigger,
                args=[job_data],
                id=job_id,
                name=f"Facebook Scrape: {schedule.get_keywords_display()}",
                replace_existing=True
            )
            
            logger.info(f"Added Facebook scrape schedule job {job_id} for keywords: {schedule.get_keywords_display()}")
            
        except Exception as e:
            logger.error(f"Error adding Facebook scrape schedule job for {schedule.id}: {str(e)}")
            raise
    
    async def _execute_schedule_job(self, job_data: ScheduleJobData):
        try:
            logger.info(f"Executing scheduled job: {job_data.schedule_type.value} - {job_data.schedule_id}")
            
            if job_data.schedule_type == ScheduleType.CRAWL:
                await self._execute_crawl_schedule(job_data)
            elif job_data.schedule_type == ScheduleType.DEAL_SCRAPE:
                await self._execute_deal_schedule(job_data)
            elif job_data.schedule_type == ScheduleType.FACEBOOK_SCRAPE:  # ADDED
                await self._execute_facebook_schedule(job_data)
            else:
                logger.error(f"Unknown schedule type: {job_data.schedule_type}")
                
        except Exception as e:
            logger.error(f"Error executing scheduled job {job_data.schedule_id}: {str(e)}")
            raise
    
    async def _execute_crawl_schedule(self, job_data: ScheduleJobData):
        schedule_id = job_data.schedule_id
        
        try:
            schedule = await CrawlSchedule.get(schedule_id)
            if not schedule:
                logger.error(f"Crawl schedule {schedule_id} not found in database")
                return

            if schedule.status != CrawlScheduleStatus.ACTIVE:
                logger.info(f"Crawl schedule {schedule_id} is no longer active, skipping")
                return

            if await self._is_url_being_crawled(schedule.base_url):
                logger.warning(f"URL {schedule.base_url} is already being crawled, skipping scheduled run")
                await self._mark_schedule_skipped(schedule, "URL already being crawled")
                return

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

            schedule.mark_run_started(task_id)
            await schedule.save()
            
            logger.info(f"Started scheduled crawl task {task_id} for {schedule.base_url}")

            asyncio.create_task(self._run_scheduled_crawl(schedule, task_id))
            
        except Exception as e:
            logger.error(f"Error executing crawl schedule {schedule_id}: {str(e)}")
            try:
                schedule = await CrawlSchedule.get(schedule_id)
                if schedule:
                    schedule.mark_run_completed(success=False, error=str(e))
                    await schedule.save()
            except:
                pass
            raise
    
    async def _execute_deal_schedule(self, job_data: ScheduleJobData):
        schedule_id = job_data.schedule_id
        
        try:
            schedule = await DealScrapeSchedule.get(schedule_id)
            if not schedule:
                logger.error(f"Deal schedule {schedule_id} not found in database")
                return

            if schedule.status != DealScheduleStatus.ACTIVE:
                logger.info(f"Deal schedule {schedule_id} is no longer active, skipping")
                return

            from app.controllers.restaurant_deal.deal_scrape_controller import DealScrapeController
            task_info = await DealScrapeController.start_deal_scraping(cities=schedule.cities)
            task_id = task_info["task_id"]

            schedule.mark_run_started(task_id)
            await schedule.save()
            
            logger.info(f"Started scheduled deal scrape task {task_id} for cities: {', '.join(schedule.cities)}")

            asyncio.create_task(self._run_scheduled_deal_scrape(schedule, task_id))
            
        except Exception as e:
            logger.error(f"Error executing deal schedule {schedule_id}: {str(e)}")
            try:
                schedule = await DealScrapeSchedule.get(schedule_id)
                if schedule:
                    schedule.mark_run_completed(success=False, error=str(e))
                    await schedule.save()
            except:
                pass
            raise
    
    async def _execute_facebook_schedule(self, job_data: ScheduleJobData):
        schedule_id = job_data.schedule_id
        
        try:
            schedule = await FacebookScrapeSchedule.get(schedule_id)
            if not schedule:
                logger.error(f"Facebook schedule {schedule_id} not found in database")
                return
            
            if schedule.status != FacebookScheduleStatus.ACTIVE:
                logger.info(f"Facebook schedule {schedule_id} is no longer active, skipping")
                return
            
            if await self._is_facebook_scraping_running():
                logger.warning(f"Facebook scraping is already running, skipping scheduled run")
                await self._mark_schedule_skipped(schedule, "Facebook scraping already running")
                return
            
            from app.controllers.fb_scrape.fb_scrape_controller import FacebookScrapeController
            task_info = await FacebookScrapeController.start_facebook_scraping(
                keywords=schedule.keywords,
                days=schedule.days,
                access_token=job_data.parameters['access_token'],
                page_id=job_data.parameters['page_id']
            )
            task_id = task_info["task_id"]
            
            schedule.mark_run_started(task_id)
            await schedule.save()
            
            logger.info(f"Started scheduled Facebook scrape task {task_id} for keywords: {schedule.get_keywords_display()}")
            
            asyncio.create_task(self._run_scheduled_facebook_scrape(schedule, task_id))
            
        except Exception as e:
            logger.error(f"Error executing Facebook schedule {schedule_id}: {str(e)}")
            try:
                schedule = await FacebookScrapeSchedule.get(schedule_id)
                if schedule:
                    schedule.mark_run_completed(success=False, error=str(e))
                    await schedule.save()
            except:
                pass
            raise
    
    async def _run_scheduled_crawl(self, schedule: CrawlSchedule, task_id: str):
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
        try:
            from app.utils.orchestrator import orchestrator
            
            result = await orchestrator.run_deal_scraping(
                task_id=task_id,
                cities=schedule.cities
            )
            
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
    
    async def _run_scheduled_facebook_scrape(self, schedule: FacebookScrapeSchedule, task_id: str):
        try:
            from app.utils.orchestrator import orchestrator
            
            result = await orchestrator.run_facebook_scraping(
                task_id=task_id,
                keywords=schedule.keywords,
                days=schedule.days,
                access_token="EAANwmjYfSZAMBO9my96Ipmky8pZCHEkDOu5eXZAaHc7ge2LZCZCsZBz7yoj7O5mfZCHlTLVey0RbZBIUgQTpkqH7goqwQLTw0kWAw4GMaiOh36qIh3jYDX6KYfOqMBVjZBChSlCLNmljS4dswIB9sZCNvQZCXZC3xlMJ9FLDLUyT0dzd9XQBG5nHv4FPSW5hkr8Kt9eO",
                page_id="185182871519466"
            )
            
            final_status = result.get("status")
            success = final_status == "completed"
            error_msg = result.get("error") if not success else None
            
            schedule.mark_run_completed(success=success, error=error_msg)
            await schedule.save()
            
            if success:
                logger.info(f"Scheduled Facebook scrape completed successfully for keywords {schedule.get_keywords_display()} (task: {task_id})")
            else:
                logger.warning(f"Scheduled Facebook scrape failed for keywords {schedule.get_keywords_display()} (task: {task_id}): {error_msg}")
                
        except Exception as e:
            error_msg = f"Error running scheduled Facebook scrape: {str(e)}"
            logger.error(error_msg)
            
            schedule.mark_run_completed(success=False, error=error_msg)
            await schedule.save()
    
    async def _is_url_being_crawled(self, base_url: str) -> bool:
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
    
    async def _is_facebook_scraping_running(self) -> bool:
        try:
            running_tasks = task_manager.list_tasks(task_type="facebook_scraping", status="running")
            return len(running_tasks) > 0
            
        except Exception as e:
            logger.error(f"Error checking if Facebook scraping is running: {str(e)}")
            return False
    
    async def _mark_schedule_skipped(self, schedule: Union[CrawlSchedule, DealScrapeSchedule, FacebookScrapeSchedule], reason: str):
        try:
            logger.info(f"Schedule {schedule.id} skipped: {reason}") 
        except Exception as e:
            logger.error(f"Error marking schedule as skipped: {str(e)}")
    
    def _job_executed_listener(self, event):
        logger.info(f"Job {event.job_id} executed successfully")
    
    def _job_error_listener(self, event):
        logger.error(f"Job {event.job_id} failed with exception: {event.exception}")
    
    def _job_missed_listener(self, event):
        logger.warning(f"Job {event.job_id} missed its scheduled execution time")
    
    async def add_crawl_schedule(self, schedule: CrawlSchedule):
        if self.running:
            await self._add_crawl_schedule_job(schedule)
    
    async def add_deal_schedule(self, schedule: DealScrapeSchedule):
        if self.running:
            await self._add_deal_schedule_job(schedule)
    
    async def add_facebook_schedule(self, schedule: FacebookScrapeSchedule):
        if self.running:
            await self._add_facebook_schedule_job(schedule)
    
    async def remove_schedule(self, schedule_id: str, schedule_type: ScheduleType):
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
        try:
            await self.remove_schedule(str(schedule.id), ScheduleType.CRAWL)
            if schedule.status == CrawlScheduleStatus.ACTIVE:
                await self._add_crawl_schedule_job(schedule)
                
        except Exception as e:
            logger.error(f"Error updating crawl schedule {schedule.id}: {str(e)}")
    
    async def update_deal_schedule(self, schedule: DealScrapeSchedule):
        try:
            await self.remove_schedule(str(schedule.id), ScheduleType.DEAL_SCRAPE)

            if schedule.status == DealScheduleStatus.ACTIVE:
                await self._add_deal_schedule_job(schedule)
                
        except Exception as e:
            logger.error(f"Error updating deal schedule {schedule.id}: {str(e)}")
    
    async def update_facebook_schedule(self, schedule: FacebookScrapeSchedule):
        try:
            await self.remove_schedule(str(schedule.id), ScheduleType.FACEBOOK_SCRAPE)
            if schedule.status == FacebookScheduleStatus.ACTIVE:
                await self._add_facebook_schedule_job(schedule)
                
        except Exception as e:
            logger.error(f"Error updating Facebook schedule {schedule.id}: {str(e)}")
    
    def get_status(self) -> Dict[str, Any]:
        try:
            jobs = self.scheduler.get_jobs() if self.running else []
            job_info = []

            job_counts = {
                'crawl': 0,
                'deal': 0,
                'facebook': 0  
            }
            
            for job in jobs:
                next_run = job.next_run_time
                job_info.append({
                    'id': job.id,
                    'name': job.name,
                    'type': job.id.split('_')[0], 
                    'next_run': next_run.isoformat() if next_run else None,
                    'next_run_karachi': next_run.astimezone(self.karachi_tz).strftime('%Y-%m-%d %H:%M:%S') if next_run else None
                })

                if job.id.startswith('crawl_'):
                    job_counts['crawl'] += 1
                elif job.id.startswith('deal_'):
                    job_counts['deal'] += 1
                elif job.id.startswith('facebook_'): 
                    job_counts['facebook'] += 1
            
            return {
                "service_name": "unified_scheduler_service",
                "running": self.running,
                "scheduler_state": self.scheduler.state if self.scheduler else "not_initialized",
                "total_jobs": len(jobs),
                "job_counts": job_counts,  
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
        try:
            if not self.running:
                return []
            
            jobs = self.scheduler.get_jobs()

            jobs.sort(key=lambda x: x.next_run_time or datetime.max.replace(tzinfo=self.karachi_tz))
            
            next_jobs = []
            for job in jobs[:limit]:
                if job.next_run_time:
                    next_jobs.append({
                        'id': job.id,
                        'name': job.name,
                        'type': job.id.split('_')[0],  
                        'next_run_time': job.next_run_time,
                        'next_run_karachi': job.next_run_time.astimezone(self.karachi_tz).strftime('%Y-%m-%d %H:%M:%S'),
                        'trigger': str(job.trigger)
                    })
            
            return next_jobs
            
        except Exception as e:
            logger.error(f"Error getting next scheduled jobs: {str(e)}")
            return []


scheduler_service = SchedulerService()