import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any

from app.models.database.crawl_schedule_model import CrawlSchedule, ScheduleStatus
from app.utils.task_manager import task_manager

logger = logging.getLogger(__name__)


class ScheduleService: 
    def __init__(self):
        self.running = False
        self.check_interval = 30  
        self.running_schedules = set()  
        self.background_task = None
        
    async def start(self):
        if self.running:
            logger.warning("Schedule service is already running")
            return
            
        self.running = True
        logger.info("Starting schedule service...")

        self.background_task = asyncio.create_task(self._schedule_loop())
        
    async def stop(self):
        if not self.running:
            return
            
        logger.info("Stopping schedule service...")
        self.running = False
        
        if self.background_task:
            self.background_task.cancel()
            try:
                await self.background_task
            except asyncio.CancelledError:
                pass
        
        logger.info("Schedule service stopped")
    
    async def _schedule_loop(self):
        logger.info(f"Schedule service started. Checking every {self.check_interval} seconds.")
        
        while self.running:
            try:
                await self._check_and_run_due_schedules()
                await asyncio.sleep(self.check_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in schedule loop: {str(e)}")
                await asyncio.sleep(self.check_interval)
    
    async def _check_and_run_due_schedules(self):
        try:
            current_time = datetime.utcnow()
            due_schedules = await CrawlSchedule.find(
                CrawlSchedule.status == ScheduleStatus.ACTIVE,
                CrawlSchedule.next_run_at <= current_time
            ).to_list()
            
            if not due_schedules:
                logger.debug("No schedules due for execution")
                return
            
            logger.info(f"Found {len(due_schedules)} schedules due for execution")
            
            for schedule in due_schedules:
                if str(schedule.id) in self.running_schedules:
                    logger.warning(f"Schedule {schedule.id} is already running, skipping")
                    continue

                if await self._is_url_being_crawled(schedule.base_url):
                    logger.warning(f"URL {schedule.base_url} is already being crawled, skipping scheduled run")
                    schedule.next_run_at = schedule.calculate_next_run()
                    await schedule.save()
                    continue

                await self._execute_schedule(schedule)
                
        except Exception as e:
            logger.error(f"Error checking due schedules: {str(e)}")
    
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
    
    async def _execute_schedule(self, schedule: CrawlSchedule):
        schedule_id = str(schedule.id)
        try:
            logger.info(f"Executing scheduled crawl for {schedule.base_url} (schedule: {schedule_id})")
            self.running_schedules.add(schedule_id)
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

            asyncio.create_task(
                self._run_scheduled_crawl(schedule, task_id)
            )
            
            logger.info(f"Started scheduled crawl task {task_id} for {schedule.base_url}")
            
        except Exception as e:
            error_msg = f"Error executing schedule {schedule_id}: {str(e)}"
            logger.error(error_msg)
            schedule.mark_run_completed(success=False, error=error_msg)
            await schedule.save()
            self.running_schedules.discard(schedule_id)
    
    async def _run_scheduled_crawl(self, schedule: CrawlSchedule, task_id: str):
        schedule_id = str(schedule.id)
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
            
        finally:
            self.running_schedules.discard(schedule_id)
    
    async def get_next_due_schedules(self, limit: int = 10) -> List[Dict[str, Any]]:
        try:
            current_time = datetime.utcnow()
            next_24_hours = current_time + timedelta(hours=24)
            
            schedules = await CrawlSchedule.find(
                CrawlSchedule.status == ScheduleStatus.ACTIVE,
                CrawlSchedule.next_run_at >= current_time,
                CrawlSchedule.next_run_at <= next_24_hours
            ).sort(CrawlSchedule.next_run_at).limit(limit).to_list()
            
            result = []
            for schedule in schedules:
                result.append({
                    "id": str(schedule.id),
                    "base_url": schedule.base_url,
                    "schedule_name": schedule.schedule_name,
                    "next_run_at": schedule.next_run_at,
                    "day_of_week": schedule.day_of_week,
                    "time_of_day": schedule.time_of_day 
                })
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting next due schedules: {str(e)}")
            return []
    
    def get_status(self) -> Dict[str, Any]:
        return {
            "running": self.running,
            "check_interval_seconds": self.check_interval,
            "currently_running_schedules": len(self.running_schedules),
            "running_schedule_ids": list(self.running_schedules)
        }
    
schedule_service = ScheduleService()