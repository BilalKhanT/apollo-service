import asyncio
import logging
from datetime import datetime
from typing import List, Optional
from pytz import timezone

from app.controllers.restaurant_deal.deal_scrape_controller import DealScrapeController
from app.models.database.restaurant_deal.deal_schedule_model import DealScrapeSchedule, ScheduleStatus
from app.models.restaurant_deal.restaurant_model import DealScrapingRequest
from app.utils.socket_manager import socket_manager

logger = logging.getLogger(__name__)

class DealScheduleService:
    def __init__(self):
        self.is_running = False
        self.check_interval = 30
        self.task: Optional[asyncio.Task] = None
        self.karachi_tz = timezone('Asia/Karachi')
        
    async def start(self):
        """Start the deal schedule monitoring service."""
        if not self.is_running:
            self.is_running = True
            self.task = asyncio.create_task(self._monitor_schedules())
            logger.info("Deal schedule service started")
            
            # Notify via WebSocket
            await socket_manager.broadcast_server_message(
                "Deal schedule monitoring service started",
                "info"
            )
        else:
            logger.warning("Deal schedule service is already running")
    
    async def stop(self):
        """Stop the deal schedule monitoring service."""
        if self.is_running:
            self.is_running = False
            if self.task:
                self.task.cancel()
                try:
                    await self.task
                except asyncio.CancelledError:
                    pass
            logger.info("Deal schedule service stopped")
            
            # Notify via WebSocket
            await socket_manager.broadcast_server_message(
                "Deal schedule monitoring service stopped",
                "warning"
            )
        else:
            logger.warning("Deal schedule service is not running")
    
    async def _monitor_schedules(self):
        """Main monitoring loop that checks for due schedules."""
        logger.info("Starting deal schedule monitoring loop")
        
        while self.is_running:
            try:
                await self._check_and_execute_due_schedules()
                await asyncio.sleep(self.check_interval)
                
            except asyncio.CancelledError:
                logger.info("Deal schedule monitoring loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in deal schedule monitoring loop: {str(e)}")
                await asyncio.sleep(self.check_interval)
    
    async def _check_and_execute_due_schedules(self):
        """Check for schedules that are due and execute them."""
        try:
            # Get current time in Karachi timezone
            now = datetime.now(self.karachi_tz)
            
            # Find active schedules that are due to run
            due_schedules = await DealScrapeSchedule.find(
                DealScrapeSchedule.status == ScheduleStatus.ACTIVE,
                DealScrapeSchedule.next_run_at <= now.replace(tzinfo=None)
            ).to_list()
            
            if due_schedules:
                logger.info(f"Found {len(due_schedules)} due deal schedules")
                
                for schedule in due_schedules:
                    try:
                        await self._execute_schedule(schedule)
                    except Exception as e:
                        logger.error(f"Failed to execute schedule {schedule.id}: {str(e)}")
                        await self._update_schedule_failure(schedule, str(e))
            else:
                # Log every 10 minutes to show the service is active
                if now.minute % 10 == 0:
                    logger.debug("No due deal schedules found")
                    
        except Exception as e:
            logger.error(f"Error checking due schedules: {str(e)}")
    
    async def _execute_schedule(self, schedule: DealScrapeSchedule):
        """Execute a specific deal schedule."""
        from app.utils.orchestrator import orchestrator
        try:
            logger.info(f"Executing deal schedule {schedule.id} for cities: {', '.join(schedule.cities)}")
            
            # Notify via WebSocket
            await socket_manager.broadcast_server_message(
                f"Starting scheduled deal scraping for cities: {', '.join(schedule.cities)}",
                "info"
            )
            
            task_info = await DealScrapeController.start_deal_scraping(cities=schedule.cities)
            
            # Execute the deal scraping
            result = await orchestrator.run_deal_scraping(task_id=task_info["task_id"], cities=schedule.cities)
            
            # Update schedule with success
            await self._update_schedule_success(schedule, result.task_id)
            
            logger.info(f"Successfully executed deal schedule {schedule.id}, task_id: {result.task_id}")
            
            # Notify via WebSocket
            await socket_manager.broadcast_server_message(
                f"Scheduled deal scraping completed successfully. Task ID: {result.task_id}",
                "success"
            )
            
        except Exception as e:
            logger.error(f"Error executing deal schedule {schedule.id}: {str(e)}")
            await self._update_schedule_failure(schedule, str(e))
            
            # Notify via WebSocket
            await socket_manager.broadcast_server_message(
                f"Scheduled deal scraping failed: {str(e)}",
                "error"
            )
            
            raise
    
    async def _update_schedule_success(self, schedule: DealScrapeSchedule, task_id: str):
        """Update schedule after successful execution."""
        try:
            now = datetime.now(self.karachi_tz).replace(tzinfo=None)
            
            # Update execution statistics
            schedule.last_run_at = now
            schedule.last_task_id = task_id
            schedule.last_error = None
            schedule.total_runs += 1
            schedule.successful_runs += 1
            
            # Calculate next run time
            schedule.next_run_at = schedule.calculate_next_run()
            schedule.update_timestamp()
            
            await schedule.save()
            
            logger.info(f"Updated schedule {schedule.id} after successful execution. Next run: {schedule.next_run_at}")
            
        except Exception as e:
            logger.error(f"Error updating schedule {schedule.id} after success: {str(e)}")
    
    async def _update_schedule_failure(self, schedule: DealScrapeSchedule, error_message: str):
        """Update schedule after failed execution."""
        try:
            now = datetime.now(self.karachi_tz).replace(tzinfo=None)
            
            # Update execution statistics
            schedule.last_run_at = now
            schedule.last_error = error_message[:500]  # Limit error message length
            schedule.total_runs += 1
            schedule.failed_runs += 1
            
            # Calculate next run time (still schedule next run even after failure)
            schedule.next_run_at = schedule.calculate_next_run()
            schedule.update_timestamp()
            
            await schedule.save()
            
            logger.warning(f"Updated schedule {schedule.id} after failed execution. Next run: {schedule.next_run_at}")
            
        except Exception as e:
            logger.error(f"Error updating schedule {schedule.id} after failure: {str(e)}")
    
    async def trigger_schedule_now(self, schedule_id: str) -> dict:
        """Manually trigger a schedule to run immediately."""
        try:
            schedule = await DealScrapeSchedule.get(schedule_id)
            if not schedule:
                raise ValueError(f"Schedule {schedule_id} not found")
            
            if schedule.status != ScheduleStatus.ACTIVE:
                raise ValueError(f"Schedule {schedule_id} is not active (status: {schedule.status})")
            
            logger.info(f"Manually triggering deal schedule {schedule_id}")
            
            await self._execute_schedule(schedule)
            
            return {
                "message": f"Schedule {schedule_id} triggered successfully",
                "task_id": schedule.last_task_id
            }
            
        except Exception as e:
            logger.error(f"Error manually triggering schedule {schedule_id}: {str(e)}")
            raise
    
    async def get_next_scheduled_runs(self, limit: int = 10) -> List[dict]:
        """Get the next scheduled runs for monitoring purposes."""
        try:
            schedules = await DealScrapeSchedule.find(
                DealScrapeSchedule.status == ScheduleStatus.ACTIVE,
                DealScrapeSchedule.next_run_at != None
            ).sort("next_run_at").limit(limit).to_list()
            
            next_runs = []
            for schedule in schedules:
                next_runs.append({
                    "schedule_id": str(schedule.id),
                    "schedule_name": schedule.schedule_name,
                    "cities": schedule.cities,
                    "next_run_at": schedule.next_run_at,
                    "day_of_week": schedule.day_of_week,
                    "time_of_day": schedule.time_of_day
                })
            
            return next_runs
            
        except Exception as e:
            logger.error(f"Error getting next scheduled runs: {str(e)}")
            return []
    
    def get_status(self) -> dict:
        """Get the current status of the deal schedule service."""
        return {
            "service_name": "deal_schedule_service",
            "is_running": self.is_running,
            "check_interval_seconds": self.check_interval,
            "timezone": str(self.karachi_tz),
            "current_time": datetime.now(self.karachi_tz).isoformat()
        }

# Global instance
deal_schedule_service = DealScheduleService()