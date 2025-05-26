from typing import Optional
from fastapi import HTTPException
import logging

from app.models.database.crawl_schedule_model import CrawlSchedule, ScheduleStatus
from app.models.database.restaurant_deal.deal_schedule_model import DealScrapeSchedule, ScheduleStatus
from app.models.schedule_model import (
    CrawlScheduleRequest, 
    CrawlScheduleResponse, 
    ScheduleListResponse,
    ScheduleStatusResponse
)

logger = logging.getLogger(__name__)


class ScheduleController:
    
    @staticmethod
    async def create_or_update_schedule(request: CrawlScheduleRequest) -> CrawlScheduleResponse:
        try:
            existing_schedule = await CrawlSchedule.find_one(
                CrawlSchedule.base_url == request.base_url
            )
            
            if existing_schedule:
                logger.info(f"Updating existing schedule for {request.base_url}")
                
                existing_schedule.schedule_name = request.schedule_name
                existing_schedule.day_of_week = request.day_of_week
                existing_schedule.time_of_day = request.time_of_day  # Store as string
                existing_schedule.max_links_to_scrape = request.max_links_to_scrape
                existing_schedule.max_pages_to_scrape = request.max_pages_to_scrape
                existing_schedule.depth_limit = request.depth_limit
                existing_schedule.domain_restriction = request.domain_restriction
                existing_schedule.scrape_pdfs_and_xls = request.scrape_pdfs_and_xls
                existing_schedule.next_run_at = existing_schedule.calculate_next_run()
                existing_schedule.update_timestamp()
                
                await existing_schedule.save()
                schedule = existing_schedule
                
            else:
                # Create new schedule
                logger.info(f"Creating new schedule for {request.base_url}")
                
                schedule = CrawlSchedule(
                    base_url=request.base_url,
                    schedule_name=request.schedule_name,
                    day_of_week=request.day_of_week,
                    time_of_day=request.time_of_day, 
                    max_links_to_scrape=request.max_links_to_scrape,
                    max_pages_to_scrape=request.max_pages_to_scrape,
                    depth_limit=request.depth_limit,
                    domain_restriction=request.domain_restriction,
                    scrape_pdfs_and_xls=request.scrape_pdfs_and_xls,
                    status=ScheduleStatus.ACTIVE
                )

                schedule.next_run_at = schedule.calculate_next_run()
                
                await schedule.insert()

            return CrawlScheduleResponse(
                id=str(schedule.id),
                base_url=schedule.base_url,
                schedule_name=schedule.schedule_name,
                day_of_week=schedule.day_of_week,
                time_of_day=schedule.time_of_day,  # Already a string
                max_links_to_scrape=schedule.max_links_to_scrape,
                max_pages_to_scrape=schedule.max_pages_to_scrape,
                depth_limit=schedule.depth_limit,
                domain_restriction=schedule.domain_restriction,
                scrape_pdfs_and_xls=schedule.scrape_pdfs_and_xls,
                status=schedule.status,
                created_at=schedule.created_at,
                updated_at=schedule.updated_at,
                last_run_at=schedule.last_run_at,
                next_run_at=schedule.next_run_at,
                total_runs=schedule.total_runs,
                successful_runs=schedule.successful_runs,
                failed_runs=schedule.failed_runs,
                last_task_id=schedule.last_task_id,
                last_error=schedule.last_error
            )
            
        except Exception as e:
            logger.error(f"Error creating/updating schedule: {str(e)}")
            raise HTTPException(
                status_code=500, 
                detail=f"Failed to create/update schedule: {str(e)}"
            )
    
    @staticmethod
    async def get_schedule(schedule_id: str) -> CrawlScheduleResponse:
        try:
            schedule = await CrawlSchedule.get(schedule_id)
            if not schedule:
                raise HTTPException(
                    status_code=404, 
                    detail=f"Schedule {schedule_id} not found"
                )
            
            return CrawlScheduleResponse(
                id=str(schedule.id),
                base_url=schedule.base_url,
                schedule_name=schedule.schedule_name,
                day_of_week=schedule.day_of_week,
                time_of_day=schedule.time_of_day,  # Already a string
                max_links_to_scrape=schedule.max_links_to_scrape,
                max_pages_to_scrape=schedule.max_pages_to_scrape,
                depth_limit=schedule.depth_limit,
                domain_restriction=schedule.domain_restriction,
                scrape_pdfs_and_xls=schedule.scrape_pdfs_and_xls,
                status=schedule.status,
                created_at=schedule.created_at,
                updated_at=schedule.updated_at,
                last_run_at=schedule.last_run_at,
                next_run_at=schedule.next_run_at,
                total_runs=schedule.total_runs,
                successful_runs=schedule.successful_runs,
                failed_runs=schedule.failed_runs,
                last_task_id=schedule.last_task_id,
                last_error=schedule.last_error
            )
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting schedule {schedule_id}: {str(e)}")
            raise HTTPException(
                status_code=500, 
                detail=f"Failed to get schedule: {str(e)}"
            )
    
    @staticmethod
    async def list_schedules(
        status: Optional[ScheduleStatus] = None,
        limit: int = 50,
        skip: int = 0
    ) -> ScheduleListResponse:
        try:
            query = {}
            if status:
                query[CrawlSchedule.status] = status

            schedules = await CrawlSchedule.find(query).skip(skip).limit(limit).to_list()
            total_count = await CrawlSchedule.find(query).count()
            schedule_responses = []
            for schedule in schedules:
                schedule_responses.append(CrawlScheduleResponse(
                    id=str(schedule.id),
                    base_url=schedule.base_url,
                    schedule_name=schedule.schedule_name,
                    day_of_week=schedule.day_of_week,
                    time_of_day=schedule.time_of_day,  # Already a string
                    max_links_to_scrape=schedule.max_links_to_scrape,
                    max_pages_to_scrape=schedule.max_pages_to_scrape,
                    depth_limit=schedule.depth_limit,
                    domain_restriction=schedule.domain_restriction,
                    scrape_pdfs_and_xls=schedule.scrape_pdfs_and_xls,
                    status=schedule.status,
                    created_at=schedule.created_at,
                    updated_at=schedule.updated_at,
                    last_run_at=schedule.last_run_at,
                    next_run_at=schedule.next_run_at,
                    total_runs=schedule.total_runs,
                    successful_runs=schedule.successful_runs,
                    failed_runs=schedule.failed_runs,
                    last_task_id=schedule.last_task_id,
                    last_error=schedule.last_error
                ))
            
            return ScheduleListResponse(
                schedules=schedule_responses,
                total_count=total_count
            )
            
        except Exception as e:
            logger.error(f"Error listing schedules: {str(e)}")
            raise HTTPException(
                status_code=500, 
                detail=f"Failed to list schedules: {str(e)}"
            )
    
    @staticmethod
    async def delete_schedule(schedule_id: str) -> dict:
        try:
            schedule = await CrawlSchedule.get(schedule_id)
            if not schedule:
                raise HTTPException(
                    status_code=404, 
                    detail=f"Schedule {schedule_id} not found"
                )
            
            await schedule.delete()
            logger.info(f"Deleted schedule {schedule_id} for {schedule.base_url}")
            
            return {"message": f"Schedule {schedule_id} deleted successfully"}
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error deleting schedule {schedule_id}: {str(e)}")
            raise HTTPException(
                status_code=500, 
                detail=f"Failed to delete schedule: {str(e)}"
            )
    
    @staticmethod
    async def get_schedule_status(schedule_id: str) -> ScheduleStatusResponse:
        try:
            schedule = await CrawlSchedule.get(schedule_id)
            if not schedule:
                raise HTTPException(
                    status_code=404, 
                    detail=f"Schedule {schedule_id} not found"
                )
            
            return ScheduleStatusResponse(
                id=str(schedule.id),
                status=schedule.status,
                next_run_at=schedule.next_run_at,
                last_run_at=schedule.last_run_at,
                total_runs=schedule.total_runs,
                successful_runs=schedule.successful_runs,
                failed_runs=schedule.failed_runs,
                last_task_id=schedule.last_task_id,
                last_error=schedule.last_error
            )
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting schedule status {schedule_id}: {str(e)}")
            raise HTTPException(
                status_code=500, 
                detail=f"Failed to get schedule status: {str(e)}"
            )
        
    @staticmethod
    async def pause_schedule(schedule_id: str) -> dict:
        """Pause a schedule (set status to PAUSED)"""
        try:
            schedule = await CrawlSchedule.get(schedule_id)
            if not schedule:
                raise HTTPException(
                    status_code=404, 
                    detail=f"Schedule {schedule_id} not found"
                )
            
            if schedule.status == ScheduleStatus.PAUSED:
                return {"message": f"Schedule {schedule_id} is already paused"}
            
            schedule.status = ScheduleStatus.PAUSED
            schedule.update_timestamp()
            await schedule.save()
            
            logger.info(f"Paused schedule {schedule_id} for {schedule.base_url}")
            
            return {"message": f"Schedule {schedule_id} paused successfully"}
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error pausing schedule {schedule_id}: {str(e)}")
            raise HTTPException(
                status_code=500, 
                detail=f"Failed to pause schedule: {str(e)}"
            )

    @staticmethod
    async def resume_schedule(schedule_id: str) -> dict:
        """Resume a paused schedule (set status to ACTIVE)"""
        try:
            schedule = await CrawlSchedule.get(schedule_id)
            if not schedule:
                raise HTTPException(
                    status_code=404, 
                    detail=f"Schedule {schedule_id} not found"
                )
            
            if schedule.status == ScheduleStatus.ACTIVE:
                return {"message": f"Schedule {schedule_id} is already active"}
            
            schedule.status = ScheduleStatus.ACTIVE
            schedule.next_run_at = schedule.calculate_next_run()
            schedule.update_timestamp()
            await schedule.save()
            
            logger.info(f"Resumed schedule {schedule_id} for {schedule.base_url}")
            
            return {"message": f"Schedule {schedule_id} resumed successfully"}
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error resuming schedule {schedule_id}: {str(e)}")
            raise HTTPException(
                status_code=500, 
                detail=f"Failed to resume schedule: {str(e)}"
            )