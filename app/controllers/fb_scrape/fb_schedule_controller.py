from typing import Optional
from fastapi import HTTPException
import logging

from app.models.database.fb_scrape.fb_schedule_model import FacebookScrapeSchedule, ScheduleStatus
from app.models.fb_scrape.fb_scrape_schedule_model import (
    FBScheduleRequest, 
    FBScheduleResponse, 
    FBScheduleListResponse,
    FBScheduleStatusResponse,
    FBScheduleUpdateRequest
)

logger = logging.getLogger(__name__)


class FBScheduleController:
    
    @staticmethod
    async def create_or_update_schedule(request: FBScheduleRequest) -> FBScheduleResponse:
        try:
            sorted_keywords = sorted([keyword.lower() for keyword in request.keywords])
            existing_schedule = await FacebookScrapeSchedule.find_one(
                FacebookScrapeSchedule.keywords == sorted_keywords
            )
            
            if existing_schedule:
                logger.info(f"Updating existing Facebook schedule for keywords: {sorted_keywords}")
                existing_schedule.schedule_name = request.schedule_name
                existing_schedule.day_of_week = request.day_of_week
                existing_schedule.time_of_day = request.time_of_day
                existing_schedule.days = request.days
                existing_schedule.update_timestamp()

                existing_schedule.next_run_at = existing_schedule.calculate_next_run()

                await existing_schedule.save()
                schedule = existing_schedule
                
                logger.info(f"Updated Facebook schedule {schedule.id} and synced with unified scheduler")
                
            else:
                logger.info(f"Creating new Facebook schedule for keywords: {sorted_keywords}")
                
                schedule = FacebookScrapeSchedule(
                    keywords=request.keywords,
                    days=request.days,
                    schedule_name=request.schedule_name,
                    day_of_week=request.day_of_week,
                    time_of_day=request.time_of_day,
                    status=ScheduleStatus.ACTIVE
                )

                schedule.next_run_at = schedule.calculate_next_run()

                await schedule.save()
                
                logger.info(f"Created Facebook schedule {schedule.id} and synced with unified scheduler")

            return FBScheduleResponse(
                id=str(schedule.id),
                keywords=schedule.keywords,
                days=schedule.days,
                schedule_name=schedule.schedule_name,
                day_of_week=schedule.day_of_week,
                time_of_day=schedule.time_of_day,
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
            logger.error(f"Error creating/updating Facebook schedule: {str(e)}")
            raise HTTPException(
                status_code=500, 
                detail=f"Failed to create/update Facebook schedule: {str(e)}"
            )
    
    @staticmethod
    async def get_schedule(schedule_id: str) -> FBScheduleResponse:
        """
        Get a specific Facebook schedule by ID.
        """
        try:
            schedule = await FacebookScrapeSchedule.get(schedule_id)
            if not schedule:
                raise HTTPException(
                    status_code=404, 
                    detail=f"Facebook schedule {schedule_id} not found"
                )
            
            return FBScheduleResponse(
                id=str(schedule.id),
                keywords=schedule.keywords,
                days=schedule.days,
                schedule_name=schedule.schedule_name,
                day_of_week=schedule.day_of_week,
                time_of_day=schedule.time_of_day,
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
            logger.error(f"Error getting Facebook schedule {schedule_id}: {str(e)}")
            raise HTTPException(
                status_code=500, 
                detail=f"Failed to get Facebook schedule: {str(e)}"
            )
    
    @staticmethod
    async def list_schedules(
        status: Optional[ScheduleStatus] = None,
        limit: int = 50,
        skip: int = 0
    ) -> FBScheduleListResponse:
        try:
            query = {}
            if status:
                query[FacebookScrapeSchedule.status] = status

            schedules = await FacebookScrapeSchedule.find(query).skip(skip).limit(limit).to_list()
            total_count = await FacebookScrapeSchedule.find(query).count()

            schedule_responses = []
            for schedule in schedules:
                schedule_responses.append(FBScheduleResponse(
                    id=str(schedule.id),
                    keywords=schedule.keywords,
                    days=schedule.days,
                    schedule_name=schedule.schedule_name,
                    day_of_week=schedule.day_of_week,
                    time_of_day=schedule.time_of_day,
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
            
            return FBScheduleListResponse(
                schedules=schedule_responses,
                total_count=total_count
            )
            
        except Exception as e:
            logger.error(f"Error listing Facebook schedules: {str(e)}")
            raise HTTPException(
                status_code=500, 
                detail=f"Failed to list Facebook schedules: {str(e)}"
            )
    
    @staticmethod
    async def delete_schedule(schedule_id: str) -> dict:
        try:
            schedule = await FacebookScrapeSchedule.get(schedule_id)
            if not schedule:
                raise HTTPException(
                    status_code=404, 
                    detail=f"Facebook schedule {schedule_id} not found"
                )
            
            keywords_info = ", ".join(schedule.keywords)

            await schedule.delete()
            
            logger.info(f"Deleted Facebook schedule {schedule_id} for keywords: {keywords_info} and removed from unified scheduler")
            
            return {"message": f"Facebook schedule {schedule_id} deleted successfully"}
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error deleting Facebook schedule {schedule_id}: {str(e)}")
            raise HTTPException(
                status_code=500, 
                detail=f"Failed to delete Facebook schedule: {str(e)}"
            )
    
    @staticmethod
    async def get_schedule_status(schedule_id: str) -> FBScheduleStatusResponse:
        try:
            schedule = await FacebookScrapeSchedule.get(schedule_id)
            if not schedule:
                raise HTTPException(
                    status_code=404, 
                    detail=f"Facebook schedule {schedule_id} not found"
                )
            
            return FBScheduleStatusResponse(
                id=str(schedule.id),
                status=schedule.status,
                next_run_at=schedule.next_run_at,
                last_run_at=schedule.last_run_at,
                total_runs=schedule.total_runs,
                successful_runs=schedule.successful_runs,
                failed_runs=schedule.failed_runs,
                last_task_id=schedule.last_task_id,
                last_error=schedule.last_error,
                keywords_count=len(schedule.keywords),
                days=schedule.days
            )
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting Facebook schedule status {schedule_id}: {str(e)}")
            raise HTTPException(
                status_code=500, 
                detail=f"Failed to get Facebook schedule status: {str(e)}"
            )
        
    @staticmethod
    async def pause_schedule(schedule_id: str) -> dict:
        try:
            schedule = await FacebookScrapeSchedule.get(schedule_id)
            if not schedule:
                raise HTTPException(
                    status_code=404, 
                    detail=f"Facebook schedule {schedule_id} not found"
                )
            
            if schedule.status == ScheduleStatus.PAUSED:
                return {"message": f"Facebook schedule {schedule_id} is already paused"}
            
            schedule.status = ScheduleStatus.PAUSED
            schedule.update_timestamp()
            
            await schedule.save()
            
            keywords_info = ", ".join(schedule.keywords)
            logger.info(f"Paused Facebook schedule {schedule_id} for keywords: {keywords_info} and removed from unified scheduler")
            
            return {"message": f"Facebook schedule {schedule_id} paused successfully"}
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error pausing Facebook schedule {schedule_id}: {str(e)}")
            raise HTTPException(
                status_code=500, 
                detail=f"Failed to pause Facebook schedule: {str(e)}"
            )

    @staticmethod
    async def resume_schedule(schedule_id: str) -> dict:
        try:
            schedule = await FacebookScrapeSchedule.get(schedule_id)
            if not schedule:
                raise HTTPException(
                    status_code=404, 
                    detail=f"Facebook schedule {schedule_id} not found"
                )
            
            if schedule.status == ScheduleStatus.ACTIVE:
                return {"message": f"Facebook schedule {schedule_id} is already active"}
            
            schedule.status = ScheduleStatus.ACTIVE
            schedule.next_run_at = schedule.calculate_next_run()
            schedule.update_timestamp()
            
            await schedule.save()
            
            keywords_info = ", ".join(schedule.keywords)
            logger.info(f"Resumed Facebook schedule {schedule_id} for keywords: {keywords_info} and added to unified scheduler")
            
            return {"message": f"Facebook schedule {schedule_id} resumed successfully"}
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error resuming Facebook schedule {schedule_id}: {str(e)}")
            raise HTTPException(
                status_code=500, 
                detail=f"Failed to resume Facebook schedule: {str(e)}"
            )
    
    @staticmethod
    async def update_schedule(schedule_id: str, request: FBScheduleUpdateRequest) -> FBScheduleResponse:
        try:
            schedule = await FacebookScrapeSchedule.get(schedule_id)
            if not schedule:
                raise HTTPException(
                    status_code=404, 
                    detail=f"Facebook schedule {schedule_id} not found"
                )

            timing_changed = False

            if request.keywords is not None:
                schedule.keywords = request.keywords
            if request.days is not None:
                schedule.days = request.days
            if request.schedule_name is not None:
                schedule.schedule_name = request.schedule_name
            if request.day_of_week is not None:
                schedule.day_of_week = request.day_of_week
                timing_changed = True
            if request.time_of_day is not None:
                schedule.time_of_day = request.time_of_day
                timing_changed = True
            if request.status is not None:
                schedule.status = request.status
                timing_changed = True  

            if timing_changed and schedule.status == ScheduleStatus.ACTIVE:
                schedule.next_run_at = schedule.calculate_next_run()
            
            schedule.update_timestamp()

            await schedule.save()
            
            logger.info(f"Updated Facebook schedule {schedule_id} and synced with unified scheduler")
            
            return FBScheduleResponse(
                id=str(schedule.id),
                keywords=schedule.keywords,
                days=schedule.days,
                schedule_name=schedule.schedule_name,
                day_of_week=schedule.day_of_week,
                time_of_day=schedule.time_of_day,
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
            logger.error(f"Error updating Facebook schedule {schedule_id}: {str(e)}")
            raise HTTPException(
                status_code=500, 
                detail=f"Failed to update Facebook schedule: {str(e)}"
            )