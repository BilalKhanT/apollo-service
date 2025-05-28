from typing import Optional
from fastapi import HTTPException
import logging

from app.models.database.restaurant_deal.deal_schedule_model import DealScrapeSchedule, ScheduleStatus
from app.models.restaurant_deal.deal_schedule_model import (
    DealScheduleRequest, 
    DealScheduleResponse, 
    DealScheduleListResponse,
    DealScheduleStatusResponse,
    DealScheduleUpdateRequest
)

logger = logging.getLogger(__name__)


class DealScheduleController:
    
    @staticmethod
    async def create_or_update_schedule(request: DealScheduleRequest) -> DealScheduleResponse:
        try:
            sorted_cities = sorted(request.cities)

            existing_schedule = await DealScrapeSchedule.find_one(
                DealScrapeSchedule.cities == sorted_cities
            )
            
            if existing_schedule:
                logger.info(f"Updating existing deal schedule for cities: {sorted_cities}")

                existing_schedule.schedule_name = request.schedule_name
                existing_schedule.day_of_week = request.day_of_week
                existing_schedule.time_of_day = request.time_of_day
                existing_schedule.update_timestamp()

                existing_schedule.next_run_at = existing_schedule.calculate_next_run()

                await existing_schedule.save()
                schedule = existing_schedule
                
                logger.info(f"Updated deal schedule {schedule.id} and synced with unified scheduler")
                
            else:
                logger.info(f"Creating new deal schedule for cities: {sorted_cities}")
                
                schedule = DealScrapeSchedule(
                    cities=sorted_cities,
                    schedule_name=request.schedule_name,
                    day_of_week=request.day_of_week,
                    time_of_day=request.time_of_day,
                    status=ScheduleStatus.ACTIVE
                )

                schedule.next_run_at = schedule.calculate_next_run()

                await schedule.save()
                
                logger.info(f"Created deal schedule {schedule.id} and synced with unified scheduler")

            return DealScheduleResponse(
                id=str(schedule.id),
                cities=schedule.cities,
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
            logger.error(f"Error creating/updating deal schedule: {str(e)}")
            raise HTTPException(
                status_code=500, 
                detail=f"Failed to create/update deal schedule: {str(e)}"
            )
    
    @staticmethod
    async def get_schedule(schedule_id: str) -> DealScheduleResponse:
        try:
            schedule = await DealScrapeSchedule.get(schedule_id)
            if not schedule:
                raise HTTPException(
                    status_code=404, 
                    detail=f"Deal schedule {schedule_id} not found"
                )
            
            return DealScheduleResponse(
                id=str(schedule.id),
                cities=schedule.cities,
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
            logger.error(f"Error getting deal schedule {schedule_id}: {str(e)}")
            raise HTTPException(
                status_code=500, 
                detail=f"Failed to get deal schedule: {str(e)}"
            )
    
    @staticmethod
    async def list_schedules(
        status: Optional[ScheduleStatus] = None,
        limit: int = 50,
        skip: int = 0
    ) -> DealScheduleListResponse:
        try:
            query = {}
            if status:
                query[DealScrapeSchedule.status] = status

            schedules = await DealScrapeSchedule.find(query).skip(skip).limit(limit).to_list()
            total_count = await DealScrapeSchedule.find(query).count()

            schedule_responses = []
            for schedule in schedules:
                schedule_responses.append(DealScheduleResponse(
                    id=str(schedule.id),
                    cities=schedule.cities,
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
            
            return DealScheduleListResponse(
                schedules=schedule_responses,
                total_count=total_count
            )
            
        except Exception as e:
            logger.error(f"Error listing deal schedules: {str(e)}")
            raise HTTPException(
                status_code=500, 
                detail=f"Failed to list deal schedules: {str(e)}"
            )
    
    @staticmethod
    async def delete_schedule(schedule_id: str) -> dict:
        try:
            schedule = await DealScrapeSchedule.get(schedule_id)
            if not schedule:
                raise HTTPException(
                    status_code=404, 
                    detail=f"Deal schedule {schedule_id} not found"
                )
            
            cities_info = ", ".join(schedule.cities)

            await schedule.delete()
            
            logger.info(f"Deleted deal schedule {schedule_id} for cities: {cities_info} and removed from unified scheduler")
            
            return {"message": f"Deal schedule {schedule_id} deleted successfully"}
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error deleting deal schedule {schedule_id}: {str(e)}")
            raise HTTPException(
                status_code=500, 
                detail=f"Failed to delete deal schedule: {str(e)}"
            )
    
    @staticmethod
    async def get_schedule_status(schedule_id: str) -> DealScheduleStatusResponse:
        try:
            schedule = await DealScrapeSchedule.get(schedule_id)
            if not schedule:
                raise HTTPException(
                    status_code=404, 
                    detail=f"Deal schedule {schedule_id} not found"
                )
            
            return DealScheduleStatusResponse(
                id=str(schedule.id),
                status=schedule.status,
                next_run_at=schedule.next_run_at,
                last_run_at=schedule.last_run_at,
                total_runs=schedule.total_runs,
                successful_runs=schedule.successful_runs,
                failed_runs=schedule.failed_runs,
                last_task_id=schedule.last_task_id,
                last_error=schedule.last_error,
                cities_count=len(schedule.cities)
            )
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting deal schedule status {schedule_id}: {str(e)}")
            raise HTTPException(
                status_code=500, 
                detail=f"Failed to get deal schedule status: {str(e)}"
            )
        
    @staticmethod
    async def pause_schedule(schedule_id: str) -> dict:
        try:
            schedule = await DealScrapeSchedule.get(schedule_id)
            if not schedule:
                raise HTTPException(
                    status_code=404, 
                    detail=f"Deal schedule {schedule_id} not found"
                )
            
            if schedule.status == ScheduleStatus.PAUSED:
                return {"message": f"Deal schedule {schedule_id} is already paused"}
            
            schedule.status = ScheduleStatus.PAUSED
            schedule.update_timestamp()

            await schedule.save()
            
            cities_info = ", ".join(schedule.cities)
            logger.info(f"Paused deal schedule {schedule_id} for cities: {cities_info} and removed from unified scheduler")
            
            return {"message": f"Deal schedule {schedule_id} paused successfully"}
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error pausing deal schedule {schedule_id}: {str(e)}")
            raise HTTPException(
                status_code=500, 
                detail=f"Failed to pause deal schedule: {str(e)}"
            )

    @staticmethod
    async def resume_schedule(schedule_id: str) -> dict:
        try:
            schedule = await DealScrapeSchedule.get(schedule_id)
            if not schedule:
                raise HTTPException(
                    status_code=404, 
                    detail=f"Deal schedule {schedule_id} not found"
                )
            
            if schedule.status == ScheduleStatus.ACTIVE:
                return {"message": f"Deal schedule {schedule_id} is already active"}
            
            schedule.status = ScheduleStatus.ACTIVE
            schedule.next_run_at = schedule.calculate_next_run()
            schedule.update_timestamp()

            await schedule.save()
            
            cities_info = ", ".join(schedule.cities)
            logger.info(f"Resumed deal schedule {schedule_id} for cities: {cities_info} and added to unified scheduler")
            
            return {"message": f"Deal schedule {schedule_id} resumed successfully"}
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error resuming deal schedule {schedule_id}: {str(e)}")
            raise HTTPException(
                status_code=500, 
                detail=f"Failed to resume deal schedule: {str(e)}"
            )
    
    @staticmethod
    async def update_schedule(schedule_id: str, request: DealScheduleUpdateRequest) -> DealScheduleResponse:
        try:
            schedule = await DealScrapeSchedule.get(schedule_id)
            if not schedule:
                raise HTTPException(
                    status_code=404, 
                    detail=f"Deal schedule {schedule_id} not found"
                )

            timing_changed = False

            if request.cities is not None:
                schedule.cities = sorted(request.cities)
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
            
            logger.info(f"Updated deal schedule {schedule_id} and synced with unified scheduler")
            
            return DealScheduleResponse(
                id=str(schedule.id),
                cities=schedule.cities,
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
            logger.error(f"Error updating deal schedule {schedule_id}: {str(e)}")
            raise HTTPException(
                status_code=500, 
                detail=f"Failed to update deal schedule: {str(e)}"
            )