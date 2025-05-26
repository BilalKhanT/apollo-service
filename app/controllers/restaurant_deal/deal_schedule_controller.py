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
        """
        Create a new deal schedule or update an existing one for the same cities combination.
        """
        try:
            # Sort cities for consistent comparison
            sorted_cities = sorted(request.cities)
            
            # Check for existing schedule with the same cities
            existing_schedule = await DealScrapeSchedule.find_one(
                DealScrapeSchedule.cities == sorted_cities
            )
            
            if existing_schedule:
                logger.info(f"Updating existing deal schedule for cities: {sorted_cities}")
                
                # Update existing schedule
                existing_schedule.schedule_name = request.schedule_name
                existing_schedule.day_of_week = request.day_of_week
                existing_schedule.time_of_day = request.time_of_day
                existing_schedule.next_run_at = existing_schedule.calculate_next_run()
                existing_schedule.update_timestamp()
                
                await existing_schedule.save()
                schedule = existing_schedule
                
            else:
                # Create new schedule
                logger.info(f"Creating new deal schedule for cities: {sorted_cities}")
                
                schedule = DealScrapeSchedule(
                    cities=sorted_cities,
                    schedule_name=request.schedule_name,
                    day_of_week=request.day_of_week,
                    time_of_day=request.time_of_day,
                    status=ScheduleStatus.ACTIVE
                )

                schedule.next_run_at = schedule.calculate_next_run()
                
                await schedule.insert()

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
        """
        Get a specific deal schedule by ID.
        """
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
        """
        List all deal schedules with optional filtering.
        """
        try:
            # Build query
            query = {}
            if status:
                query[DealScrapeSchedule.status] = status

            # Get schedules with pagination
            schedules = await DealScrapeSchedule.find(query).skip(skip).limit(limit).to_list()
            total_count = await DealScrapeSchedule.find(query).count()
            
            # Convert to response format
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
        """
        Delete a deal schedule by ID.
        """
        try:
            schedule = await DealScrapeSchedule.get(schedule_id)
            if not schedule:
                raise HTTPException(
                    status_code=404, 
                    detail=f"Deal schedule {schedule_id} not found"
                )
            
            cities_info = ", ".join(schedule.cities)
            await schedule.delete()
            logger.info(f"Deleted deal schedule {schedule_id} for cities: {cities_info}")
            
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
        """
        Get the status of a specific deal schedule.
        """
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
        """
        Pause a deal schedule (set status to PAUSED).
        """
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
            logger.info(f"Paused deal schedule {schedule_id} for cities: {cities_info}")
            
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
        """
        Resume a paused deal schedule (set status to ACTIVE).
        """
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
            logger.info(f"Resumed deal schedule {schedule_id} for cities: {cities_info}")
            
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
    async def update_schedule(schedule_id: str, request: 'DealScheduleUpdateRequest') -> DealScheduleResponse:
        """
        Update an existing deal schedule with partial data.
        """
        try:
            schedule = await DealScrapeSchedule.get(schedule_id)
            if not schedule:
                raise HTTPException(
                    status_code=404, 
                    detail=f"Deal schedule {schedule_id} not found"
                )
            
            # Update only provided fields
            update_fields = {}
            if request.cities is not None:
                update_fields['cities'] = sorted(request.cities)
            if request.schedule_name is not None:
                update_fields['schedule_name'] = request.schedule_name
            if request.day_of_week is not None:
                update_fields['day_of_week'] = request.day_of_week
            if request.time_of_day is not None:
                update_fields['time_of_day'] = request.time_of_day
            if request.status is not None:
                update_fields['status'] = request.status
            
            # Apply updates
            for field, value in update_fields.items():
                setattr(schedule, field, value)
            
            # Recalculate next run time if timing-related fields changed
            if any(field in update_fields for field in ['day_of_week', 'time_of_day', 'status']):
                if schedule.status == ScheduleStatus.ACTIVE:
                    schedule.next_run_at = schedule.calculate_next_run()
            
            schedule.update_timestamp()
            await schedule.save()
            
            logger.info(f"Updated deal schedule {schedule_id}")
            
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