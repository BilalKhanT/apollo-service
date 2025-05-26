from datetime import timedelta
from fastapi import APIRouter, HTTPException, Query, status
from typing import Optional
import logging

from app.models.restaurant_deal.deal_schedule_model import (
    DealScheduleRequest, 
    DealScheduleResponse, 
    DealScheduleListResponse,
    DealScheduleActionResponse,
    ScheduleStatus
)
from app.models.base import ErrorResponse
from app.controllers.restaurant_deal.deal_schedule_controller import DealScheduleController

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/deals/schedule", tags=["Deal Scheduling"])

@router.post(
    "",
    response_model=DealScheduleResponse,
    responses={
        201: {
            "description": "Deal schedule created successfully",
            "model": DealScheduleResponse
        },
        200: {
            "description": "Deal schedule updated successfully", 
            "model": DealScheduleResponse
        },
        400: {
            "description": "Invalid request parameters",
            "model": ErrorResponse
        },
        500: {
            "description": "Internal server error",
            "model": ErrorResponse
        }
    },
    summary="Create or update a deal scraping schedule",
    description="Create a new deal scraping schedule or update an existing one for the same cities combination."
)
async def create_or_update_deal_schedule(request: DealScheduleRequest) -> DealScheduleResponse:
    try:
        return await DealScheduleController.create_or_update_schedule(request)
    except HTTPException:
        raise
    except ValueError as e:
        logger.error(f"Validation error creating deal schedule: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid request parameters: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error creating/updating deal schedule: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create/update deal schedule: {str(e)}"
        )

@router.get(
    "",
    response_model=DealScheduleListResponse,
    responses={
        200: {
            "description": "Deal schedules retrieved successfully",
            "model": DealScheduleListResponse
        },
        500: {
            "description": "Internal server error",
            "model": ErrorResponse
        }
    },
    summary="List all deal scraping schedules",
    description="Retrieve a paginated list of all deal scraping schedules with optional status filtering."
)
async def list_deal_schedules(
    status: Optional[ScheduleStatus] = Query(
        None, 
        description="Filter schedules by status",
        example=ScheduleStatus.ACTIVE
    ),
    limit: int = Query(
        50, 
        ge=1, 
        le=100, 
        description="Number of schedules to return",
        example=10
    ),
    skip: int = Query(
        0, 
        ge=0, 
        description="Number of schedules to skip for pagination",
        example=0
    )
) -> DealScheduleListResponse:
    try:
        response = await DealScheduleController.list_schedules(status=status, limit=limit, skip=skip)
        
        # Convert timestamps to Karachi time for display
        karachi_offset = timedelta(hours=5)
        if hasattr(response, 'schedules') and response.schedules:
            for schedule in response.schedules:
                if hasattr(schedule, 'last_run_at') and schedule.last_run_at:
                    schedule.last_run_at = schedule.last_run_at + karachi_offset

                if hasattr(schedule, 'next_run_at') and schedule.next_run_at:
                    schedule.next_run_at = schedule.next_run_at + karachi_offset

        return response
    except Exception as e:
        logger.error(f"Error listing deal schedules: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list deal schedules: {str(e)}"
        )
    
@router.delete(
    "/{schedule_id}",
    response_model=DealScheduleActionResponse,
    responses={
        200: {
            "description": "Deal schedule deleted successfully",
            "model": DealScheduleActionResponse
        },
        404: {
            "description": "Schedule not found",
            "model": ErrorResponse
        },
        500: {
            "description": "Internal server error",
            "model": ErrorResponse
        }
    },
    summary="Delete a deal schedule",
    description="Permanently delete a deal scraping schedule. This action cannot be undone."
)
async def delete_deal_schedule(schedule_id: str) -> DealScheduleActionResponse:
    try:
        result = await DealScheduleController.delete_schedule(schedule_id)
        return DealScheduleActionResponse(
            success=True,
            message=result["message"],
            schedule_id=schedule_id,
            action="deleted"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting deal schedule {schedule_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete deal schedule: {str(e)}"
        )

@router.post(
    "/{schedule_id}/pause",
    response_model=DealScheduleActionResponse,
    responses={
        200: {
            "description": "Deal schedule paused successfully",
            "model": DealScheduleActionResponse
        },
        404: {
            "description": "Schedule not found",
            "model": ErrorResponse
        },
        500: {
            "description": "Internal server error",
            "model": ErrorResponse
        }
    },
    summary="Pause a deal schedule",
    description="Pause a schedule to temporarily stop automatic execution. The schedule can be resumed later."
)
async def pause_deal_schedule(schedule_id: str) -> DealScheduleActionResponse:
    try:
        result = await DealScheduleController.pause_schedule(schedule_id)
        return DealScheduleActionResponse(
            success=True,
            message=result["message"],
            schedule_id=schedule_id,
            action="paused"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error pausing deal schedule {schedule_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to pause deal schedule: {str(e)}"
        )

@router.post(
    "/{schedule_id}/resume",
    response_model=DealScheduleActionResponse,
    responses={
        200: {
            "description": "Deal schedule resumed successfully",
            "model": DealScheduleActionResponse
        },
        404: {
            "description": "Schedule not found",
            "model": ErrorResponse
        },
        500: {
            "description": "Internal server error",
            "model": ErrorResponse
        }
    },
    summary="Resume a paused deal schedule",
    description="Resume a paused schedule to restart automatic execution."
)
async def resume_deal_schedule(schedule_id: str) -> DealScheduleActionResponse:
    try:
        result = await DealScheduleController.resume_schedule(schedule_id)
        return DealScheduleActionResponse(
            success=True,
            message=result["message"],
            schedule_id=schedule_id,
            action="resumed"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error resuming deal schedule {schedule_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to resume deal schedule: {str(e)}"
        )