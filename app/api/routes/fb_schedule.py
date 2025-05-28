from datetime import timedelta
from fastapi import APIRouter, HTTPException, Query, status
from typing import Optional
import logging

from app.models.fb_scrape.fb_scrape_schedule_model import (
    FBScheduleRequest, 
    FBScheduleResponse, 
    FBScheduleListResponse,
    FBScheduleActionResponse,
    ScheduleStatus
)
from app.models.base import ErrorResponse
from app.controllers.fb_scrape.fb_schedule_controller import FBScheduleController

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/facebook/schedule", tags=["Facebook Scheduling"])

@router.post(
    "",
    response_model=FBScheduleResponse,
    responses={
        201: {
            "description": "Facebook schedule created successfully",
            "model": FBScheduleResponse
        },
        200: {
            "description": "Facebook schedule updated successfully", 
            "model": FBScheduleResponse
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
    summary="Create or update a Facebook scraping schedule",
    description="Create a new Facebook scraping schedule or update an existing one for the same keywords combination."
)
async def create_or_update_facebook_schedule(request: FBScheduleRequest) -> FBScheduleResponse:
    try:
        return await FBScheduleController.create_or_update_schedule(request)
    except HTTPException:
        raise
    except ValueError as e:
        logger.error(f"Validation error creating Facebook schedule: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid request parameters: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error creating/updating Facebook schedule: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create/update Facebook schedule: {str(e)}"
        )

@router.get(
    "",
    response_model=FBScheduleListResponse,
    responses={
        200: {
            "description": "Facebook schedules retrieved successfully",
            "model": FBScheduleListResponse
        },
        500: {
            "description": "Internal server error",
            "model": ErrorResponse
        }
    },
    summary="List all Facebook scraping schedules",
    description="Retrieve a paginated list of all Facebook scraping schedules with optional status filtering."
)
async def list_facebook_schedules(
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
) -> FBScheduleListResponse:
    try:
        response = await FBScheduleController.list_schedules(status=status, limit=limit, skip=skip)
        
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
        logger.error(f"Error listing Facebook schedules: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list Facebook schedules: {str(e)}"
        )
    
@router.delete(
    "/{schedule_id}",
    response_model=FBScheduleActionResponse,
    responses={
        200: {
            "description": "Facebook schedule deleted successfully",
            "model": FBScheduleActionResponse
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
    summary="Delete a Facebook schedule",
    description="Permanently delete a Facebook scraping schedule. This action cannot be undone."
)
async def delete_facebook_schedule(schedule_id: str) -> FBScheduleActionResponse:
    try:
        result = await FBScheduleController.delete_schedule(schedule_id)
        return FBScheduleActionResponse(
            success=True,
            message=result["message"],
            schedule_id=schedule_id,
            action="deleted"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting Facebook schedule {schedule_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete Facebook schedule: {str(e)}"
        )

@router.post(
    "/{schedule_id}/pause",
    response_model=FBScheduleActionResponse,
    responses={
        200: {
            "description": "Facebook schedule paused successfully",
            "model": FBScheduleActionResponse
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
    summary="Pause a Facebook schedule",
    description="Pause a schedule to temporarily stop automatic execution. The schedule can be resumed later."
)
async def pause_facebook_schedule(schedule_id: str) -> FBScheduleActionResponse:
    try:
        result = await FBScheduleController.pause_schedule(schedule_id)
        return FBScheduleActionResponse(
            success=True,
            message=result["message"],
            schedule_id=schedule_id,
            action="paused"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error pausing Facebook schedule {schedule_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to pause Facebook schedule: {str(e)}"
        )

@router.post(
    "/{schedule_id}/resume",
    response_model=FBScheduleActionResponse,
    responses={
        200: {
            "description": "Facebook schedule resumed successfully",
            "model": FBScheduleActionResponse
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
    summary="Resume a paused Facebook schedule",
    description="Resume a paused schedule to restart automatic execution."
)
async def resume_facebook_schedule(schedule_id: str) -> FBScheduleActionResponse:
    try:
        result = await FBScheduleController.resume_schedule(schedule_id)
        return FBScheduleActionResponse(
            success=True,
            message=result["message"],
            schedule_id=schedule_id,
            action="resumed"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error resuming Facebook schedule {schedule_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to resume Facebook schedule: {str(e)}"
        )