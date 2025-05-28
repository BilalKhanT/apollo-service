from datetime import timedelta
from fastapi import APIRouter, HTTPException, Query, status
from typing import Optional
import logging

from app.models.apollo_scrape.schedule_model import (
    CrawlScheduleRequest, 
    CrawlScheduleResponse, 
    ScheduleListResponse,
    ScheduleActionResponse,
    ScheduleStatus
)
from app.models.base import ErrorResponse
from app.controllers.apollo_scrape.schedule_controller import ScheduleController

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/schedule", tags=["Scheduled Crawls"])

@router.post(
    "",
    response_model=CrawlScheduleResponse,
    responses={
        201: {
            "description": "Schedule created successfully",
            "model": CrawlScheduleResponse
        },
        200: {
            "description": "Schedule updated successfully", 
            "model": CrawlScheduleResponse
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
    summary="Create or update a crawl schedule",
    description="Create a new crawl schedule or update an existing one for the same base URL."
)
async def create_or_update_schedule(request: CrawlScheduleRequest) -> CrawlScheduleResponse:
    try:
        return await ScheduleController.create_or_update_schedule(request)
    except HTTPException:
        raise
    except ValueError as e:
        logger.error(f"Validation error creating schedule: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid request parameters: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error creating/updating schedule: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create/update schedule: {str(e)}"
        )

@router.get(
    "",
    response_model=ScheduleListResponse,
    responses={
        200: {
            "description": "Schedules retrieved successfully",
            "model": ScheduleListResponse
        },
        500: {
            "description": "Internal server error",
            "model": ErrorResponse
        }
    },
    summary="List all crawl schedules",
    description="Retrieve a paginated list of all crawl schedules with optional status filtering."
)
async def list_schedules(
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
) -> ScheduleListResponse:
    try:
        response = await ScheduleController.list_schedules(status=status, limit=limit, skip=skip)
        karachi_offset = timedelta(hours=5)
        if hasattr(response, 'schedules') and response.schedules:
            for schedule in response.schedules:
                if hasattr(schedule, 'last_run_at') and schedule.last_run_at:
                    schedule.last_run_at = schedule.last_run_at + karachi_offset

                if hasattr(schedule, 'next_run_at') and schedule.next_run_at:
                    schedule.next_run_at = schedule.next_run_at + karachi_offset

        return response
    except Exception as e:
        logger.error(f"Error listing schedules: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list schedules: {str(e)}"
        )

@router.delete(
    "/{schedule_id}",
    response_model=ScheduleActionResponse,
    responses={
        200: {
            "description": "Schedule deleted successfully",
            "model": ScheduleActionResponse
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
    summary="Delete a crawl schedule",
    description="Permanently delete a crawl schedule. This action cannot be undone."
)
async def delete_schedule(schedule_id: str) -> ScheduleActionResponse:
    try:
        result = await ScheduleController.delete_schedule(schedule_id)
        return ScheduleActionResponse(
            success=True,
            message=result["message"],
            schedule_id=schedule_id,
            action="deleted"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting schedule {schedule_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete schedule: {str(e)}"
        )

@router.post(
    "/{schedule_id}/pause",
    response_model=ScheduleActionResponse,
    responses={
        200: {
            "description": "Schedule paused successfully",
            "model": ScheduleActionResponse
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
    summary="Pause a crawl schedule",
    description="Pause a schedule to temporarily stop automatic execution. The schedule can be resumed later."
)
async def pause_schedule(schedule_id: str) -> ScheduleActionResponse:
    try:
        result = await ScheduleController.pause_schedule(schedule_id)
        return ScheduleActionResponse(
            success=True,
            message=result["message"],
            schedule_id=schedule_id,
            action="paused"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error pausing schedule {schedule_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to pause schedule: {str(e)}"
        )

@router.post(
    "/{schedule_id}/resume",
    response_model=ScheduleActionResponse,
    responses={
        200: {
            "description": "Schedule resumed successfully",
            "model": ScheduleActionResponse
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
    summary="Resume a paused crawl schedule",
    description="Resume a paused schedule to restart automatic execution."
)
async def resume_schedule(schedule_id: str) -> ScheduleActionResponse:
    try:
        result = await ScheduleController.resume_schedule(schedule_id)
        return ScheduleActionResponse(
            success=True,
            message=result["message"],
            schedule_id=schedule_id,
            action="resumed"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error resuming schedule {schedule_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to resume schedule: {str(e)}"
        )