from fastapi import APIRouter, Query
from typing import Optional
import logging

from app.models.schedule_model import (
    CrawlScheduleRequest, 
    CrawlScheduleResponse, 
    ScheduleListResponse,
    ScheduleStatusResponse,
    ScheduleStatus
)
from app.controllers.schedule_controller import ScheduleController
from app.services.schedule_service import schedule_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/schedule", tags=["Scheduled Crawls"])


@router.post("", response_model=CrawlScheduleResponse)
async def create_or_update_schedule(request: CrawlScheduleRequest):
    return await ScheduleController.create_or_update_schedule(request)


@router.get("", response_model=ScheduleListResponse)
async def list_schedules(
    status: Optional[ScheduleStatus] = Query(None, description="Filter by schedule status"),
    limit: int = Query(50, ge=1, le=100, description="Number of schedules to return"),
    skip: int = Query(0, ge=0, description="Number of schedules to skip")
):
    """List all crawl schedules with optional filtering and pagination."""
    return await ScheduleController.list_schedules(status=status, limit=limit, skip=skip)

@router.delete("/{schedule_id}")
async def delete_schedule(schedule_id: str):
    return await ScheduleController.delete_schedule(schedule_id)

@router.post("/{schedule_id}/pause")
async def pause_schedule(schedule_id: str):
    """Pause a schedule (stops it from executing but keeps it in the database)"""
    return await ScheduleController.pause_schedule(schedule_id)


@router.post("/{schedule_id}/resume")
async def resume_schedule(schedule_id: str):
    """Resume a paused schedule"""
    return await ScheduleController.resume_schedule(schedule_id)


