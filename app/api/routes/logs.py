from fastapi import APIRouter, HTTPException
import logging
from app.models import LogResponse
from app.core.task_manager import task_manager

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/logs", tags=["Logs"])

@router.get("/{task_id}", response_model=LogResponse)
async def get_task_logs(task_id: str):
    
    logger.info(f"Retrieving logs for task: {task_id}")
    task = task_manager.get_task_status(task_id)
    if not task:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
    
    logs = task_manager.get_and_clear_logs(task_id)
    return LogResponse(
        logs=logs,
        count=len(logs)
    )