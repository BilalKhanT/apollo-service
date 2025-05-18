from fastapi import APIRouter, HTTPException
from app.models import LogResponse
from app.utils.task_manager import task_manager

router = APIRouter(prefix="/api/logs", tags=["Logs"])

@router.get("/{task_id}", response_model=LogResponse)
async def get_task_logs(task_id: str):
    task = task_manager.get_task_status(task_id)
    if not task:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
    
    logs = task_manager.get_and_clear_logs(task_id)
    
    return LogResponse(
        logs=logs,
        count=len(logs)
    )