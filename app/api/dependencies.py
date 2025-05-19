from fastapi import HTTPException, status
from app.core.task_manager import task_manager
from typing import Dict, Any


async def get_task_or_404(task_id: str) -> Dict[str, Any]:
    """
    Get a task by ID or raise 404 if not found.
    """
    task = task_manager.get_task_status(task_id)
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task {task_id} not found"
        )
    return task