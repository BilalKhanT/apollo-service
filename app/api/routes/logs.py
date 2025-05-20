from fastapi import APIRouter
from app.models import LogResponse
from app.controllers.logs_controller import LogsController

router = APIRouter(prefix="/api/logs", tags=["Logs"])

@router.get("/{task_id}", response_model=LogResponse)
async def get_task_logs(task_id: str):
    return await LogsController.get_task_logs(task_id)