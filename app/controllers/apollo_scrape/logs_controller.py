from app.utils.task_manager import task_manager
from app.models.apollo_scrape.log_model import LogResponse
from fastapi import HTTPException

class LogsController:
    @staticmethod
    async def get_task_logs(task_id: str) -> LogResponse:
        task = task_manager.get_task_status(task_id)
        if not task:
            raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
        
        logs = task_manager.get_and_clear_logs(task_id)
        
        return LogResponse(
            logs=logs,
            count=len(logs)
        )