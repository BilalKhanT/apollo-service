from app.utils.task_manager import task_manager
from app.models.log_model import LogResponse
from fastapi import HTTPException
import logging

logger = logging.getLogger(__name__)

class LogsController:
    @staticmethod
    async def get_task_logs(task_id: str) -> LogResponse:
        """
        Get task logs and clear them after fetching (standard behavior).
        With improved non-blocking implementation to prevent hanging during polling.
        """
        try:
            task = task_manager.get_task_status(task_id)
            if not task:
                # Check if task exists in database for completed tasks
                # This handles the case where server restarted but we still want to see final logs
                raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
            
            # Use get_and_clear_logs with improved non-blocking implementation
            logs = task_manager.get_and_clear_logs(task_id)
            
            return LogResponse(
                logs=logs,
                count=len(logs)
            )
        
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error retrieving logs for task {task_id}: {str(e)}")
            raise HTTPException(
                status_code=500, 
                detail=f"Internal error retrieving logs: {str(e)}"
            )