from fastapi import APIRouter, HTTPException, logger, status
from app.models.log_model import LogResponse
from app.models.base import ErrorResponse
from app.controllers.logs_controller import LogsController

router = APIRouter(prefix="/api/logs", tags=["Logs"])

@router.get(
    "/{task_id}",
    response_model=LogResponse,
    responses={
        200: {
            "description": "Logs retrieved successfully",
            "model": LogResponse
        },
        404: {
            "description": "Task not found",
            "model": ErrorResponse
        },
        500: {
            "description": "Internal server error",
            "model": ErrorResponse
        }
    },
    summary="Get task logs",
    description="Retrieve all log entries for a specific task. This endpoint clears the logs after retrieval."
)
async def get_task_logs(
    task_id: str,
) -> LogResponse:
    try:
        return await LogsController.get_task_logs(task_id)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting logs for task {task_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve task logs: {str(e)}"
        )