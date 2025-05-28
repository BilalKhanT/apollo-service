from typing import List
from pydantic import BaseModel, Field
from app.models.base import BaseResponse

class LogEntry(BaseModel):
    task_id: str = Field(description="Task ID this log belongs to", example="123e4567-e89b-12d3-a456-426614174000")
    timestamp: str = Field(description="Log timestamp in ISO format", example="2025-01-27T15:30:45.000Z")
    level: str = Field(description="Log level", example="info")
    message: str = Field(description="Log message", example="Processing completed successfully")
    
    class Config:
        schema_extra = {
            "example": {
                "task_id": "123e4567-e89b-12d3-a456-426614174000",
                "timestamp": "2025-01-27T15:30:45.000Z",
                "level": "info",
                "message": "Processing completed successfully"
            }
        }

class LogResponse(BaseResponse):
    logs: List[LogEntry] = Field(description="List of log entries")
    count: int = Field(description="Number of log entries returned", example=25)
    
    class Config:
        schema_extra = {
            "example": {
                "success": True,
                "message": "Logs retrieved successfully",
                "timestamp": "2025-01-27T16:00:00.000Z",
                "logs": [
                    {
                        "task_id": "123e4567-e89b-12d3-a456-426614174000",
                        "timestamp": "2025-01-27T15:30:45.000Z",
                        "level": "info",
                        "message": "Processing completed successfully"
                    }
                ],
                "count": 25
            }
        }