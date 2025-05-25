from pydantic import BaseModel, Field
from typing import Optional, Any, Dict, List
from datetime import datetime
from enum import Enum

class TaskStatus(str, Enum):
    CREATED = "created"
    RUNNING = "running" 
    CRAWLING = "crawling"
    PROCESSING = "processing"
    CLUSTERING = "clustering"
    YEAR_EXTRACTION = "year_extraction"
    SCRAPING = "scraping"
    DOWNLOADING = "downloading"
    PREPARING = "preparing"
    PREPARING_DOWNLOAD = "preparing_download"
    FINALIZING = "finalizing"
    COMPLETED = "completed"
    FAILED = "failed"
    STOPPED = "stopped"
    ERROR = "error"

class BaseResponse(BaseModel):
    success: bool = Field(default=True, description="Whether the operation was successful")
    message: Optional[str] = Field(default=None, description="Optional message")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Response timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
        schema_extra = {
            "example": {
                "success": True,
                "message": "Operation completed successfully",
                "timestamp": "2025-01-27T10:30:00.000Z"
            }
        }

class DataResponse(BaseResponse):
    data: Any = Field(description="Response data payload")
    
    class Config:
        schema_extra = {
            "example": {
                "success": True,
                "message": "Data retrieved successfully",
                "timestamp": "2025-01-27T10:30:00.000Z",
                "data": {}
            }
        }

class ListResponse(BaseResponse):
    data: List[Any] = Field(description="List of items")
    total_count: int = Field(description="Total number of items")
    page: int = Field(default=1, description="Current page number")
    page_size: int = Field(default=50, description="Number of items per page")
    
    class Config:
        schema_extra = {
            "example": {
                "success": True,
                "message": "Items retrieved successfully",
                "timestamp": "2025-01-27T10:30:00.000Z",
                "data": [],
                "total_count": 100,
                "page": 1,
                "page_size": 50
            }
        }

class ErrorResponse(BaseModel):
    success: bool = Field(default=False)
    error: str = Field(description="Error message")
    error_code: Optional[str] = Field(default=None, description="Error code for programmatic handling")
    details: Optional[Dict[str, Any]] = Field(default=None, description="Additional error details")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        schema_extra = {
            "example": {
                "success": False,
                "error": "Task not found",
                "error_code": "TASK_NOT_FOUND", 
                "details": {"task_id": "123e4567-e89b-12d3-a456-426614174000"},
                "timestamp": "2025-01-27T10:30:00.000Z"
            }
        }

class HealthResponse(BaseModel):
    status: str = Field(description="Overall system status")
    version: str = Field(description="API version")
    database: Dict[str, Any] = Field(description="Database connection status")
    schedule_service: Dict[str, Any] = Field(description="Schedule service status")
    websocket: Dict[str, Any] = Field(description="WebSocket service status")
    
    class Config:
        schema_extra = {
            "example": {
                "status": "healthy",
                "version": "2.2.0",
                "database": {
                    "status": "connected"
                },
                "schedule_service": {
                    "running": True,
                    "check_interval_seconds": 15
                },
                "websocket": {
                    "status": "healthy",
                    "stats": {
                        "connected_clients": 5,
                        "active_clients": 3
                    }
                }
            }
        }