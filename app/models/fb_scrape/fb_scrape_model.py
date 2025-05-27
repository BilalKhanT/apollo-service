from pydantic import BaseModel, validator, Field  # Fixed import
from datetime import datetime
from typing import Dict, List
from app.models.base import BaseResponse


class FacebookScrapingRequest(BaseModel):
    keywords: List[str] = Field(description="List of keywords to filter Facebook posts", example=["offer", "discount", "deal"], min_items=1)
    days: int = Field(description="Number of days to look back for posts", example=30, ge=1, le=365)
    
    @validator('keywords')
    def validate_keywords(cls, v):
        if not v:
            raise ValueError('At least one keyword must be provided')
        # Clean and validate keywords
        cleaned_keywords = []
        for keyword in v:
            if keyword and keyword.strip():
                cleaned_keywords.append(keyword.strip())
        if not cleaned_keywords:
            raise ValueError('At least one valid keyword must be provided')
        return cleaned_keywords

    @validator('days')
    def validate_days(cls, v):
        if v < 1:
            raise ValueError('Days must be at least 1')
        if v > 365:
            raise ValueError('Days cannot exceed 365')
        return v

    class Config:
        json_schema_extra = {
            "example": {
                "keywords": ["offer", "discount", "deal"],
                "days": 30
            }
        }


class FacebookScrapingResponse(BaseResponse):
    task_id: str = Field(description="Unique task identifier for tracking")
    keywords_requested: List[str] = Field(description="Keywords that were requested for scraping")
    days_requested: int = Field(description="Number of days that were requested for scraping")
    
    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "message": "Facebook scraping started for 3 keywords",
                "timestamp": "2025-05-27T10:00:00.000Z",
                "task_id": "c94851e8-3547-4d2c-bfc9-17f4c3d6d838",
                "keywords_requested": ["offer", "discount", "deal"],
                "days_requested": 30
            }
        }


class FacebookResultSummaryMinimal(BaseModel):
    task_id: str = Field(description="Task identifier")
    created_at: datetime = Field(description="When the task was started")
    completed_at: datetime = Field(description="When the task was completed")
    keywords_requested: List[str] = Field(description="Keywords that were requested")
    days_requested: int = Field(description="Number of days that were requested")
    posts_processed: int = Field(description="Total posts processed")
    categories_found: Dict[str, int] = Field(description="Number of posts by category")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
        json_schema_extra = {
            "example": {
                "task_id": "c94851e8-3547-4d2c-bfc9-17f4c3d6d838",
                "created_at": "2025-05-27T14:58:44.857956",
                "completed_at": "2025-05-27T14:59:25.236544",
                "keywords_requested": ["offer", "discount", "deal"],
                "days_requested": 30,
                "posts_processed": 45,
                "categories_found": {
                    "mobilePhones": 12,
                    "foodDining": 8,
                    "electronics": 6,
                    "other": 19
                }
            }
        }


class FacebookResultSummary(BaseModel):
    task_id: str = Field(description="Task identifier")
    keywords_requested: List[str] = Field(description="Keywords that were requested")
    days_requested: int = Field(description="Number of days that were requested")
    posts_processed: int = Field(description="Total posts processed")
    categories_found: Dict[str, int] = Field(description="Number of posts by category")
    keyword_matches: Dict[str, Dict[str, int]] = Field(description="Keyword match statistics")
    execution_time_seconds: float = Field(description="Time taken to complete")
    output_directory: str = Field(description="Directory where posts were saved")
    created_at: datetime = Field(description="When the task was started")
    completed_at: datetime = Field(description="When the task was completed")
    date_range: Dict[str, str] = Field(description="Date range scraped")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
        json_schema_extra = {
            "example": {
                "task_id": "c94851e8-3547-4d2c-bfc9-17f4c3d6d838",
                "keywords_requested": ["offer", "discount", "deal"],
                "days_requested": 30,
                "posts_processed": 45,
                "categories_found": {
                    "mobilePhones": 12,
                    "foodDining": 8,
                    "electronics": 6,
                    "other": 19
                },
                "keyword_matches": {
                    "offer": {"loose_matches": 25, "strict_matches": 20},
                    "discount": {"loose_matches": 15, "strict_matches": 12},
                    "deal": {"loose_matches": 18, "strict_matches": 15}
                },
                "execution_time_seconds": 45.23,
                "output_directory": "/apollo_data/facebook/facebook_data_2025-05-27_14-58",
                "created_at": "2025-05-27T14:58:44.857956",
                "completed_at": "2025-05-27T14:59:25.236544",
                "date_range": {
                    "start_date": "2025-04-27",
                    "end_date": "2025-05-27"
                }
            }
        }


class FacebookResultsResponseMinimal(BaseResponse):
    data: List[FacebookResultSummaryMinimal] = Field(description="List of minimal Facebook scraping results")
    total_count: int = Field(description="Total number of results available")
    page: int = Field(description="Current page number")
    page_size: int = Field(description="Number of results per page")
    has_more: bool = Field(description="Whether there are more results available")
    
    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "message": "Retrieved 2 Facebook scraping results",
                "timestamp": "2025-05-27T10:00:00.000Z",
                "data": [
                    {
                        "task_id": "c94851e8-3547-4d2c-bfc9-17f4c3d6d838",
                        "created_at": "2025-05-27T14:58:44.857956",
                        "completed_at": "2025-05-27T14:59:25.236544", 
                        "keywords_requested": ["offer", "discount", "deal"],
                        "days_requested": 30,
                        "posts_processed": 45,
                        "categories_found": {
                            "mobilePhones": 12,
                            "foodDining": 8,
                            "electronics": 6,
                            "other": 19
                        }
                    }
                ],
                "total_count": 5,
                "page": 1,
                "page_size": 50,
                "has_more": False
            }
        }


class FacebookResultsResponse(BaseResponse):
    data: List[FacebookResultSummary] = Field(description="List of Facebook scraping results")
    total_count: int = Field(description="Total number of results available")
    page: int = Field(description="Current page number")
    page_size: int = Field(description="Number of results per page")
    has_more: bool = Field(description="Whether there are more results available")
    
    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "message": "Retrieved 2 Facebook scraping results",
                "timestamp": "2025-05-27T10:00:00.000Z",
                "data": [
                    {
                        "task_id": "c94851e8-3547-4d2c-bfc9-17f4c3d6d838",
                        "keywords_requested": ["offer", "discount", "deal"],
                        "days_requested": 30,
                        "posts_processed": 45,
                        "execution_time_seconds": 45.23
                    }
                ],
                "total_count": 5,
                "page": 1,
                "page_size": 50,
                "has_more": False
            }
        }


class FacebookStopResponse(BaseResponse):
    task_id: str = Field(description="ID of the task that was stopped")
    was_running: bool = Field(description="Whether the task was actually running when stopped")
    cleanup_completed: bool = Field(description="Whether cleanup was completed successfully")
    
    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "message": "Facebook scraping task stopped successfully",
                "timestamp": "2025-05-27T10:00:00.000Z",
                "task_id": "c94851e8-3547-4d2c-bfc9-17f4c3d6d838",
                "was_running": True,
                "cleanup_completed": True
            }
        }