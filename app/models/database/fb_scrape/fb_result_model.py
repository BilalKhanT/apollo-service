from datetime import datetime
from typing import Any, Dict, List, Optional
from beanie import Document
from pydantic import BaseModel, Field


class FacebookPostData(BaseModel):
    post_id: str
    message: str
    created_time: str
    category: str
    attachments: Optional[List[Dict[str, Any]]] = Field(default_factory=list)
    scraped_at: datetime = Field(default_factory=datetime.utcnow)

class FacebookResult(Document):
    task_id: str = Field(..., unique=True, index=True)
    keywords_requested: List[str] = Field(description="Keywords that were requested for scraping")
    days_requested: int = Field(description="Number of days that were requested for scraping")
    posts_processed: int = Field(description="Total posts processed")
    categories_found: Dict[str, int] = Field(default_factory=dict, description="Number of posts by category")
    keyword_matches: Dict[str, Dict[str, int]] = Field(default_factory=dict, description="Keyword match statistics")
    execution_time_seconds: float = Field(description="Total time taken to complete the task")
    output_directory: str = Field(description="Directory where posts were saved")
    date_range: Dict[str, str] = Field(description="Date range that was scraped")
    posts_data: List[FacebookPostData] = Field(default_factory=list, description="Complete post data")
    created_at: datetime = Field(description="When the task was originally started")
    completed_at: datetime = Field(default_factory=datetime.utcnow, description="When the task was completed")
    
    class Settings:
        name = "facebook_results"
        indexes = [
            "task_id",
            "created_at",
            "keywords_requested"
        ]

    def get_summary(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "keywords_requested": self.keywords_requested,
            "days_requested": self.days_requested,
            "posts_processed": self.posts_processed,
            "categories_found": self.categories_found,
            "keyword_matches": self.keyword_matches,
            "execution_time_seconds": self.execution_time_seconds,
            "output_directory": self.output_directory,
            "created_at": self.created_at,
            "completed_at": self.completed_at,
            "date_range": self.date_range
        }

    def get_minimal_summary(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "created_at": self.created_at,
            "completed_at": self.completed_at,
            "keywords_requested": self.keywords_requested,
            "days_requested": self.days_requested,
            "posts_processed": self.posts_processed,
            "categories_found": self.categories_found
        }