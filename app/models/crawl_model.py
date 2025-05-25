from pydantic import BaseModel, Field, validator
from typing import Optional
from datetime import datetime
from .base import TaskStatus, BaseResponse, DataResponse

class CrawlRequest(BaseModel):
    base_url: str = Field(
        ..., 
        description="The starting URL for crawling",
        example="https://example.com"
    )
    max_links_to_scrape: Optional[int] = Field(
        default=None, 
        description="Maximum number of links to scrape (unlimited if null)",
        example=1000,
        ge=1
    )
    max_pages_to_scrape: Optional[int] = Field(
        default=None, 
        description="Maximum number of pages to scrape (unlimited if null)",
        example=500,
        ge=1
    )
    depth_limit: Optional[int] = Field(
        default=None, 
        description="Maximum crawling depth (unlimited if null)",
        example=3,
        ge=1
    )
    domain_restriction: bool = Field(
        default=True, 
        description="Whether to restrict crawling to the base domain only",
        example=True
    )
    scrape_pdfs_and_xls: bool = Field(
        default=True, 
        description="Whether to include PDF and Excel files in crawling",
        example=True
    )
    stop_scraper: bool = Field(
        default=False, 
        description="Whether to stop any currently running scrapers",
        example=False
    )
    
    @validator('base_url')
    def validate_base_url(cls, v):
        if not v.startswith(('http://', 'https://')):
            raise ValueError('base_url must start with http:// or https://')
        return v
    
    class Config:
        schema_extra = {
            "example": {
                "base_url": "https://example.com",
                "max_links_to_scrape": 1000,
                "max_pages_to_scrape": 500,
                "depth_limit": 3,
                "domain_restriction": True,
                "scrape_pdfs_and_xls": True,
                "stop_scraper": False
            }
        }

class CrawlSummary(BaseModel):
    total_links_found: int = Field(description="Total number of links discovered", example=1250)
    total_pages_scraped: int = Field(description="Total number of pages successfully scraped", example=487)
    total_unique_links: int = Field(description="Number of unique links found", example=1100)
    total_direct_document_links: int = Field(description="Number of direct document links found", example=45)
    total_404_urls: int = Field(description="Number of URLs that returned 404", example=12)
    total_error_urls: int = Field(description="Number of URLs that encountered errors", example=8)
    execution_time_seconds: float = Field(description="Total execution time in seconds", example=145.7)
    crawl_date: str = Field(description="Date and time when crawl was performed", example="2025-01-27 15:30:45")

class CrawlStatus(BaseModel):
    id: str = Field(description="Unique task identifier", example="123e4567-e89b-12d3-a456-426614174000")
    status: TaskStatus = Field(description="Current task status", example=TaskStatus.RUNNING)
    progress: float = Field(
        default=0.0, 
        description="Progress percentage (0-100)", 
        example=75.5,
        ge=0.0,
        le=100.0
    )
    current_stage: str = Field(description="Current processing stage", example="clustering")
    links_found: int = Field(default=0, description="Number of links found so far", example=1250)
    pages_scraped: int = Field(default=0, description="Number of pages scraped so far", example=487)
    error: Optional[str] = Field(default=None, description="Error message if task failed")
    clusters_ready: bool = Field(default=False, description="Whether clusters are ready for scraping", example=True)
    created_at: Optional[datetime] = Field(default=None, description="Task creation timestamp")
    updated_at: Optional[datetime] = Field(default=None, description="Last update timestamp")
    execution_time_seconds: Optional[float] = Field(default=None, description="Current execution time", example=145.7)
    
    class Config:
        use_enum_values = True
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
        schema_extra = {
            "example": {
                "id": "123e4567-e89b-12d3-a456-426614174000",
                "status": "running", 
                "progress": 75.5,
                "current_stage": "clustering",
                "links_found": 1250,
                "pages_scraped": 487,
                "error": None,
                "clusters_ready": True,
                "created_at": "2025-01-27T15:30:45.000Z",
                "updated_at": "2025-01-27T15:45:30.000Z",
                "execution_time_seconds": 145.7
            }
        }

class CrawlResponse(DataResponse):
    data: CrawlStatus = Field(description="Crawl task status information")
    
    class Config:
        schema_extra = {
            "example": {
                "success": True,
                "message": "Crawl task started successfully",
                "timestamp": "2025-01-27T15:30:45.000Z",
                "data": {
                    "id": "123e4567-e89b-12d3-a456-426614174000",
                    "status": "created",
                    "progress": 0.0,
                    "current_stage": "initializing",
                    "links_found": 0,
                    "pages_scraped": 0,
                    "error": None,
                    "clusters_ready": False,
                    "created_at": "2025-01-27T15:30:45.000Z",
                    "updated_at": "2025-01-27T15:30:45.000Z",
                    "execution_time_seconds": 0.0
                }
            }
        }

class CrawlStopRequest(BaseModel):
    force: bool = Field(
        default=False, 
        description="Whether to force stop the task immediately",
        example=False
    )
    
    class Config:
        schema_extra = {
            "example": {
                "force": False
            }
        }

class CrawlStopResponse(BaseResponse):
    task_id: str = Field(description="ID of the stopped task", example="123e4567-e89b-12d3-a456-426614174000")
    cleanup_completed: bool = Field(description="Whether cleanup was completed", example=True)
    
    class Config:
        schema_extra = {
            "example": {
                "success": True,
                "message": "Crawler stopped gracefully",
                "timestamp": "2025-01-27T15:45:30.000Z",
                "task_id": "123e4567-e89b-12d3-a456-426614174000",
                "cleanup_completed": True
            }
        }