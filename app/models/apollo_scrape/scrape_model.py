from typing import Dict, List, Optional
from pydantic import BaseModel, Field, field_validator, ValidationInfo
from datetime import datetime
from app.models.base import DataResponse, TaskStatus

class ScrapingRequest(BaseModel):
    cluster_ids: Dict[str, List[str]] = Field(
        default_factory=dict,
        description="Dictionary mapping cluster IDs to their respective links",
        example={
            "1.1": ["https://example.com/link1", "https://example.com/link2"],
            "1.2": ["https://example.com/link3"],
            "2.1": ["https://example.com/link4", "https://example.com/link5"]
        }
    )
    years: Dict[str, List[str]] = Field(
        default_factory=dict,
        description="Dictionary mapping years (including 'No Year') to their respective links",
        example={
            "2023": ["https://example.com/2023_link1", "https://example.com/2023_link2"],
            "2024": ["https://example.com/2024_link1"],
            "No Year": ["https://example.com/no_year_link1", "https://example.com/no_year_link2"]
        }
    )
    crawl_task_id: Optional[str] = Field(
        default=None,
        description="Specific crawl task ID to use (uses most recent if null)",
        example="123e4567-e89b-12d3-a456-426614174000"
    )
    
    @field_validator('cluster_ids')
    @classmethod
    def validate_cluster_ids(cls, v: Dict[str, List[str]]) -> Dict[str, List[str]]:
        if not v:
            raise ValueError('At least one cluster ID must be provided')
        
        for cluster_id, links in v.items():
            if not cluster_id or not cluster_id.strip():
                raise ValueError('Cluster ID cannot be empty')
            if not links:
                raise ValueError(f'Cluster ID "{cluster_id}" must have at least one link')
            if not all(isinstance(link, str) and link.strip() for link in links):
                raise ValueError(f'All links for cluster ID "{cluster_id}" must be non-empty strings')
        
        return v
    
    @field_validator('years')
    @classmethod
    def validate_years(cls, v: Dict[str, List[str]]) -> Dict[str, List[str]]:
        for year, links in v.items():
            if year != "No Year" and (not year.isdigit() or len(year) != 4):
                raise ValueError(f'Invalid year format: "{year}". Must be 4 digits or "No Year"')

            if not links:
                raise ValueError(f'Year "{year}" must have at least one link')
            if not all(isinstance(link, str) and link.strip() for link in links):
                raise ValueError(f'All links for year "{year}" must be non-empty strings')
        
        return v
    
    @field_validator('years')
    @classmethod
    def validate_at_least_one_target(cls, v: Dict[str, List[str]], info: ValidationInfo) -> Dict[str, List[str]]:
        cluster_ids = info.data.get('cluster_ids', {})
        if not cluster_ids and not v:
            raise ValueError('At least one cluster ID or year must be provided')
        return v
    
    class Config:
        json_schema_extra = {
            "example": {
                "cluster_ids": {
                    "1.1": ["https://example.com/cluster1_link1", "https://example.com/cluster1_link2"],
                    "1.2": ["https://example.com/cluster2_link1"],
                    "2.1": ["https://example.com/cluster3_link1", "https://example.com/cluster3_link2"]
                },
                "years": {
                    "2023": ["https://example.com/2023_file1.pdf", "https://example.com/2023_file2.pdf"],
                    "2024": ["https://example.com/2024_file1.pdf"],
                    "No Year": ["https://example.com/misc_file1.pdf", "https://example.com/misc_file2.pdf"]
                },
                "crawl_task_id": "123e4567-e89b-12d3-a456-426614174000"
            }
        }

class ScrapingProgress(BaseModel):
    current_cluster: Optional[str] = Field(default=None, description="Currently processing cluster ID", example="1.1")
    current_url: Optional[str] = Field(default=None, description="Currently processing URL", example="https://example.com/page1")
    current_year: Optional[str] = Field(default=None, description="Currently downloading year", example="2024")
    current_file: Optional[str] = Field(default=None, description="Currently downloading file URL")
    pages_processed: int = Field(default=0, description="Total pages processed", example=45)
    files_processed: int = Field(default=0, description="Total files processed", example=123)
    
    class Config:
        schema_extra = {
            "example": {
                "current_cluster": "1.1",
                "current_url": "https://example.com/page1",
                "current_year": "2024",
                "current_file": "https://example.com/files/document.pdf",
                "pages_processed": 45,
                "files_processed": 123
            }
        }

class ScrapingSummary(BaseModel):
    clusters_scraped: int = Field(description="Number of clusters scraped", example=3)
    pages_scraped: int = Field(description="Total pages successfully scraped", example=145)
    pages_failed: int = Field(description="Total pages that failed to scrape", example=5)
    files_downloaded: int = Field(description="Total files successfully downloaded", example=89)
    files_failed: int = Field(description="Total files that failed to download", example=3)
    scrape_output_dir: Optional[str] = Field(description="Directory containing scraped content")
    download_output_dir: Optional[str] = Field(description="Directory containing downloaded files")
    execution_time_seconds: float = Field(description="Total execution time", example=320.5)
    
    class Config:
        schema_extra = {
            "example": {
                "clusters_scraped": 3,
                "pages_scraped": 145,
                "pages_failed": 5,
                "files_downloaded": 89,
                "files_failed": 3,
                "scrape_output_dir": "/apollo_data/scraped/scrape_20250127-153045",
                "download_output_dir": "/apollo_data/downloads/download_20250127-160030",
                "execution_time_seconds": 320.5
            }
        }

class ScrapingStatus(BaseModel):
    id: str = Field(description="Unique task identifier", example="456e7890-e89b-12d3-a456-426614174000")
    status: TaskStatus = Field(description="Current task status", example=TaskStatus.SCRAPING)
    progress: float = Field(
        default=0.0, 
        description="Progress percentage (0-100)", 
        example=65.2,
        ge=0.0,
        le=100.0
    )
    pages_scraped: int = Field(default=0, description="Number of pages scraped", example=145)
    files_downloaded: int = Field(default=0, description="Number of files downloaded", example=89)
    error: Optional[str] = Field(default=None, description="Error message if task failed")
    progress_details: Optional[ScrapingProgress] = Field(default=None, description="Detailed progress information")
    created_at: Optional[datetime] = Field(default=None, description="Task creation timestamp")
    updated_at: Optional[datetime] = Field(default=None, description="Last update timestamp")
    execution_time_seconds: Optional[float] = Field(default=None, description="Current execution time", example=320.5)
    
    class Config:
        use_enum_values = True
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
        schema_extra = {
            "example": {
                "id": "456e7890-e89b-12d3-a456-426614174000",
                "status": "scraping",
                "progress": 65.2,
                "pages_scraped": 145,
                "files_downloaded": 89,
                "error": None,
                "progress_details": {
                    "current_cluster": "1.1",
                    "current_url": "https://example.com/page1",
                    "pages_processed": 45,
                    "files_processed": 123
                },
                "created_at": "2025-01-27T16:00:00.000Z",
                "updated_at": "2025-01-27T16:05:30.000Z",
                "execution_time_seconds": 320.5
            }
        }

class ScrapingResponse(DataResponse):
    data: ScrapingStatus = Field(description="Scraping task status information")
    
    class Config:
        schema_extra = {
            "example": {
                "success": True,
                "message": "Scraping task started successfully",
                "timestamp": "2025-01-27T16:00:00.000Z",
                "data": {
                    "id": "456e7890-e89b-12d3-a456-426614174000",
                    "status": "created",
                    "progress": 0.0,
                    "pages_scraped": 0,
                    "files_downloaded": 0,
                    "error": None,
                    "created_at": "2025-01-27T16:00:00.000Z",
                    "updated_at": "2025-01-27T16:00:00.000Z",
                    "execution_time_seconds": 0.0
                }
            }
        }