from beanie import Document
from typing import Dict, List, Optional
from pydantic import BaseModel, Field
from datetime import datetime
from enum import Enum

class UploadStatus(str, Enum):
    INITIAL = "Initial"
    COMPLETED = "Completed"
    FAILED = "Failed"

class Cluster(BaseModel):
    id: str
    path: str
    url_count: int
    urls: List[str]


class DomainCluster(BaseModel):
    id: str
    count: int
    clusters: List[Cluster]


class YearCluster(BaseModel):
    year: str
    files: List[str]


class CrawlResult(Document):
    task_id: str = Field(..., unique=True, index=True)
    scrape_id: str = Field(default="")
    link_found: int
    pages_scraped: int
    is_scraped: bool = Field(default=False)
    is_uploaded: UploadStatus = Field(default=UploadStatus.INITIAL)
    error: Optional[str] = None
    clusters: Optional[Dict[str, DomainCluster]] = None
    yearclusters: Optional[Dict[str, List[str]]] = None  
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Settings:
        name = "crawl_results"
        
    def update_timestamp(self):
        self.updated_at = datetime.utcnow()