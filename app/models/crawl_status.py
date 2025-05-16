from typing import Optional
from pydantic import BaseModel, Field


class CrawlStatus(BaseModel):
    """Response model for crawl status"""
    id: str
    status: str
    progress: float = Field(default=0.0, description="Progress percentage (0-100)")
    current_stage: str
    links_found: int = 0
    pages_scraped: int = 0
    error: Optional[str] = None
    clusters_ready: bool = False