from typing import Optional
from pydantic import BaseModel


class ScrapingStatus(BaseModel):
    """Response model for scraping and downloading status"""
    id: str
    status: str
    progress: float = 0.0
    pages_scraped: int = 0
    files_downloaded: int = 0
    error: Optional[str] = None