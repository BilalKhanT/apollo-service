from typing import List, Optional
from pydantic import BaseModel, Field


class ScrapingRequest(BaseModel):
    cluster_ids: List[str] = Field(description="IDs of clusters to scrape")
    years: List[str] = Field(default=[], description="Years of files to download")

class ScrapingStatus(BaseModel):
    id: str
    status: str
    progress: float = 0.0
    pages_scraped: int = 0
    files_downloaded: int = 0
    error: Optional[str] = None