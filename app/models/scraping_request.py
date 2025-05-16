from typing import List
from pydantic import BaseModel, Field


class ScrapingRequest(BaseModel):
    """Request model for initiating scraping and downloading"""
    cluster_ids: List[str] = Field(description="IDs of clusters to scrape")
    years: List[str] = Field(default=[], description="Years of files to download")