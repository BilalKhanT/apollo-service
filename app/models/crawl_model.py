from pydantic import BaseModel, Field
from typing import Optional

class CrawlRequest(BaseModel):
    base_url: str
    max_links_to_scrape: Optional[int] = None
    max_pages_to_scrape: Optional[int] = None
    depth_limit: Optional[int] = None
    domain_restriction: bool = True
    scrape_pdfs_and_xls: bool = True
    stop_scraper: bool = False

class CrawlStatus(BaseModel):
    id: str
    status: str
    progress: float = Field(default=0.0, description="Progress percentage (0-100)")
    current_stage: str
    links_found: int = 0
    pages_scraped: int = 0
    error: Optional[str] = None
    clusters_ready: bool = False