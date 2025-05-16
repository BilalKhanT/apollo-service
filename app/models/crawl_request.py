from pydantic import BaseModel
from typing import Optional

class CrawlRequest(BaseModel):
    base_url: str
    max_links_to_scrape: Optional[int] = None
    max_pages_to_scrape: Optional[int] = None
    depth_limit: Optional[int] = None
    domain_restriction: bool = True
    scrape_pdfs_and_xls: bool = True
    stop_scraper: bool = False