from pydantic import BaseModel, Field
from typing import List, Dict, Optional, Union, Any

class CrawlRequest(BaseModel):
    base_url: str
    max_links_to_scrape: Optional[int] = None
    max_pages_to_scrape: Optional[int] = None
    depth_limit: Optional[int] = None
    domain_restriction: bool = True
    scrape_pdfs_and_xls: bool = True
    stop_scraper: bool = False

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

class Cluster(BaseModel):
    """Model for a URL cluster"""
    id: str
    path: str
    url_count: int
    urls: List[str]

class DomainCluster(BaseModel):
    """Model for a domain and its clusters"""
    id: str
    count: int
    clusters: List[Cluster]

class ClusterResult(BaseModel):
    """Response model for clustering results"""
    summary: Dict[str, Any]
    clusters: Dict[str, DomainCluster]

class YearCluster(BaseModel):
    """Model for files clustered by year"""
    year: str
    files: List[str]

class ScrapingRequest(BaseModel):
    """Request model for initiating scraping and downloading"""
    cluster_ids: List[str] = Field(description="IDs of clusters to scrape")
    years: List[str] = Field(default=[], description="Years of files to download")

class ScrapingStatus(BaseModel):
    """Response model for scraping and downloading status"""
    id: str
    status: str
    progress: float = 0.0
    pages_scraped: int = 0
    files_downloaded: int = 0
    error: Optional[str] = None