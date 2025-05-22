from beanie import Document
from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Annotated
from datetime import datetime
from pymongo import IndexModel, ASCENDING
from beanie import Indexed

class CrawlSummary(BaseModel):
    base_url: str
    total_urls_discovered: int = 0
    total_pages_scraped: int = 0
    total_links_found: int = 0
    total_unique_links: int = 0
    total_direct_document_links: int = 0
    total_404_urls: int = 0
    total_error_urls: int = 0
    crawl_date: str = Field(default_factory=lambda: datetime.now().isoformat())
    domain_restriction: bool = True
    max_depth: Optional[int] = None
    is_complete: bool = False
    execution_time_seconds: float = 0.0

class ProcessSummary(BaseModel):
    total_links: int = 0
    file_links_count: int = 0
    social_media_links_count: int = 0
    bank_links_count: int = 0
    misc_links_count: int = 0
    processing_time_seconds: float = 0.0

class ClusterSummary(BaseModel):
    total_domains: int = 0
    total_clusters: int = 0
    total_urls: int = 0

class YearExtractionSummary(BaseModel):
    total_years: int = 0
    total_files: int = 0

class Cluster(BaseModel):
    id: str
    path: str
    url_count: int
    urls: List[str]

class DomainCluster(BaseModel):
    id: str
    count: int
    clusters: List[Cluster]

# New model to store raw crawl data
class CrawlData(Document):
    crawl_result_id: Annotated[str, Indexed()]
    task_id: Annotated[str, Indexed()]
    all_links: List[str] = Field(default_factory=list)
    document_links: List[str] = Field(default_factory=list)
    not_found_urls: List[str] = Field(default_factory=list)
    error_urls: Dict[str, Dict] = Field(default_factory=dict)
    created_at: Annotated[datetime, Indexed()] = Field(default_factory=datetime.utcnow)
    
    class Settings:
        name = "crawl_data"

# New model to store processed links
class ProcessedLinks(Document):
    crawl_result_id: Annotated[str, Indexed()]
    task_id: Annotated[str, Indexed()]
    file_links: List[str] = Field(default_factory=list)
    social_media_links: List[str] = Field(default_factory=list)
    bank_links: List[str] = Field(default_factory=list) 
    misc_links: List[str] = Field(default_factory=list)
    created_at: Annotated[datetime, Indexed()] = Field(default_factory=datetime.utcnow)
    
    class Settings:
        name = "processed_links"

class CrawlResult(Document):
    task_id: Annotated[str, Indexed(unique=True)]  
    crawl_id: Annotated[str, Indexed(unique=True)] 
    created_at: Annotated[datetime, Indexed()] = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    crawl_complete: bool = False
    process_complete: bool = False
    cluster_complete: bool = False
    year_extraction_complete: bool = False
    scraping_complete: bool = False  # New field to track scraping status
    crawl_summary: Optional[CrawlSummary] = None
    process_summary: Optional[ProcessSummary] = None
    cluster_summary: Optional[ClusterSummary] = None
    year_extraction_summary: Optional[YearExtractionSummary] = None
    output_files: Dict[str, str] = Field(default_factory=dict)
    
    class Settings:
        name = "crawl_results"
        indexes = [
            IndexModel([("crawl_summary.base_url", ASCENDING)]),
            IndexModel([
                ("crawl_complete", ASCENDING),
                ("process_complete", ASCENDING),
                ("cluster_complete", ASCENDING),
                ("year_extraction_complete", ASCENDING),
                ("scraping_complete", ASCENDING)
            ])
        ]

class ClusterDocument(Document):
    crawl_result_id: Annotated[str, Indexed()]  
    task_id: Annotated[str, Indexed()] 
    domain: str
    cluster_data: DomainCluster
    created_at: Annotated[datetime, Indexed()] = Field(default_factory=datetime.utcnow)
    
    class Settings:
        name = "clusters"
        indexes = [
            IndexModel([("domain", ASCENDING)]),
            IndexModel([("cluster_data.id", ASCENDING)])
        ]

class YearDocument(Document):
    crawl_result_id: Annotated[str, Indexed()] 
    task_id: Annotated[str, Indexed()]  
    year: str
    files: List[str]
    files_count: int
    created_at: Annotated[datetime, Indexed()] = Field(default_factory=datetime.utcnow)
    
    class Settings:
        name = "years"
        indexes = [
            IndexModel([("year", ASCENDING)])
        ]

class ClusterResponse(BaseModel):
    id: str
    name: str
    type: str
    url_count: int
    domain: Optional[str] = None
    urls: Optional[List[str]] = None
    clusters: Optional[List[Cluster]] = None

class YearResponse(BaseModel):
    year: str
    files_count: int
    files: Optional[List[str]] = None