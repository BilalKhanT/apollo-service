from beanie import Document
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field


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
    task_id: str = Field(..., unique=True)
    link_found: int
    pages_scraped: int
    is_scraped: bool = False
    error: Optional[str] = None
    clusters: Optional[Dict[str, DomainCluster]] = None
    yearclusters: Optional[List[YearCluster]] = None

    class Settings:
        name = "crawl_results"
