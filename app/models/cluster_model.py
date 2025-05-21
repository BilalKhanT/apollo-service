from typing import Any, Dict, List, Optional
from pydantic import BaseModel


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

class ClusterResult(BaseModel):
    summary: Dict[str, Any]
    clusters: Dict[str, DomainCluster]

class ClusterDetailResponse(BaseModel):
    id: str
    name: str
    type: str  
    url_count: int
    urls: Optional[List[str]] = None
    clusters: Optional[List[Cluster]] = None

class YearDetailResponse(BaseModel):
    year: str
    files_count: int
    files: List[str]