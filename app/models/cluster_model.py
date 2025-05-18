from typing import Any, Dict, List
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