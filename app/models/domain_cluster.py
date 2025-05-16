from typing import List
from pydantic import BaseModel
from app.models.cluster import Cluster


class DomainCluster(BaseModel):
    """Model for a domain and its clusters"""
    id: str
    count: int
    clusters: List[Cluster]