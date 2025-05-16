from typing import Any, Dict
from pydantic import BaseModel
from app.models.domain_cluster import DomainCluster


class ClusterResult(BaseModel):
    """Response model for clustering results"""
    summary: Dict[str, Any]
    clusters: Dict[str, DomainCluster]