from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field
from app.models.base import BaseResponse

class Cluster(BaseModel):
    id: str = Field(description="Unique cluster identifier", example="1.1")
    path: str = Field(description="URL path pattern for this cluster", example="/products")
    url_count: int = Field(description="Number of URLs in this cluster", example=25)
    urls: Optional[List[str]] = Field(default=None, description="List of URLs in this cluster")
    
    class Config:
        schema_extra = {
            "example": {
                "id": "1.1",
                "path": "/products",
                "url_count": 25,
                "urls": [
                    "https://example.com/products/item1",
                    "https://example.com/products/item2"
                ]
            }
        }

class DomainCluster(BaseModel):
    id: str = Field(description="Unique domain cluster identifier", example="1")
    domain: str = Field(description="Domain name", example="example.com")
    count: int = Field(description="Total URLs in this domain", example=150)
    clusters: List[Cluster] = Field(description="List of path-based clusters within this domain")
    
    class Config:
        schema_extra = {
            "example": {
                "id": "1",
                "domain": "example.com",
                "count": 150,
                "clusters": [
                    {
                        "id": "1.1",
                        "path": "/products",
                        "url_count": 25,
                        "urls": ["https://example.com/products/item1"]
                    }
                ]
            }
        }

class YearCluster(BaseModel):
    year: str = Field(description="Year identifier", example="2024")
    files_count: int = Field(description="Number of files for this year", example=45)
    files: List[str] = Field(description="List of file URLs for this year")
    
    class Config:
        schema_extra = {
            "example": {
                "year": "2024",
                "files_count": 45,
                "files": [
                    "https://example.com/files/2024/report.pdf",
                    "https://example.com/files/2024/data.xlsx"
                ]
            }
        }

class ClusterSummary(BaseModel):
    total_domains: int = Field(description="Number of domains found", example=3)
    total_clusters: int = Field(description="Total number of clusters", example=15)
    total_urls: int = Field(description="Total number of URLs clustered", example=1250)
    
    class Config:
        schema_extra = {
            "example": {
                "total_domains": 3,
                "total_clusters": 15,
                "total_urls": 1250
            }
        }

class ClusterResult(BaseModel):
    summary: ClusterSummary = Field(description="Clustering summary statistics")
    clusters: Dict[str, DomainCluster] = Field(description="Domain-based clusters")
    
    class Config:
        schema_extra = {
            "example": {
                "summary": {
                    "total_domains": 3,
                    "total_clusters": 15,
                    "total_urls": 1250
                },
                "clusters": {
                    "example.com": {
                        "id": "1",
                        "domain": "example.com",
                        "count": 150,
                        "clusters": []
                    }
                }
            }
        }

class ClusterDetailResponse(BaseModel):
    id: str = Field(description="Cluster identifier", example="1.1")
    name: str = Field(description="Human-readable cluster name", example="example.com - /products")
    type: str = Field(description="Cluster type", example="path")
    url_count: int = Field(description="Number of URLs in cluster", example=25)
    urls: Optional[List[str]] = Field(default=None, description="List of URLs if requested")
    clusters: Optional[List[Cluster]] = Field(default=None, description="Sub-clusters if this is a domain cluster")
    
    class Config:
        schema_extra = {
            "example": {
                "id": "1.1",
                "name": "example.com - /products",
                "type": "path",
                "url_count": 25,
                "urls": [
                    "https://example.com/products/item1",
                    "https://example.com/products/item2"
                ],
                "clusters": None
            }
        }

class YearDetailResponse(BaseModel):
    year: str = Field(description="Year identifier", example="2024")
    files_count: int = Field(description="Number of files for this year", example=45)
    files: List[str] = Field(description="List of file URLs")
    
    class Config:
        schema_extra = {
            "example": {
                "year": "2024",
                "files_count": 45,
                "files": [
                    "https://example.com/files/2024/report.pdf",
                    "https://example.com/files/2024/data.xlsx"
                ]
            }
        }

class ClustersListResponse(BaseResponse):
    clusters: List[Dict[str, Any]] = Field(description="List of available clusters")
    years: List[Dict[str, Any]] = Field(description="List of available years")
    clusters_available: bool = Field(description="Whether clusters are available")
    years_available: bool = Field(description="Whether years are available")
    clusters_error: Optional[str] = Field(default=None, description="Error message for clusters")
    years_error: Optional[str] = Field(default=None, description="Error message for years")
    
    class Config:
        schema_extra = {
            "example": {
                "success": True,
                "message": "Clusters and years retrieved successfully",
                "timestamp": "2025-01-27T16:00:00.000Z",
                "clusters": [
                    {
                        "id": "1.1",
                        "name": "example.com - /products",
                        "type": "path",
                        "url_count": 25
                    }
                ],
                "years": [
                    {
                        "year": "2024",
                        "files_count": 45
                    }
                ],
                "clusters_available": True,
                "years_available": True,
                "clusters_error": None,
                "years_error": None
            }
        }