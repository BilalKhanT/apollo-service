# app/models/__init__.py
from .crawl_model import CrawlRequest, CrawlStatus
from .scrape_model import ScrapingRequest, ScrapingStatus
from .cluster_model import Cluster, DomainCluster, YearCluster, ClusterResult, ClusterDetailResponse, YearDetailResponse
from .log_model import LogEntry, LogResponse 
# from .database.database_models import (
#     CrawlResult,
#     ClusterDocument,
#     YearDocument,
#     CrawlData,
#     ProcessedLinks,
#     CrawlSummary,
#     ProcessSummary,
#     ClusterSummary,
#     YearExtractionSummary,
#     Cluster as DBCluster,  # Rename to avoid conflict with cluster_model.Cluster
#     DomainCluster as DBDomainCluster  # Rename to avoid conflict with cluster_model.DomainCluster
# )

__all__ = [
    'CrawlRequest',
    'CrawlStatus',
    'ClusterResult',
    'YearCluster',
    'ScrapingRequest',
    'ScrapingStatus',
    'Cluster',
    'DomainCluster',
    'LogEntry',
    'LogResponse',
    'ClusterDetailResponse',
    'YearDetailResponse',
    'CrawlResult',
    'ClusterDocument',
    'YearDocument',
    'CrawlData',
    'ProcessedLinks',
    'CrawlSummary',
    'ProcessSummary',
    'ClusterSummary',
    'YearExtractionSummary',
    'DBCluster',
    'DBDomainCluster',
]