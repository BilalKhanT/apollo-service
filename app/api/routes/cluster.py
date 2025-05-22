from fastapi import APIRouter, Query
from typing import Optional
from app.controllers.cluster_controller import ClusterController
from app.models.cluster_model import ClusterDetailResponse, YearDetailResponse

router = APIRouter(prefix="/api", tags=["Clusters"])

@router.get("/clusters")
async def get_clusters(
    crawl_task_id: Optional[str] = Query(None, description="ID of the crawl task to get clusters and years for")
):
    """
    Get available clusters and years from database.
    
    If crawl_task_id is provided, returns clusters/years for that specific crawl.
    If not provided, returns clusters/years from the most recent completed crawl.
    """
    return await ClusterController.get_clusters(crawl_task_id)

@router.get("/tasks/{task_id}/clusters/{cluster_id}", response_model=ClusterDetailResponse)
async def get_cluster_by_id(
    task_id: str,
    cluster_id: str
):
    """
    Get detailed information about a specific cluster from database.
    
    Args:
        task_id: ID of the crawl task
        cluster_id: ID of the cluster
    """
    return await ClusterController.get_cluster_by_id(cluster_id, task_id)

@router.get("/tasks/{task_id}/years/{year}", response_model=YearDetailResponse)
async def get_year_by_id(
    task_id: str,
    year: str
):
    """
    Get detailed information about files for a specific year from database.
    
    Args:
        task_id: ID of the crawl task
        year: Year to get files for
    """
    return await ClusterController.get_year_by_id(year, task_id)