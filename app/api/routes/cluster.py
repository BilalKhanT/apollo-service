from fastapi import APIRouter, Query
from typing import Optional
from app.controllers.cluster_controller import ClusterController
from app.models.cluster_model import ClusterDetailResponse, YearDetailResponse

router = APIRouter(prefix="/api", tags=["Clusters"])

@router.get("/clusters")
async def get_clusters(
    crawl_task_id: Optional[str] = Query(None, description="ID of the crawl task to get clusters and years for")
):
    return await ClusterController.get_clusters(crawl_task_id)

@router.get("/tasks/{task_id}/clusters/{cluster_id}", response_model=ClusterDetailResponse)
async def get_cluster_by_id(
    task_id: str,
    cluster_id: str
):
    return await ClusterController.get_cluster_by_id(cluster_id, task_id)

@router.get("/tasks/{task_id}/years/{year}", response_model=YearDetailResponse)
async def get_year_by_id(
    task_id: str,
    year: str
):
    return await ClusterController.get_year_by_id(year, task_id)