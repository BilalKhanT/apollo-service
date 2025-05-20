from fastapi import APIRouter, Query
from typing import Optional
from app.controllers.cluster_controller import ClusterController

router = APIRouter(prefix="/api", tags=["Clusters"])

@router.get("/clusters")
async def get_clusters(
    crawl_task_id: Optional[str] = Query(None, description="ID of the crawl task to get clusters and years for")
):
    return await ClusterController.get_clusters(crawl_task_id)