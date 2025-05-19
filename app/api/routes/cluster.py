from fastapi import APIRouter, HTTPException, Query
from typing import Optional
import logging
from app.core.task_manager import task_manager
from app.core.orchestrator import orchestrator

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["Clusters"])

@router.get("/clusters")
async def get_clusters(
    crawl_task_id: Optional[str] = Query(None, description="ID of the crawl task to get clusters and years for")
):
    url_clusters_file = None
    year_clusters_file = None
    
    response = {
        "clusters": [],
        "years": [],
        "clusters_available": False,
        "years_available": False
    }
    
    if crawl_task_id:
        logger.info(f"Retrieving clusters for crawl task: {crawl_task_id}")
        task_status = task_manager.get_task_status(crawl_task_id)
        
        if not task_status:
            raise HTTPException(status_code=404, detail=f"Task {crawl_task_id} not found")
        
        if task_status["status"] != "completed":
            raise HTTPException(status_code=400, detail=f"Task {crawl_task_id} is not completed")
        
        result = task_status.get("result", {})
        output_files = result.get("output_files", {})
        
        if result.get("cluster_complete", False):
            url_clusters_file = output_files.get("url_clusters_file")
            response["clusters_available"] = True
        else:
            response["clusters_error"] = f"Task {crawl_task_id} does not have cluster results"
        
        if result.get("year_extraction_complete", False):
            year_clusters_file = output_files.get("year_clusters_file")
            response["years_available"] = True
        else:
            response["years_error"] = f"Task {crawl_task_id} does not have year extraction results"
    
    if crawl_task_id is None or response["clusters_available"]:
        try:
            clusters = orchestrator.get_available_clusters(url_clusters_file)
            response["clusters"] = clusters
        except Exception as e:
            logger.error(f"Error retrieving clusters: {str(e)}")
            response["clusters_error"] = f"Error retrieving clusters: {str(e)}"
    
    if crawl_task_id is None or response["years_available"]:
        try:
            years = orchestrator.get_available_years(year_clusters_file)
            response["years"] = years
        except Exception as e:
            logger.error(f"Error retrieving years: {str(e)}")
            response["years_error"] = f"Error retrieving years: {str(e)}"
    
    return response