from fastapi import APIRouter, HTTPException, Query
from typing import Optional
import logging
from app.models import ScrapingRequest, ScrapingStatus
from app.core.task_manager import task_manager
from app.core.celery_tasks.scrape_tasks import run_scraper

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/scrape", tags=["Scraping"])

@router.post("", response_model=ScrapingStatus)
async def start_scrape(
    request: ScrapingRequest, 
    crawl_task_id: Optional[str] = Query(None, description="ID of the crawl task to use for scraping")
):
   
    try:
        logger.info(f"Starting scrape for clusters: {request.cluster_ids} and years: {request.years}")
        url_clusters_file = None
        year_clusters_file = None
        
        if crawl_task_id:
            task_status = task_manager.get_task_status(crawl_task_id)
            if not task_status:
                raise HTTPException(status_code=404, detail=f"Task {crawl_task_id} not found")
            
            if task_status["status"] != "completed":
                raise HTTPException(status_code=400, detail=f"Task {crawl_task_id} is not completed")
            
            result = task_status.get("result", {}) or {}  
            output_files = result.get("output_files", {})
            
            url_clusters_file = output_files.get("url_clusters_file")
            year_clusters_file = output_files.get("year_clusters_file")
        
        task_id = task_manager.run_task(
            task_type="scrape",
            task_func=run_scraper,
            params={
                "cluster_ids": request.cluster_ids,
                "years": request.years,
                "url_clusters_file": url_clusters_file,
                "year_clusters_file": year_clusters_file
            }
        )
        
        task_status = task_manager.get_task_status(task_id)
        if not task_status:
            raise HTTPException(status_code=500, detail="Failed to create task")
            
        return ScrapingStatus(
            id=task_id,
            status=task_status["status"],
            progress=task_status["progress"],
            pages_scraped=0,
            files_downloaded=0,
            error=task_status.get("error")
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error starting scrape: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error starting scrape: {str(e)}")

@router.get("/{task_id}", response_model=ScrapingStatus)
async def get_scrape_status(task_id: str):

    try:
        logger.info(f"Getting status for scrape task: {task_id}")
        task_status = task_manager.get_task_status(task_id)
        
        if not task_status:
            raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
        
        if task_status["type"] != "scrape":
            raise HTTPException(status_code=400, detail=f"Task {task_id} is not a scrape task")
        
        result = task_status.get("result")
        if result is None:
            result = {}
            
        scrape_results = result.get("scrape_results", {})
        download_results = result.get("download_results", {})
        pages_scraped = scrape_results.get("pages_scraped", 0)
        files_downloaded = download_results.get("files_downloaded", 0)
        
        return ScrapingStatus(
            id=task_id,
            status=task_status["status"],
            progress=task_status["progress"],
            pages_scraped=pages_scraped,
            files_downloaded=files_downloaded,
            error=task_status.get("error")
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting scrape status: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error getting scrape status: {str(e)}")