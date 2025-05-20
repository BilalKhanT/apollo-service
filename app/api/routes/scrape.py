from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from typing import Optional
from app.models import ScrapingRequest, ScrapingStatus
from app.utils.task_manager import task_manager
from app.utils.orchestrator import orchestrator

router = APIRouter(prefix="/api/scrape", tags=["Scraping"])

@router.post("", response_model=ScrapingStatus)
async def start_scrape(
    request: ScrapingRequest, 
    background_tasks: BackgroundTasks,
    crawl_task_id: Optional[str] = Query(None, description="ID of the crawl task to use for scraping")
):
    url_clusters_file = None
    year_clusters_file = None
    
    if crawl_task_id:
        task_status = task_manager.get_task_status(crawl_task_id)
        if not task_status:
            raise HTTPException(status_code=404, detail=f"Task {crawl_task_id} not found")
        
        if task_status["status"] != "completed":
            raise HTTPException(status_code=400, detail=f"Task {crawl_task_id} is not completed")
        
        result = task_status.get("result", {})
        output_files = result.get("output_files", {})
        
        url_clusters_file = output_files.get("url_clusters_file")
        year_clusters_file = output_files.get("year_clusters_file")
    
    task_id = task_manager.create_task(
        task_type="scrape",
        params={
            "cluster_ids": request.cluster_ids,
            "years": request.years,
            "url_clusters_file": url_clusters_file,
            "year_clusters_file": year_clusters_file
        }
    )
    
    background_tasks.add_task(
        orchestrator.run_scrape_download,
        task_id=task_id,
        cluster_ids=request.cluster_ids,
        years=request.years,
        url_clusters_file=url_clusters_file,
        year_clusters_file=year_clusters_file
    )
    
    task_status = task_manager.get_task_status(task_id)
    
    return ScrapingStatus(
        id=task_id,
        status=task_status["status"],
        progress=task_status["progress"],
        pages_scraped=0,
        files_downloaded=0,
        error=task_status.get("error")
    )

@router.get("/{task_id}", response_model=ScrapingStatus)
async def get_scrape_status(task_id: str):
    
    task_status = task_manager.get_task_status(task_id)
    
    if not task_status:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
    
    if task_status.get("type") != "scrape":
        raise HTTPException(status_code=400, detail=f"Task {task_id} is not a scrape task")
    
    pages_scraped = 0
    files_downloaded = 0
    current_status = task_status.get("status", "unknown")
    result = task_status.get("result", {})
    
    if result and isinstance(result, dict):
        scrape_results = result.get("scrape_results", {})
        if isinstance(scrape_results, dict):
            pages_scraped = scrape_results.get("pages_scraped", 0)
        
        if current_status in ["scraping", "preparing"]:
            partial_scrape = result.get("scrape_partial_results", {})
            if isinstance(partial_scrape, dict) and partial_scrape.get("pages_scraped", 0) > 0:
                pages_scraped = partial_scrape.get("pages_scraped", 0)
         
        download_results = result.get("download_results", {})
        if isinstance(download_results, dict):
            files_downloaded = download_results.get("files_downloaded", 0)

        if current_status in ["downloading", "preparing_download"]:
            partial_download = result.get("download_partial_results", {})
            if isinstance(partial_download, dict) and partial_download.get("files_downloaded", 0) > 0:
                files_downloaded = partial_download.get("files_downloaded", 0)
    
    return ScrapingStatus(
        id=task_id,
        status=current_status,
        progress=task_status.get("progress", 0.0),
        pages_scraped=pages_scraped,
        files_downloaded=files_downloaded,
        error=task_status.get("error")
    )