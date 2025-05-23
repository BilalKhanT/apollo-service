from fastapi import APIRouter, BackgroundTasks, Query
from typing import Optional
from app.models import ScrapingRequest, ScrapingStatus
from app.controllers.scrape_controller import ScrapeController
from app.utils.orchestrator import orchestrator

router = APIRouter(prefix="/api/scrape", tags=["Scraping"])

@router.post("", response_model=ScrapingStatus)
async def start_scrape(
    request: ScrapingRequest, 
    background_tasks: BackgroundTasks,
    crawl_task_id: Optional[str] = Query(None, description="ID of the crawl task to use for scraping")
):
    scrape_status = await ScrapeController.start_scrape(
        cluster_ids=request.cluster_ids,
        years=request.years,
        crawl_task_id=crawl_task_id
    )
    
    background_tasks.add_task(
        orchestrator.run_scrape_download,
        task_id=scrape_status.id,
        cluster_ids=request.cluster_ids,
        years=request.years,
        url_clusters_file=None,  
        year_clusters_file=None
    )
    
    return scrape_status

@router.get("/{task_id}", response_model=ScrapingStatus)
async def get_scrape_status(task_id: str):
    return await ScrapeController.get_scrape_status(task_id)