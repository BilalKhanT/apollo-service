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
    """
    Start a scraping task.
    
    Args:
        request: Scraping request with cluster IDs and years
        background_tasks: FastAPI background tasks
        crawl_task_id: Optional ID of the crawl task to use for scraping
    """
    scrape_status = await ScrapeController.start_scrape(
        cluster_ids=request.cluster_ids,
        years=request.years,
        crawl_task_id=crawl_task_id
    )
    
    background_tasks.add_task(
        run_scrape_with_async_wrapper,
        task_id=scrape_status.id,
        cluster_ids=request.cluster_ids,
        years=request.years,
        url_clusters_file=None,  
        year_clusters_file=None
    )
    
    return scrape_status

async def run_scrape_with_async_wrapper(
    task_id: str,
    cluster_ids: list,
    years: list = None,
    url_clusters_file: str = None,
    year_clusters_file: str = None
):
    """
    Async wrapper for the scrape/download orchestrator.
    """
    try:
        # Run the scrape/download workflow
        result = await orchestrator.run_scrape_download(
            task_id=task_id,
            cluster_ids=cluster_ids,
            years=years,
            url_clusters_file=url_clusters_file,
            year_clusters_file=year_clusters_file
        )
        return result
    except Exception as e:
        # Error will be handled by the orchestrator and stored in task manager
        raise

@router.get("/{task_id}", response_model=ScrapingStatus)
async def get_scrape_status(task_id: str):
    """
    Get the status of a scraping task.
    
    Args:
        task_id: ID of the scraping task
    """
    return await ScrapeController.get_scrape_status(task_id)