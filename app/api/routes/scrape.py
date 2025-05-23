from fastapi import APIRouter, BackgroundTasks, Query
from typing import Optional
import logging
import asyncio
from app.models import ScrapingRequest, ScrapingStatus
from app.controllers.scrape_controller import ScrapeController
from app.utils.orchestrator import orchestrator

logger = logging.getLogger(__name__)

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
    
    # Run the scrape in background using async task
    background_tasks.add_task(
        run_scrape_background,
        task_id=scrape_status.id,
        cluster_ids=request.cluster_ids,
        years=request.years,
        crawl_task_id=crawl_task_id
    )
    
    return scrape_status

async def run_scrape_background(
    task_id: str,
    cluster_ids: list,
    years: list = None,
    crawl_task_id: str = None
):
    """Run scrape in background with proper async handling"""
    try:
        await orchestrator.run_scrape_download(
            task_id=task_id,
            cluster_ids=cluster_ids,
            years=years,
            crawl_task_id=crawl_task_id
        )
    except Exception as e:
        logger.error(f"Error in background scrape task {task_id}: {str(e)}")

@router.get("/{task_id}", response_model=ScrapingStatus)
async def get_scrape_status(task_id: str):
    return await ScrapeController.get_scrape_status(task_id)