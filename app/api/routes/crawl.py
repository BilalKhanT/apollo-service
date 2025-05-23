from fastapi import APIRouter, BackgroundTasks, HTTPException, Response
from typing import Dict, Any, List
import json
import logging
import asyncio
from app.controllers.crawl_result_controller import CrawlResultController
from app.models import CrawlRequest, CrawlStatus
from app.controllers.crawl_controller import CrawlController
from app.models.database.crawl_result_model import CrawlResult
from app.utils.orchestrator import orchestrator

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/crawl", tags=["Crawling"])

@router.post("", response_model=CrawlStatus)
async def start_crawl(
    request: CrawlRequest, 
    background_tasks: BackgroundTasks
):
    crawl_status = await CrawlController.start_crawl(
        base_url=request.base_url,
        max_links_to_scrape=request.max_links_to_scrape,
        max_pages_to_scrape=request.max_pages_to_scrape,
        depth_limit=request.depth_limit,
        domain_restriction=request.domain_restriction,
        scrape_pdfs_and_xls=request.scrape_pdfs_and_xls,
        stop_scraper=request.stop_scraper
    )
    
    # Run the crawl in background using async task
    background_tasks.add_task(
        run_crawl_background,
        task_id=crawl_status.id,
        base_url=request.base_url,
        max_links_to_scrape=request.max_links_to_scrape,
        max_pages_to_scrape=request.max_pages_to_scrape,
        depth_limit=request.depth_limit,
        domain_restriction=request.domain_restriction,
        scrape_pdfs_and_xls=request.scrape_pdfs_and_xls,
        stop_scraper=request.stop_scraper
    )
    
    return crawl_status

async def run_crawl_background(
    task_id: str,
    base_url: str,
    max_links_to_scrape: int = None,
    max_pages_to_scrape: int = None,
    depth_limit: int = None,
    domain_restriction: bool = True,
    scrape_pdfs_and_xls: bool = True,
    stop_scraper: bool = False
):
    """Run crawl in background with proper async handling"""
    try:
        await orchestrator.run_crawl(
            task_id=task_id,
            base_url=base_url,
            max_links_to_scrape=max_links_to_scrape,
            max_pages_to_scrape=max_pages_to_scrape,
            depth_limit=depth_limit,
            domain_restriction=domain_restriction,
            scrape_pdfs_and_xls=scrape_pdfs_and_xls,
            stop_scraper=stop_scraper
        )
    except Exception as e:
        logger.error(f"Error in background crawl task {task_id}: {str(e)}")

@router.get("/{task_id}", response_model=CrawlStatus)
async def get_crawl_status(task_id: str):
    return await CrawlController.get_crawl_status(task_id)

@router.post("/{task_id}/stop")
async def stop_crawl_task(task_id: str) -> Dict[str, Any]:
    try:
        return await CrawlController.stop_crawl_task(task_id)
    except HTTPException as e:
        return Response(
            status_code=e.status_code,
            content=json.dumps({"detail": e.detail}),
            media_type="application/json"
        )
    
@router.get("/", response_model=List[CrawlResult])
async def get_crawl_results():
    return await CrawlResultController.list_crawl_results()