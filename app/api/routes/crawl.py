from fastapi import APIRouter, BackgroundTasks, HTTPException, Response
from typing import Dict, Any
import json
import logging
from app.models import CrawlRequest, CrawlStatus
from app.controllers.crawl_controller import CrawlController
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
    
    background_tasks.add_task(
        orchestrator.run_crawl,
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