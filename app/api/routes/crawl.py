# app/api/routes/crawl.py

from fastapi import APIRouter, HTTPException, Depends, Query, Response, status
from typing import List, Dict, Any
import json
import logging

from app.models import CrawlRequest, CrawlStatus
from app.core.task_manager import task_manager
from app.core.celery_tasks.crawl_tasks import run_crawler
from app.api.dependencies import get_task_or_404

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/crawl", tags=["Crawling"])

@router.post("", response_model=CrawlStatus)
async def start_crawl(request: CrawlRequest):
   
    logger.info(f"Starting crawl for URL: {request.base_url}")
    task_id = task_manager.run_task(
        task_type="crawl",
        task_func=run_crawler,
        params={
            "base_url": request.base_url,
            "max_links_to_scrape": request.max_links_to_scrape,
            "max_pages_to_scrape": request.max_pages_to_scrape,
            "depth_limit": request.depth_limit,
            "domain_restriction": request.domain_restriction,
            "scrape_pdfs_and_xls": request.scrape_pdfs_and_xls,
            "stop_scraper": request.stop_scraper
        }
    )
    
    task_status = task_manager.get_task_status(task_id)
    
    return CrawlStatus(
        id=task_id,
        status=task_status["status"],
        progress=task_status["progress"],
        current_stage=task_status["status"],
        links_found=0,
        pages_scraped=0,
        error=task_status.get("error"),
        clusters_ready=False
    )

@router.get("/{task_id}", response_model=CrawlStatus)
async def get_crawl_status(task: Dict[str, Any] = Depends(get_task_or_404)):
    
    task_id = task["id"]
    logger.info(f"Getting status for crawl task: {task_id}")
    
    if task["type"] != "crawl":
        raise HTTPException(status_code=400, detail=f"Task {task_id} is not a crawl task")
    
    result = task.get("result") or {}
    
    crawl_results = result.get("crawl_results", {})
    links_found = crawl_results.get("total_links_found", 0)
    pages_scraped = crawl_results.get("total_pages_scraped", 0)
    
    clusters_ready = (
        result.get("cluster_complete", False) and
        result.get("year_extraction_complete", False)
    )
    
    current_stage = task["status"]
    
    return CrawlStatus(
        id=task_id,
        status=task["status"],
        progress=task["progress"],
        current_stage=current_stage,
        links_found=links_found,
        pages_scraped=pages_scraped,
        error=task.get("error"),
        clusters_ready=clusters_ready
    )