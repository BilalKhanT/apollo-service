from fastapi import APIRouter, BackgroundTasks, HTTPException, Depends, Query, Response, status
from typing import List, Dict, Any
import json
import logging

from app.models import CrawlRequest, CrawlStatus
from app.utils.task_manager import task_manager
from app.utils.orchestrator import orchestrator

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/crawl", tags=["Crawling"])

@router.post("", response_model=CrawlStatus)
async def start_crawl(
    request: CrawlRequest, 
    background_tasks: BackgroundTasks
):
    task_id = task_manager.create_task(
        task_type="crawl",
        params=request.dict()
    )
    
    background_tasks.add_task(
        orchestrator.run_crawl,
        task_id=task_id,
        base_url=request.base_url,
        max_links_to_scrape=request.max_links_to_scrape,
        max_pages_to_scrape=request.max_pages_to_scrape,
        depth_limit=request.depth_limit,
        domain_restriction=request.domain_restriction,
        scrape_pdfs_and_xls=request.scrape_pdfs_and_xls,
        stop_scraper=request.stop_scraper
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
async def get_crawl_status(task_id: str):
    task_status = task_manager.get_task_status(task_id)
    
    if not task_status:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
    
    result = task_status.get("result") or {}
    
    if task_status["type"] != "crawl":
        raise HTTPException(status_code=400, detail=f"Task {task_id} is not a crawl task")
    
    crawl_results = result.get("crawl_results", {})
    links_found = crawl_results.get("total_links_found", 0)
    pages_scraped = crawl_results.get("total_pages_scraped", 0)
    
    clusters_ready = (
        result.get("cluster_complete", False) and
        result.get("year_extraction_complete", False)
    )
    
    current_stage = task_status["status"]
    
    return CrawlStatus(
        id=task_id,
        status=task_status["status"],
        progress=task_status["progress"],
        current_stage=current_stage,
        links_found=links_found,
        pages_scraped=pages_scraped,
        error=task_status.get("error"),
        clusters_ready=clusters_ready
    )

@router.post("/{task_id}/stop")
async def stop_crawl_task(task_id: str) -> Dict[str, Any]:
    task_status = task_manager.get_task_status(task_id)
    
    if not task_status:
        return Response(
            status_code=status.HTTP_404_NOT_FOUND,
            content=json.dumps({"detail": f"Task {task_id} not found"}),
            media_type="application/json"
        )
    
    if task_status.get("type") != "crawl":
        return Response(
            status_code=status.HTTP_400_BAD_REQUEST,
            content=json.dumps({"detail": f"Task {task_id} is not a crawl task"}),
            media_type="application/json"
        )
    
    stop_result = orchestrator.stop_crawl(task_id)
    
    if stop_result.get("success"):
        return {
            "id": task_id,
            "status": "stopped",
            "message": stop_result.get("message"),
            "cleanup_completed": stop_result.get("cleanup_completed", False)
        }
    else:
        return Response(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=json.dumps({
                "detail": stop_result.get("message", "Failed to stop task"),
                "id": task_id
            }),
            media_type="application/json"
        )