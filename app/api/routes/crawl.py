from fastapi import APIRouter, BackgroundTasks, HTTPException, Response, Query
from typing import Dict, Any, List
import json
import logging
import asyncio
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
    """
    Start a new crawl task with database integration.
    """
    # Create the crawl task
    crawl_status = await CrawlController.start_crawl(
        base_url=request.base_url,
        max_links_to_scrape=request.max_links_to_scrape,
        max_pages_to_scrape=request.max_pages_to_scrape,
        depth_limit=request.depth_limit,
        domain_restriction=request.domain_restriction,
        scrape_pdfs_and_xls=request.scrape_pdfs_and_xls,
        stop_scraper=request.stop_scraper
    )
    
    # Start the background crawl process with database integration
    background_tasks.add_task(
        run_crawl_with_async_wrapper,
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

async def run_crawl_with_async_wrapper(
    task_id: str,
    base_url: str,
    max_links_to_scrape: int = None,
    max_pages_to_scrape: int = None,
    depth_limit: int = None,
    domain_restriction: bool = True,
    scrape_pdfs_and_xls: bool = True,
    stop_scraper: bool = False
):
    """
    Async wrapper for the crawl orchestrator to handle database operations.
    """
    try:
        # Run the crawl workflow with database integration
        result = await orchestrator.run_crawl(
            task_id=task_id,
            base_url=base_url,
            max_links_to_scrape=max_links_to_scrape,
            max_pages_to_scrape=max_pages_to_scrape,
            depth_limit=depth_limit,
            domain_restriction=domain_restriction,
            scrape_pdfs_and_xls=scrape_pdfs_and_xls,
            stop_scraper=stop_scraper
        )
        logger.info(f"Crawl task {task_id} completed successfully")
        return result
    except Exception as e:
        logger.error(f"Error in crawl task {task_id}: {str(e)}")
        # The error will be handled by the orchestrator and stored in task manager
        raise

@router.get("/{task_id}", response_model=CrawlStatus)
async def get_crawl_status(task_id: str):
    """
    Get the status of a crawl task.
    """
    return await CrawlController.get_crawl_status(task_id)

@router.post("/{task_id}/stop")
async def stop_crawl_task(task_id: str) -> Dict[str, Any]:
    """
    Stop a running crawl task.
    """
    try:
        return await CrawlController.stop_crawl_task(task_id)
    except HTTPException as e:
        return Response(
            status_code=e.status_code,
            content=json.dumps({"detail": e.detail}),
            media_type="application/json"
        )

@router.get("/history/list")
async def get_crawl_history(limit: int = 10) -> Dict[str, Any]:
    """
    Get crawl history from database.
    
    Args:
        limit: Maximum number of crawls to return (default: 10)
    """
    try:
        history = await CrawlController.get_crawl_history(limit)
        return {
            "crawls": history,
            "count": len(history)
        }
    except HTTPException as e:
        return Response(
            status_code=e.status_code,
            content=json.dumps({"detail": e.detail}),
            media_type="application/json"
        )

@router.get("/results/all")
async def get_all_crawl_results(
    limit: int = Query(50, description="Maximum number of results to return", ge=1, le=100)
) -> Dict[str, Any]:
    """
    Get all crawl results from database.
    
    Args:
        limit: Maximum number of results to return (default: 50, max: 100)
        
    Returns:
        List of all crawl results with their completion status and summaries
    """
    try:
        return await CrawlController.get_all_crawl_results(limit)
    except HTTPException as e:
        return Response(
            status_code=e.status_code,
            content=json.dumps({"detail": e.detail}),
            media_type="application/json"
        )

@router.get("/details/{crawl_result_id}")
async def get_crawl_details(crawl_result_id: str) -> Dict[str, Any]:
    """
    Get detailed information about a specific crawl from database.
    
    Args:
        crawl_result_id: Database ID of the crawl result
    """
    try:
        return await CrawlController.get_crawl_details(crawl_result_id)
    except HTTPException as e:
        return Response(
            status_code=e.status_code,
            content=json.dumps({"detail": e.detail}),
            media_type="application/json"
        )