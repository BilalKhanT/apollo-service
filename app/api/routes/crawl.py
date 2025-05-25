from fastapi import APIRouter, BackgroundTasks, HTTPException, status
import logging
from app.models.crawl_model import (
    CrawlRequest, 
    CrawlResponse, 
    CrawlStopResponse
)
from app.models.base import ErrorResponse
from app.controllers.crawl_controller import CrawlController
from app.utils.orchestrator import orchestrator

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/crawl", tags=["Crawling"])

@router.post(
    "",
    response_model=CrawlResponse,
    responses={
        201: {
            "description": "Crawl task created successfully",
            "model": CrawlResponse
        },
        400: {
            "description": "Invalid request parameters",
            "model": ErrorResponse
        },
        500: {
            "description": "Internal server error",
            "model": ErrorResponse
        }
    },
    status_code=status.HTTP_201_CREATED,
    summary="Start a new crawl operation",
    description="Initiates a new web crawling task with the specified parameters. The task runs asynchronously and returns immediately with a task ID for tracking progress."
)
async def start_crawl(
    request: CrawlRequest, 
    background_tasks: BackgroundTasks
) -> CrawlResponse:
    try:
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
        
        return CrawlResponse(
            success=True,
            message="Crawl task started successfully",
            data=crawl_status
        )
        
    except ValueError as e:
        logger.error(f"Validation error starting crawl: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid request parameters: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error starting crawl: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start crawl task: {str(e)}"
        )

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

@router.get(
    "/{task_id}",
    response_model=CrawlResponse,
    responses={
        200: {
            "description": "Crawl status retrieved successfully",
            "model": CrawlResponse
        },
        404: {
            "description": "Task not found",
            "model": ErrorResponse
        },
        400: {
            "description": "Invalid task type",
            "model": ErrorResponse
        }
    },
    summary="Get crawl task status",
    description="Retrieve the current status and progress of a crawling task by its ID."
)
async def get_crawl_status(task_id: str) -> CrawlResponse:
    try:
        crawl_status = await CrawlController.get_crawl_status(task_id)
        return CrawlResponse(
            success=True,
            message="Crawl status retrieved successfully",
            data=crawl_status
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting crawl status for task {task_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve crawl status: {str(e)}"
        )

@router.post(
    "/{task_id}/stop",
    response_model=CrawlStopResponse,
    responses={
        200: {
            "description": "Crawl task stopped successfully",
            "model": CrawlStopResponse
        },
        404: {
            "description": "Task not found",
            "model": ErrorResponse
        },
        400: {
            "description": "Invalid task type or already stopped",
            "model": ErrorResponse
        },
        500: {
            "description": "Failed to stop task",
            "model": ErrorResponse
        }
    },
    summary="Stop a running crawl task",
    description="Gracefully stop a running crawl task and perform cleanup operations."
)
async def stop_crawl_task(
    task_id: str, 
) -> CrawlStopResponse:
    try:
        result = await CrawlController.stop_crawl_task(task_id)
        return CrawlStopResponse(
            success=True,
            message=result.get("message", "Crawl task stopped successfully"),
            task_id=task_id,
            cleanup_completed=result.get("cleanup_completed", False)
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error stopping crawl task {task_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to stop crawl task: {str(e)}"
        )