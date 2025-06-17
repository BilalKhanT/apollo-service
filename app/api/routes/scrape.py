from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, status
from typing import Dict, List, Optional
import logging
from app.models.apollo_scrape.scrape_model import (
    ScrapeCleanupResponse,
    ScrapingRequest, 
    ScrapingResponse
)
from app.models.base import ErrorResponse
from app.controllers.apollo_scrape.scrape_controller import ScrapeController
from app.utils.orchestrator import orchestrator

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/scrape", tags=["Scraping"])

@router.post(
    "",
    response_model=ScrapingResponse,
    responses={
        201: {
            "description": "Scraping task created successfully",
            "model": ScrapingResponse
        },
        400: {
            "description": "Invalid request parameters",
            "model": ErrorResponse
        },
        404: {
            "description": "Crawl result not found",
            "model": ErrorResponse
        },
        500: {
            "description": "Internal server error",
            "model": ErrorResponse
        }
    },
    status_code=status.HTTP_201_CREATED,
    summary="Start a new scraping and downloading operation",
    description="Initiates content scraping from specified clusters and file downloading for specified years."
)
async def start_scrape(
    request: ScrapingRequest, 
    background_tasks: BackgroundTasks,
    crawl_task_id: Optional[str] = Query(
        None, 
        description="ID of the crawl task to use for scraping (uses most recent if not specified)",
        example="123e4567-e89b-12d3-a456-426614174000"
    )
) -> ScrapingResponse:
    try:
        scrape_status = await ScrapeController.start_scrape(
            bot_id=request.bot_id,
            cluster_data=request.cluster_ids,  
            year_data=request.years,          
            crawl_task_id=crawl_task_id or request.crawl_task_id
        )

        background_tasks.add_task(
            run_scrape_background,
            bot_id=request.bot_id,
            task_id=scrape_status.id,
            cluster_data=request.cluster_ids, 
            year_data=request.years,          
            crawl_task_id=crawl_task_id or request.crawl_task_id
        )
        
        return ScrapingResponse(
            success=True,
            message="Scraping task started successfully",
            data=scrape_status
        )
        
    except HTTPException:
        raise
    except ValueError as e:
        logger.error(f"Validation error starting scrape: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid request parameters: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error starting scrape: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start scraping task: {str(e)}"
        )

async def run_scrape_background(
    bot_id: str,
    task_id: str,
    cluster_data: Dict[str, List[str]], 
    year_data: Dict[str, List[str]] = None,  
    crawl_task_id: str = None
):
    try:
        await orchestrator.run_scrape_download(
            bot_id=bot_id,
            task_id=task_id,
            cluster_data=cluster_data,  
            year_data=year_data,       
            crawl_task_id=crawl_task_id
        )
    except Exception as e:
        logger.error(f"Error in background scrape task {task_id}: {str(e)}")

@router.get(
    "/{task_id}",
    response_model=ScrapingResponse,
    responses={
        200: {
            "description": "Scraping status retrieved successfully",
            "model": ScrapingResponse
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
    summary="Get scraping task status",
    description="Retrieve the current status and progress of a scraping task by its ID."
)
async def get_scrape_status(task_id: str) -> ScrapingResponse:
    try:
        scrape_status = await ScrapeController.get_scrape_status(task_id)
        return ScrapingResponse(
            success=True,
            message="Scraping status retrieved successfully",
            data=scrape_status
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting scrape status for task {task_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve scraping status: {str(e)}"
        )
    
@router.post(
    "/web/{task_id}/cleanup",
    response_model=ScrapeCleanupResponse, 
    responses={
        200: {
            "description": "Web cleanup task started successfully"
        },
        404: {
            "description": "Task not found",
            "model": ErrorResponse
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
    summary="Start cleanup for a web scraping task",
    description="Initiates cleanup and data processing for a completed web scraping task."
)
async def start_web_cleanup(
    task_id: str,
    background_tasks: BackgroundTasks
) -> ScrapeCleanupResponse:
    try:
        cleanup_task_info = await ScrapeController.start_web_cleanup(task_id)
        
        background_tasks.add_task(
            run_web_cleanup_background,
            original_task_id=task_id,
            cleanup_task_id=cleanup_task_info["task_id"]
        )
        
        return cleanup_task_info
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error starting web cleanup: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start web cleanup task: {str(e)}"
        )

async def run_web_cleanup_background(
    original_task_id: str,
    cleanup_task_id: str
):
    try:
        await orchestrator.run_web_cleanup(
            original_task_id=original_task_id,
            cleanup_task_id=cleanup_task_id
        )
    except Exception as e:
        logger.error(f"Error in background web cleanup task {cleanup_task_id}: {str(e)}")