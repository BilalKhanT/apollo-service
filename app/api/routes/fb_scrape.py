from fastapi import APIRouter, BackgroundTasks, HTTPException, Path, Query, status
from typing import List, Optional
import logging
from app.models.fb_scrape.fb_scrape_model import (
    FacebookResultSummary,
    FacebookScrapingRequest,
    FacebookScrapingResponse, 
    FacebookResultsResponse,
    FacebookStopResponse,
    FacebookResultsResponseMinimal
)
from app.models.base import ErrorResponse
from app.controllers.fb_scrape.fb_scrape_controller import FacebookScrapeController
from app.utils.orchestrator import orchestrator

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/facebook", tags=["Facebook Scraping"])

@router.post(
    "",
    response_model=FacebookScrapingResponse,
    responses={
        201: {
            "description": "Facebook scraping task created successfully",
            "model": FacebookScrapingResponse
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
    summary="Start a new Facebook scraping operation",
    description="Initiates a new Facebook post scraping task with specified keywords and date range."
)
async def start_facebook_scraping(
    request: FacebookScrapingRequest,
    background_tasks: BackgroundTasks
) -> FacebookScrapingResponse:
    try:
        task_info = await FacebookScrapeController.start_facebook_scraping(
            keywords=request.keywords,
            days=request.days,
            # access_token="EAANwmjYfSZAMBO9my96Ipmky8pZCHEkDOu5eXZAaHc7ge2LZCZCsZBz7yoj7O5mfZCHlTLVey0RbZBIUgQTpkqH7goqwQLTw0kWAw4GMaiOh36qIh3jYDX6KYfOqMBVjZBChSlCLNmljS4dswIB9sZCNvQZCXZC3xlMJ9FLDLUyT0dzd9XQBG5nHv4FPSW5hkr8Kt9eO",
            # page_id="185182871519466"
        )
        
        background_tasks.add_task(
            run_facebook_scraping_background,
            task_id=task_info["task_id"],
            keywords=request.keywords,
            days=request.days,
            # access_token="EAANwmjYfSZAMBO9my96Ipmky8pZCHEkDOu5eXZAaHc7ge2LZCZCsZBz7yoj7O5mfZCHlTLVey0RbZBIUgQTpkqH7goqwQLTw0kWAw4GMaiOh36qIh3jYDX6KYfOqMBVjZBChSlCLNmljS4dswIB9sZCNvQZCXZC3xlMJ9FLDLUyT0dzd9XQBG5nHv4FPSW5hkr8Kt9eO",
            # page_id="185182871519466"
        )
        
        return FacebookScrapingResponse(
            success=True,
            message=f"Facebook scraping started for {len(request.keywords)} keywords over {request.days} days",
            task_id=task_info["task_id"],
            keywords_requested=request.keywords,
            days_requested=request.days
        )
        
    except HTTPException:
        raise
    except ValueError as e:
        logger.error(f"Validation error starting Facebook scraping: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid request parameters: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error starting Facebook scraping: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start Facebook scraping task: {str(e)}"
        )

async def run_facebook_scraping_background(
    task_id: str,
    keywords: List[str],
    days: int,
    # access_token: str,
    # page_id: str
):
    try:
        await orchestrator.run_facebook_scraping(
            task_id=task_id,
            keywords=keywords,
            days=days,
            # access_token=access_token,
            # page_id=page_id
        )
    except Exception as e:
        logger.error(f"Error in background Facebook scraping task {task_id}: {str(e)}")

@router.post(
    "/{task_id}/stop",
    response_model=FacebookStopResponse,
    responses={
        200: {
            "description": "Facebook scraping task stopped successfully",
            "model": FacebookStopResponse
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
    summary="Stop a running Facebook scraping task",
    description="Stop a running Facebook scraping task and perform cleanup operations."
)
async def stop_facebook_scraping_task(task_id: str) -> FacebookStopResponse:
    try:
        result = await FacebookScrapeController.stop_facebook_scraping_task(task_id)
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error stopping Facebook scraping task {task_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to stop Facebook scraping task: {str(e)}"
        )

@router.get(
    "/results",
    response_model=FacebookResultsResponseMinimal,
    responses={
        200: {
            "description": "Facebook scraping results retrieved successfully",
            "model": FacebookResultsResponse
        },
        500: {
            "description": "Internal server error",
            "model": ErrorResponse
        }
    },
    summary="Get all past Facebook scraping results",
    description="Retrieve a paginated list of all completed Facebook scraping results from the database."
)
async def get_all_facebook_results(
    page: int = Query(
        1, 
        ge=1, 
        description="Page number for pagination",
        example=1
    ),
    page_size: int = Query(
        50, 
        ge=1, 
        le=100, 
        description="Number of results per page",
        example=50
    ),
    limit: Optional[int] = Query(
        None,
        ge=1,
        le=1000,
        description="Maximum number of results to return (overrides pagination if provided)",
        example=None
    )
) -> FacebookResultsResponse:
    try:
        results_data = await FacebookScrapeController.get_all_facebook_results(
            page=page,
            page_size=page_size,
            limit=limit
        )
        
        return FacebookResultsResponseMinimal(
            success=True,
            message=f"Retrieved {results_data['returned_count']} Facebook scraping results",
            data=results_data["data"],
            total_count=results_data["total_count"],
            page=results_data["page"],
            page_size=results_data["page_size"],
            has_more=results_data["has_more"]
        )
        
    except Exception as e:
        logger.error(f"Error retrieving Facebook scraping results: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve Facebook scraping results: {str(e)}"
        )
    
@router.get("/result/{task_id}", response_model=FacebookResultSummary)
async def get_facebook_result_by_task_id(
    task_id: str = Path(..., description="Task ID of the Facebook scraping operation")
):
    """
    Get the result of a specific Facebook scraping task by task ID.
    """
    result = await FacebookScrapeController.get_facebook_result_by_task_id(task_id)
    
    if not result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No Facebook result found for task ID {task_id}"
        )
    
    return result