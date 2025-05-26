from fastapi import APIRouter, BackgroundTasks, HTTPException, Path, Query, status
from typing import Optional
import logging
from app.models.restaurant_deal.restaurant_model import (
    DealResultSummary,
    DealScrapingRequest,
    DealScrapingResponse, 
    DealResultsResponse,
    DealStopResponse,
    DealResultsResponseMinimal
)
from app.models.base import ErrorResponse
from app.controllers.restaurant_deal.deal_scrape_controller import DealScrapeController
from app.utils.orchestrator import orchestrator

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/deals", tags=["Deal Scraping"])

@router.post(
    "",
    response_model=DealScrapingResponse,
    responses={
        201: {
            "description": "Deal scraping task created successfully",
            "model": DealScrapingResponse
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
    summary="Start a new deal scraping operation",
    description="Initiates a new restaurant deal scraping task. If cities list is empty, all available cities will be fetched and processed."
)
async def start_deal_scraping(
    request: DealScrapingRequest,
    background_tasks: BackgroundTasks
) -> DealScrapingResponse:
    try:
        task_info = await DealScrapeController.start_deal_scraping(cities=request.cities)
        background_tasks.add_task(
            run_deal_scraping_background,
            task_id=task_info["task_id"],
            cities=request.cities
        )
        
        return DealScrapingResponse(
            success=True,
            message=f"Deal scraping started for {len(request.cities) if request.cities else 'all available'} cities",
            task_id=task_info["task_id"],
            cities_requested=request.cities
        )
        
    except HTTPException:
        raise
    except ValueError as e:
        logger.error(f"Validation error starting deal scraping: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid request parameters: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error starting deal scraping: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start deal scraping task: {str(e)}"
        )

async def run_deal_scraping_background(task_id: str, cities: list):
    try:
        await orchestrator.run_deal_scraping(
            task_id=task_id,
            cities=cities
        )
    except Exception as e:
        logger.error(f"Error in background deal scraping task {task_id}: {str(e)}")

@router.post(
    "/{task_id}/stop",
    response_model=DealStopResponse,
    responses={
        200: {
            "description": "Deal scraping task stopped successfully",
            "model": DealStopResponse
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
    summary="Stop a running deal scraping task",
    description="Stop a running deal scraping task and perform cleanup operations."
)
async def stop_deal_scraping_task(task_id: str) -> DealStopResponse:
    try:
        result = await DealScrapeController.stop_deal_scraping_task(task_id)
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error stopping deal scraping task {task_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to stop deal scraping task: {str(e)}"
        )

@router.get(
    "/results",
    response_model=DealResultsResponseMinimal,
    responses={
        200: {
            "description": "Deal scraping results retrieved successfully",
            "model": DealResultsResponse
        },
        500: {
            "description": "Internal server error",
            "model": ErrorResponse
        }
    },
    summary="Get all past deal scraping results",
    description="Retrieve a paginated list of all completed deal scraping results from the database."
)
async def get_all_deal_results(
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
) -> DealResultsResponse:
    try:
        results_data = await DealScrapeController.get_all_deal_results(
            page=page,
            page_size=page_size,
            limit=limit
        )
        
        return DealResultsResponseMinimal(
            success=True,
            message=f"Retrieved {results_data['returned_count']} deal scraping results",
            data=results_data["data"],
            total_count=results_data["total_count"],
            page=results_data["page"],
            page_size=results_data["page_size"],
            has_more=results_data["has_more"]
        )
        
    except Exception as e:
        logger.error(f"Error retrieving deal scraping results: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve deal scraping results: {str(e)}"
        )
    
@router.get("/result/{task_id}", response_model=DealResultSummary)
async def get_deal_result_by_task_id(
    task_id: str = Path(..., description="Task ID of the deal scraping operation")
):
    """
    Get the result of a specific deal scraping task by task ID.
    """
    result = await DealScrapeController.get_deal_result_by_task_id(task_id)
    
    if not result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No deal result found for task ID {task_id}"
        )
    
    return result
