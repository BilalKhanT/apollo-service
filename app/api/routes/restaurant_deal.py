from fastapi import APIRouter, BackgroundTasks, HTTPException, status, Query
from typing import List, Optional
import logging
from app.models.restaurant_deals.restaurant_deal_model import (
    RestaurantRequest, 
    RestaurantResponse, 
    RestaurantStopResponse
)
from app.models.base import ErrorResponse
from app.controllers.restaurant_deal_controller import RestaurantController
from app.utils.orchestrator import orchestrator

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/restaurant", tags=["Restaurant Deals Scraper"])

@router.post(
    "",
    response_model=RestaurantResponse,
    responses={
        201: {
            "description": "Restaurant scraping task created successfully",
            "model": RestaurantResponse
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
    summary="Start a new restaurant deals scraping operation",
    description="Initiates a new restaurant deals scraping task for the specified cities. The task runs asynchronously and returns immediately with a task ID for tracking progress."
)
async def start_restaurant_scraping(
    request: RestaurantRequest, 
    background_tasks: BackgroundTasks
) -> RestaurantResponse:
    try:
        restaurant_status = await RestaurantController.start_restaurant_scraping(
            cities=request.cities
        )

        background_tasks.add_task(
            run_restaurant_scraping_background,
            task_id=restaurant_status.id,
            cities=request.cities
        )
        
        return RestaurantResponse(
            success=True,
            message="Restaurant scraping task started successfully",
            data=restaurant_status
        )
        
    except ValueError as e:
        logger.error(f"Validation error starting restaurant scraping: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid request parameters: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error starting restaurant scraping: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start restaurant scraping task: {str(e)}"
        )

async def run_restaurant_scraping_background(
    task_id: str,
    cities: List[str]
):
    """
    Background task function to run restaurant scraping.
    """
    try:
        await orchestrator.run_restaurant_scraping(
            task_id=task_id,
            cities=cities
        )
    except Exception as e:
        logger.error(f"Error in background restaurant scraping task {task_id}: {str(e)}")

@router.get(
    "/{task_id}",
    response_model=RestaurantResponse,
    responses={
        200: {
            "description": "Restaurant scraping status retrieved successfully",
            "model": RestaurantResponse
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
    summary="Get restaurant scraping task status",
    description="Retrieve the current status and progress of a restaurant scraping task by its ID."
)
async def get_restaurant_status(task_id: str) -> RestaurantResponse:
    try:
        restaurant_status = await RestaurantController.get_restaurant_status(task_id)
        return RestaurantResponse(
            success=True,
            message="Restaurant scraping status retrieved successfully",
            data=restaurant_status
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting restaurant scraping status for task {task_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve restaurant scraping status: {str(e)}"
        )

@router.post(
    "/{task_id}/stop",
    response_model=RestaurantStopResponse,
    responses={
        200: {
            "description": "Restaurant scraping task stopped successfully",
            "model": RestaurantStopResponse
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
    summary="Stop a running restaurant scraping task",
    description="Gracefully stop a running restaurant scraping task and perform cleanup operations."
)
async def stop_restaurant_task(
    task_id: str, 
) -> RestaurantStopResponse:
    try:
        result = await RestaurantController.stop_restaurant_task(task_id)
        return RestaurantStopResponse(
            success=result.get("success", True),
            message=result.get("message", "Restaurant scraping task stopped successfully"),
            task_id=task_id
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error stopping restaurant scraping task {task_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to stop restaurant scraping task: {str(e)}"
        )

@router.get(
    "",
    response_model=List[RestaurantResponse],
    responses={
        200: {
            "description": "Restaurant scraping history retrieved successfully"
        },
        500: {
            "description": "Internal server error",
            "model": ErrorResponse
        }
    },
    summary="Get restaurant scraping task history",
    description="Retrieve the history of all restaurant scraping tasks with their current status and progress information."
)
async def get_restaurant_history(
    limit: int = Query(
        10, 
        ge=1, 
        le=100, 
        description="Maximum number of tasks to return",
        example=10
    )
) -> List[RestaurantResponse]:
    try:
        history = await RestaurantController.get_restaurant_history(limit=limit)
        
        # Convert each status to a response
        responses = []
        for restaurant_status in history:
            responses.append(RestaurantResponse(
                success=True,
                message="Restaurant scraping task retrieved successfully",
                data=restaurant_status
            ))
        
        return responses
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting restaurant scraping history: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve restaurant scraping history: {str(e)}"
        )