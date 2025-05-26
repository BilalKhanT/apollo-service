from typing import List, Optional
from fastapi import HTTPException
from app.utils.task_manager import task_manager
from app.utils.realtime_publisher import realtime_publisher
from app.models.restaurant_deals.restaurant_deal_model import RestaurantStatus, RestaurantProgress
from app.models.database.restaurant_result_model import RestaurantResult
from app.models.base import TaskStatus
import logging

logger = logging.getLogger(__name__)


class RestaurantController:
    @staticmethod
    async def start_restaurant_scraping(
        cities: List[str]
    ) -> RestaurantStatus:
        """
        Start a new restaurant scraping task.
        
        Args:
            cities: List of cities to scrape restaurant deals from
            
        Returns:
            RestaurantStatus object with task information
        """
        try:
            # Create task in task manager
            task_id = task_manager.create_task(
                task_type="restaurant",
                params={
                    "cities": cities
                }
            )
            
            task_status = task_manager.get_task_status(task_id)
            
            # Start real-time publishing for this task
            try:
                await realtime_publisher.start_publishing(task_id, interval=2.0)
                logger.info(f"Started real-time publishing for restaurant task {task_id}")
            except Exception as e:
                logger.warning(f"Failed to start real-time publishing for task {task_id}: {str(e)}")
            
            return RestaurantStatus(
                id=task_id,
                status=TaskStatus(task_status["status"]),
                progress=task_status["progress"],
                cities_requested=cities,
                cities_processed=0,
                restaurants_processed=0,
                deals_processed=0,
                error=task_status.get("error")
            )
            
        except Exception as e:
            logger.error(f"Error starting restaurant scraping: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to start restaurant scraping: {str(e)}"
            )
    
    @staticmethod
    async def get_restaurant_status(task_id: str) -> RestaurantStatus:
        """
        Get the current status of a restaurant scraping task.
        
        Args:
            task_id: The ID of the task to get status for
            
        Returns:
            RestaurantStatus object with current task information
        """
        try:
            # Get task status from task manager
            task_status = task_manager.get_task_status(task_id)
            
            if not task_status:
                raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
            
            if task_status.get("type") != "restaurant":
                raise HTTPException(status_code=400, detail=f"Task {task_id} is not a restaurant scraping task")
            
            # Extract progress data from task result
            cities_requested = []
            cities_processed = 0
            restaurants_processed = 0
            deals_processed = 0
            progress_details = None
            
            # Get task parameters
            params = task_status.get("params", {})
            if params:
                cities_requested = params.get("cities", [])
            
            # Get current progress from partial results
            result = task_status.get("result", {})
            if result and isinstance(result, dict):
                # Get partial results for ongoing tasks
                partial_results = result.get("restaurant_partial_results", {})
                if isinstance(partial_results, dict):
                    cities_processed = partial_results.get("cities_processed", 0)
                    restaurants_processed = partial_results.get("restaurants_processed", 0)
                    deals_processed = partial_results.get("deals_processed", 0)
                    
                    # Create progress details
                    progress_details = RestaurantProgress(
                        current_city=partial_results.get("current_city"),
                        current_restaurant=partial_results.get("current_restaurant"),
                        cities_processed=cities_processed,
                        restaurants_processed=restaurants_processed,
                        deals_processed=deals_processed,
                        total_cities=partial_results.get("total_cities", 0),
                        total_restaurants=partial_results.get("total_restaurants", 0)
                    )
                
                # Get final results for completed tasks
                if not partial_results and result:
                    cities_processed = result.get("cities_processed", 0)
                    restaurants_processed = result.get("restaurants_processed", 0)
                    deals_processed = result.get("deals_processed", 0)
            
            current_status = task_status.get("status", "unknown")
            
            # Start real-time publishing if task is active and not already publishing
            if current_status in ["created", "running", "initializing", "fetching_cities", "using_specified_cities", "fetching_restaurants", "fetching_deals", "writing_files", "generating_summary"]:
                if not realtime_publisher.is_publishing(task_id):
                    try:
                        await realtime_publisher.start_publishing(task_id, interval=2.0)
                        logger.debug(f"Started real-time publishing for existing restaurant task {task_id}")
                    except Exception as e:
                        logger.warning(f"Failed to start real-time publishing for task {task_id}: {str(e)}")
            
            return RestaurantStatus(
                id=task_id,
                status=TaskStatus(current_status),
                progress=task_status.get("progress", 0.0),
                cities_requested=cities_requested,
                cities_processed=cities_processed,
                restaurants_processed=restaurants_processed,
                deals_processed=deals_processed,
                error=task_status.get("error"),
                progress_details=progress_details,
                created_at=task_status.get("created_at"),
                updated_at=task_status.get("updated_at"),
                execution_time_seconds=result.get("execution_time_seconds") if result else None
            )
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting restaurant scraping status for task {task_id}: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to retrieve restaurant scraping status: {str(e)}"
            )
    
    @staticmethod
    async def stop_restaurant_task(task_id: str) -> dict:
        """
        Stop a running restaurant scraping task.
        
        Args:
            task_id: The ID of the task to stop
            
        Returns:
            Dictionary with stop operation result
        """
        try:
            # Get task status
            task_status = task_manager.get_task_status(task_id)
            
            if not task_status:
                raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
            
            if task_status.get("type") != "restaurant":
                raise HTTPException(status_code=400, detail=f"Task {task_id} is not a restaurant scraping task")
            
            current_status = task_status.get("status")
            if current_status in ["completed", "failed", "stopped"]:
                return {
                    "success": True,
                    "message": f"Task {task_id} is already in '{current_status}' state",
                    "task_id": task_id
                }
            
            # Stop real-time publishing first
            try:
                await realtime_publisher.stop_publishing(task_id)
                logger.info(f"Stopped real-time publishing for restaurant task {task_id}")
            except Exception as e:
                logger.warning(f"Failed to stop real-time publishing for task {task_id}: {str(e)}")
            
            # Update task status to stopped
            task_manager.update_task_status(
                task_id,
                status="stopped",
                progress=100.0,
                result={
                    **task_status.get("result", {}),
                    "stopped_at": task_status.get("updated_at"),
                    "stopped_gracefully": True
                }
            )
            
            # Try to update database record if it exists
            try:
                restaurant_result = await RestaurantResult.find_one(RestaurantResult.task_id == task_id)
                if restaurant_result:
                    restaurant_result.status = "stopped"
                    restaurant_result.update_timestamp()
                    await restaurant_result.save()
            except Exception as e:
                logger.warning(f"Failed to update database record for task {task_id}: {str(e)}")
            
            return {
                "success": True,
                "message": "Restaurant scraping task stopped successfully",
                "task_id": task_id
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error stopping restaurant scraping task {task_id}: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to stop restaurant scraping task: {str(e)}"
            )
    
    @staticmethod
    async def get_restaurant_history(limit: int = 10) -> List[RestaurantStatus]:
        """
        Get the history of restaurant scraping tasks.
        
        Args:
            limit: Maximum number of tasks to return
            
        Returns:
            List of RestaurantStatus objects
        """
        try:
            # Get recent restaurant tasks from task manager
            tasks = task_manager.list_tasks(task_type="restaurant", limit=limit)
            
            history = []
            for task in tasks:
                task_id = task.get("id")
                
                # Extract progress data
                cities_requested = []
                cities_processed = 0
                restaurants_processed = 0
                deals_processed = 0
                progress_details = None
                
                # Get task parameters
                params = task.get("params", {})
                if params:
                    cities_requested = params.get("cities", [])
                
                # Get progress from results
                result = task.get("result", {})
                if result and isinstance(result, dict):
                    # Check for partial results first
                    partial_results = result.get("restaurant_partial_results", {})
                    if isinstance(partial_results, dict):
                        cities_processed = partial_results.get("cities_processed", 0)
                        restaurants_processed = partial_results.get("restaurants_processed", 0)
                        deals_processed = partial_results.get("deals_processed", 0)
                        
                        # Create progress details
                        progress_details = RestaurantProgress(
                            current_city=partial_results.get("current_city"),
                            current_restaurant=partial_results.get("current_restaurant"),
                            cities_processed=cities_processed,
                            restaurants_processed=restaurants_processed,
                            deals_processed=deals_processed,
                            total_cities=partial_results.get("total_cities", 0),
                            total_restaurants=partial_results.get("total_restaurants", 0)
                        )
                    
                    # Get final results if no partial results
                    if not partial_results and result:
                        cities_processed = result.get("cities_processed", 0)
                        restaurants_processed = result.get("restaurants_processed", 0)
                        deals_processed = result.get("deals_processed", 0)
                
                restaurant_status = RestaurantStatus(
                    id=task_id,
                    status=TaskStatus(task.get("status", "unknown")),
                    progress=task.get("progress", 0.0),
                    cities_requested=cities_requested,
                    cities_processed=cities_processed,
                    restaurants_processed=restaurants_processed,
                    deals_processed=deals_processed,
                    error=task.get("error"),
                    progress_details=progress_details,
                    created_at=task.get("created_at"),
                    updated_at=task.get("updated_at"),
                    execution_time_seconds=result.get("execution_time_seconds") if result else None
                )
                history.append(restaurant_status)
            
            return history
            
        except Exception as e:
            logger.error(f"Error getting restaurant scraping history: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to retrieve restaurant scraping history: {str(e)}"
            )