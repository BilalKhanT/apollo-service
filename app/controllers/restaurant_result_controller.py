from typing import List, Optional, Dict, Any
from fastapi import HTTPException
import logging
from app.models.database.restaurant_result_model import RestaurantResult, RestaurantSummaryData

logger = logging.getLogger(__name__)


class RestaurantResultController:
    @staticmethod
    async def create_restaurant_result(
        task_id: str,
        cities_requested: List[str],
        cities_processed: int,
        restaurants_processed: int,
        deals_processed: int,
        total_cities: int,
        total_restaurants: int,
        output_directory: str,
        execution_time_seconds: float,
        status: str,
        database_summary: Optional[Dict[str, Any]] = None
    ) -> RestaurantResult:
        """
        Create a new restaurant scraping result in the database.
        
        Args:
            task_id: Unique task identifier
            cities_requested: List of cities that were requested to be scraped
            cities_processed: Number of cities successfully processed
            restaurants_processed: Number of restaurants processed
            deals_processed: Number of deals processed
            total_cities: Total number of cities found
            total_restaurants: Total number of restaurants found
            output_directory: Directory where results were saved
            execution_time_seconds: Total execution time
            status: Final status of the task
            database_summary: Optional summary data for database storage
            
        Returns:
            RestaurantResult object
        """
        try:
            # Check if result already exists
            existing = await RestaurantResult.find_one(RestaurantResult.task_id == task_id)
            if existing:
                logger.warning(f"Restaurant result for task {task_id} already exists, updating instead")
                # Update existing record
                existing.cities_processed = cities_processed
                existing.restaurants_processed = restaurants_processed
                existing.deals_processed = deals_processed
                existing.total_cities = total_cities
                existing.total_restaurants = total_restaurants
                existing.output_directory = output_directory
                existing.execution_time_seconds = execution_time_seconds
                existing.status = status
                
                # Update database summary if provided
                if database_summary:
                    existing.database_summary = RestaurantSummaryData(**database_summary)
                
                existing.update_timestamp()
                await existing.save()
                return existing
            
            # Create new result
            restaurant_result = RestaurantResult(
                task_id=task_id,
                cities_requested=cities_requested,
                cities_processed=cities_processed,
                restaurants_processed=restaurants_processed,
                deals_processed=deals_processed,
                total_cities=total_cities,
                total_restaurants=total_restaurants,
                output_directory=output_directory,
                execution_time_seconds=execution_time_seconds,
                status=status
            )
            
            # Add database summary if provided
            if database_summary:
                restaurant_result.database_summary = RestaurantSummaryData(**database_summary)
            
            await restaurant_result.insert()
            logger.info(f"Created restaurant result for task {task_id}")
            return restaurant_result
            
        except Exception as e:
            logger.error(f"Error creating restaurant result for task {task_id}: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Failed to create restaurant result: {str(e)}")
    
    @staticmethod
    async def get_restaurant_result(task_id: str) -> Optional[RestaurantResult]:
        """
        Get a restaurant scraping result by task ID.
        
        Args:
            task_id: The task ID to search for
            
        Returns:
            RestaurantResult object if found, None otherwise
        """
        try:
            return await RestaurantResult.find_one(RestaurantResult.task_id == task_id)
        except Exception as e:
            logger.error(f"Error fetching restaurant result for task {task_id}: {str(e)}")
            return None
    
    @staticmethod
    async def list_restaurant_results(limit: int = 50, skip: int = 0) -> List[RestaurantResult]:
        """
        List all restaurant scraping results with pagination.
        
        Args:
            limit: Maximum number of results to return
            skip: Number of results to skip
            
        Returns:
            List of RestaurantResult objects
        """
        try:
            # Sort by created_at in descending order (latest first)
            return await RestaurantResult.find_all().sort([
                ("created_at", -1),  # -1 for descending order
                ("updated_at", -1)   # Secondary sort field
            ]).skip(skip).limit(limit).to_list()
        except Exception as e:
            logger.error(f"Error listing restaurant results: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Failed to list restaurant results: {str(e)}")
    
    @staticmethod
    async def update_restaurant_result_status(
        task_id: str, 
        status: str, 
        error: Optional[str] = None
    ) -> bool:
        """
        Update the status of a restaurant scraping result.
        
        Args:
            task_id: The task ID to update
            status: New status to set
            error: Optional error message
            
        Returns:
            True if update was successful, False otherwise
        """
        try:
            restaurant_result = await RestaurantResult.find_one(RestaurantResult.task_id == task_id)
            if not restaurant_result:
                logger.warning(f"Restaurant result not found for task {task_id}")
                return False
            
            restaurant_result.status = status
            if error:
                restaurant_result.error = error
            restaurant_result.update_timestamp()
            await restaurant_result.save()
            
            logger.info(f"Updated restaurant result {task_id} status to {status}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating restaurant result {task_id} status: {str(e)}")
            return False
    
    @staticmethod
    async def update_restaurant_result_progress(
        task_id: str,
        cities_processed: int,
        restaurants_processed: int,
        deals_processed: int,
        total_cities: int = None,
        total_restaurants: int = None
    ) -> bool:
        """
        Update the progress of a restaurant scraping result.
        
        Args:
            task_id: The task ID to update
            cities_processed: Number of cities processed
            restaurants_processed: Number of restaurants processed
            deals_processed: Number of deals processed
            total_cities: Total number of cities (optional)
            total_restaurants: Total number of restaurants (optional)
            
        Returns:
            True if update was successful, False otherwise
        """
        try:
            restaurant_result = await RestaurantResult.find_one(RestaurantResult.task_id == task_id)
            if not restaurant_result:
                logger.warning(f"Restaurant result not found for task {task_id}")
                return False
            
            restaurant_result.cities_processed = cities_processed
            restaurant_result.restaurants_processed = restaurants_processed
            restaurant_result.deals_processed = deals_processed
            
            if total_cities is not None:
                restaurant_result.total_cities = total_cities
            if total_restaurants is not None:
                restaurant_result.total_restaurants = total_restaurants
            
            restaurant_result.update_timestamp()
            await restaurant_result.save()
            
            logger.debug(f"Updated restaurant result {task_id} progress")
            return True
            
        except Exception as e:
            logger.error(f"Error updating restaurant result {task_id} progress: {str(e)}")
            return False
    
    @staticmethod
    async def delete_restaurant_result(task_id: str) -> bool:
        """
        Delete a restaurant scraping result.
        
        Args:
            task_id: The task ID to delete
            
        Returns:
            True if deletion was successful, False otherwise
        """
        try:
            restaurant_result = await RestaurantResult.find_one(RestaurantResult.task_id == task_id)
            if not restaurant_result:
                logger.warning(f"Restaurant result not found for task {task_id}")
                return False
            
            await restaurant_result.delete()
            logger.info(f"Deleted restaurant result for task {task_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting restaurant result {task_id}: {str(e)}")
            return False
    
    @staticmethod
    async def get_restaurant_results_by_cities(cities: List[str]) -> List[RestaurantResult]:
        """
        Get restaurant scraping results that include any of the specified cities.
        
        Args:
            cities: List of cities to search for
            
        Returns:
            List of RestaurantResult objects that contain any of the specified cities
        """
        try:
            # Find results where cities_requested contains any of the specified cities
            results = []
            all_results = await RestaurantResult.find_all().to_list()
            
            for result in all_results:
                # Check if any requested city matches our search cities
                if any(city in result.cities_requested for city in cities):
                    results.append(result)
            
            # Sort by creation date (latest first)
            results.sort(key=lambda x: x.created_at, reverse=True)
            
            return results
            
        except Exception as e:
            logger.error(f"Error getting restaurant results by cities {cities}: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Failed to get restaurant results by cities: {str(e)}")
    
    @staticmethod
    async def get_restaurant_statistics() -> Dict[str, Any]:
        """
        Get overall statistics for all restaurant scraping results.
        
        Returns:
            Dictionary with statistics about restaurant scraping tasks
        """
        try:
            all_results = await RestaurantResult.find_all().to_list()
            
            if not all_results:
                return {
                    "total_tasks": 0,
                    "completed_tasks": 0,
                    "failed_tasks": 0,
                    "running_tasks": 0,
                    "total_cities_processed": 0,
                    "total_restaurants_processed": 0,
                    "total_deals_processed": 0,
                    "average_execution_time": 0.0,
                    "most_popular_cities": []
                }
            
            # Calculate statistics
            total_tasks = len(all_results)
            completed_tasks = len([r for r in all_results if r.status == "completed"])
            failed_tasks = len([r for r in all_results if r.status == "failed"])
            running_tasks = len([r for r in all_results if r.status in ["running", "created", "initializing"]])
            
            total_cities_processed = sum(r.cities_processed for r in all_results)
            total_restaurants_processed = sum(r.restaurants_processed for r in all_results)
            total_deals_processed = sum(r.deals_processed for r in all_results)
            
            # Calculate average execution time (only for completed tasks)
            completed_results = [r for r in all_results if r.status == "completed" and r.execution_time_seconds > 0]
            average_execution_time = (
                sum(r.execution_time_seconds for r in completed_results) / len(completed_results)
                if completed_results else 0.0
            )
            
            # Find most popular cities
            city_counts = {}
            for result in all_results:
                for city in result.cities_requested:
                    city_counts[city] = city_counts.get(city, 0) + 1
            
            most_popular_cities = sorted(city_counts.items(), key=lambda x: x[1], reverse=True)[:10]
            
            return {
                "total_tasks": total_tasks,
                "completed_tasks": completed_tasks,
                "failed_tasks": failed_tasks,
                "running_tasks": running_tasks,
                "total_cities_processed": total_cities_processed,
                "total_restaurants_processed": total_restaurants_processed,
                "total_deals_processed": total_deals_processed,
                "average_execution_time": round(average_execution_time, 2),
                "most_popular_cities": [{"city": city, "count": count} for city, count in most_popular_cities]
            }
            
        except Exception as e:
            logger.error(f"Error getting restaurant statistics: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Failed to get restaurant statistics: {str(e)}")