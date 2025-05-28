from typing import List, Optional, Dict, Any
from fastapi import HTTPException, status
from app.utils.task_manager import task_manager
from app.utils.realtime_publisher import realtime_publisher
from app.models.database.fb_scrape.fb_result_model import FacebookResult
from app.models.fb_scrape.fb_scrape_model import FacebookResultSummary, FacebookStopResponse
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class FacebookScrapeController:
    """
    Controller for handling Facebook scraping operations.
    Follows the same patterns as CrawlController and ScrapeController.
    """
    
    @staticmethod
    async def start_facebook_scraping(
        keywords: List[str], 
        days: int,
        access_token: str,
        page_id: str
    ) -> Dict[str, Any]:
        """
        Start a new Facebook scraping task.
        
        Args:
            keywords: List of keywords to filter posts
            days: Number of days to look back for posts
            access_token: Facebook API access token
            page_id: Facebook page ID to scrape
            
        Returns:
            Dictionary with task information
        """
        try:
            # Create task in task manager
            task_id = task_manager.create_task(
                task_type="facebook_scraping",
                params={
                    "keywords": keywords,
                    "days": days,
                    "access_token": access_token,  # Note: Consider security implications
                    "page_id": page_id
                }
            )
            
            task_status = task_manager.get_task_status(task_id)
            
            # Start real-time publishing for this task
            try:
                await realtime_publisher.start_publishing(task_id, interval=2.0)
                logger.info(f"Started real-time publishing for Facebook scraping task {task_id}")
            except Exception as e:
                logger.warning(f"Failed to start real-time publishing for task {task_id}: {str(e)}")
            
            return {
                "task_id": task_id,
                "status": task_status["status"],
                "progress": task_status["progress"],
                "keywords_requested": keywords,
                "days_requested": days,
                "created_at": task_status["created_at"],
                "error": task_status.get("error")
            }
            
        except Exception as e:
            logger.error(f"Error starting Facebook scraping: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to start Facebook scraping: {str(e)}"
            )
    
    @staticmethod
    async def get_facebook_scraping_status(task_id: str) -> Dict[str, Any]:
        """
        Get the status of a Facebook scraping task.
        
        Args:
            task_id: Task ID to get status for
            
        Returns:
            Dictionary with task status information
        """
        try:
            task_status = task_manager.get_task_status(task_id)
            
            if not task_status:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Task {task_id} not found"
                )
            
            if task_status.get("type") != "facebook_scraping":
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Task {task_id} is not a Facebook scraping task"
                )
            
            # Extract progress data from results
            result = task_status.get("result", {})
            current_status = task_status.get("status", "unknown")
            
            # Get partial results for ongoing tasks
            partial_results = result.get("facebook_scrape_partial_results", {})
            
            # Get completed results
            completed_results = result.get("facebook_scrape_results", {})
            
            # Determine which results to use
            if current_status in ["running", "initializing", "scraping_posts", "processing_posts"]:
                # Use partial results for active tasks
                posts_processed = partial_results.get("posts_processed", 0)
                posts_found = partial_results.get("posts_found", 0)
                current_keyword = partial_results.get("current_keyword", "")
            else:
                # Use completed results for finished tasks
                posts_processed = completed_results.get("posts_processed", 0)
                posts_found = completed_results.get("posts_found", 0)
                current_keyword = completed_results.get("current_keyword", "")
            
            # Start real-time publishing if task is active and not already publishing
            if current_status in ["created", "running", "initializing", "scraping_posts", "processing_posts"]:
                if not realtime_publisher.is_publishing(task_id):
                    try:
                        await realtime_publisher.start_publishing(task_id, interval=2.0)
                        logger.debug(f"Started real-time publishing for existing Facebook scraping task {task_id}")
                    except Exception as e:
                        logger.warning(f"Failed to start real-time publishing for task {task_id}: {str(e)}")
            
            return {
                "task_id": task_id,
                "status": current_status,
                "progress": task_status.get("progress", 0.0),
                "posts_processed": posts_processed,
                "posts_found": posts_found,
                "current_keyword": current_keyword,
                "error": task_status.get("error"),
                "created_at": task_status.get("created_at"),
                "updated_at": task_status.get("updated_at"),
                "execution_time_seconds": None  # Will be calculated by frontend if needed
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting Facebook scraping status for task {task_id}: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to retrieve Facebook scraping status: {str(e)}"
            )
    
    @staticmethod
    async def stop_facebook_scraping_task(task_id: str) -> FacebookStopResponse:
        """
        Stop a running Facebook scraping task.
        
        Args:
            task_id: Task ID to stop
            
        Returns:
            FacebookStopResponse with stop result information
        """
        try:
            if not task_id:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Task ID is required"
                )
                
            task_status = task_manager.get_task_status(task_id)
            
            if not task_status:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Task {task_id} not found"
                )
            
            if task_status.get("type") != "facebook_scraping":
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Task {task_id} is not a Facebook scraping task"
                )
            
            current_status = task_status.get("status")
            was_running = current_status in ["created", "running", "initializing", "scraping_posts", "processing_posts"]
            
            # Check if task is already completed or failed
            if current_status in ["completed", "failed", "stopped"]:
                return FacebookStopResponse(
                    success=True,
                    message=f"Task {task_id} is already in '{current_status}' state",
                    task_id=task_id,
                    was_running=False,
                    cleanup_completed=True
                )
            
            # Stop real-time publishing first
            try:
                # Check if publisher is actually running for this task
                if realtime_publisher.is_publishing(task_id):
                    await realtime_publisher.stop_publishing(task_id)
                    logger.info(f"Stopped real-time publishing for Facebook scraping task {task_id}")
                else:
                    logger.debug(f"Real-time publishing was not active for task {task_id}")
            except Exception as e:
                logger.warning(f"Failed to stop real-time publishing for task {task_id}: {str(e)}")
                # Don't fail the entire stop operation if publishing stop fails
            
            # Use the orchestrator to stop the actual Facebook scraping process
            try:
                from app.utils.orchestrator import orchestrator
                stop_result = orchestrator.stop_facebook_scraping(task_id)
                
                if stop_result.get("success"):
                    logger.info(f"Facebook scraping task {task_id} stopped successfully via orchestrator")
                    cleanup_completed = stop_result.get("cleanup_completed", False)
                else:
                    logger.warning(f"Orchestrator reported failure stopping task {task_id}: {stop_result.get('message')}")
                    # Still mark as stopped in task manager for consistency
                    cleanup_completed = False
            except Exception as e:
                logger.error(f"Error calling orchestrator stop for task {task_id}: {str(e)}")
                cleanup_completed = False
            
            # Always update task status to stopped to ensure consistency
            task_manager.update_task_status(
                task_id,
                status="stopped",
                progress=95.0,  # Set to 95% as per requirement
                result={
                    **task_status.get("result", {}),
                    "stopped_at": datetime.now().isoformat(),
                    "stopped_gracefully": True
                }
            )
            
            logger.info(f"Facebook scraping task {task_id} marked as stopped")
            
            return FacebookStopResponse(
                success=True,
                message="Facebook scraping task stopped successfully",
                task_id=task_id,
                was_running=was_running,
                cleanup_completed=cleanup_completed
            )
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error stopping Facebook scraping task {task_id}: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to stop Facebook scraping task: {str(e)}"
            )
    
    @staticmethod
    async def get_all_facebook_results(
        page: int = 1,
        page_size: int = 50,
        limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Get all past Facebook scraping results from database.
        
        Args:
            page: Page number for pagination
            page_size: Number of results per page
            limit: Maximum number of results to return (overrides pagination if provided)
            
        Returns:
            Dictionary with paginated Facebook results
        """
        try:
            # Calculate skip and limit for pagination
            skip = (page - 1) * page_size
            
            if limit:
                # If limit is provided, ignore pagination
                actual_limit = limit
                skip = 0
            else:
                actual_limit = page_size
            
            # Get total count
            total_count = await FacebookResult.count()
            
            # Get paginated results, sorted by creation date (newest first)
            facebook_results = await FacebookResult.find_all().sort([
                ("created_at", -1),  # -1 for descending order (newest first)
                ("completed_at", -1)  # Secondary sort field
            ]).skip(skip).limit(actual_limit).to_list()
            
            # Convert to summary format
            results_data = []
            for facebook_result in facebook_results:
                minimal_summary = facebook_result.get_minimal_summary()
                
                # Import here to avoid circular imports
                from app.models.fb_scrape.fb_scrape_model import FacebookResultSummaryMinimal
                facebook_summary = FacebookResultSummaryMinimal(**minimal_summary)
                results_data.append(facebook_summary)
            
            # Calculate pagination info
            has_more = False
            if not limit:
                has_more = (skip + actual_limit) < total_count
            
            logger.info(f"Retrieved {len(results_data)} Facebook results (page {page}, total: {total_count})")
            
            return {
                "data": results_data,
                "total_count": total_count,
                "page": page,
                "page_size": page_size,
                "has_more": has_more,
                "returned_count": len(results_data)
            }
            
        except Exception as e:
            logger.error(f"Error retrieving Facebook results: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to retrieve Facebook results: {str(e)}"
            )
    
    @staticmethod
    async def get_facebook_result_by_task_id(task_id: str) -> Optional[FacebookResultSummary]:
        """
        Get a specific Facebook result by task ID.
        
        Args:
            task_id: Task ID to get result for
            
        Returns:
            FacebookResultSummary if found, None otherwise
        """
        try:
            facebook_result = await FacebookResult.find_one(FacebookResult.task_id == task_id)
            
            if not facebook_result:
                return None
            
            # Convert to summary using the model's method
            summary_data = facebook_result.get_summary()
            return FacebookResultSummary(**summary_data)
            
        except Exception as e:
            logger.error(f"Error retrieving Facebook result for task {task_id}: {str(e)}")
            return None

    @staticmethod
    async def save_facebook_result(
        task_id: str,
        keywords_requested: List[str],
        days_requested: int,
        posts_processed: int,
        categories_found: Dict[str, int],
        keyword_matches: Dict[str, Dict[str, int]],
        execution_time_seconds: float,
        output_directory: str,
        date_range: Dict[str, str],
        posts_data: List[Dict[str, Any]]
    ) -> FacebookResult:
        """
        Save Facebook scraping results to database.
        This method is called by the orchestrator when scraping completes successfully.
        """
        try:
            # Check if result already exists
            existing_result = await FacebookResult.find_one(FacebookResult.task_id == task_id)
            if existing_result:
                logger.warning(f"Facebook result for task {task_id} already exists")
                return existing_result
            
            # Get the original task creation time from task manager
            task_status = task_manager.get_task_status(task_id)
            if task_status and task_status.get('created_at'):
                # Parse the ISO format datetime string from task manager
                from datetime import datetime
                original_created_at = datetime.fromisoformat(task_status['created_at'].replace('Z', '+00:00'))
                # Convert to UTC datetime (remove timezone info for MongoDB)
                original_created_at = original_created_at.replace(tzinfo=None)
            else:
                # Fallback to current time if we can't get original creation time
                logger.warning(f"Could not get original creation time for task {task_id}, using current time")
                original_created_at = datetime.utcnow()
            
            # Create new result with correct timestamps
            facebook_result = FacebookResult(
                task_id=task_id,
                keywords_requested=keywords_requested,
                days_requested=days_requested,
                posts_processed=posts_processed,
                categories_found=categories_found,
                keyword_matches=keyword_matches,
                execution_time_seconds=execution_time_seconds,
                output_directory=output_directory,
                date_range=date_range,
                posts_data=posts_data,
                created_at=original_created_at,  
                completed_at=datetime.utcnow()   
            )
            
            await facebook_result.insert()
            logger.info(f"Saved Facebook result for task {task_id} to database")
            logger.info(f"Task created at: {original_created_at}, completed at: {facebook_result.completed_at}")
            
            return facebook_result
            
        except Exception as e:
            logger.error(f"Error saving Facebook result for task {task_id}: {str(e)}")
            raise Exception(f"Failed to save Facebook result: {str(e)}")
    
    @staticmethod
    async def delete_facebook_result(task_id: str) -> bool:
        """
        Delete a Facebook result by task ID.
        
        Args:
            task_id: Task ID to delete result for
            
        Returns:
            True if deleted, False if not found
        """
        try:
            facebook_result = await FacebookResult.find_one(FacebookResult.task_id == task_id)
            
            if not facebook_result:
                logger.warning(f"Facebook result for task {task_id} not found")
                return False
            
            await facebook_result.delete()
            logger.info(f"Deleted Facebook result for task {task_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error deleting Facebook result for task {task_id}: {str(e)}")
            raise Exception(f"Failed to delete Facebook result: {str(e)}")