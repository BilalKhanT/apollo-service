from typing import List, Optional, Dict, Any
from fastapi import HTTPException, status
import pytz
from app.utils.task_manager import task_manager
from app.utils.realtime_publisher import realtime_publisher
from app.models.database.restaurant_deal.restaurant_result_model import DealResult
from app.models.restaurant_deal.restaurant_model import DealResultSummary, DealStopResponse
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class DealScrapeController:
    
    @staticmethod
    async def start_deal_scraping(cities: List[str]) -> Dict[str, Any]:
        try:
            task_id = task_manager.create_task(
                task_type="deal_scraping",
                params={
                    "cities": cities,
                    "country": "Pakistan",
                    "language": "en"
                }
            )
            
            task_status = task_manager.get_task_status(task_id)

            try:
                await realtime_publisher.start_publishing(task_id, interval=2.0)
                logger.info(f"Started real-time publishing for deal scraping task {task_id}")
            except Exception as e:
                logger.warning(f"Failed to start real-time publishing for task {task_id}: {str(e)}")
            
            return {
                "task_id": task_id,
                "status": task_status["status"],
                "progress": task_status["progress"],
                "cities_requested": cities,
                "created_at": task_status["created_at"],
                "error": task_status.get("error")
            }
            
        except Exception as e:
            logger.error(f"Error starting deal scraping: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to start deal scraping: {str(e)}"
            )
    
    @staticmethod
    async def get_deal_scraping_status(task_id: str) -> Dict[str, Any]:
        try:
            task_status = task_manager.get_task_status(task_id)
            
            if not task_status:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Task {task_id} not found"
                )
            
            if task_status.get("type") != "deal_scraping":
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Task {task_id} is not a deal scraping task"
                )

            result = task_status.get("result", {})
            current_status = task_status.get("status", "unknown")

            partial_results = result.get("deal_scrape_partial_results", {})

            completed_results = result.get("deal_scrape_results", {})

            if current_status in ["running", "initializing", "fetching_cities", "scraping_deals"]:
                cities_processed = partial_results.get("cities_processed", 0)
                restaurants_processed = partial_results.get("restaurants_processed", 0)
                deals_found = partial_results.get("deals_found", 0)
                total_cities = partial_results.get("total_cities", 0)
                current_city = partial_results.get("current_city", "")
            else:
                cities_processed = completed_results.get("cities_processed", 0)
                restaurants_processed = completed_results.get("restaurants_processed", 0)
                deals_found = completed_results.get("deals_found", 0)
                total_cities = completed_results.get("total_cities", 0)
                current_city = completed_results.get("current_city", "")

            if current_status in ["created", "running", "initializing", "fetching_cities", "scraping_deals"]:
                if not realtime_publisher.is_publishing(task_id):
                    try:
                        await realtime_publisher.start_publishing(task_id, interval=2.0)
                        logger.debug(f"Started real-time publishing for existing deal scraping task {task_id}")
                    except Exception as e:
                        logger.warning(f"Failed to start real-time publishing for task {task_id}: {str(e)}")
            
            return {
                "task_id": task_id,
                "status": current_status,
                "progress": task_status.get("progress", 0.0),
                "cities_processed": cities_processed,
                "restaurants_processed": restaurants_processed,
                "deals_found": deals_found,
                "total_cities": total_cities,
                "current_city": current_city,
                "error": task_status.get("error"),
                "created_at": task_status.get("created_at"),
                "updated_at": task_status.get("updated_at"),
                "execution_time_seconds": None  
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting deal scraping status for task {task_id}: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to retrieve deal scraping status: {str(e)}"
            )
    
    @staticmethod
    async def stop_deal_scraping_task(task_id: str) -> DealStopResponse:
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
            
            if task_status.get("type") != "deal_scraping":
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Task {task_id} is not a deal scraping task"
                )
            
            current_status = task_status.get("status")
            was_running = current_status in ["created", "running", "initializing", "fetching_cities", "scraping_deals"]

            if current_status in ["completed", "failed", "stopped"]:
                return DealStopResponse(
                    success=True,
                    message=f"Task {task_id} is already in '{current_status}' state",
                    task_id=task_id,
                    was_running=False,
                    cleanup_completed=True
                )

            try:
                if realtime_publisher.is_publishing(task_id):
                    await realtime_publisher.stop_publishing(task_id)
                    logger.info(f"Stopped real-time publishing for deal scraping task {task_id}")
                else:
                    logger.debug(f"Real-time publishing was not active for task {task_id}")
            except Exception as e:
                logger.warning(f"Failed to stop real-time publishing for task {task_id}: {str(e)}")

            try:
                from app.utils.orchestrator import orchestrator
                stop_result = orchestrator.stop_deal_scraping(task_id)
                
                if stop_result.get("success"):
                    logger.info(f"Deal scraping task {task_id} stopped successfully via orchestrator")
                    cleanup_completed = stop_result.get("cleanup_completed", False)
                else:
                    logger.warning(f"Orchestrator reported failure stopping task {task_id}: {stop_result.get('message')}")
                    cleanup_completed = False
            except Exception as e:
                logger.error(f"Error calling orchestrator stop for task {task_id}: {str(e)}")
                cleanup_completed = False

            task_manager.update_task_status(
                task_id,
                status="stopped",
                progress=95.0,  
                result={
                    **task_status.get("result", {}),
                    "stopped_at": datetime.now().isoformat(),
                    "stopped_gracefully": True
                }
            )
            
            logger.info(f"Deal scraping task {task_id} marked as stopped")
            
            return DealStopResponse(
                success=True,
                message="Deal scraping task stopped successfully",
                task_id=task_id,
                was_running=was_running,
                cleanup_completed=cleanup_completed
            )
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error stopping deal scraping task {task_id}: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to stop deal scraping task: {str(e)}"
            )
    
    @staticmethod
    async def get_all_deal_results(
        page: int = 1,
        page_size: int = 50,
        limit: Optional[int] = None
    ) -> Dict[str, Any]:
        try:
            skip = (page - 1) * page_size
            
            if limit:
                actual_limit = limit
                skip = 0
            else:
                actual_limit = page_size

            total_count = await DealResult.count()

            deal_results = await DealResult.find_all().sort([
                ("created_at", -1),  
                ("completed_at", -1)  
            ]).skip(skip).limit(actual_limit).to_list()

            results_data = []
            for deal_result in deal_results:
                minimal_summary = {
                    "task_id": deal_result.task_id,
                    "created_at": deal_result.created_at,
                    "completed_at": deal_result.completed_at,
                    "cities_requested": deal_result.cities_requested,
                    "cities_processed": deal_result.cities_processed,
                    "restaurants_processed": deal_result.restaurants_processed,
                    "deals_processed": deal_result.deals_processed
                }

                from app.models.restaurant_deal.restaurant_model import DealResultSummaryMinimal
                deal_summary = DealResultSummaryMinimal(**minimal_summary)
                results_data.append(deal_summary)

            has_more = False
            if not limit:
                has_more = (skip + actual_limit) < total_count
            
            logger.info(f"Retrieved {len(results_data)} deal results (page {page}, total: {total_count})")
            
            return {
                "data": results_data,
                "total_count": total_count,
                "page": page,
                "page_size": page_size,
                "has_more": has_more,
                "returned_count": len(results_data)
            }
            
        except Exception as e:
            logger.error(f"Error retrieving deal results: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to retrieve deal results: {str(e)}"
            )
    
    @staticmethod
    async def get_deal_result_by_task_id(task_id: str) -> Optional[DealResultSummary]:
        try:
            deal_result = await DealResult.find_one(DealResult.task_id == task_id)
            
            if not deal_result:
                return None

            summary_data = deal_result.get_summary()
            return DealResultSummary(**summary_data)
            
        except Exception as e:
            logger.error(f"Error retrieving deal result for task {task_id}: {str(e)}")
            return None

    @staticmethod
    async def save_deal_result(
        task_id: str,
        cities_requested: List[str],
        cities_processed: int,
        restaurants_processed: int,
        deals_processed: int,
        execution_time_seconds: float,
        restaurants_data: List[Dict[str, Any]],
        summary_by_city: Dict[str, Dict[str, int]]
    ) -> DealResult:
        try:
            existing_result = await DealResult.find_one(DealResult.task_id == task_id)
            if existing_result:
                logger.warning(f"Deal result for task {task_id} already exists")
                return existing_result

            task_status = task_manager.get_task_status(task_id)
            if task_status and task_status.get('created_at'):
                from datetime import datetime
                original_created_at = datetime.fromisoformat(task_status['created_at'].replace('Z', '+00:00'))
                original_created_at = original_created_at.replace(tzinfo=None)
            else:
                logger.warning(f"Could not get original creation time for task {task_id}, using current time")
                original_created_at = datetime.utcnow()

            karachi_tz = pytz.timezone('Asia/Karachi')
            completed_at = datetime.now(karachi_tz).replace(tzinfo=None)

            deal_result = DealResult(
                task_id=task_id,
                cities_requested=cities_requested,
                cities_processed=cities_processed,
                restaurants_processed=restaurants_processed,
                deals_processed=deals_processed,
                execution_time_seconds=execution_time_seconds,
                restaurants_data=restaurants_data,
                summary_by_city=summary_by_city,
                created_at=original_created_at,  
                completed_at=completed_at  
            )
            
            await deal_result.insert()
            logger.info(f"Saved deal result for task {task_id} to database")
            logger.info(f"Task created at: {original_created_at}, completed at: {deal_result.completed_at}")
            
            return deal_result
            
        except Exception as e:
            logger.error(f"Error saving deal result for task {task_id}: {str(e)}")
            raise Exception(f"Failed to save deal result: {str(e)}")
    
    @staticmethod
    async def delete_deal_result(task_id: str) -> bool:
        try:
            deal_result = await DealResult.find_one(DealResult.task_id == task_id)
            
            if not deal_result:
                logger.warning(f"Deal result for task {task_id} not found")
                return False
            
            await deal_result.delete()
            logger.info(f"Deleted deal result for task {task_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error deleting deal result for task {task_id}: {str(e)}")
            raise Exception(f"Failed to delete deal result: {str(e)}")