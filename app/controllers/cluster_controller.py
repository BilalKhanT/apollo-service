from typing import Dict, Any, Optional
from fastapi import HTTPException
from app.utils.task_manager import task_manager
from app.utils.orchestrator import orchestrator
from app.models.database.database_models import CrawlResult
import logging

logger = logging.getLogger(__name__)

class ClusterController:
    @staticmethod
    async def get_clusters(crawl_task_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get available clusters and years from database.
        Enhanced to handle server restarts gracefully.
        
        Args:
            crawl_task_id: Optional task ID to get results for a specific crawl
            
        Returns:
            Dictionary containing clusters and years data
        """
        response = {
            "clusters": [],
            "years": [],
            "clusters_available": False,
            "years_available": False
        }
        
        crawl_result_id = None
        
        # If a specific task ID is provided, find the crawl result
        if crawl_task_id:
            try:
                logger.info(f"Looking for crawl results for task_id: {crawl_task_id}")
                
                # First check if task exists in task manager (for active tasks)
                task_status = task_manager.get_task_status(crawl_task_id)
                
                if task_status:
                    logger.info(f"Task {crawl_task_id} found in memory with status: {task_status.get('status')}")
                    
                    # Task exists in memory - check if it's completed
                    if task_status["status"] not in ["completed"]:
                        # For non-completed tasks, check if we have partial results in database
                        result = task_status.get("result", {})
                        crawl_result_id = result.get("crawl_result_id")
                        
                        if not crawl_result_id:
                            logger.warning(f"Task {crawl_task_id} is not completed and has no crawl_result_id")
                            raise HTTPException(
                                status_code=400, 
                                detail=f"Task {crawl_task_id} is not completed. Status: {task_status['status']}"
                            )
                    else:
                        # Get the crawl result ID from the completed task
                        result = task_status.get("result", {})
                        crawl_result_id = result.get("crawl_result_id")
                else:
                    logger.info(f"Task {crawl_task_id} not found in memory, checking database...")
                    
                    # Task not in memory - look directly in database by task_id
                    # This handles the server restart case
                    crawl_result = await CrawlResult.find_one(CrawlResult.task_id == crawl_task_id)
                    
                    if not crawl_result:
                        logger.error(f"No crawl result found in database for task_id: {crawl_task_id}")
                        raise HTTPException(
                            status_code=404, 
                            detail=f"No crawl result found for task {crawl_task_id}. Task may not exist or crawling may not have started."
                        )
                    
                    crawl_result_id = str(crawl_result.id)
                    logger.info(f"Found crawl_result_id {crawl_result_id} in database for task_id {crawl_task_id}")
                
                if not crawl_result_id:
                    logger.error(f"No crawl_result_id found for task {crawl_task_id}")
                    raise HTTPException(
                        status_code=400, 
                        detail=f"Task {crawl_task_id} does not have associated crawl results"
                    )
                
                # Check if clusters and years are available for this specific crawl
                try:
                    crawl_result = await CrawlResult.get(crawl_result_id)
                    if crawl_result:
                        response["clusters_available"] = crawl_result.cluster_complete
                        response["years_available"] = crawl_result.year_extraction_complete
                        logger.info(f"Crawl result {crawl_result_id}: clusters_available={crawl_result.cluster_complete}, years_available={crawl_result.year_extraction_complete}")
                    else:
                        logger.error(f"Crawl result {crawl_result_id} not found in database")
                        raise HTTPException(
                            status_code=404, 
                            detail=f"Crawl result {crawl_result_id} not found in database"
                        )
                except Exception as e:
                    logger.error(f"Error checking crawl result {crawl_result_id}: {str(e)}")
                    response["clusters_error"] = f"Error checking crawl result: {str(e)}"
                    response["years_error"] = f"Error checking crawl result: {str(e)}"
                    
            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"Error finding crawl result for task {crawl_task_id}: {str(e)}")
                raise HTTPException(
                    status_code=500, 
                    detail=f"Error finding crawl result: {str(e)}"
                )
        else:
            # No specific task ID provided, check if we have any completed crawls
            try:
                logger.info("No task_id provided, looking for latest completed crawls...")
                
                latest_cluster_crawl = await CrawlResult.find(
                    CrawlResult.cluster_complete == True
                ).sort([("created_at", -1)]).first()
                
                latest_year_crawl = await CrawlResult.find(
                    CrawlResult.year_extraction_complete == True
                ).sort([("created_at", -1)]).first()
                
                response["clusters_available"] = latest_cluster_crawl is not None
                response["years_available"] = latest_year_crawl is not None
                
                logger.info(f"Latest crawls: clusters_available={response['clusters_available']}, years_available={response['years_available']}")
                
            except Exception as e:
                logger.error(f"Error checking for available crawls: {str(e)}")
                response["clusters_error"] = f"Error checking for available crawls: {str(e)}"
                response["years_error"] = f"Error checking for available crawls: {str(e)}"
        
        # Get clusters if available
        if response["clusters_available"]:
            try:
                logger.info(f"Retrieving clusters for crawl_result_id: {crawl_result_id}")
                clusters = await orchestrator.get_available_clusters(crawl_result_id)
                response["clusters"] = clusters
                logger.info(f"Retrieved {len(clusters)} clusters")
            except Exception as e:
                logger.error(f"Error retrieving clusters: {str(e)}")
                response["clusters_error"] = f"Error retrieving clusters: {str(e)}"
                response["clusters_available"] = False
        
        # Get years if available
        if response["years_available"]:
            try:
                logger.info(f"Retrieving years for crawl_result_id: {crawl_result_id}")
                years = await orchestrator.get_available_years(crawl_result_id)
                response["years"] = years
                logger.info(f"Retrieved {len(years)} years")
            except Exception as e:
                logger.error(f"Error retrieving years: {str(e)}")
                response["years_error"] = f"Error retrieving years: {str(e)}"
                response["years_available"] = False
        
        return response
    
    @staticmethod
    async def get_cluster_by_id(cluster_id: str, crawl_task_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get a specific cluster by ID from database.
        Enhanced to handle server restarts gracefully.
        
        Args:
            cluster_id: ID of the cluster to retrieve
            crawl_task_id: Optional task ID to get cluster from a specific crawl
            
        Returns:
            Dictionary containing cluster data
        """
        crawl_result_id = None
        
        # If a specific task ID is provided, find the crawl result in database
        if crawl_task_id:
            try:
                logger.info(f"Getting cluster {cluster_id} for task_id: {crawl_task_id}")
                
                # First check if task exists in task manager (for active tasks)
                task_status = task_manager.get_task_status(crawl_task_id)
                
                if task_status:
                    # Task exists in memory
                    if task_status["status"] != "completed":
                        raise HTTPException(
                            status_code=400, 
                            detail=f"Task {crawl_task_id} is not completed"
                        )
                    
                    # Get the crawl result ID from the task result
                    result = task_status.get("result", {})
                    crawl_result_id = result.get("crawl_result_id")
                else:
                    # Task not in memory - look directly in database by task_id
                    # This handles the server restart case
                    crawl_result = await CrawlResult.find_one(CrawlResult.task_id == crawl_task_id)
                    
                    if not crawl_result:
                        raise HTTPException(
                            status_code=404, 
                            detail=f"No crawl result found for task {crawl_task_id}"
                        )
                    
                    crawl_result_id = str(crawl_result.id)
                
                if not crawl_result_id:
                    raise HTTPException(
                        status_code=400, 
                        detail=f"Task {crawl_task_id} does not have associated crawl results"
                    )
                
                # Verify that clusters are available for this crawl
                try:
                    crawl_result = await CrawlResult.get(crawl_result_id)
                    if not crawl_result or not crawl_result.cluster_complete:
                        raise HTTPException(
                            status_code=400, 
                            detail=f"Task {crawl_task_id} does not have cluster results"
                        )
                except Exception as e:
                    logger.error(f"Error checking crawl result {crawl_result_id}: {str(e)}")
                    raise HTTPException(
                        status_code=500, 
                        detail=f"Error checking crawl result: {str(e)}"
                    )
                    
            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"Error finding crawl result for task {crawl_task_id}: {str(e)}")
                raise HTTPException(
                    status_code=500, 
                    detail=f"Error finding crawl result: {str(e)}"
                )
        
        # Get the cluster from database
        try:
            logger.info(f"Retrieving cluster {cluster_id} from database")
            cluster = await orchestrator.get_cluster_by_id(cluster_id, crawl_result_id)
            if not cluster:
                raise HTTPException(
                    status_code=404, 
                    detail=f"Cluster {cluster_id} not found"
                )
            
            return cluster
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error retrieving cluster {cluster_id}: {str(e)}")
            raise HTTPException(
                status_code=500, 
                detail=f"Error retrieving cluster: {str(e)}"
            )

    @staticmethod
    async def get_year_by_id(year: str, crawl_task_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get year data by year from database.
        Enhanced to handle server restarts gracefully.
        
        Args:
            year: Year to retrieve data for
            crawl_task_id: Optional task ID to get year data from a specific crawl
            
        Returns:
            Dictionary containing year data
        """
        crawl_result_id = None
        
        # If a specific task ID is provided, find the crawl result in database
        if crawl_task_id:
            try:
                logger.info(f"Getting year {year} for task_id: {crawl_task_id}")
                
                # First check if task exists in task manager (for active tasks)
                task_status = task_manager.get_task_status(crawl_task_id)
                
                if task_status:
                    # Task exists in memory
                    if task_status["status"] != "completed":
                        raise HTTPException(
                            status_code=400, 
                            detail=f"Task {crawl_task_id} is not completed"
                        )
                    
                    # Get the crawl result ID from the task result
                    result = task_status.get("result", {})
                    crawl_result_id = result.get("crawl_result_id")
                else:
                    # Task not in memory - look directly in database by task_id
                    # This handles the server restart case
                    crawl_result = await CrawlResult.find_one(CrawlResult.task_id == crawl_task_id)
                    
                    if not crawl_result:
                        raise HTTPException(
                            status_code=404, 
                            detail=f"No crawl result found for task {crawl_task_id}"
                        )
                    
                    crawl_result_id = str(crawl_result.id)
                
                if not crawl_result_id:
                    raise HTTPException(
                        status_code=400, 
                        detail=f"Task {crawl_task_id} does not have associated crawl results"
                    )
                
                # Verify that year extraction is available for this crawl
                try:
                    crawl_result = await CrawlResult.get(crawl_result_id)
                    if not crawl_result or not crawl_result.year_extraction_complete:
                        raise HTTPException(
                            status_code=400, 
                            detail=f"Task {crawl_task_id} does not have year extraction results"
                        )
                except Exception as e:
                    logger.error(f"Error checking crawl result {crawl_result_id}: {str(e)}")
                    raise HTTPException(
                        status_code=500, 
                        detail=f"Error checking crawl result: {str(e)}"
                    )
                    
            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"Error finding crawl result for task {crawl_task_id}: {str(e)}")
                raise HTTPException(
                    status_code=500, 
                    detail=f"Error finding crawl result: {str(e)}"
                )
        
        # Get the year data from database
        try:
            logger.info(f"Retrieving year {year} from database")
            year_data = await orchestrator.get_year_by_id(year, crawl_result_id)
            if not year_data:
                raise HTTPException(
                    status_code=404, 
                    detail=f"Year {year} not found"
                )
            
            return year_data
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error retrieving year {year}: {str(e)}")
            raise HTTPException(
                status_code=500, 
                detail=f"Error retrieving year data: {str(e)}"
            )