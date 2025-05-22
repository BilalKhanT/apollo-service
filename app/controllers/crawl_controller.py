from typing import Dict, Any, List, Optional
from fastapi import HTTPException, status
from app.utils.task_manager import task_manager
from app.utils.orchestrator import orchestrator
from app.models.crawl_model import CrawlStatus
from app.models.database.database_models import CrawlResult
import asyncio

class CrawlController:
    @staticmethod
    async def start_crawl(
        base_url: str,
        max_links_to_scrape: Optional[int] = None,
        max_pages_to_scrape: Optional[int] = None,
        depth_limit: Optional[int] = None,
        domain_restriction: bool = True,
        scrape_pdfs_and_xls: bool = True,
        stop_scraper: bool = False
    ) -> CrawlStatus:
        """
        Start a crawl task with database integration.
        """
        # Create task in task manager
        task_id = task_manager.create_task(
            task_type="crawl",
            params={
                "base_url": base_url,
                "max_links_to_scrape": max_links_to_scrape,
                "max_pages_to_scrape": max_pages_to_scrape,
                "depth_limit": depth_limit,
                "domain_restriction": domain_restriction,
                "scrape_pdfs_and_xls": scrape_pdfs_and_xls,
                "stop_scraper": stop_scraper
            }
        )
        
        task_status = task_manager.get_task_status(task_id)
        
        return CrawlStatus(
            id=task_id,
            status=task_status["status"],
            progress=task_status["progress"],
            current_stage=task_status["status"],
            links_found=0,
            pages_scraped=0,
            error=task_status.get("error"),
            clusters_ready=False
        )
    
    @staticmethod
    async def get_crawl_status(task_id: str) -> CrawlStatus:
        """
        Get crawl status with database integration support.
        """
        task_status = task_manager.get_task_status(task_id)
        
        if not task_status:
            raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
        
        result = task_status.get("result") or {}
        
        if task_status["type"] != "crawl":
            raise HTTPException(status_code=400, detail=f"Task {task_id} is not a crawl task")
        
        # Extract crawl results
        crawl_results = result.get("crawl_results", {})
        links_found = crawl_results.get("total_links_found", 0)
        pages_scraped = crawl_results.get("total_pages_scraped", 0)
        
        # Check if clusters are ready based on database storage
        clusters_ready = False
        if task_status["status"] == "completed":
            # For completed tasks, check if we have database results
            clusters_ready = (
                result.get("cluster_complete", False) and
                result.get("year_extraction_complete", False) and
                result.get("database_saved", False)
            )
        
        # Determine current stage based on status
        current_stage = task_status["status"]
        
        # Map internal status to user-friendly stage names
        stage_mapping = {
            "created": "Initializing",
            "running": "Crawling",
            "crawling": "Crawling websites",
            "processing": "Processing links",
            "clustering": "Clustering URLs",
            "year_extraction": "Extracting years",
            "saving_to_database": "Saving to database",
            "completed": "Completed",
            "failed": "Failed",
            "stopped": "Stopped"
        }
        
        current_stage = stage_mapping.get(current_stage, current_stage)
        
        return CrawlStatus(
            id=task_id,
            status=task_status["status"],
            progress=task_status["progress"],
            current_stage=current_stage,
            links_found=links_found,
            pages_scraped=pages_scraped,
            error=task_status.get("error"),
            clusters_ready=clusters_ready
        )
    
    @staticmethod
    async def stop_crawl_task(task_id: str) -> Dict[str, Any]:
        """
        Stop a crawl task with proper cleanup.
        """
        task_status = task_manager.get_task_status(task_id)
        
        if not task_status:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Task {task_id} not found"
            )
        
        if task_status.get("type") != "crawl":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Task {task_id} is not a crawl task"
            )
        
        # Use orchestrator to stop the crawl (this handles both file and database cleanup)
        stop_result = orchestrator.stop_crawl(task_id)
        
        if stop_result.get("success"):
            return {
                "id": task_id,
                "status": "stopped",
                "message": stop_result.get("message"),
                "cleanup_completed": stop_result.get("cleanup_completed", False)
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=stop_result.get("message", "Failed to stop task")
            )

    @staticmethod
    async def get_crawl_history(limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get crawl history from database.
        
        Args:
            limit: Maximum number of crawls to return
            
        Returns:
            List of crawl summaries
        """
        try:
            crawl_results = await CrawlResult.find().sort([("created_at", -1)]).limit(limit).to_list()
            
            history = []
            for crawl_result in crawl_results:
                crawl_data = {
                    "id": str(crawl_result.id),
                    "task_id": crawl_result.task_id,
                    "crawl_id": crawl_result.crawl_id,
                    "created_at": crawl_result.created_at.isoformat(),
                    "updated_at": crawl_result.updated_at.isoformat(),
                    "crawl_complete": crawl_result.crawl_complete,
                    "process_complete": crawl_result.process_complete,
                    "cluster_complete": crawl_result.cluster_complete,
                    "year_extraction_complete": crawl_result.year_extraction_complete,
                    "scraping_complete": crawl_result.scraping_complete
                }
                
                # Add crawl summary if available
                if crawl_result.crawl_summary:
                    crawl_data["summary"] = {
                        "base_url": crawl_result.crawl_summary.base_url,
                        "total_pages_scraped": crawl_result.crawl_summary.total_pages_scraped,
                        "total_links_found": crawl_result.crawl_summary.total_links_found,
                        "total_unique_links": crawl_result.crawl_summary.total_unique_links,
                        "execution_time_seconds": crawl_result.crawl_summary.execution_time_seconds,
                        "is_complete": crawl_result.crawl_summary.is_complete
                    }
                
                # Add cluster and year counts if available
                if crawl_result.cluster_summary:
                    crawl_data["cluster_summary"] = {
                        "total_domains": crawl_result.cluster_summary.total_domains,
                        "total_clusters": crawl_result.cluster_summary.total_clusters,
                        "total_urls": crawl_result.cluster_summary.total_urls
                    }
                
                if crawl_result.year_extraction_summary:
                    crawl_data["year_summary"] = {
                        "total_years": crawl_result.year_extraction_summary.total_years,
                        "total_files": crawl_result.year_extraction_summary.total_files
                    }
                
                history.append(crawl_data)
            
            return history
            
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error retrieving crawl history: {str(e)}"
            )

    @staticmethod
    async def get_crawl_details(crawl_result_id: str) -> Dict[str, Any]:
        """
        Get detailed crawl information from database.
        
        Args:
            crawl_result_id: Database ID of the crawl result
            
        Returns:
            Detailed crawl information
        """
        try:
            crawl_result = await CrawlResult.get(crawl_result_id)
            
            if not crawl_result:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Crawl result {crawl_result_id} not found"
                )
            
            # Build detailed response
            details = {
                "id": str(crawl_result.id),
                "task_id": crawl_result.task_id,
                "crawl_id": crawl_result.crawl_id,
                "created_at": crawl_result.created_at.isoformat(),
                "updated_at": crawl_result.updated_at.isoformat(),
                "completion_status": {
                    "crawl_complete": crawl_result.crawl_complete,
                    "process_complete": crawl_result.process_complete,
                    "cluster_complete": crawl_result.cluster_complete,
                    "year_extraction_complete": crawl_result.year_extraction_complete,
                    "scraping_complete": crawl_result.scraping_complete
                }
            }
            
            # Add detailed summaries
            if crawl_result.crawl_summary:
                details["crawl_summary"] = crawl_result.crawl_summary.dict()
            
            if crawl_result.process_summary:
                details["process_summary"] = crawl_result.process_summary.dict()
            
            if crawl_result.cluster_summary:
                details["cluster_summary"] = crawl_result.cluster_summary.dict()
            
            if crawl_result.year_extraction_summary:
                details["year_extraction_summary"] = crawl_result.year_extraction_summary.dict()
            
            # Add output files information if available
            if crawl_result.output_files:
                details["output_files"] = crawl_result.output_files
            
            return details
            
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error retrieving crawl details: {str(e)}"
            )

    @staticmethod
    async def get_all_crawl_results(limit: int = 50) -> Dict[str, Any]:
        """
        Get all crawl results from database.
        
        Args:
            limit: Maximum number of results to return
            
        Returns:
            Dictionary containing list of crawl results and metadata
        """
        try:
            results = await orchestrator.get_all_crawl_results(limit)
            
            return {
                "crawl_results": results,
                "count": len(results),
                "limit": limit
            }
            
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error retrieving all crawl results: {str(e)}"
            )