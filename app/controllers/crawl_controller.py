from typing import Dict, Any, Optional
from fastapi import HTTPException, status
from app.utils.task_manager import task_manager
from app.utils.orchestrator import orchestrator
from app.models.crawl_model import CrawlStatus

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
        task_status = task_manager.get_task_status(task_id)
        
        if not task_status:
            raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
        
        result = task_status.get("result") or {}
        
        if task_status["type"] != "crawl":
            raise HTTPException(status_code=400, detail=f"Task {task_id} is not a crawl task")
        
        crawl_results = result.get("crawl_results", {})
        links_found = crawl_results.get("total_links_found", 0)
        pages_scraped = crawl_results.get("total_pages_scraped", 0)
        
        clusters_ready = (
            result.get("cluster_complete", False) and
            result.get("year_extraction_complete", False)
        )
        
        current_stage = task_status["status"]
        
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