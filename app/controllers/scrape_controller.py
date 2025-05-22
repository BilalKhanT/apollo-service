from typing import List, Optional
from fastapi import HTTPException
from app.utils.task_manager import task_manager
from app.utils.orchestrator import orchestrator
from app.models.scrape_model import ScrapingStatus
from app.models.database.database_models import CrawlResult

class ScrapeController:
    @staticmethod
    async def start_scrape(
        cluster_ids: List[str],
        years: List[str],
        crawl_task_id: Optional[str] = None
    ) -> ScrapingStatus:
        url_clusters_file = None
        year_clusters_file = None
        crawl_result_id = None
        
        if crawl_task_id:
            task_status = task_manager.get_task_status(crawl_task_id)
            if not task_status:
                raise HTTPException(status_code=404, detail=f"Task {crawl_task_id} not found")
            
            if task_status["status"] != "completed":
                raise HTTPException(status_code=400, detail=f"Task {crawl_task_id} is not completed")
            
            result = task_status.get("result", {})
            crawl_result_id = result.get("crawl_result_id")
            
            if not crawl_result_id:
                raise HTTPException(status_code=400, detail=f"Task {crawl_task_id} does not have associated crawl results")
            
            # Verify that the crawl result exists and has required data
            try:
                crawl_result = await CrawlResult.get(crawl_result_id)
                if not crawl_result:
                    raise HTTPException(status_code=404, detail=f"Crawl result {crawl_result_id} not found")
                
                # Check if clusters and years are available
                if cluster_ids and not crawl_result.cluster_complete:
                    raise HTTPException(status_code=400, detail=f"Clusters are not ready for crawl {crawl_task_id}")
                
                if years and not crawl_result.year_extraction_complete:
                    raise HTTPException(status_code=400, detail=f"Year extraction is not ready for crawl {crawl_task_id}")
                    
            except HTTPException:
                raise
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Error verifying crawl result: {str(e)}")
            
            # Get output files from task result for backward compatibility
            output_files = result.get("output_files", {})
            url_clusters_file = output_files.get("url_clusters_file")
            year_clusters_file = output_files.get("year_clusters_file")
        
        task_id = task_manager.create_task(
            task_type="scrape",
            params={
                "cluster_ids": cluster_ids,
                "years": years,
                "crawl_task_id": crawl_task_id,
                "crawl_result_id": crawl_result_id,
                "url_clusters_file": url_clusters_file,
                "year_clusters_file": year_clusters_file
            }
        )
        
        task_status = task_manager.get_task_status(task_id)
        
        return ScrapingStatus(
            id=task_id,
            status=task_status["status"],
            progress=task_status["progress"],
            pages_scraped=0,
            files_downloaded=0,
            error=task_status.get("error")
        )
    
    @staticmethod
    async def get_scrape_status(task_id: str) -> ScrapingStatus:
        task_status = task_manager.get_task_status(task_id)
        
        if not task_status:
            raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
        
        if task_status.get("type") != "scrape":
            raise HTTPException(status_code=400, detail=f"Task {task_id} is not a scrape task")
        
        pages_scraped = 0
        files_downloaded = 0
        current_status = task_status.get("status", "unknown")
        result = task_status.get("result", {})
        
        if result and isinstance(result, dict):
            scrape_results = result.get("scrape_results", {})
            if isinstance(scrape_results, dict):
                pages_scraped = scrape_results.get("pages_scraped", 0)
            
            if current_status in ["scraping", "preparing"]:
                partial_scrape = result.get("scrape_partial_results", {})
                if isinstance(partial_scrape, dict) and partial_scrape.get("pages_scraped", 0) > 0:
                    pages_scraped = partial_scrape.get("pages_scraped", 0)
             
            download_results = result.get("download_results", {})
            if isinstance(download_results, dict):
                files_downloaded = download_results.get("files_downloaded", 0)

            if current_status in ["downloading", "preparing_download"]:
                partial_download = result.get("download_partial_results", {})
                if isinstance(partial_download, dict) and partial_download.get("files_downloaded", 0) > 0:
                    files_downloaded = partial_download.get("files_downloaded", 0)
        
        return ScrapingStatus(
            id=task_id,
            status=current_status,
            progress=task_status.get("progress", 0.0),
            pages_scraped=pages_scraped,
            files_downloaded=files_downloaded,
            error=task_status.get("error")
        )