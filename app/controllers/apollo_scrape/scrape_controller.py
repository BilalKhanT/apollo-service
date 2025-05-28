from typing import Dict, List, Optional
from fastapi import HTTPException
from app.utils.task_manager import task_manager
from app.utils.realtime_publisher import realtime_publisher
from app.models.apollo_scrape.scrape_model import ScrapingStatus
from app.controllers.apollo_scrape.crawl_result_controller import CrawlResultController
import logging

logger = logging.getLogger(__name__)


class ScrapeController:

    @staticmethod
    async def start_scrape(
        cluster_data: Dict[str, List[str]],  
        year_data: Dict[str, List[str]],     
        crawl_task_id: Optional[str] = None
    ) -> ScrapingStatus:

        if cluster_data:
            for cluster_id, links in cluster_data.items():
                if not cluster_id or not cluster_id.strip():
                    raise HTTPException(status_code=400, detail="Cluster ID cannot be empty")
                if not links or not all(isinstance(link, str) and link.strip() for link in links):
                    raise HTTPException(status_code=400, detail=f"Invalid links for cluster {cluster_id}")

        if year_data:
            for year, links in year_data.items():
                if year != "No Year" and (not year.isdigit() or len(year) != 4):
                    raise HTTPException(status_code=400, detail=f"Invalid year format: {year}")
                if not links or not all(isinstance(link, str) and link.strip() for link in links):
                    raise HTTPException(status_code=400, detail=f"Invalid links for year {year}")

        if crawl_task_id:
            crawl_result = await CrawlResultController.get_crawl_result(crawl_task_id)
            if not crawl_result:
                raise HTTPException(status_code=404, detail=f"Crawl result for task {crawl_task_id} not found")

            if cluster_data and crawl_result.clusters:
                available_cluster_ids = set()
                for domain_data in crawl_result.clusters.values():
                    available_cluster_ids.add(domain_data.id)
                    for cluster in domain_data.clusters:
                        available_cluster_ids.add(cluster.id)
                
                invalid_clusters = [cid for cid in cluster_data.keys() if cid not in available_cluster_ids]
                if invalid_clusters:
                    raise HTTPException(
                        status_code=400, 
                        detail=f"Invalid cluster IDs: {invalid_clusters}. Available clusters: {list(available_cluster_ids)}"
                    )

            if year_data and crawl_result.yearclusters:
                available_years = set(crawl_result.yearclusters.keys())
                invalid_years = [year for year in year_data.keys() if year not in available_years]
                if invalid_years:
                    raise HTTPException(
                        status_code=400, 
                        detail=f"Invalid years: {invalid_years}. Available years: {list(available_years)}"
                    )

        task_id = task_manager.create_task(
            task_type="scrape",
            params={
                "cluster_data": cluster_data,  
                "year_data": year_data,        
                "crawl_task_id": crawl_task_id
            }
        )
        
        task_status = task_manager.get_task_status(task_id)

        try:
            await realtime_publisher.start_publishing(task_id, interval=1.5)
            logger.info(f"Started real-time publishing for scrape task {task_id}")
        except Exception as e:
            logger.warning(f"Failed to start real-time publishing for task {task_id}: {str(e)}")
        
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

            if current_status in ["scraping", "preparing", "initializing", "checking_database"]:
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

        if current_status in ["created", "running", "initializing", "checking_database", "preparing", "scraping", "preparing_download", "downloading", "finalizing"]:
            if not realtime_publisher.is_publishing(task_id):
                try:
                    await realtime_publisher.start_publishing(task_id, interval=1.5)
                    logger.debug(f"Started real-time publishing for existing scrape task {task_id}")
                except Exception as e:
                    logger.warning(f"Failed to start real-time publishing for task {task_id}: {str(e)}")
        
        return ScrapingStatus(
            id=task_id,
            status=current_status,
            progress=task_status.get("progress", 0.0),
            pages_scraped=pages_scraped,
            files_downloaded=files_downloaded,
            error=task_status.get("error")
        )