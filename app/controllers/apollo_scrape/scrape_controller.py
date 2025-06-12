from typing import Dict, List, Optional, Set
from fastapi import HTTPException
from app.utils.task_manager import task_manager
from app.utils.realtime_publisher import realtime_publisher
from app.models.apollo_scrape.scrape_model import ScrapingStatus
from app.controllers.apollo_scrape.crawl_result_controller import CrawlResultController
from app.controllers.apollo_scrape.user_pref_controller import UserPreferenceController
import logging

logger = logging.getLogger(__name__)

class ScrapeController:

    @staticmethod
    async def get_user_preference_clusters() -> Set[str]:
        try:
            user_pref = await UserPreferenceController.get_user_preference()
            if user_pref and user_pref.clusters:
                cluster_names = set(user_pref.clusters.keys())
                logger.info(f"Found {len(cluster_names)} clusters in user preferences: {list(cluster_names)}")
                return cluster_names
            return set()
        except Exception as e:
            logger.warning(f"Could not load user preference clusters: {str(e)}")
            return set()

    @staticmethod
    async def get_user_preference_years() -> Set[str]:
        try:
            user_pref = await UserPreferenceController.get_user_preference()
            if user_pref and user_pref.years:
                year_names = set(user_pref.years.keys())
                logger.info(f"Found {len(year_names)} years in user preferences: {list(year_names)}")
                return year_names
            return set()
        except Exception as e:
            logger.warning(f"Could not load user preference years: {str(e)}")
            return set()

    @staticmethod
    async def validate_clusters(
        cluster_data: Dict[str, List[str]], 
        crawl_task_id: Optional[str] = None
    ) -> Dict[str, List[str]]:
        if not cluster_data:
            return {}

        validated_data = {}
        for cluster_id, urls in cluster_data.items():
            if not cluster_id or not cluster_id.strip():
                logger.warning(f"Empty cluster ID found, skipping")
                continue
                
            if not urls or not isinstance(urls, list):
                logger.warning(f"Invalid URLs for cluster {cluster_id}, skipping")
                continue
                
            valid_urls = [url for url in urls if url and isinstance(url, str) and url.strip()]
            if valid_urls:
                validated_data[cluster_id] = valid_urls
                logger.info(f"Validated cluster '{cluster_id}': {len(valid_urls)} URLs")
            else:
                logger.warning(f"No valid URLs found for cluster {cluster_id}")

        logger.info(f"Cluster validation completed: {len(validated_data)} clusters validated")
        return validated_data

    @staticmethod
    async def validate_years(
        year_data: Dict[str, List[str]], 
        crawl_task_id: Optional[str] = None
    ) -> Dict[str, List[str]]:
        if not year_data:
            return {}

        validated_data = {}
        for year, urls in year_data.items():
            if not year or not year.strip():
                logger.warning(f"Empty year found, skipping")
                continue
                
            if not urls or not isinstance(urls, list):
                logger.warning(f"Invalid URLs for year {year}, skipping")
                continue
                
            valid_urls = [url for url in urls if url and isinstance(url, str) and url.strip()]
            if valid_urls:
                validated_data[year] = valid_urls
                logger.info(f"Validated year '{year}': {len(valid_urls)} URLs")
            else:
                logger.warning(f"No valid URLs found for year {year}")

        logger.info(f"Year validation completed: {len(validated_data)} years validated")
        return validated_data

    @staticmethod
    async def start_scrape(
        bot_id: str,
        cluster_data: Dict[str, List[str]],  
        year_data: Dict[str, List[str]] = None,     
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
                if not year or not year.strip():
                    raise HTTPException(status_code=400, detail="Year/cluster name cannot be empty")
                if not links or not all(isinstance(link, str) and link.strip() for link in links):
                    raise HTTPException(status_code=400, detail=f"Invalid links for year/cluster {year}")

        # Validate cluster and year data
        validated_cluster_data = await ScrapeController.validate_clusters(cluster_data, crawl_task_id)
        validated_year_data = await ScrapeController.validate_years(year_data or {}, crawl_task_id)

        total_clusters = len(validated_cluster_data) if validated_cluster_data else 0
        total_years = len(validated_year_data) if validated_year_data else 0
        
        logger.info(f"Starting scrape task with:")
        logger.info(f"  - {total_clusters} cluster(s): {list(validated_cluster_data.keys()) if validated_cluster_data else []}")
        logger.info(f"  - {total_years} year/cluster(s): {list(validated_year_data.keys()) if validated_year_data else []}")

        # Create task with simplified parameters
        task_id = task_manager.create_task(
            task_type="scrape",
            params={
                "cluster_data": validated_cluster_data,  
                "year_data": validated_year_data,        
                "crawl_task_id": crawl_task_id,
                "total_clusters": total_clusters,
                "total_years": total_years
            }
        )
        
        task_status = task_manager.get_task_status(task_id)

        try:
            await realtime_publisher.start_publishing(task_id, interval=1.5)
            logger.info(f"Started real-time publishing for scrape task {task_id}")
        except Exception as e:
            logger.warning(f"Failed to start real-time publishing for task {task_id}: {str(e)}")
        
        logger.info(f"Scrape task {task_id} created successfully with simplified validation")
        
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