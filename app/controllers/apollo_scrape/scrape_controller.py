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

        crawled_clusters = set()
        user_pref_clusters = set()

        if crawl_task_id:
            try:
                crawl_result = await CrawlResultController.get_crawl_result(crawl_task_id)
                if crawl_result and crawl_result.clusters:
                    for domain_data in crawl_result.clusters.values():
                        crawled_clusters.add(domain_data.id)
                        for cluster in domain_data.clusters:
                            crawled_clusters.add(cluster.id)
                    logger.info(f"Found {len(crawled_clusters)} clusters in crawl data")
            except Exception as e:
                logger.warning(f"Could not load crawled clusters: {str(e)}")

        user_pref_clusters = await ScrapeController.get_user_preference_clusters()

        crawled_found = []
        user_pref_found = []
        custom_clusters = []
        
        for cluster_id in cluster_data.keys():
            if cluster_id in crawled_clusters:
                crawled_found.append(cluster_id)
            elif cluster_id in user_pref_clusters:
                user_pref_found.append(cluster_id)
            else:
                custom_clusters.append(cluster_id)

        logger.info(f"Cluster validation results:")
        logger.info(f"  - From crawl data: {crawled_found}")
        logger.info(f"  - From user preferences: {user_pref_found}")
        logger.info(f"  - Custom clusters: {custom_clusters}")
        
        if custom_clusters:
            logger.info(f"Proceeding with custom clusters: {custom_clusters}")

        return cluster_data

    @staticmethod
    async def validate_years(
        year_data: Dict[str, List[str]], 
        crawl_task_id: Optional[str] = None
    ) -> Dict[str, List[str]]:
        if not year_data:
            return {}

        crawled_years = set()
        user_pref_years = set()

        if crawl_task_id:
            try:
                crawl_result = await CrawlResultController.get_crawl_result(crawl_task_id)
                if crawl_result and crawl_result.yearclusters:
                    crawled_years = set(crawl_result.yearclusters.keys())
                    logger.info(f"Found {len(crawled_years)} years in crawl data")
            except Exception as e:
                logger.warning(f"Could not load crawled years: {str(e)}")

        user_pref_years = await ScrapeController.get_user_preference_years()

        crawled_found = []
        user_pref_found = []
        custom_years = []
        
        for year_name in year_data.keys():
            if year_name in crawled_years:
                crawled_found.append(year_name)
            elif year_name in user_pref_years:
                user_pref_found.append(year_name)
            else:
                custom_years.append(year_name)

        logger.info(f"Year validation results:")
        logger.info(f"  - From crawl data: {crawled_found}")
        logger.info(f"  - From user preferences: {user_pref_found}")
        logger.info(f"  - Custom years/clusters: {custom_years}")
        
        if custom_years:
            logger.info(f"Proceeding with custom years/clusters: {custom_years}")

        return year_data

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
                if not year or not year.strip():
                    raise HTTPException(status_code=400, detail="Year/cluster name cannot be empty")
                if not links or not all(isinstance(link, str) and link.strip() for link in links):
                    raise HTTPException(status_code=400, detail=f"Invalid links for year/cluster {year}")

        if crawl_task_id:
            try:
                crawl_result = await CrawlResultController.get_crawl_result(crawl_task_id)
                if not crawl_result:
                    raise HTTPException(status_code=404, detail=f"Crawl result for task {crawl_task_id} not found")
                logger.info(f"Using crawl task {crawl_task_id} for reference data")
            except HTTPException:
                raise
            except Exception as e:
                logger.warning(f"Could not verify crawl task {crawl_task_id}: {str(e)}")

        validated_cluster_data = await ScrapeController.validate_clusters(cluster_data, crawl_task_id)
        validated_year_data = await ScrapeController.validate_years(year_data, crawl_task_id)

        total_clusters = len(validated_cluster_data) if validated_cluster_data else 0
        total_years = len(validated_year_data) if validated_year_data else 0
        
        logger.info(f"Starting scrape task with:")
        logger.info(f"  - {total_clusters} cluster(s): {list(validated_cluster_data.keys()) if validated_cluster_data else []}")
        logger.info(f"  - {total_years} year/cluster(s): {list(validated_year_data.keys()) if validated_year_data else []}")

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
        
        logger.info(f"Scrape task {task_id} created successfully with flexible validation")
        
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