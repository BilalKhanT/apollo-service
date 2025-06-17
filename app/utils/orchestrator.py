from datetime import datetime
import os
import logging
import time
import traceback
import asyncio
from typing import Dict, Any, List, Optional
import json
from pathlib import Path
import httpx
import pytz
from app.controllers.fb_scrape.fb_scrape_controller import FacebookScrapeController
from app.services.fb_scrape.fb_scrape_service import FacebookScrapingService
from app.utils.task_manager import task_manager
from app.utils.config import (
    CRAWLER_USER_AGENT, CRAWLER_TIMEOUT, CRAWLER_NUM_WORKERS,
    CRAWLER_DELAY_BETWEEN_REQUESTS, CRAWLER_INACTIVITY_TIMEOUT,
    CRAWLER_SAVE_INTERVAL, CRAWLER_RESPECT_ROBOTS_TXT,
    DEFAULT_URL_PATTERNS_TO_IGNORE, DOCUMENT_BULK_URL, FILE_EXTENSIONS,
    SOCIAL_MEDIA_KEYWORDS, BANK_KEYWORDS, CLUSTER_MIN_SIZE,
    CLUSTER_PATH_DEPTH, CLUSTER_SIMILARITY_THRESHOLD,
    EXPIRY_DAYS,MAX_DOWNLOAD_WORKERS, DATA_DIR, ACCESS_TOKEN, PAGE_ID
)
from app.services.apollo_scrape.apollo import Apollo
from app.services.apollo_scrape.link_processor import LinkProcessor
from app.services.apollo_scrape.url_clusterer import URLClusterer
from app.services.apollo_scrape.year_extractor import YearExtractor
from app.services.apollo_scrape.scraper import ClusterScraper
from app.services.apollo_scrape.downloader import FileDownloader
from app.services.restaurant_deal.deal_scrape_service import DealScrapperService
from app.controllers.apollo_scrape.crawl_result_controller import CrawlResultController
from app.controllers.restaurant_deal.deal_scrape_controller import DealScrapeController
from app.controllers.apollo_scrape.scrape_data_controller import ScrapeDataController
from app.models.database.apollo_scraper.crawl_result_model import UploadStatus

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class ApolloOrchestrator:
    
    def __init__(self, base_directory: str = None):
        self.base_directory = base_directory or DATA_DIR
        self.logger = logger

        os.makedirs(self.base_directory, exist_ok=True)
        self.temp_dir = os.path.join(self.base_directory, "temp")  
        self.scrape_dir = os.path.join(self.base_directory, "scraped")
        self.download_dir = os.path.join(self.base_directory, "downloads")
        self.metadata_dir = os.path.join(self.base_directory, "metadata")
        
        for directory in [self.temp_dir, self.scrape_dir, self.download_dir, self.metadata_dir]:
            os.makedirs(directory, exist_ok=True)

    def publish_log(self, task_id: str, message: str, level: str = "info"):
        task_manager.publish_log(task_id, message, level)

        if level == "debug":
            self.logger.debug(message)
        elif level == "info":
            self.logger.info(message)
        elif level == "warning":
            self.logger.warning(message)
        elif level == "error":
            self.logger.error(message)
    
    def cleanup_temp_files(self, files_to_cleanup: List[str]):
        for file_path in files_to_cleanup:
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    self.logger.debug(f"Cleaned up temp file: {file_path}")
            except Exception as e:
                self.logger.warning(f"Failed to cleanup temp file {file_path}: {str(e)}")
    
    async def _start_realtime_publishing(self, task_id: str, interval: float = 1.0):
        try:
            from app.utils.realtime_publisher import realtime_publisher
            await realtime_publisher.start_publishing(task_id, interval=interval)
            self.publish_log(task_id, "Started real-time publishing for task", "info")
            return True
        except ImportError:
            self.logger.debug(f"WebSocket not available for task {task_id}, continuing without real-time publishing")
            return False
        except Exception as e:
            self.publish_log(task_id, f"Warning: Could not start real-time publishing: {str(e)}", "warning")
            return False
    
    async def _stop_realtime_publishing(self, task_id: str):
        try:
            from app.utils.realtime_publisher import realtime_publisher
            await realtime_publisher.stop_publishing(task_id)
            self.publish_log(task_id, "Stopped real-time publishing for task", "info")
        except ImportError:
            pass
        except Exception as e:
            self.publish_log(task_id, f"Warning: Could not stop real-time publishing: {str(e)}", "warning")
    
    async def run_crawl(
        self,
        task_id: str,
        base_url: str,
        max_links_to_scrape: Optional[int] = None,
        max_pages_to_scrape: Optional[int] = None,
        depth_limit: Optional[int] = None,
        domain_restriction: bool = True,
        scrape_pdfs_and_xls: bool = True,
        stop_scraper: bool = False
    ) -> Dict[str, Any]:
        await self._start_realtime_publishing(task_id, interval=1.0)

        processor = None
        clusterer = None
        year_extractor = None
        crawler = None

        if max_links_to_scrape is None:
            max_links_to_scrape = float("inf")
        if max_pages_to_scrape is None:
            max_pages_to_scrape = float("inf")
        if depth_limit is None:
            depth_limit = float("inf")

        task_manager.update_task_status(
            task_id,
            status="running",
            progress=0.0
        )

        self.publish_log(task_id, f"Starting crawl workflow for {base_url}", "info")

        if stop_scraper:
            self.publish_log(task_id, "Stop signal received. Checking for running crawlers...", "info")
            running_tasks = task_manager.list_tasks(task_type="crawl", status="running")
            for task in running_tasks:
                if task['id'] == task_id:
                    continue
                
                self.publish_log(task_id, f"Stopping crawler task {task['id']}...", "info")
            
            await self._stop_realtime_publishing(task_id)
            
            task_manager.update_task_status(task_id, status="completed", progress=100.0)
            return {"status": "stopped", "message": "Stop signal sent to all running crawlers"}

        temp_files = []     
        try:
            task_manager.update_task_status(
                task_id,
                status="crawling",
                progress=5.0
            )

            timestamp = time.strftime("%Y%m%d-%H%M%S")
            crawl_id = f"{timestamp}_{base_url.replace('://', '_').replace('/', '_')[:30]}"

            all_links_file = os.path.join(self.temp_dir, f"{crawl_id}_all_links.json")
            categorized_file = os.path.join(self.temp_dir, f"{crawl_id}_categorized.json")
            url_clusters_file = os.path.join(self.temp_dir, f"{crawl_id}_url_clusters.json")
            year_clusters_file = os.path.join(self.temp_dir, f"{crawl_id}_year_clusters.json")
            
            temp_files.extend([all_links_file, categorized_file, url_clusters_file, year_clusters_file])

            self.publish_log(task_id, f"Starting crawler for {base_url} with limits: max_links={max_links_to_scrape}, max_pages={max_pages_to_scrape}, depth_limit={depth_limit}", "info")

            def status_callback(status_data):
                execution_time_seconds = status_data.get('execution_time_seconds', 0)
                crawler_progress = status_data.get('progress', 0)
                
                if not hasattr(self, '_last_progress'):
                    self._last_progress = 5.0
                    self._last_update_time = time.time()

                if (max_links_to_scrape == float("inf") or 
                    max_pages_to_scrape == float("inf") or 
                    depth_limit == float("inf")):
                    
                    current_time = time.time()
                    time_since_update = current_time - self._last_update_time
                    
                    if time_since_update >= 2.0:
                        increase_amount = (time_since_update / 5.0)
                        new_progress = min(95.0, self._last_progress + increase_amount)
                        
                        self._last_update_time = current_time
                        progress = new_progress
                    else:
                        progress = self._last_progress
                else:
                    if crawler_progress >= 99.0:
                        progress = 40.0
                    else:
                        progress = (crawler_progress / 100) * 40.0
                
                self._last_progress = progress

                crawl_result = {
                    "crawl_results": {
                        "total_links_found": status_data.get('links_found', 0),
                        "total_pages_scraped": status_data.get('pages_scraped', 0),
                        "execution_time_seconds": execution_time_seconds
                    }
                }

                task_manager.update_task_status(
                    task_id,
                    progress=progress,
                    result=crawl_result
                )

                if status_data.get('pages_scraped', 0) % 2 == 0:
                    self.publish_log(
                        task_id,
                        f"Crawl progress: {status_data.get('pages_scraped', 0)} pages scraped, "
                        f"{status_data.get('links_found', 0)} links found, progress: {progress:.1f}%",
                        "info"
                    )

            crawler = Apollo(
                base_url=base_url,
                output_file=all_links_file,
                max_links_to_scrape=max_links_to_scrape,
                max_pages_to_scrape=max_pages_to_scrape,
                depth_limit=depth_limit,
                domain_restriction=domain_restriction,
                url_patterns_to_ignore=DEFAULT_URL_PATTERNS_TO_IGNORE,
                scrape_pdfs_and_xls=scrape_pdfs_and_xls,
                delay_between_requests=CRAWLER_DELAY_BETWEEN_REQUESTS,
                respect_robots_txt=CRAWLER_RESPECT_ROBOTS_TXT,
                user_agent=CRAWLER_USER_AGENT,
                timeout=CRAWLER_TIMEOUT,
                num_workers=CRAWLER_NUM_WORKERS,
                save_interval=CRAWLER_SAVE_INTERVAL,
                inactivity_timeout=CRAWLER_INACTIVITY_TIMEOUT
            )

            setattr(self, f"crawler_{task_id}", crawler)
            crawler.register_status_callback(status_callback)

            self.publish_log(task_id, f"Starting crawler for {base_url}", "info")
            loop = asyncio.get_event_loop()
            crawl_result = await loop.run_in_executor(None, crawler.start)

            crawler_status = crawler.get_status()

            crawl_completed_successfully = (
                crawl_result and 
                crawl_result.get('summary') and
                crawl_result['summary'].get('total_pages_scraped', 0) > 0 and
                crawl_result['summary'].get('total_links_found', 0) > 0
            )

            manually_stopped = (
                (crawler_status.get('was_stopped') or crawler_status.get('status') == 'stopped') 
                and not crawl_completed_successfully
            )
            
            if manually_stopped:
                self.publish_log(task_id, "Crawling was manually stopped by user", "info")

                if hasattr(self, f"crawler_{task_id}"):
                    delattr(self, f"crawler_{task_id}")

                self.cleanup_temp_files(temp_files)

                task_manager.update_task_status(
                    task_id,
                    status="stopped",
                    progress=95.0,
                    result={
                        **task_manager.get_task_status(task_id).get("result", {}),
                        "crawl_results": crawl_result.get("summary", {}) if crawl_result else {},
                        "stopped_at": datetime.now().isoformat(),
                        "stopped_gracefully": True
                    }
                )

                self.publish_log(task_id, "Crawling manually stopped. Results NOT saved to database.", "info")
                await self._stop_realtime_publishing(task_id)
                return task_manager.get_task_status(task_id)
            
            elif not crawl_completed_successfully:
                error_msg = "Crawling did not complete successfully"
                self.publish_log(task_id, error_msg, "error")

                if hasattr(self, f"crawler_{task_id}"):
                    delattr(self, f"crawler_{task_id}")

                self.cleanup_temp_files(temp_files)

                await self._stop_realtime_publishing(task_id)
                
                task_manager.update_task_status(
                    task_id,
                    status="failed",
                    error=error_msg
                )
                return task_manager.get_task_status(task_id)

            completion_reason = "by reaching limits" if crawler_status.get('status') == 'stopped' else "normally"
            self.publish_log(
                task_id,
                f"Crawling completed successfully {completion_reason}. Found {crawl_result['summary']['total_links_found']} links, scraped {crawl_result['summary']['total_pages_scraped']} pages.",
                "info"
            )

            task_manager.update_task_status(
                task_id,
                progress=40.0,
                result={"crawl_complete": True, "crawl_results": crawl_result["summary"]}
            )

            task_manager.update_task_status(
                task_id,
                status="processing",
                progress=45.0
            )

            self.publish_log(task_id, "Processing links...", "info")
            processor = LinkProcessor(
                input_file=all_links_file,
                output_file=categorized_file,
                num_workers=CRAWLER_NUM_WORKERS,
                file_extensions=FILE_EXTENSIONS,
                social_media_keywords=SOCIAL_MEDIA_KEYWORDS,
                bank_keywords=BANK_KEYWORDS
            )

            process_result = processor.process()

            self.publish_log(
                task_id,
                f"Link processing completed. Categorized {process_result['summary']['total_links']} links into {process_result['summary']['file_links_count']} file links, {process_result['summary']['bank_links_count']} bank links, {process_result['summary']['social_media_links_count']} social media links, and {process_result['summary']['misc_links_count']} miscellaneous links.",
                "info"
            )

            task_manager.update_task_status(
                task_id,
                progress=60.0,
                result={
                    **task_manager.get_task_status(task_id)["result"],
                    "process_complete": True,
                    "process_results": process_result["summary"]
                }
            )

            if process_result['summary']['bank_links_count'] > 0:
                task_manager.update_task_status(
                    task_id,
                    status="clustering",
                    progress=65.0
                )

                self.publish_log(task_id, "Clustering URLs...", "info")
                clusterer = URLClusterer(
                    input_file=categorized_file,
                    output_file=url_clusters_file,
                    min_cluster_size=CLUSTER_MIN_SIZE,
                    path_depth=CLUSTER_PATH_DEPTH,
                    similarity_threshold=CLUSTER_SIMILARITY_THRESHOLD
                )

                cluster_result = await clusterer.cluster()

                self.publish_log(
                    task_id,
                    f"URL clustering completed. Identified {cluster_result['summary']['total_domains']} domains and {cluster_result['summary']['total_clusters']} clusters across {cluster_result['summary']['total_urls']} URLs. Skipped {cluster_result['summary']['skipped_urls']} already clustered URLs.",
                    "info"
                )
            else:
                self.publish_log(task_id, "No bank links found, skipping URL clustering", "info")
                cluster_result = {
                    'summary': {'total_domains': 0, 'total_clusters': 0, 'total_urls': 0, 'skipped_urls': 0},
                    'clusters': {}
                }

            task_manager.update_task_status(
                task_id,
                progress=80.0,
                result={
                    **task_manager.get_task_status(task_id)["result"],
                    "cluster_complete": True,
                    "cluster_results": cluster_result["summary"]
                }
            )

            if process_result['summary']['file_links_count'] > 0:
                task_manager.update_task_status(
                    task_id,
                    status="year_extraction",
                    progress=85.0
                )

                self.publish_log(task_id, "Extracting years from file URLs...", "info")
                year_extractor = YearExtractor(
                    input_file=categorized_file,
                    output_file=year_clusters_file
                )

                year_result = await year_extractor.process()

                self.publish_log(
                    task_id,
                    f"Year extraction completed. Identified {year_result['summary']['total_years']} distinct years across {year_result['summary']['total_urls']} files. Skipped {year_result['summary']['skipped_urls']} already year-clustered URLs.",
                    "info"
                )
            else:
                self.publish_log(task_id, "No file links found, skipping year extraction", "info")
                year_result = {
                    'summary': {'total_years': 0, 'total_urls': 0, 'skipped_urls': 0, 'year_distribution': {}},
                    'years': {}
                }

            task_manager.update_task_status(
                task_id,
                progress=90.0,
                result={
                    **task_manager.get_task_status(task_id)["result"],
                    "year_extraction_complete": True,
                    "year_extraction_results": year_result["summary"]
                }
            )

            task_manager.update_task_status(
                task_id,
                status="saving_to_database",
                progress=95.0
            )
            
            self.publish_log(task_id, "Saving crawl results to database...", "info")
            
            try:
                await CrawlResultController.create_crawl_result(
                    task_id=task_id,
                    link_found=crawl_result["summary"]["total_links_found"],
                    pages_scraped=crawl_result["summary"]["total_pages_scraped"],
                    clusters=cluster_result["clusters"],
                    yearclusters=year_result["years"]
                )
                
                self.publish_log(task_id, "Crawl results saved to database successfully", "info")
                
            except Exception as db_error:
                error_msg = f"Failed to save crawl results to database: {str(db_error)}"
                self.publish_log(task_id, error_msg, "error")

                self.cleanup_temp_files(temp_files)

                await self._stop_realtime_publishing(task_id)
                
                task_manager.update_task_status(
                    task_id,
                    status="failed",
                    error=error_msg
                )
                return task_manager.get_task_status(task_id)

            if hasattr(self, f"crawler_{task_id}"):
                delattr(self, f"crawler_{task_id}")

            task_manager.update_task_status(
                task_id,
                status="completed",
                progress=100.0,
                result={
                    **task_manager.get_task_status(task_id)["result"],
                    "database_save_complete": True,
                    "crawl_complete": True,
                    "cluster_complete": True,
                    "year_extraction_complete": True
                }
            )
            
            self.publish_log(task_id, f"Crawl workflow completed successfully for {base_url}. Results saved to database.", "info")

            self.cleanup_temp_files(temp_files)

            return task_manager.get_task_status(task_id)
        
        except Exception as e:
            error_msg = f"Error in crawl workflow: {str(e)}"
            self.publish_log(task_id, error_msg, "error")

            if hasattr(self, f"crawler_{task_id}"):
                try:
                    delattr(self, f"crawler_{task_id}")
                except Exception:
                    pass

            self.cleanup_temp_files(temp_files)

            await self._stop_realtime_publishing(task_id)
            
            task_manager.update_task_status(
                task_id,
                status="failed",
                error=error_msg
            )
            return task_manager.get_task_status(task_id)

    def stop_crawl(self, task_id: str) -> Dict[str, Any]:

        self.publish_log(task_id, f"Attempting to stop crawl task {task_id} gracefully...", "info")

        task_status = task_manager.get_task_status(task_id)
        
        if not task_status:
            error_msg = f"Task {task_id} not found"
            self.publish_log(task_id, error_msg, "error")
            return {"success": False, "message": error_msg}

        if task_status.get("type") != "crawl":
            error_msg = f"Task {task_id} is not a crawl task"
            self.publish_log(task_id, error_msg, "error")
            return {"success": False, "message": error_msg}

        current_status = task_status.get("status")
        if current_status in ["completed", "failed", "stopped"]:
            msg = f"Task {task_id} is already in '{current_status}' state"
            self.publish_log(task_id, msg, "info")
            return {"success": True, "message": msg}

        try:
            try:
                import asyncio
                from app.utils.realtime_publisher import realtime_publisher

                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        asyncio.create_task(realtime_publisher.stop_publishing(task_id))
                    else:
                        loop.run_until_complete(realtime_publisher.stop_publishing(task_id))
                except RuntimeError:
                    asyncio.run(realtime_publisher.stop_publishing(task_id))
                    
                self.publish_log(task_id, "Stopped real-time publishing for task", "info")
            except Exception as e:
                self.publish_log(task_id, f"Warning: Could not stop real-time publishing: {str(e)}", "warning")

            crawler_instance = getattr(self, f"crawler_{task_id}", None)
            
            if crawler_instance:
                self.publish_log(task_id, "Stopping crawler...", "info")
                stop_result = crawler_instance.stop()
                
                if stop_result:
                    cleanup_result = crawler_instance.cleanup()

                    delattr(self, f"crawler_{task_id}")

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
                    
                    self.publish_log(task_id, "Crawler stopped gracefully", "info")
                    return {
                        "success": True, 
                        "message": "Crawler stopped gracefully",
                        "cleanup_completed": cleanup_result
                    }
                else:
                    self.publish_log(task_id, "Failed to stop crawler", "error")
                    return {"success": False, "message": "Failed to stop crawler"}
            else:
                self.publish_log(task_id, "No active crawler instance found, updating task status to stopped", "info")

                task_manager.update_task_status(
                    task_id,
                    status="stopped",
                    progress=95.0,  
                    result={
                        **task_status.get("result", {}),
                        "stopped_at": datetime.now().isoformat(),
                        "stopped_gracefully": False
                    }
                )
                
                return {
                    "success": True, 
                    "message": "Task marked as stopped but no active crawler found",
                    "cleanup_completed": False
                }
        
        except Exception as e:
            error_msg = f"Error stopping crawler: {str(e)}"
            self.publish_log(task_id, error_msg, "error")

            task_manager.update_task_status(
                task_id,
                status="failed",
                error=error_msg
            )
            
            return {"success": False, "message": error_msg}

    async def run_scrape_download(
        self,
        bot_id: str,
        task_id: str,
        cluster_data: Dict[str, List[str]],  
        year_data: Dict[str, List[str]] = None,  
        crawl_task_id: str = None
    ) -> Dict[str, Any]:

        await self._start_realtime_publishing(task_id, interval=1.5)

        task_manager.update_task_status(
            task_id,
            status="initializing",
            progress=0.0
        )

        self.publish_log(task_id, "Starting simplified scrape and download workflow", "info")
        
        try:
            task_manager.update_task_status(
                task_id,
                status="preparing",
                progress=3.0
            )

            # SCRAPING PHASE - SIMPLIFIED (already updated)
            if cluster_data:
                task_manager.update_task_status(
                    task_id,
                    status="scraping",
                    progress=5.0
                )

                # Mark as scraped if crawl task provided
                if crawl_task_id:
                    await CrawlResultController.mark_as_scraped(crawl_task_id, task_id)

                scrape_output_dir = os.path.join(self.scrape_dir, f"{task_id}")
                metadata_output_dir = os.path.join(self.metadata_dir, f"{task_id}")

                self.publish_log(task_id, f"Preparing to scrape {len(cluster_data)} clusters with direct URLs", "info")
                
                # Log cluster details
                total_urls = 0
                for cluster_id, urls in cluster_data.items():
                    url_count = len(urls) if urls else 0
                    total_urls += url_count
                    self.publish_log(task_id, f"Cluster '{cluster_id}': {url_count} URLs", "info")
                
                self.publish_log(task_id, f"Total URLs to scrape: {total_urls}", "info")

                # Initialize scraper with simplified approach
                scraper = ClusterScraper(
                    output_dir=scrape_output_dir,
                    metadata_dir=metadata_output_dir,
                    expiry_days=EXPIRY_DAYS
                )

                self.publish_log(task_id, f"Starting direct scraping of {len(cluster_data)} clusters", "info")
                
                loop = asyncio.get_event_loop()
                scrape_result = await loop.run_in_executor(
                    None, 
                    scraper.scrape_clusters,  
                    bot_id,
                    cluster_data,  
                    task_id
                )

                self.publish_log(
                    task_id,
                    f"Scraping completed. Scraped {scrape_result['pages_scraped']} pages from {len(scrape_result['clusters_scraped'])} clusters.",
                    "info"
                )

                task_manager.update_task_status(
                    task_id,
                    progress=60.0,
                    result={
                        "scrape_complete": True,
                        "scrape_results": {
                            "pages_scraped": scrape_result["pages_scraped"],
                            "pages_failed": scrape_result["pages_failed"],
                            "pages_skipped_short": scrape_result["pages_skipped_short"],
                            "clusters_scraped": len(scrape_result["clusters_scraped"]),
                            "scrape_output_dir": scrape_output_dir,
                            "metadata_output_dir": metadata_output_dir,
                            "execution_time_seconds": scrape_result["execution_time_seconds"]
                        }
                    }
                )
            else:
                self.publish_log(task_id, "No clusters specified for scraping. Skipping scrape step.", "info")
                task_manager.update_task_status(
                    task_id,
                    progress=60.0,
                    result={
                        "scrape_complete": False,
                        "scrape_skipped": True
                    }
                )
                # Set metadata_output_dir for download phase even if scraping is skipped
                metadata_output_dir = os.path.join(self.metadata_dir, f"{task_id}")

            if year_data:
                task_manager.update_task_status(
                    task_id,
                    status="preparing_download",
                    progress=62.0
                )

                self.publish_log(task_id, "Scraping phase complete. Preparing for download phase...", "info")
                await asyncio.sleep(0.5)

                task_manager.update_task_status(
                    task_id,
                    status="downloading",
                    progress=65.0
                )

                download_output_dir = os.path.join(self.download_dir, f"{task_id}")

                # Log year details
                total_files = 0
                for year, files in year_data.items():
                    file_count = len(files) if files else 0
                    total_files += file_count
                    self.publish_log(task_id, f"Year '{year}': {file_count} files", "info")
                
                self.publish_log(task_id, f"Total files to download: {total_files}", "info")

                downloader = FileDownloader(
                    metadata_dir=metadata_output_dir,
                    max_workers=MAX_DOWNLOAD_WORKERS,
                    timeout=CRAWLER_TIMEOUT
                )

                self.publish_log(task_id, f"Starting direct download for years: {list(year_data.keys())}", "info")
                
                loop = asyncio.get_event_loop()
                download_result = await loop.run_in_executor(
                    None,
                    downloader.download_files_direct,  
                    bot_id,
                    year_data, 
                    download_output_dir,
                    task_id
                )

                self.publish_log(
                    task_id,
                    f"File downloading completed. Successfully downloaded {download_result['successful']} files, failed to download {download_result['failed']} files.",
                    "info"
                )

                task_manager.update_task_status(
                    task_id,
                    progress=90.0,
                    result={
                        **task_manager.get_task_status(task_id)["result"],
                        "download_complete": True,
                        "download_results": {
                            "files_downloaded": download_result["successful"],
                            "files_failed": download_result["failed"],
                            "download_output_dir": download_output_dir,
                            "execution_time_seconds": download_result.get("execution_time_seconds", 0)
                        }
                    }
                )
            else:
                self.publish_log(task_id, "No years specified for downloading. Skipping download step.", "info")
                task_manager.update_task_status(
                    task_id,
                    progress=90.0,
                    result={
                        **task_manager.get_task_status(task_id)["result"],
                        "download_complete": False,
                        "download_skipped": True
                    }
                )

            # CLEANUP PHASE - Add this after download phase completes
            task_manager.update_task_status(
                task_id,
                status="cleaning_up",
                progress=92.0
            )

            self.publish_log(task_id, "Starting cleanup of scraped data and downloaded files...", "info")

            try:
                cleanup_success = await ScrapeDataController.web_scraper_cleanup(task_id=task_id)
                
                if cleanup_success:
                    await CrawlResultController.mark_as_uploaded(task_id=crawl_task_id, upload=True)
                    self.publish_log(task_id, "Successfully cleaned up all files and sent to Momento", "info")
                else:
                    await CrawlResultController.mark_as_uploaded(task_id=crawl_task_id, upload=False)
                    self.publish_log(task_id, "Cleanup completed with some warnings - check logs for details", "warning")
                    
            except Exception as cleanup_error:
                error_msg = f"Cleanup phase failed: {str(cleanup_error)}"
                self.publish_log(task_id, error_msg, "warning")
                self.publish_log(task_id, traceback.format_exc(), "error")

            # FINALIZE
            task_manager.update_task_status(
                task_id,
                status="finalizing",
                progress=96.0
            )
            
            self.publish_log(task_id, "Finalizing simplified workflow and generating summary...", "info")
            await asyncio.sleep(0.5)

            task_manager.update_task_status(
                task_id,
                status="completed",
                progress=100.0
            )
            
            self.publish_log(task_id, "Simplified scrape and download workflow completed successfully", "info")
            
            return task_manager.get_task_status(task_id)
        
        except Exception as e:
            error_msg = f"Error in simplified scrape/download workflow: {str(e)}"
            self.publish_log(task_id, error_msg, "error")
            self.publish_log(task_id, traceback.format_exc(), "error")
            await self._stop_realtime_publishing(task_id)
            
            task_manager.update_task_status(
                task_id,
                status="failed",
                error=error_msg
            )
            return task_manager.get_task_status(task_id)
        
        except Exception as e:
            error_msg = f"Error in scrape/download workflow: {str(e)}"
            self.publish_log(task_id, error_msg, "error")
            self.publish_log(task_id, traceback.format_exc(), "error")
            await self._stop_realtime_publishing(task_id)
            
            task_manager.update_task_status(
                task_id,
                status="failed",
                error=error_msg
            )
            return task_manager.get_task_status(task_id)

    async def get_available_clusters(self, crawl_task_id: str = None) -> List[Dict[str, Any]]:
        try:
            crawl_result = None
            if crawl_task_id:
                crawl_result = await CrawlResultController.get_crawl_result(crawl_task_id)
            else:
                crawl_results = await CrawlResultController.list_crawl_results()
                if crawl_results:
                    crawl_results.sort(key=lambda x: x.created_at, reverse=True)
                    crawl_result = crawl_results[0]
            
            if not crawl_result or not crawl_result.clusters:
                return []
            
            clusters_info = []
            for domain, domain_data in crawl_result.clusters.items():
                clusters_info.append({
                    "id": domain_data.id,  
                    "name": domain,
                    "type": "domain",
                    "url_count": domain_data.count 
                })
                
                for sub_cluster in domain_data.clusters:
                    clusters_info.append({
                        "id": sub_cluster.id,  
                        "name": f"{domain} - {sub_cluster.path}",  
                        "type": "path",
                        "url_count": sub_cluster.url_count  
                    })
            
            return clusters_info
        
        except Exception as e:
            logger.error(f"Error getting available clusters: {str(e)}")
            return []
    
    async def get_available_years(self, crawl_task_id: str = None) -> List[Dict[str, Any]]:
        try:
            crawl_result = None
            
            if crawl_task_id:
                crawl_result = await CrawlResultController.get_crawl_result(crawl_task_id)
            else:
                crawl_results = await CrawlResultController.list_crawl_results()
                if crawl_results:
                    crawl_results.sort(key=lambda x: x.created_at, reverse=True)
                    crawl_result = crawl_results[0]
            
            if not crawl_result or not crawl_result.yearclusters:
                return []

            years_info = []

            for year, files in crawl_result.yearclusters.items():
                years_info.append({
                    "year": year,
                    "files_count": len(files)
                })

            return sorted(
                years_info,
                key=lambda y: (y["year"] == "No Year", y["year"]),
                reverse=True
            )
        
        except Exception as e:
            logger.error(f"Error getting available years: {str(e)}")
            return []
        
    async def get_cluster_by_id(self, cluster_id: str, crawl_task_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        try:
            crawl_result = None
            if crawl_task_id:
                crawl_result = await CrawlResultController.get_crawl_result(crawl_task_id)
            else:
                crawl_results = await CrawlResultController.list_crawl_results()
                if crawl_results:
                    crawl_results.sort(key=lambda x: x.created_at, reverse=True)
                    crawl_result = crawl_results[0]
            
            if not crawl_result or not crawl_result.clusters:
                return None

            for domain, domain_data in crawl_result.clusters.items():
                if domain_data.id == cluster_id:  
                    return {
                        "id": domain_data.id,
                        "name": domain,
                        "type": "domain",
                        "url_count": domain_data.count,
                        "clusters": [
                            {
                                "id": cluster.id,
                                "path": cluster.path,
                                "url_count": cluster.url_count,
                                "urls": cluster.urls
                            }
                            for cluster in domain_data.clusters
                        ]
                    }
                
                for cluster in domain_data.clusters:
                    if cluster.id == cluster_id:  
                        return {
                            "id": cluster.id,
                            "name": f"{domain} - {cluster.path}",
                            "type": "path",
                            "url_count": cluster.url_count,
                            "urls": cluster.urls
                        }
            
            return None
        
        except Exception as e:
            self.logger.error(f"Error getting cluster by ID: {str(e)}")
            return None
        
    async def get_year_by_id(self, year: str, crawl_task_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        try:
            crawl_result = None
            
            if crawl_task_id:
                crawl_result = await CrawlResultController.get_crawl_result(crawl_task_id)
            else:
                crawl_results = await CrawlResultController.list_crawl_results()
                if crawl_results:
                    crawl_results.sort(key=lambda x: x.created_at, reverse=True)
                    crawl_result = crawl_results[0]
            
            if not crawl_result or not crawl_result.yearclusters:
                return None
            
            if year in crawl_result.yearclusters:
                return {
                    "year": year,
                    "files_count": len(crawl_result.yearclusters[year]),
                    "files": crawl_result.yearclusters[year]
                }
            
            return None
        
        except Exception as e:
            self.logger.error(f"Error getting year by ID: {str(e)}")
            return None
        
    # Restaurant Deal Scraping Flow
        
    async def run_deal_scraping(
    self,
    task_id: str,
    cities: List[str] = None
) -> Dict[str, Any]:
        await self._start_realtime_publishing(task_id, interval=2.0)

        task_manager.update_task_status(
            task_id,
            status="initializing",
            progress=0.0
        )

        self.publish_log(task_id, "Starting deal scraping workflow", "info")
        
        try:
            task_manager.update_task_status(
                task_id,
                status="initializing",
                progress=5.0
            )

            timestamp = time.strftime("%Y%m%d-%H%M%S")
            deal_output_dir = os.path.join(self.base_directory, "deals", f"deals_{timestamp}")

            self.publish_log(task_id, "Initializing deal scrapper service", "info")
            deal_scrapper = DealScrapperService(
                country="Pakistan",
                language="en", 
                output_dir=deal_output_dir,
                max_workers=10,
                request_delay=0.5,
                progress_update_interval=5
            )

            setattr(self, f"deal_scrapper_{task_id}", deal_scrapper)

            task_manager.update_task_status(
                task_id,
                status="scraping_deals",
                progress=10.0,
                result={
                    "deal_scrape_initialized": True,
                    "cities_requested": cities or [],
                    "output_directory": deal_output_dir
                }
            )

            self.publish_log(task_id, f"Starting deal scraping for cities: {cities or 'all available'}", "info")

            loop = asyncio.get_event_loop()
            scraping_result = await loop.run_in_executor(
                None,
                deal_scrapper.scrape_deals,
                cities,
                task_id
            )

            if scraping_result["status"] == "failed":
                error_msg = scraping_result.get("error", "Deal scraping failed")
                self.publish_log(task_id, error_msg, "error")

                if hasattr(self, f"deal_scrapper_{task_id}"):
                    delattr(self, f"deal_scrapper_{task_id}")

                await self._stop_realtime_publishing(task_id)
                
                task_manager.update_task_status(
                    task_id,
                    status="failed",
                    error=error_msg
                )
                return task_manager.get_task_status(task_id)

            if scraping_result["status"] == "stopped":
                self.publish_log(task_id, "Deal scraping was stopped by user", "info")

                if hasattr(self, f"deal_scrapper_{task_id}"):
                    delattr(self, f"deal_scrapper_{task_id}")

                task_manager.update_task_status(
                    task_id,
                    status="stopped",
                    progress=95.0,
                    result={
                        **task_manager.get_task_status(task_id)["result"],
                        "deal_scrape_results": scraping_result
                    }
                )
                return task_manager.get_task_status(task_id)

            self.publish_log(
                task_id,
                f"Deal scraping completed. Processed {scraping_result['cities_processed']} cities, "
                f"{scraping_result['restaurants_processed']} restaurants, found {scraping_result['deals_found']} deals.",
                "info"
            )

            task_manager.update_task_status(
                task_id,
                status="saving_to_database",
                progress=95.0,
                result={
                    **task_manager.get_task_status(task_id)["result"],
                    "deal_scrape_results": scraping_result
                }
            )

            self.publish_log(task_id, "Saving deal scraping results to database...", "info")
            
            try:
                cities_requested = cities or []
                cities_processed = scraping_result["cities_processed"]
                restaurants_processed = scraping_result["restaurants_processed"] 
                deals_processed = scraping_result["deals_found"]
                execution_time_seconds = scraping_result["execution_time_seconds"]
                restaurants_data = []
                summary_by_city = {}
                cities_data = scraping_result.get("cities_data", {})
                for city, restaurants in cities_data.items():
                    city_restaurants = 0
                    city_deals = 0
                    
                    for restaurant_name, deals in restaurants.items():
                        if deals: 
                            restaurant_data = {
                                "name": restaurant_name,
                                "location": city,
                                "city": city,
                                "cuisine_type": None,  
                                "rating": None,        
                                "deals": deals,
                                "contact_info": None,  
                                "scraped_at": datetime.now()
                            }
                            restaurants_data.append(restaurant_data)
                            city_restaurants += 1
                            city_deals += len(deals)
                    
                    if city_restaurants > 0:
                        summary_by_city[city] = {
                            "restaurants": city_restaurants,
                            "deals": city_deals
                        }

                await DealScrapeController.save_deal_result(
                    task_id=task_id,
                    cities_requested=cities_requested,
                    cities_processed=cities_processed,
                    restaurants_processed=restaurants_processed,
                    deals_processed=deals_processed,
                    execution_time_seconds=execution_time_seconds,
                    restaurants_data=restaurants_data,
                    summary_by_city=summary_by_city
                )
                
                self.publish_log(task_id, "Deal scraping results saved to database successfully", "info")
                
            except Exception as db_error:
                error_msg = f"Failed to save deal scraping results to database: {str(db_error)}"
                self.publish_log(task_id, error_msg, "error")

                if hasattr(self, f"deal_scrapper_{task_id}"):
                    delattr(self, f"deal_scrapper_{task_id}")

                await self._stop_realtime_publishing(task_id)
                
                task_manager.update_task_status(
                    task_id,
                    status="failed",
                    error=error_msg
                )
                return task_manager.get_task_status(task_id)
            
            task_manager.update_task_status(
                task_id,
                status="completed",
                progress=100.0,
                result={
                    **task_manager.get_task_status(task_id)["result"],
                    "database_save_complete": True,
                    "deal_scraping_complete": True
                }
            )
            
            self.publish_log(task_id, "Deal scraping workflow completed successfully. Results saved to database.", "info")

            if hasattr(self, f"deal_scrapper_{task_id}"):
                delattr(self, f"deal_scrapper_{task_id}")

            return task_manager.get_task_status(task_id)
            
        except Exception as e:
            error_msg = f"Error in deal scraping workflow: {str(e)}"
            self.publish_log(task_id, error_msg, "error")
            self.publish_log(task_id, traceback.format_exc(), "error")

            if hasattr(self, f"deal_scrapper_{task_id}"):
                delattr(self, f"deal_scrapper_{task_id}")

            await self._stop_realtime_publishing(task_id)
            
            task_manager.update_task_status(
                task_id,
                status="failed",
                error=error_msg
            )
            return task_manager.get_task_status(task_id)

    def stop_deal_scraping(self, task_id: str) -> Dict[str, Any]:
        self.publish_log(task_id, f"Attempting to stop deal scraping task {task_id} gracefully...", "info")

        task_status = task_manager.get_task_status(task_id)
        
        if not task_status:
            error_msg = f"Task {task_id} not found"
            self.publish_log(task_id, error_msg, "error")
            return {"success": False, "message": error_msg}

        if task_status.get("type") != "deal_scraping":
            error_msg = f"Task {task_id} is not a deal scraping task"
            self.publish_log(task_id, error_msg, "error")
            return {"success": False, "message": error_msg}

        current_status = task_status.get("status")
        if current_status in ["completed", "failed", "stopped"]:
            msg = f"Task {task_id} is already in '{current_status}' state"
            self.publish_log(task_id, msg, "info")
            return {"success": True, "message": msg}

        try:
            try:
                import asyncio
                from app.utils.realtime_publisher import realtime_publisher

                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        asyncio.create_task(realtime_publisher.stop_publishing(task_id))
                    else:
                        loop.run_until_complete(realtime_publisher.stop_publishing(task_id))
                except RuntimeError:
                    asyncio.run(realtime_publisher.stop_publishing(task_id))
                    
                self.publish_log(task_id, "Stopped real-time publishing for task", "info")
            except Exception as e:
                self.publish_log(task_id, f"Warning: Could not stop real-time publishing: {str(e)}", "warning")

            deal_scrapper_instance = getattr(self, f"deal_scrapper_{task_id}", None)
            
            if deal_scrapper_instance:
                self.publish_log(task_id, "Stopping deal scrapper...", "info")
                stop_result = deal_scrapper_instance.stop()
                
                if stop_result:
                    delattr(self, f"deal_scrapper_{task_id}")

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
                    
                    self.publish_log(task_id, "Deal scrapper stopped gracefully", "info")
                    return {
                        "success": True, 
                        "message": "Deal scrapper stopped gracefully",
                        "cleanup_completed": True
                    }
                else:
                    self.publish_log(task_id, "Failed to stop deal scrapper", "error")
                    return {"success": False, "message": "Failed to stop deal scrapper"}
            else:
                self.publish_log(task_id, "No active deal scrapper instance found, updating task status to stopped", "info")

                task_manager.update_task_status(
                    task_id,
                    status="stopped",
                    progress=95.0,
                    result={
                        **task_status.get("result", {}),
                        "stopped_at": datetime.now().isoformat(),
                        "stopped_gracefully": False
                    }
                )
                
                return {
                    "success": True, 
                    "message": "Task marked as stopped but no active deal scrapper found",
                    "cleanup_completed": False
                }
        
        except Exception as e:
            error_msg = f"Error stopping deal scrapper: {str(e)}"
            self.publish_log(task_id, error_msg, "error")

            task_manager.update_task_status(
                task_id,
                status="failed",
                error=error_msg
            )
            
            return {"success": False, "message": error_msg}
        
    # FB Scraping Flow

    async def run_facebook_scraping(
        self,
        bot_id: str,
        task_id: str,
        keywords: List[str],
        days: int,
        # access_token: str,
        # page_id: str
    ) -> Dict[str, Any]:

        await self._start_realtime_publishing(task_id, interval=2.0)

        task_manager.update_task_status(
            task_id,
            status="initializing",
            progress=0.0
        )

        self.publish_log(task_id, "Starting Facebook scraping workflow", "info")
        
        try:
            task_manager.update_task_status(
                task_id,
                status="initializing",
                progress=5.0
            )

            facebook_output_dir = os.path.join(self.base_directory, "facebook", f"{task_id}")

            self.publish_log(task_id, f"Initializing Facebook scrapping service {ACCESS_TOKEN} and {PAGE_ID}", "info")
            facebook_scrapper = FacebookScrapingService(
                access_token=ACCESS_TOKEN,
                page_id=PAGE_ID,
                output_dir=facebook_output_dir,
                progress_update_interval=5
            )

            setattr(self, f"facebook_scrapper_{task_id}", facebook_scrapper)

            task_manager.update_task_status(
                task_id,
                status="scraping_posts",
                progress=10.0,
                result={
                    "facebook_scrape_initialized": True,
                    "keywords_requested": keywords,
                    "days_requested": days,
                    "output_directory": facebook_output_dir
                }
            )

            self.publish_log(task_id, f"Starting Facebook scraping for keywords: {keywords} (last {days} days)", "info")

            loop = asyncio.get_event_loop()
            scraping_result = await loop.run_in_executor(
                None,
                facebook_scrapper.scrape_facebook_posts,
                bot_id,
                keywords,
                days,
                task_id
            )

            if scraping_result["status"] == "failed":
                error_msg = scraping_result.get("error", "Facebook scraping failed")
                self.publish_log(task_id, error_msg, "error")

                if hasattr(self, f"facebook_scrapper_{task_id}"):
                    delattr(self, f"facebook_scrapper_{task_id}")

                await self._stop_realtime_publishing(task_id)
                
                task_manager.update_task_status(
                    task_id,
                    status="failed",
                    error=error_msg
                )
                return task_manager.get_task_status(task_id)

            if scraping_result["status"] == "stopped":
                self.publish_log(task_id, "Facebook scraping was stopped by user", "info")

                if hasattr(self, f"facebook_scrapper_{task_id}"):
                    delattr(self, f"facebook_scrapper_{task_id}")

                task_manager.update_task_status(
                    task_id,
                    status="stopped",
                    progress=95.0,
                    result={
                        **task_manager.get_task_status(task_id)["result"],
                        "facebook_scrape_results": scraping_result,
                        "stopped_at": datetime.now().isoformat(),
                        "stopped_gracefully": True
                    }
                )

                self.publish_log(task_id, "Facebook scraping stopped. Results NOT saved to database.", "info")
                return task_manager.get_task_status(task_id)

            if scraping_result["status"] != "completed":
                error_msg = f"Unexpected scraping status: {scraping_result['status']}"
                self.publish_log(task_id, error_msg, "error")

                if hasattr(self, f"facebook_scrapper_{task_id}"):
                    delattr(self, f"facebook_scrapper_{task_id}")

                await self._stop_realtime_publishing(task_id)
                
                task_manager.update_task_status(
                    task_id,
                    status="failed",
                    error=error_msg
                )
                return task_manager.get_task_status(task_id)

            self.publish_log(
                task_id,
                f"Facebook scraping completed successfully. Processed {scraping_result['posts_processed']} posts, "
                f"found {scraping_result['posts_found']} matching posts.",
                "info"
            )

            is_uploaded = False
            try:
                cleanup_success = await ScrapeDataController.fb_scraper_cleanup(task_id=task_id)
                
                if cleanup_success:
                    is_uploaded = True
                    self.publish_log(task_id, "Successfully cleaned up all files and sent to Momento", "info")
                else:
                    self.publish_log(task_id, "Cleanup completed with some warnings - check logs for details", "warning")
                    
            except Exception as cleanup_error:
                error_msg = f"Cleanup phase failed: {str(cleanup_error)}"
                self.publish_log(task_id, error_msg, "warning")
                self.publish_log(task_id, traceback.format_exc(), "error")

            task_manager.update_task_status(
                task_id,
                status="saving_to_database",
                progress=95.0,
                result={
                    **task_manager.get_task_status(task_id)["result"],
                    "facebook_scrape_results": scraping_result
                }
            )

            self.publish_log(task_id, "Saving Facebook scraping results to database...", "info")
            
            try:
                keywords_requested = keywords
                days_requested = days
                posts_processed = scraping_result["posts_processed"]
                categories_found = scraping_result.get("categories_found", {})
                keyword_matches = scraping_result.get("keyword_matches", {})
                execution_time_seconds = scraping_result["execution_time_seconds"]
                output_directory = scraping_result["output_directory"]
                date_range = scraping_result["date_range"]
                posts_data = []

                for post in scraping_result.get("posts_data", []):
                    post_data = {
                        "post_id": post.get("id", ""),
                        "message": post.get("message", ""),
                        "created_time": post.get("created_time", ""),
                        "category": post.get("category", "other"),
                        "attachments": post.get("attachments", []),
                        "scraped_at": datetime.now()
                    }
                    posts_data.append(post_data)

                await self.save_facebook_result(
                    task_id=task_id,
                    is_uploaded=is_uploaded,
                    keywords_requested=keywords_requested,
                    days_requested=days_requested,
                    posts_processed=posts_processed,
                    categories_found=categories_found,
                    keyword_matches=keyword_matches,
                    execution_time_seconds=execution_time_seconds,
                    output_directory=output_directory,
                    date_range=date_range,
                    posts_data=posts_data
                )
                
                self.publish_log(task_id, "Facebook scraping results saved to database successfully", "info")
                
            except Exception as db_error:
                error_msg = f"Failed to save Facebook scraping results to database: {str(db_error)}"
                self.publish_log(task_id, error_msg, "error")

                if hasattr(self, f"facebook_scrapper_{task_id}"):
                    delattr(self, f"facebook_scrapper_{task_id}")

                await self._stop_realtime_publishing(task_id)
                
                task_manager.update_task_status(
                    task_id,
                    status="failed",
                    error=error_msg
                )
                return task_manager.get_task_status(task_id)
            
            task_manager.update_task_status(
                task_id,
                status="completed",
                progress=100.0,
                result={
                    **task_manager.get_task_status(task_id)["result"],
                    "database_save_complete": True,
                    "facebook_scraping_complete": True
                }
            )
            
            self.publish_log(task_id, "Facebook scraping workflow completed successfully. Results saved to database.", "info")

            if hasattr(self, f"facebook_scrapper_{task_id}"):
                delattr(self, f"facebook_scrapper_{task_id}")

            return task_manager.get_task_status(task_id)
            
        except Exception as e:
            error_msg = f"Error in Facebook scraping workflow: {str(e)}"
            self.publish_log(task_id, error_msg, "error")
            self.publish_log(task_id, traceback.format_exc(), "error")

            if hasattr(self, f"facebook_scrapper_{task_id}"):
                delattr(self, f"facebook_scrapper_{task_id}")

            await self._stop_realtime_publishing(task_id)
            
            task_manager.update_task_status(
                task_id,
                status="failed",
                error=error_msg
            )
            return task_manager.get_task_status(task_id)

    def stop_facebook_scraping(self, task_id: str) -> Dict[str, Any]:
        self.publish_log(task_id, f"Attempting to stop Facebook scraping task {task_id} gracefully...", "info")

        task_status = task_manager.get_task_status(task_id)
        
        if not task_status:
            error_msg = f"Task {task_id} not found"
            self.publish_log(task_id, error_msg, "error")
            return {"success": False, "message": error_msg}

        if task_status.get("type") != "facebook_scraping":
            error_msg = f"Task {task_id} is not a Facebook scraping task"
            self.publish_log(task_id, error_msg, "error")
            return {"success": False, "message": error_msg}

        current_status = task_status.get("status")
        if current_status in ["completed", "failed", "stopped"]:
            msg = f"Task {task_id} is already in '{current_status}' state"
            self.publish_log(task_id, msg, "info")
            return {"success": True, "message": msg}

        try:
            try:
                import asyncio
                from app.utils.realtime_publisher import realtime_publisher

                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        asyncio.create_task(realtime_publisher.stop_publishing(task_id))
                    else:
                        loop.run_until_complete(realtime_publisher.stop_publishing(task_id))
                except RuntimeError:
                    asyncio.run(realtime_publisher.stop_publishing(task_id))
                    
                self.publish_log(task_id, "Stopped real-time publishing for task", "info")
            except Exception as e:
                self.publish_log(task_id, f"Warning: Could not stop real-time publishing: {str(e)}", "warning")

            facebook_scrapper_instance = getattr(self, f"facebook_scrapper_{task_id}", None)
            
            if facebook_scrapper_instance:
                self.publish_log(task_id, "Stopping Facebook scrapper...", "info")
                stop_result = facebook_scrapper_instance.stop()
                
                if stop_result:
                    delattr(self, f"facebook_scrapper_{task_id}")
                    
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
                    
                    self.publish_log(task_id, "Facebook scrapper stopped gracefully", "info")
                    return {
                        "success": True, 
                        "message": "Facebook scrapper stopped gracefully",
                        "cleanup_completed": True
                    }
                else:
                    self.publish_log(task_id, "Failed to stop Facebook scrapper", "error")
                    return {"success": False, "message": "Failed to stop Facebook scrapper"}
            else:
                self.publish_log(task_id, "No active Facebook scrapper instance found, updating task status to stopped", "info")

                task_manager.update_task_status(
                    task_id,
                    status="stopped",
                    progress=95.0,
                    result={
                        **task_status.get("result", {}),
                        "stopped_at": datetime.now().isoformat(),
                        "stopped_gracefully": False
                    }
                )
                
                return {
                    "success": True, 
                    "message": "Task marked as stopped but no active Facebook scrapper found",
                    "cleanup_completed": False
                }
        
        except Exception as e:
            error_msg = f"Error stopping Facebook scrapper: {str(e)}"
            self.publish_log(task_id, error_msg, "error")

            task_manager.update_task_status(
                task_id,
                status="failed",
                error=error_msg
            )
            
            return {"success": False, "message": error_msg}

    async def save_facebook_result(
        self,
        task_id: str,
        is_uploaded: bool,
        keywords_requested: List[str],
        days_requested: int,
        posts_processed: int,
        categories_found: Dict[str, int],
        keyword_matches: Dict[str, Dict[str, int]],
        execution_time_seconds: float,
        output_directory: str,
        date_range: Dict[str, str],
        posts_data: List[Dict[str, Any]]
    ) -> None:
        try:
            from app.models.database.fb_scrape.fb_result_model import FacebookResult, FacebookPostData
            
            self.logger.info(f"Attempting to save Facebook result for task {task_id}")
            self.logger.info(f"Posts data count: {len(posts_data)}")

            existing_result = await FacebookResult.find_one({"task_id": task_id})
            if existing_result:
                self.logger.warning(f"Facebook result for task {task_id} already exists, skipping save")
                return

            task_status = task_manager.get_task_status(task_id)
            if task_status and task_status.get('created_at'):
                try:
                    original_created_at = datetime.fromisoformat(task_status['created_at'].replace('Z', '+00:00'))
                    original_created_at = original_created_at.replace(tzinfo=None)
                    self.logger.info(f"Using original creation time: {original_created_at}")
                except Exception as dt_error:
                    self.logger.warning(f"Error parsing creation time: {dt_error}, using current time")
                    original_created_at = datetime.utcnow()
            else:
                self.logger.warning(f"Could not get original creation time for task {task_id}, using current time")
                original_created_at = datetime.utcnow()
            
            karachi_tz = pytz.timezone('Asia/Karachi')
            completed_at = datetime.now(karachi_tz).replace(tzinfo=None)
            
            processed_posts_data = []
            for post in posts_data:
                try:
                    post_obj = FacebookPostData(
                        post_id=post.get("post_id", post.get("id", "unknown")),
                        message=post.get("message", ""),
                        created_time=post.get("created_time", ""),
                        category=post.get("category", "other"),
                        attachments=post.get("attachments", []),
                        scraped_at=datetime.utcnow()
                    )
                    processed_posts_data.append(post_obj)
                except Exception as post_error:
                    self.logger.warning(f"Error processing post data: {post_error}, skipping post")
                    continue
            
            self.logger.info(f"Successfully processed {len(processed_posts_data)} posts for database")

            if not task_id or not isinstance(task_id, str):
                raise ValueError("task_id must be a non-empty string")
            if not isinstance(keywords_requested, list):
                raise ValueError("keywords_requested must be a list")
            if not isinstance(days_requested, int) or days_requested <= 0:
                raise ValueError("days_requested must be a positive integer")
            if not isinstance(posts_processed, int) or posts_processed < 0:
                raise ValueError("posts_processed must be a non-negative integer")

            facebook_result = FacebookResult(
                task_id=task_id,
                is_uploaded=UploadStatus.COMPLETED if is_uploaded else UploadStatus.FAILED,
                keywords_requested=keywords_requested,
                days_requested=days_requested,
                posts_processed=posts_processed,
                categories_found=categories_found or {},
                keyword_matches=keyword_matches or {},
                execution_time_seconds=execution_time_seconds,
                output_directory=output_directory,
                date_range=date_range or {},
                posts_data=processed_posts_data,  
                created_at=original_created_at,
                completed_at=completed_at
            )
            
            self.logger.info(f"Created FacebookResult object, attempting to save to database...")

            max_retries = 3
            for attempt in range(max_retries):
                try:
                    await facebook_result.insert()
                    self.logger.info(f"Successfully saved Facebook result for task {task_id} to database")
                    self.logger.info(f"Task created at: {original_created_at}, completed at: {facebook_result.completed_at}")
                    return
                except Exception as insert_error:
                    self.logger.error(f"Attempt {attempt + 1} failed to insert Facebook result: {str(insert_error)}")
                    if attempt == max_retries - 1:
                        raise insert_error
                    await asyncio.sleep(1)  
            
        except Exception as e:
            self.logger.error(f"Error saving Facebook result for task {task_id}: {str(e)}")
            self.logger.error(f"Error type: {type(e).__name__}")
            self.logger.error(f"Error details: {repr(e)}")
            import traceback
            self.logger.error(f"Full traceback: {traceback.format_exc()}")
            raise Exception(f"Failed to save Facebook result: {str(e)}")

    # Clean Up Methods
    async def run_facebook_cleanup(
        self,
        original_task_id: str,
        cleanup_task_id: str
    ) -> Dict[str, Any]:        
        await self._start_realtime_publishing(cleanup_task_id, interval=2.0)

        task_manager.update_task_status(
            cleanup_task_id,
            status="initializing",
            progress=0.0
        )

        self.publish_log(cleanup_task_id, f"Starting Facebook cleanup for original task {original_task_id}", "info")
        
        try:
            task_manager.update_task_status(
                cleanup_task_id,
                status="validating",
                progress=5.0
            )

            self.publish_log(cleanup_task_id, f"Validating original task {original_task_id}", "info")
            
            base_path = Path("apollo_data")
            facebook_task_path = base_path / "facebook" / original_task_id
            
            if not facebook_task_path.exists():
                error_msg = f"No data found for Facebook task {original_task_id}"
                self.publish_log(cleanup_task_id, error_msg, "error")
                
                await self._stop_realtime_publishing(cleanup_task_id)
                task_manager.update_task_status(
                    cleanup_task_id,
                    status="failed",
                    error=error_msg
                )
                return task_manager.get_task_status(cleanup_task_id)

            task_manager.update_task_status(
                cleanup_task_id,
                status="collecting",
                progress=15.0
            )

            self.publish_log(cleanup_task_id, "Collecting Facebook scraped files for processing...", "info")
            
            try:
                files_data = await self._collect_fb_files_with_progress(original_task_id, cleanup_task_id)
                
                files_count = len(files_data["files"])
                self.publish_log(cleanup_task_id, f"Collected {files_count} files for processing", "info")
                
                task_manager.update_task_status(
                    cleanup_task_id,
                    progress=35.0,
                    result={
                        "files_collected": files_count,
                        "collection_completed": True
                    }
                )

                if files_count == 0:
                    self.publish_log(cleanup_task_id, "No files found to process, performing directory cleanup only", "info")
                    
                    task_manager.update_task_status(
                        cleanup_task_id,
                        status="cleaning_directories",
                        progress=80.0
                    )

                    await ScrapeDataController._cleanup_empty_fb_directories(original_task_id, base_path)
                    
                    task_manager.update_task_status(
                        cleanup_task_id,
                        status="completed",
                        progress=100.0,
                        result={
                            **task_manager.get_task_status(cleanup_task_id)["result"],
                            "files_processed": 0,
                            "cleanup_completed": True,
                            "directory_cleanup_only": True
                        }
                    )
                    
                    self.publish_log(cleanup_task_id, "Facebook cleanup completed (directory cleanup only)", "info")
                    await self._stop_realtime_publishing(cleanup_task_id)
                    return task_manager.get_task_status(cleanup_task_id)

                task_manager.update_task_status(
                    cleanup_task_id,
                    status="processing",
                    progress=45.0
                )

                self.publish_log(cleanup_task_id, "Creating batch for document processing...", "info")
                
                batch_id = await self._create_batch_with_progress(
                    cleanup_task_id, 
                    original_task_id, 
                    files_data["files"], 
                    files_data["metadata_list"], 
                    "facebook"
                )

                if not batch_id:
                    error_msg = "Failed to create and submit batch for processing"
                    self.publish_log(cleanup_task_id, error_msg, "error")
                    
                    await self._stop_realtime_publishing(cleanup_task_id)
                    task_manager.update_task_status(
                        cleanup_task_id,
                        status="failed",
                        error=error_msg
                    )
                    return task_manager.get_task_status(cleanup_task_id)

                self.publish_log(cleanup_task_id, f"Batch {batch_id} submitted successfully", "info")
                
                task_manager.update_task_status(
                    cleanup_task_id,
                    progress=65.0,
                    result={
                        **task_manager.get_task_status(cleanup_task_id)["result"],
                        "batch_id": batch_id,
                        "batch_submitted": True
                    }
                )

                task_manager.update_task_status(
                    cleanup_task_id,
                    status="uploading",
                    progress=70.0
                )

                self.publish_log(cleanup_task_id, f"Waiting for batch {batch_id} to complete processing...", "info")
                
                success = await self._wait_for_batch_with_progress(cleanup_task_id, batch_id)
                
                if not success:
                    error_msg = f"Batch {batch_id} processing failed"
                    self.publish_log(cleanup_task_id, error_msg, "error")
                    
                    await self._stop_realtime_publishing(cleanup_task_id)
                    task_manager.update_task_status(
                        cleanup_task_id,
                        status="failed",
                        error=error_msg
                    )
                    return task_manager.get_task_status(cleanup_task_id)

                self.publish_log(cleanup_task_id, f"Batch {batch_id} processed successfully", "info")

                task_manager.update_task_status(
                    cleanup_task_id,
                    status="cleaning_files",
                    progress=85.0
                )

                self.publish_log(cleanup_task_id, "Cleaning up processed files...", "info")

                await FacebookScrapeController.mark_as_uploaded(task_id=original_task_id, upload=True)
                
                cleanup_result = await ScrapeDataController._cleanup_processed_fb_files(
                    original_task_id, base_path, files_data["files"]
                )
                
                await ScrapeDataController._cleanup_empty_fb_directories(original_task_id, base_path)
                
                files_processed = cleanup_result.get("deleted_count", 0)
                self.publish_log(cleanup_task_id, f"Successfully cleaned up {files_processed} files", "info")

                task_manager.update_task_status(
                    cleanup_task_id,
                    status="completed",
                    progress=100.0,
                    result={
                        **task_manager.get_task_status(cleanup_task_id)["result"],
                        "files_processed": files_processed,
                        "cleanup_completed": True,
                        "batch_processing_successful": True
                    }
                )
                
                self.publish_log(cleanup_task_id, f"Facebook cleanup completed successfully for task {original_task_id}", "info")
                
                await self._stop_realtime_publishing(cleanup_task_id)
                return task_manager.get_task_status(cleanup_task_id)
                
            except Exception as cleanup_error:
                error_msg = f"Cleanup operation failed: {str(cleanup_error)}"
                self.publish_log(cleanup_task_id, error_msg, "error")
                self.publish_log(cleanup_task_id, traceback.format_exc(), "error")
                
                await self._stop_realtime_publishing(cleanup_task_id)
                task_manager.update_task_status(
                    cleanup_task_id,
                    status="failed",
                    error=error_msg
                )
                return task_manager.get_task_status(cleanup_task_id)
                
        except Exception as e:
            error_msg = f"Error in Facebook cleanup workflow: {str(e)}"
            self.publish_log(cleanup_task_id, error_msg, "error")
            self.publish_log(cleanup_task_id, traceback.format_exc(), "error")
            
            await self._stop_realtime_publishing(cleanup_task_id)
            task_manager.update_task_status(
                cleanup_task_id,
                status="failed",
                error=error_msg
            )
            return task_manager.get_task_status(cleanup_task_id)

    async def run_web_cleanup(
        self,
        original_task_id: str,
        cleanup_task_id: str,
    ) -> Dict[str, Any]:        
        await self._start_realtime_publishing(cleanup_task_id, interval=2.0)

        task_manager.update_task_status(
            cleanup_task_id,
            status="initializing", 
            progress=0.0
        )

        self.publish_log(cleanup_task_id, f"Starting web cleanup for original task {original_task_id}", "info")
        
        try:
            task_manager.update_task_status(
                cleanup_task_id,
                status="validating",
                progress=5.0
            )

            self.publish_log(cleanup_task_id, f"Validating original task {original_task_id}", "info")
            
            base_path = Path("apollo_data")
            scraped_path = base_path / "scraped" / original_task_id
            downloads_path = base_path / "downloads" / original_task_id
            
            if not scraped_path.exists() and not downloads_path.exists():
                error_msg = f"No data found for web scraping task {original_task_id}"
                self.publish_log(cleanup_task_id, error_msg, "error")
                
                await self._stop_realtime_publishing(cleanup_task_id)
                task_manager.update_task_status(
                    cleanup_task_id,
                    status="failed",
                    error=error_msg
                )
                return task_manager.get_task_status(cleanup_task_id)

            task_manager.update_task_status(
                cleanup_task_id,
                status="collecting",
                progress=15.0
            )

            self.publish_log(cleanup_task_id, "Collecting web scraped files for processing...", "info")
            
            try:                
                files_data = await self._collect_web_files_with_progress(original_task_id, cleanup_task_id)
                
                files_count = len(files_data["files"])
                self.publish_log(cleanup_task_id, f"Collected {files_count} files for processing", "info")
                
                task_manager.update_task_status(
                    cleanup_task_id,
                    progress=35.0,
                    result={
                        "files_collected": files_count,
                        "collection_completed": True
                    }
                )

                if files_count == 0:
                    self.publish_log(cleanup_task_id, "No files found to process, performing directory cleanup only", "info")
                    
                    task_manager.update_task_status(
                        cleanup_task_id,
                        status="cleaning_directories",
                        progress=80.0
                    )

                    await ScrapeDataController._cleanup_empty_directories(original_task_id, base_path)
                    
                    task_manager.update_task_status(
                        cleanup_task_id,
                        status="completed",
                        progress=100.0,
                        result={
                            **task_manager.get_task_status(cleanup_task_id)["result"],
                            "files_processed": 0,
                            "cleanup_completed": True,
                            "directory_cleanup_only": True
                        }
                    )
                    
                    self.publish_log(cleanup_task_id, "Web cleanup completed (directory cleanup only)", "info")
                    await self._stop_realtime_publishing(cleanup_task_id)
                    return task_manager.get_task_status(cleanup_task_id)

                task_manager.update_task_status(
                    cleanup_task_id,
                    status="processing",
                    progress=45.0
                )

                self.publish_log(cleanup_task_id, "Creating batch for document processing...", "info")
                
                batch_id = await self._create_batch_with_progress(
                    cleanup_task_id,
                    original_task_id, 
                    files_data["files"], 
                    files_data["metadata_list"],
                    "website"
                )

                if not batch_id:
                    error_msg = "Failed to create and submit batch for processing"
                    self.publish_log(cleanup_task_id, error_msg, "error")
                    
                    await self._stop_realtime_publishing(cleanup_task_id)
                    task_manager.update_task_status(
                        cleanup_task_id,
                        status="failed",
                        error=error_msg
                    )
                    return task_manager.get_task_status(cleanup_task_id)

                self.publish_log(cleanup_task_id, f"Batch {batch_id} submitted successfully", "info")
                
                task_manager.update_task_status(
                    cleanup_task_id,
                    progress=65.0,
                    result={
                        **task_manager.get_task_status(cleanup_task_id)["result"],
                        "batch_id": batch_id,
                        "batch_submitted": True
                    }
                )

                task_manager.update_task_status(
                    cleanup_task_id,
                    status="uploading",
                    progress=70.0
                )

                self.publish_log(cleanup_task_id, f"Waiting for batch {batch_id} to complete processing...", "info")
                
                success = await self._wait_for_batch_with_progress(cleanup_task_id, batch_id)
                
                if not success:
                    error_msg = f"Batch {batch_id} processing failed"
                    self.publish_log(cleanup_task_id, error_msg, "error")
                    
                    await self._stop_realtime_publishing(cleanup_task_id)
                    task_manager.update_task_status(
                        cleanup_task_id,
                        status="failed",
                        error=error_msg
                    )
                    return task_manager.get_task_status(cleanup_task_id)

                self.publish_log(cleanup_task_id, f"Batch {batch_id} processed successfully", "info")

                task_manager.update_task_status(
                    cleanup_task_id,
                    status="cleaning_files",
                    progress=85.0
                )

                self.publish_log(cleanup_task_id, "Cleaning up processed files...", "info")

                await CrawlResultController.mark_as_uploaded(task_id=original_task_id, upload=True)
                
                cleanup_result = await ScrapeDataController._cleanup_processed_files(
                    original_task_id, base_path, files_data["files"]
                )
                
                await ScrapeDataController._cleanup_empty_directories(original_task_id, base_path)
                
                files_processed = cleanup_result.get("deleted_count", 0)
                self.publish_log(cleanup_task_id, f"Successfully cleaned up {files_processed} files", "info")

                task_manager.update_task_status(
                    cleanup_task_id,
                    status="completed",
                    progress=100.0,
                    result={
                        **task_manager.get_task_status(cleanup_task_id)["result"],
                        "files_processed": files_processed,
                        "cleanup_completed": True,
                        "batch_processing_successful": True
                    }
                )
                
                self.publish_log(cleanup_task_id, f"Web cleanup completed successfully for task {original_task_id}", "info")
                
                await self._stop_realtime_publishing(cleanup_task_id)
                return task_manager.get_task_status(cleanup_task_id)
                
            except Exception as cleanup_error:
                error_msg = f"Cleanup operation failed: {str(cleanup_error)}"
                self.publish_log(cleanup_task_id, error_msg, "error")
                self.publish_log(cleanup_task_id, traceback.format_exc(), "error")
                
                await self._stop_realtime_publishing(cleanup_task_id)
                task_manager.update_task_status(
                    cleanup_task_id,
                    status="failed", 
                    error=error_msg
                )
                return task_manager.get_task_status(cleanup_task_id)
                
        except Exception as e:
            error_msg = f"Error in web cleanup workflow: {str(e)}"
            self.publish_log(cleanup_task_id, error_msg, "error")
            self.publish_log(cleanup_task_id, traceback.format_exc(), "error")
            
            await self._stop_realtime_publishing(cleanup_task_id)
            task_manager.update_task_status(
                cleanup_task_id,
                status="failed",
                error=error_msg
            )
            return task_manager.get_task_status(cleanup_task_id)

    async def _collect_fb_files_with_progress(
        self,
        task_id: str,
        cleanup_task_id: str,
    ) -> Dict[str, Any]:
        
        self.publish_log(cleanup_task_id, "Scanning Facebook data directories...", "info")
        
        task_manager.update_task_status(
            cleanup_task_id,
            progress=20.0
        )
        
        base_path = Path("apollo_data")
        files_data = await ScrapeDataController._collect_fb_scraped_data(task_id, base_path)
        
        self.publish_log(cleanup_task_id, f"Found {len(files_data['files'])} files to process", "info")
        
        task_manager.update_task_status(
            cleanup_task_id,
            progress=30.0
        )
        
        return files_data

    async def _collect_web_files_with_progress(
        self,
        task_id: str,
        cleanup_task_id: str,
    ) -> Dict[str, Any]:        
        self.publish_log(cleanup_task_id, "Scanning web scraping data directories...", "info")
        
        task_manager.update_task_status(
            cleanup_task_id,
            progress=20.0
        )
        
        base_path = Path("apollo_data")
        files_data = await ScrapeDataController._collect_web_scraped_data(task_id, base_path)
        
        self.publish_log(cleanup_task_id, f"Found {len(files_data['files'])} files to process", "info")
        
        task_manager.update_task_status(
            cleanup_task_id,
            progress=30.0
        )
        
        return files_data

    async def _create_batch_with_progress(
        self,
        cleanup_task_id: str,
        task_id: str,
        files: List[Dict[str, Any]],
        metadata_list: List[Dict[str, Any]],
        source: str
    ) -> Optional[str]:        
        self.publish_log(cleanup_task_id, "Preparing batch package...", "info")
        
        task_manager.update_task_status(
            cleanup_task_id,
            progress=50.0
        )
        
        batch_id = await ScrapeDataController._create_and_send_batch(
            task_id, files, metadata_list, source
        )
        
        if batch_id:
            self.publish_log(cleanup_task_id, f"Batch {batch_id} created and submitted", "info")
        else:
            self.publish_log(cleanup_task_id, "Failed to create batch", "error")
        
        task_manager.update_task_status(
            cleanup_task_id,
            progress=60.0
        )
        
        return batch_id

    async def _wait_for_batch_with_progress(
        self,
        cleanup_task_id: str,
        batch_id: str
    ) -> bool:        
        progress_start = 70.0
        progress_end = 80.0
        
        original_max_checks = ScrapeDataController.MAX_STATUS_CHECKS
        
        for check_count in range(original_max_checks):
            try:
                check_progress = (check_count / original_max_checks) * (progress_end - progress_start)
                current_progress = progress_start + check_progress
                
                task_manager.update_task_status(
                    cleanup_task_id,
                    progress=min(current_progress, progress_end)
                )
                
                url = f"{DOCUMENT_BULK_URL}/xiva/faq/api/v1/documents/bulk-ingest/{batch_id}"
                
                async with httpx.AsyncClient(timeout=30.0) as client:
                    response = await client.get(url)
                    
                    if response.status_code == 200:
                        result = response.json()
                        
                        status = result.get("status")
                        processed_docs = result.get("processed_documents", 0)
                        total_docs = result.get("total_documents", 0)
                        failed_docs = result.get("failed_documents", 0)
                        
                        if total_docs > 0:
                            doc_progress = (processed_docs / total_docs) * (progress_end - progress_start)
                            current_progress = progress_start + doc_progress
                            
                            task_manager.update_task_status(
                                cleanup_task_id,
                                progress=min(current_progress, progress_end)
                            )
                        
                        if check_count % 5 == 0: 
                            self.publish_log(
                                cleanup_task_id,
                                f"Batch processing status: {status} - {processed_docs}/{total_docs} documents processed",
                                "info"
                            )
                        
                        if status == "completed":
                            if failed_docs == 0:
                                self.publish_log(cleanup_task_id, f"Batch processing completed successfully", "info")
                                return True
                            else:
                                self.publish_log(cleanup_task_id, f"Batch completed with {failed_docs} failures", "warning")
                                return True
                                
                        elif status == "failed":
                            self.publish_log(cleanup_task_id, f"Batch processing failed", "error")
                            return False
                    else:
                        self.publish_log(cleanup_task_id, f"Error checking batch status: {response.status_code}", "warning")
            
            except Exception as e:
                self.publish_log(cleanup_task_id, f"Error checking batch status: {str(e)}", "warning")

            await asyncio.sleep(ScrapeDataController.STATUS_CHECK_INTERVAL)
        
        self.publish_log(cleanup_task_id, f"Batch processing timeout after {original_max_checks} checks", "error")
        return False

orchestrator = ApolloOrchestrator()