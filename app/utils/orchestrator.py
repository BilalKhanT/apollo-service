from datetime import datetime
import os
import logging
import time
import traceback
import asyncio
from typing import Dict, Any, List, Optional
import json

from app.utils.task_manager import task_manager
from app.utils.config import (
    CRAWLER_USER_AGENT, CRAWLER_TIMEOUT, CRAWLER_NUM_WORKERS,
    CRAWLER_DELAY_BETWEEN_REQUESTS, CRAWLER_INACTIVITY_TIMEOUT,
    CRAWLER_SAVE_INTERVAL, CRAWLER_RESPECT_ROBOTS_TXT,
    DEFAULT_URL_PATTERNS_TO_IGNORE, FILE_EXTENSIONS,
    SOCIAL_MEDIA_KEYWORDS, BANK_KEYWORDS, CLUSTER_MIN_SIZE,
    CLUSTER_PATH_DEPTH, CLUSTER_SIMILARITY_THRESHOLD,
    SCRAPER_OUTPUT_DIR, METADATA_DIR, EXPIRY_DAYS,
    FILE_DOWNLOAD_DIR, MAX_DOWNLOAD_WORKERS, DATA_DIR
)

# Import from services
from app.services.apollo import Apollo
from app.services.link_processor import LinkProcessor
from app.services.url_clusterer import URLClusterer
from app.services.year_extractor import YearExtractor
from app.services.scraper import ClusterScraper
from app.services.downloader import FileDownloader

# Import database controller
from app.controllers.crawl_result_controller import CrawlResultController

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class ApolloOrchestrator:
    """
    Enhanced orchestrator with real-time WebSocket integration.
    Orchestrates the entire Apollo workflow:
    1. Crawling (DATABASE-ONLY - NO FILES)
    2. Link processing (temp files for processing)
    3. URL clustering (temp files for processing)
    4. Year extraction (temp files for processing)
    5. Content scraping (file-based output)
    6. File downloading (file-based output)
    
    Now includes real-time WebSocket updates for live progress tracking.
    """
    
    def __init__(self, base_directory: str = None):
        """
        Initialize the orchestrator.
        
        Args:
            base_directory: Base directory for storing temporary processing files
        """
        self.base_directory = base_directory or DATA_DIR
        self.logger = logger
        
        # Create directory structure for temporary processing files only
        os.makedirs(self.base_directory, exist_ok=True)
        self.temp_dir = os.path.join(self.base_directory, "temp")  # Temporary processing files
        self.scrape_dir = os.path.join(self.base_directory, "scraped")
        self.download_dir = os.path.join(self.base_directory, "downloads")
        self.metadata_dir = os.path.join(self.base_directory, "metadata")
        
        for directory in [self.temp_dir, self.scrape_dir, self.download_dir, self.metadata_dir]:
            os.makedirs(directory, exist_ok=True)

    def publish_log(self, task_id: str, message: str, level: str = "info"):
        """
        Publish a log message to the task manager for real-time updates.
        Enhanced with WebSocket integration while maintaining backward compatibility.
    
        Args:
            task_id: The ID of the task
            message: The log message
            level: Log level (debug, info, warning, error)
        """
        # Log to task manager (this will auto-publish via WebSocket if available)
        task_manager.publish_log(task_id, message, level)
            
        # Also log to regular logger (preserving existing functionality)
        if level == "debug":
            self.logger.debug(message)
        elif level == "info":
            self.logger.info(message)
        elif level == "warning":
            self.logger.warning(message)
        elif level == "error":
            self.logger.error(message)
    
    def cleanup_temp_files(self, files_to_cleanup: List[str]):
        """Clean up temporary processing files (unchanged)"""
        for file_path in files_to_cleanup:
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    self.logger.debug(f"Cleaned up temp file: {file_path}")
            except Exception as e:
                self.logger.warning(f"Failed to cleanup temp file {file_path}: {str(e)}")
    
    async def _start_realtime_publishing(self, task_id: str, interval: float = 1.0):
        """
        Start real-time publishing for a task (WebSocket enhancement).
        Gracefully handles cases where WebSocket is not available.
        """
        try:
            from app.utils.realtime_publisher import realtime_publisher
            await realtime_publisher.start_publishing(task_id, interval=interval)
            self.publish_log(task_id, "Started real-time publishing for task", "info")
            return True
        except ImportError:
            # WebSocket not available, continue without it
            self.logger.debug(f"WebSocket not available for task {task_id}, continuing without real-time publishing")
            return False
        except Exception as e:
            self.publish_log(task_id, f"Warning: Could not start real-time publishing: {str(e)}", "warning")
            return False
    
    async def _stop_realtime_publishing(self, task_id: str):
        """
        Stop real-time publishing for a task (WebSocket enhancement).
        Gracefully handles cases where WebSocket is not available.
        """
        try:
            from app.utils.realtime_publisher import realtime_publisher
            await realtime_publisher.stop_publishing(task_id)
            self.publish_log(task_id, "Stopped real-time publishing for task", "info")
        except ImportError:
            # WebSocket not available, nothing to stop
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
        """
        Run the complete crawling workflow (crawl, process, cluster) - DATABASE ONLY.
        NO FILES are created for crawl results. All data stored in database.
        Enhanced with real-time WebSocket updates while preserving all existing functionality.
        
        Args:
            task_id: Task ID for tracking progress
            base_url: Starting URL for crawling
            max_links_to_scrape: Maximum number of links to scrape (None for unlimited)
            max_pages_to_scrape: Maximum number of pages to scrape (None for unlimited)
            depth_limit: Maximum depth to crawl (None for unlimited)
            domain_restriction: Whether to restrict crawling to the base domain
            scrape_pdfs_and_xls: Whether to scrape PDFs and XLS files
            stop_scraper: Whether to stop the scraper
            
        Returns:
            Dictionary with crawling results
        """
        # Start real-time publishing for this task (WebSocket enhancement)
        await self._start_realtime_publishing(task_id, interval=1.0)
        
        # Handle None values by replacing with infinity (existing functionality)
        if max_links_to_scrape is None:
            max_links_to_scrape = float("inf")
        if max_pages_to_scrape is None:
            max_pages_to_scrape = float("inf")
        if depth_limit is None:
            depth_limit = float("inf")
        
        # Update task status (existing functionality)
        task_manager.update_task_status(
            task_id,
            status="running",
            progress=0.0
        )
        
        # Log start of crawling workflow (existing functionality)
        self.publish_log(task_id, f"Starting crawl workflow for {base_url}", "info")
        
        # If stop_scraper is True, check if there's a running crawler and stop it (existing functionality)
        if stop_scraper:
            self.publish_log(task_id, "Stop signal received. Checking for running crawlers...", "info")
            running_tasks = task_manager.list_tasks(task_type="crawl", status="running")
            for task in running_tasks:
                # Skip the current task
                if task['id'] == task_id:
                    continue
                
                # Try to stop the crawler
                self.publish_log(task_id, f"Stopping crawler task {task['id']}...", "info")
                # TODO: Implement a mechanism to stop running crawlers
            
            # Stop real-time publishing before returning
            await self._stop_realtime_publishing(task_id)
            
            task_manager.update_task_status(task_id, status="completed", progress=100.0)
            return {"status": "stopped", "message": "Stop signal sent to all running crawlers"}
        
        # List of temporary files to cleanup (existing functionality)
        temp_files = []
        
        try:
            # Step 1: Crawling (existing functionality preserved)
            task_manager.update_task_status(
                task_id,
                status="crawling",
                progress=5.0
            )
            
            # Generate unique filenames for temporary processing (existing functionality)
            timestamp = time.strftime("%Y%m%d-%H%M%S")
            crawl_id = f"{timestamp}_{base_url.replace('://', '_').replace('/', '_')[:30]}"
            
            # Define temporary file paths (existing functionality)
            all_links_file = os.path.join(self.temp_dir, f"{crawl_id}_all_links.json")
            categorized_file = os.path.join(self.temp_dir, f"{crawl_id}_categorized.json")
            url_clusters_file = os.path.join(self.temp_dir, f"{crawl_id}_url_clusters.json")
            year_clusters_file = os.path.join(self.temp_dir, f"{crawl_id}_year_clusters.json")
            
            temp_files.extend([all_links_file, categorized_file, url_clusters_file, year_clusters_file])
            
            # Log the limits being used (existing functionality)
            self.publish_log(task_id, f"Starting crawler for {base_url} with limits: max_links={max_links_to_scrape}, max_pages={max_pages_to_scrape}, depth_limit={depth_limit}", "info")
            
            # Define a callback for Apollo status updates (existing functionality preserved)
            def status_callback(status_data):
                # Get the current time elapsed since the crawler started
                execution_time_seconds = status_data.get('execution_time_seconds', 0)
                crawler_progress = status_data.get('progress', 0)
                
                # Initialize progress tracking if not already done
                if not hasattr(self, '_last_progress'):
                    self._last_progress = 5.0  # Start at 5%
                    self._last_update_time = time.time()
                
                # SIMPLE APPROACH:
                # Check if we're dealing with unlimited crawling (any parameter is infinity)
                if (max_links_to_scrape == float("inf") or 
                    max_pages_to_scrape == float("inf") or 
                    depth_limit == float("inf")):
                    
                    # Calculate time since last progress update
                    current_time = time.time()
                    time_since_update = current_time - self._last_update_time
                    
                    # Increase progress by 1% every 2 seconds, but never exceed 95%
                    if time_since_update >= 2.0:  # Every 2 seconds
                        increase_amount = (time_since_update / 5.0)  # 1% per 5 seconds
                        new_progress = min(95.0, self._last_progress + increase_amount)
                        
                        # Update the last progress update time
                        self._last_update_time = current_time
                        progress = new_progress
                    else:
                        # No change in progress if less than 2 seconds have passed
                        progress = self._last_progress
                else:
                    # For bounded crawls, calculate progress normally (0-40% range)
                    if crawler_progress >= 99.0:
                        # Full completion of crawl phase
                        progress = 40.0
                    else:
                        # Normal scaling from crawler progress (0-100) to overall progress (0-40)
                        progress = (crawler_progress / 100) * 40.0
                
                # Update last progress value
                self._last_progress = progress
                
                # Prepare the crawl result
                crawl_result = {
                    "crawl_results": {
                        "total_links_found": status_data.get('links_found', 0),
                        "total_pages_scraped": status_data.get('pages_scraped', 0),
                        "execution_time_seconds": execution_time_seconds
                    }
                }
                
                # Update task status with progress and result
                task_manager.update_task_status(
                    task_id,
                    progress=progress,
                    result=crawl_result
                )
                
                # Log progress updates periodically (enhanced frequency for better WebSocket updates)
                if status_data.get('pages_scraped', 0) % 2 == 0:  # Every 2 pages
                    self.publish_log(
                        task_id,
                        f"Crawl progress: {status_data.get('pages_scraped', 0)} pages scraped, "
                        f"{status_data.get('links_found', 0)} links found, progress: {progress:.1f}%",
                        "info"
                    )
            
            # Create the crawler (existing functionality)
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

            # Store the crawler instance for potential stopping (existing functionality)
            setattr(self, f"crawler_{task_id}", crawler)
            
            # Register the status callback (existing functionality)
            crawler.register_status_callback(status_callback)
            
            # Start the crawler (existing functionality)
            self.publish_log(task_id, f"Starting crawler for {base_url}", "info")
            loop = asyncio.get_event_loop()
            crawl_result = await loop.run_in_executor(None, crawler.start)
            
            # Log completion of crawling (existing functionality)
            self.publish_log(
                task_id,
                f"Crawling completed. Found {crawl_result['summary']['total_links_found']} links, scraped {crawl_result['summary']['total_pages_scraped']} pages.",
                "info"
            )
            
            # Update progress (existing functionality)
            task_manager.update_task_status(
                task_id,
                progress=40.0,
                result={"crawl_complete": True, "crawl_results": crawl_result["summary"]}
            )
            
            # Step 2: Link processing (existing functionality preserved)
            task_manager.update_task_status(
                task_id,
                status="processing",
                progress=45.0
            )
            
            # Create the link processor (existing functionality)
            self.publish_log(task_id, "Processing links...", "info")
            processor = LinkProcessor(
                input_file=all_links_file,
                output_file=categorized_file,
                num_workers=CRAWLER_NUM_WORKERS,
                file_extensions=FILE_EXTENSIONS,
                social_media_keywords=SOCIAL_MEDIA_KEYWORDS,
                bank_keywords=BANK_KEYWORDS
            )
            
            # Process links (existing functionality)
            process_result = await loop.run_in_executor(None, processor.process)
            
            # Log link processing results (existing functionality)
            self.publish_log(
                task_id,
                f"Link processing completed. Categorized {process_result['summary']['total_links']} links into {process_result['summary']['file_links_count']} file links, {process_result['summary']['bank_links_count']} bank links, {process_result['summary']['social_media_links_count']} social media links, and {process_result['summary']['misc_links_count']} miscellaneous links.",
                "info"
            )
            
            # Update progress (existing functionality)
            task_manager.update_task_status(
                task_id,
                progress=60.0,
                result={
                    **task_manager.get_task_status(task_id)["result"],
                    "process_complete": True,
                    "process_results": process_result["summary"]
                }
            )
            
            # Step 3: URL clustering (existing functionality preserved)
            task_manager.update_task_status(
                task_id,
                status="clustering",
                progress=65.0
            )
            
            # Create the URL clusterer (existing functionality)
            self.publish_log(task_id, "Clustering URLs...", "info")
            clusterer = URLClusterer(
                input_file=categorized_file,
                output_file=url_clusters_file,
                min_cluster_size=CLUSTER_MIN_SIZE,
                path_depth=CLUSTER_PATH_DEPTH,
                similarity_threshold=CLUSTER_SIMILARITY_THRESHOLD
            )
            
            # Cluster URLs (existing functionality)
            cluster_result = await loop.run_in_executor(None, clusterer.cluster)

            # Clean up crawler reference (existing functionality)
            if hasattr(self, f"crawler_{task_id}"):
                delattr(self, f"crawler_{task_id}")
            
            # Log URL clustering results (existing functionality)
            self.publish_log(
                task_id,
                f"URL clustering completed. Identified {cluster_result['summary']['total_domains']} domains and {cluster_result['summary']['total_clusters']} clusters across {cluster_result['summary']['total_urls']} URLs.",
                "info"
            )
            
            # Update progress (existing functionality)
            task_manager.update_task_status(
                task_id,
                progress=80.0,
                result={
                    **task_manager.get_task_status(task_id)["result"],
                    "cluster_complete": True,
                    "cluster_results": cluster_result["summary"]
                }
            )
            
            # Step 4: Year extraction (existing functionality preserved)
            task_manager.update_task_status(
                task_id,
                status="year_extraction",
                progress=85.0
            )
            
            # Create the year extractor (existing functionality)
            self.publish_log(task_id, "Extracting years from file URLs...", "info")
            year_extractor = YearExtractor(
                input_file=categorized_file,
                output_file=year_clusters_file
            )
            
            # Extract years (existing functionality)
            year_result = await loop.run_in_executor(None, year_extractor.process)
            
            # Log year extraction results (existing functionality)
            self.publish_log(
                task_id,
                f"Year extraction completed. Identified {len(year_result)} distinct years across {sum(len(files) for files in year_result.values())} files.",
                "info"
            )
            
            # Update progress (existing functionality)
            task_manager.update_task_status(
                task_id,
                progress=90.0,
                result={
                    **task_manager.get_task_status(task_id)["result"],
                    "year_extraction_complete": True,
                    "year_extraction_results": {
                        "total_years": len(year_result),
                        "total_files": sum(len(files) for files in year_result.values())
                    }
                }
            )
            
            # Step 5: SAVE TO DATABASE (existing functionality preserved)
            task_manager.update_task_status(
                task_id,
                status="saving_to_database",
                progress=95.0
            )
            
            self.publish_log(task_id, "Saving crawl results to database...", "info")
            
            try:
                # Save to database - ONLY ON SUCCESS (existing functionality)
                await CrawlResultController.create_crawl_result(
                    task_id=task_id,
                    link_found=crawl_result["summary"]["total_links_found"],
                    pages_scraped=crawl_result["summary"]["total_pages_scraped"],
                    clusters=cluster_result["clusters"],
                    yearclusters=year_result
                )
                
                self.publish_log(task_id, "Crawl results saved to database successfully", "info")
                
            except Exception as db_error:
                error_msg = f"Failed to save crawl results to database: {str(db_error)}"
                self.publish_log(task_id, error_msg, "error")
                
                # Clean up temp files before failing (existing functionality)
                self.cleanup_temp_files(temp_files)
                
                # Stop real-time publishing on error (WebSocket enhancement)
                await self._stop_realtime_publishing(task_id)
                
                task_manager.update_task_status(
                    task_id,
                    status="failed",
                    error=error_msg
                )
                return task_manager.get_task_status(task_id)
            
            # Update status to completed (existing functionality)
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
            
            # Clean up temporary files (existing functionality)
            self.cleanup_temp_files(temp_files)
            
            # Real-time publisher will auto-stop when task completes (WebSocket enhancement)
            # No need to manually stop here as it will be handled automatically
            
            # Return final status (existing functionality)
            return task_manager.get_task_status(task_id)
        
        except Exception as e:
            error_msg = f"Error in crawl workflow: {str(e)}"
            self.publish_log(task_id, error_msg, "error")
            
            # Clean up temp files on error (existing functionality)
            self.cleanup_temp_files(temp_files)
            
            # Stop real-time publishing on error (WebSocket enhancement)
            await self._stop_realtime_publishing(task_id)
            
            task_manager.update_task_status(
                task_id,
                status="failed",
                error=error_msg
            )
            return task_manager.get_task_status(task_id)
    
    def stop_crawl(self, task_id: str) -> Dict[str, Any]:
        """
        Stop a running crawl process gracefully with proper cleanup.
        Enhanced with real-time publishing cleanup while preserving existing functionality.
    
        Args:
            task_id: ID of the task to stop
        
        Returns:
            Dictionary with stop result
        """
        self.publish_log(task_id, f"Attempting to stop crawl task {task_id} gracefully...", "info")
    
        # Get the task status (existing functionality)
        task_status = task_manager.get_task_status(task_id)
        
        if not task_status:
            error_msg = f"Task {task_id} not found"
            self.publish_log(task_id, error_msg, "error")
            return {"success": False, "message": error_msg}
        
        # Check if task is a crawl task (existing functionality)
        if task_status.get("type") != "crawl":
            error_msg = f"Task {task_id} is not a crawl task"
            self.publish_log(task_id, error_msg, "error")
            return {"success": False, "message": error_msg}
        
        # Check if task is already completed or failed (existing functionality)
        current_status = task_status.get("status")
        if current_status in ["completed", "failed", "stopped"]:
            msg = f"Task {task_id} is already in '{current_status}' state"
            self.publish_log(task_id, msg, "info")
            return {"success": True, "message": msg}
        
        # Task is running, try to stop it (existing functionality enhanced)
        try:
            # Stop real-time publishing first (WebSocket enhancement)
            try:
                import asyncio
                from app.utils.realtime_publisher import realtime_publisher
                
                # Run in async context if available
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
            
            # Get the crawler instance from context if available (existing functionality)
            crawler_instance = getattr(self, f"crawler_{task_id}", None)
            
            if crawler_instance:
                # Stop the crawler directly (existing functionality)
                self.publish_log(task_id, "Stopping crawler...", "info")
                stop_result = crawler_instance.stop()
                
                if stop_result:
                    # Clean up the crawler (existing functionality)
                    cleanup_result = crawler_instance.cleanup()
                    
                    # Remove reference to the crawler (existing functionality)
                    delattr(self, f"crawler_{task_id}")
                    
                    # Update task status (existing functionality)
                    task_manager.update_task_status(
                        task_id,
                        status="stopped",
                        progress=100.0,
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
                # No direct crawler instance, just update the task status (existing functionality)
                self.publish_log(task_id, "No active crawler instance found, updating task status to stopped", "info")
                
                # Update task status to stopped (existing functionality)
                task_manager.update_task_status(
                    task_id,
                    status="stopped",
                    progress=100.0,
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
            
            # Try to update task status anyway (existing functionality)
            task_manager.update_task_status(
                task_id,
                status="failed",
                error=error_msg
            )
            
            return {"success": False, "message": error_msg}

    async def run_scrape_download(
        self,
        task_id: str,
        cluster_ids: List[str],
        years: List[str] = None,
        crawl_task_id: str = None
    ) -> Dict[str, Any]:
        """
        Run the scraping and downloading workflow with enhanced progress tracking.
        Now retrieves data from DATABASE instead of files.
        Enhanced with real-time WebSocket updates while preserving all existing functionality.
        
        Args:
            task_id: Task ID for tracking progress
            cluster_ids: IDs of clusters to scrape
            years: Years of files to download
            crawl_task_id: Task ID of the crawl to use data from
            
        Returns:
            Dictionary with scraping and downloading results
        """
        # Start real-time publishing for this task (WebSocket enhancement)
        await self._start_realtime_publishing(task_id, interval=1.5)
        
        # Initialize task status (existing functionality)
        task_manager.update_task_status(
            task_id,
            status="initializing",
            progress=0.0
        )
        
        # Log start of the workflow (existing functionality)
        self.publish_log(task_id, "Starting scrape and download workflow", "info")
        
        try:
            # === INITIALIZATION PHASE (0-5%) === (existing functionality preserved)
            # Update task status
            task_manager.update_task_status(
                task_id,
                status="checking_database",
                progress=1.0
            )
            
            # Get crawl result from database (existing functionality)
            crawl_result = None
            if crawl_task_id:
                # Get specific crawl result
                crawl_result = await CrawlResultController.get_crawl_result(crawl_task_id)
                if not crawl_result:
                    error_msg = f"Crawl result for task {crawl_task_id} not found in database"
                    self.publish_log(task_id, error_msg, "error")
                    raise Exception(error_msg)
                self.publish_log(task_id, f"Using crawl result from task: {crawl_result.task_id}", "info")
            else:
                # Get the most recent crawl result
                crawl_results = await CrawlResultController.list_crawl_results()
                if crawl_results:
                    # Sort by created_at to get the most recent
                    crawl_results.sort(key=lambda x: x.created_at, reverse=True)
                    crawl_result = crawl_results[0]
                    self.publish_log(task_id, f"Using most recent crawl result: {crawl_result.task_id}", "info")
                else:
                    error_msg = "No crawl results found in database"
                    self.publish_log(task_id, error_msg, "error")
                    raise Exception(error_msg)
            
            # Update progress after database check (existing functionality)
            task_manager.update_task_status(
                task_id,
                status="preparing",
                progress=3.0
            )
            
            # === SCRAPING PHASE (5-60%) === (existing functionality preserved)
            if cluster_ids and crawl_result:
                task_manager.update_task_status(
                    task_id,
                    status="scraping",
                    progress=5.0
                )
                
                # Mark the crawl result as scraped (existing functionality)
                await CrawlResultController.mark_as_scraped(crawl_result.task_id)
                
                # Define scraper output directory (existing functionality)
                timestamp = time.strftime("%Y%m%d-%H%M%S")
                scrape_output_dir = os.path.join(self.scrape_dir, f"scrape_{timestamp}")
                metadata_output_dir = os.path.join(self.metadata_dir, f"metadata_{timestamp}")
                
                # Create temporary clusters file for scraper compatibility (existing functionality)
                temp_clusters_file = os.path.join(self.temp_dir, f"temp_clusters_{timestamp}.json")
                
                # Convert database clusters to file format for scraper (existing functionality)
                clusters_dict = {}
                for domain_name, domain_data in crawl_result.clusters.items():
                    clusters_dict[domain_name] = {
                        "id": domain_data.id,
                        "count": domain_data.count,
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

                clusters_data = {
                    "summary": {
                        "total_domains": len(crawl_result.clusters),
                        "total_clusters": sum(len(domain_data.clusters) for domain_data in crawl_result.clusters.values()),
                        "total_urls": sum(sum(cluster.url_count for cluster in domain_data.clusters) for domain_data in crawl_result.clusters.values())
                    },
                    "clusters": clusters_dict
                }
                
                with open(temp_clusters_file, 'w', encoding='utf-8') as f:
                    json.dump(clusters_data, f, indent=2)
                
                # Create the scraper with task ID for progress tracking (existing functionality)
                self.publish_log(task_id, f"Preparing to scrape {len(cluster_ids)} clusters", "info")
                scraper = ClusterScraper(
                    json_file_path=temp_clusters_file,
                    output_dir=scrape_output_dir,
                    metadata_dir=metadata_output_dir,
                    expiry_days=EXPIRY_DAYS
                )
                
                # Scrape the clusters with task ID for continuous progress updates (existing functionality)
                self.publish_log(task_id, f"Starting scraping of clusters: {cluster_ids}", "info")
                loop = asyncio.get_event_loop()
                scrape_result = await loop.run_in_executor(
                    None, 
                    scraper.scrape_clusters, 
                    cluster_ids, 
                    task_id
                )
                
                # Clean up temporary file (existing functionality)
                try:
                    os.remove(temp_clusters_file)
                except:
                    pass
                
                # Log scraping results (existing functionality)
                self.publish_log(
                    task_id,
                    f"Scraping completed. Scraped {scrape_result['pages_scraped']} pages from {len(scrape_result['clusters_scraped'])} clusters.",
                    "info"
                )
                
                # Update progress to 60% (existing functionality)
                task_manager.update_task_status(
                    task_id,
                    progress=60.0,
                    result={
                        "scrape_complete": True,
                        "scrape_results": {
                            "pages_scraped": scrape_result["pages_scraped"],
                            "clusters_scraped": len(scrape_result["clusters_scraped"]),
                            "scrape_output_dir": scrape_output_dir,
                            "metadata_output_dir": metadata_output_dir
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
            
            # === TRANSITION PHASE (60-65%) === (existing functionality preserved)
            # Add a brief "preparing download" phase for smoother transition
            if years and crawl_result:
                task_manager.update_task_status(
                    task_id,
                    status="preparing_download",
                    progress=62.0
                )
                
                # Log the transition
                self.publish_log(task_id, "Scraping phase complete. Preparing for download phase...", "info")
                
                # Short sleep to ensure status updates are visible
                await asyncio.sleep(0.5)
            
            # === DOWNLOADING PHASE (65-90%) === (existing functionality preserved)
            if years and crawl_result:
                task_manager.update_task_status(
                    task_id,
                    status="downloading",
                    progress=65.0
                )
                
                # Define download output directory (existing functionality)
                timestamp = time.strftime("%Y%m%d-%H%M%S")
                download_output_dir = os.path.join(self.download_dir, f"download_{timestamp}")
                
                # Create temporary year clusters file for downloader compatibility (existing functionality)
                temp_year_file = os.path.join(self.temp_dir, f"temp_years_{timestamp}.json")
                
                with open(temp_year_file, 'w', encoding='utf-8') as f:
                    json.dump(crawl_result.yearclusters, f, indent=2)
                
                # Create the downloader with task ID for progress tracking (existing functionality)
                self.publish_log(task_id, f"Preparing to download files for years: {years}", "info")
                downloader = FileDownloader(
                    max_workers=MAX_DOWNLOAD_WORKERS,
                    timeout=CRAWLER_TIMEOUT
                )
                
                # Download the files with task ID for continuous progress updates (existing functionality)
                self.publish_log(task_id, f"Starting download of files for years: {years}", "info")
                loop = asyncio.get_event_loop()
                download_result = await loop.run_in_executor(
                    None,
                    downloader.download_files_by_year,
                    temp_year_file,
                    years,
                    download_output_dir,
                    task_id
                )
                
                # Clean up temporary file (existing functionality)
                try:
                    os.remove(temp_year_file)
                except:
                    pass
                
                # Log downloading results (existing functionality)
                self.publish_log(
                    task_id,
                    f"File downloading completed. Successfully downloaded {download_result['successful']} files, failed to download {download_result['failed']} files.",
                    "info"
                )
                
                # Update progress to 90% (existing functionality)
                task_manager.update_task_status(
                    task_id,
                    progress=90.0,
                    result={
                        **task_manager.get_task_status(task_id)["result"],
                        "download_complete": True,
                        "download_results": {
                            "files_downloaded": download_result["successful"],
                            "files_failed": download_result["failed"],
                            "download_output_dir": download_output_dir
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
            
            # === FINALIZATION PHASE (90-100%) === (existing functionality preserved)
            task_manager.update_task_status(
                task_id,
                status="finalizing",
                progress=95.0
            )
            
            self.publish_log(task_id, "Finalizing workflow and generating summary...", "info")
            
            # Add a brief delay to show the finalizing step (existing functionality)
            await asyncio.sleep(0.5)
            
            # Update status to completed (existing functionality)
            task_manager.update_task_status(
                task_id,
                status="completed",
                progress=100.0
            )
            
            self.publish_log(task_id, "Scrape and download workflow completed successfully", "info")
            
            # Real-time publisher will auto-stop when task completes (WebSocket enhancement)
            # No need to manually stop here as it will be handled automatically
            
            # Return final status (existing functionality)
            return task_manager.get_task_status(task_id)
        
        except Exception as e:
            error_msg = f"Error in scrape/download workflow: {str(e)}"
            self.publish_log(task_id, error_msg, "error")
            self.publish_log(task_id, traceback.format_exc(), "error")
            
            # Stop real-time publishing on error (WebSocket enhancement)
            await self._stop_realtime_publishing(task_id)
            
            task_manager.update_task_status(
                task_id,
                status="failed",
                error=error_msg
            )
            return task_manager.get_task_status(task_id)
    
    # === ALL REMAINING METHODS PRESERVED EXACTLY AS THEY WERE === (existing functionality)
    async def get_available_clusters(self, crawl_task_id: str = None) -> List[Dict[str, Any]]:
        """Get available clusters from DATABASE (existing functionality preserved)"""
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
        """
        Get available years for downloading from DATABASE (existing functionality preserved).
        
        Args:
            crawl_task_id: Task ID to get years for (if None, gets most recent)
            
        Returns:
            List of dictionaries with year information
        """
        try:
            crawl_result = None
            
            if crawl_task_id:
                # Get specific crawl result
                crawl_result = await CrawlResultController.get_crawl_result(crawl_task_id)
            else:
                # Get the most recent crawl result
                crawl_results = await CrawlResultController.list_crawl_results()
                if crawl_results:
                    crawl_results.sort(key=lambda x: x.created_at, reverse=True)
                    crawl_result = crawl_results[0]
            
            if not crawl_result or not crawl_result.yearclusters:
                return []
            
            # Create a list of available years
            years_info = []
            
            # Go through years
            for year, files in crawl_result.yearclusters.items():
                years_info.append({
                    "year": year,
                    "files_count": len(files)
                })
            
            # Sort by year (newest first, but "No Year" at the end)
            return sorted(
                years_info,
                key=lambda y: (y["year"] == "No Year", y["year"]),
                reverse=True
            )
        
        except Exception as e:
            logger.error(f"Error getting available years: {str(e)}")
            return []
        
    async def get_cluster_by_id(self, cluster_id: str, crawl_task_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Get cluster by ID from DATABASE (existing functionality preserved)"""
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
        """
        Get year by ID from DATABASE (existing functionality preserved).
        
        Args:
            year: Year to find
            crawl_task_id: Task ID to get year from (if None, gets most recent)
            
        Returns:
            Year data if found, None otherwise
        """
        try:
            crawl_result = None
            
            if crawl_task_id:
                # Get specific crawl result
                crawl_result = await CrawlResultController.get_crawl_result(crawl_task_id)
            else:
                # Get the most recent crawl result
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
        
    async def run_restaurant_scraping(
        self,
        task_id: str,
        cities: List[str]
    ) -> Dict[str, Any]:
        """
        Run the restaurant deals scraping workflow with enhanced progress tracking.
        Enhanced with real-time WebSocket updates while preserving all existing functionality.
        
        Args:
            task_id: Task ID for tracking progress
            cities: List of cities to scrape restaurant deals from
            
        Returns:
            Dictionary with scraping results
        """
        # Start real-time publishing for this task (WebSocket enhancement)
        await self._start_realtime_publishing(task_id, interval=2.0)
        
        # Update task status (existing functionality)
        task_manager.update_task_status(
            task_id,
            status="running",
            progress=0.0
        )
        
        # Log start of restaurant scraping workflow (existing functionality)
        self.publish_log(task_id, f"Starting restaurant deals scraping for cities: {', '.join(cities)}", "info")
        
        try:
            # Step 1: Initialize scraper service
            task_manager.update_task_status(
                task_id,
                status="initializing",
                progress=5.0
            )
            
            # Generate unique output directory for this task
            timestamp = time.strftime("%Y%m%d-%H%M%S")
            output_dir = os.path.join(self.base_directory, "restaurant_deals", f"restaurant_{timestamp}")
            os.makedirs(output_dir, exist_ok=True)
            
            # Import and create the restaurant scraper service
            from app.services.restaurant_deal_scrapper_service import RestaurantDealsScraperService
            
            scraper_service = RestaurantDealsScraperService(
                cities_list=cities,
                country="Pakistan",
                language="en",
                output_dir=output_dir
            )
            
            self.publish_log(task_id, f"Initialized restaurant scraper for {len(cities)} cities", "info")
            
            # Step 2: Run the scraping process with task ID for progress tracking
            task_manager.update_task_status(
                task_id,
                status="scraping",
                progress=10.0
            )
            
            self.publish_log(task_id, "Starting restaurant deals scraping process", "info")
            
            # Run scraping in executor to avoid blocking
            loop = asyncio.get_event_loop()
            scraping_result = await loop.run_in_executor(
                None, 
                scraper_service.scrape_all_deals,
                task_id
            )
            
            # Step 3: Save results to database
            task_manager.update_task_status(
                task_id,
                status="saving_to_database",
                progress=95.0
            )
            
            self.publish_log(task_id, "Saving restaurant scraping results to database", "info")
            
            try:
                # Import database controller
                from app.controllers.restaurant_result_controller import RestaurantResultController
                
                # Save to database
                await RestaurantResultController.create_restaurant_result(
                    task_id=task_id,
                    cities_requested=cities,
                    cities_processed=scraping_result.get("cities_processed", 0),
                    restaurants_processed=scraping_result.get("restaurants_processed", 0),
                    deals_processed=scraping_result.get("deals_processed", 0),
                    total_cities=scraping_result.get("total_cities", 0),
                    total_restaurants=scraping_result.get("total_restaurants", 0),
                    output_directory=output_dir,
                    execution_time_seconds=scraping_result.get("execution_time_seconds", 0.0),
                    status=scraping_result.get("status", "completed"),
                    database_summary=scraping_result.get("database_summary")
                )
                
                self.publish_log(task_id, "Restaurant scraping results saved to database successfully", "info")
                
            except Exception as db_error:
                error_msg = f"Failed to save restaurant scraping results to database: {str(db_error)}"
                self.publish_log(task_id, error_msg, "error")
                
                # Stop real-time publishing on error
                await self._stop_realtime_publishing(task_id)
                
                task_manager.update_task_status(
                    task_id,
                    status="failed",
                    error=error_msg
                )
                return task_manager.get_task_status(task_id)
            
            # Step 4: Final completion
            final_status = "completed" if scraping_result.get("status") == "completed" else scraping_result.get("status", "failed")
            
            task_manager.update_task_status(
                task_id,
                status=final_status,
                progress=100.0,
                result={
                    "restaurant_scraping_complete": True,
                    "cities_requested": cities,
                    "cities_processed": scraping_result.get("cities_processed", 0),
                    "restaurants_processed": scraping_result.get("restaurants_processed", 0),
                    "deals_processed": scraping_result.get("deals_processed", 0),
                    "total_cities": scraping_result.get("total_cities", 0),
                    "total_restaurants": scraping_result.get("total_restaurants", 0),
                    "output_directory": output_dir,
                    "execution_time_seconds": scraping_result.get("execution_time_seconds", 0.0),
                    "database_summary": scraping_result.get("database_summary")
                }
            )
            
            self.publish_log(task_id, f"Restaurant scraping completed successfully for cities: {', '.join(cities)}. Results saved to database.", "info")
            
            # Real-time publisher will auto-stop when task completes (WebSocket enhancement)
            # No need to manually stop here as it will be handled automatically
            
            # Return final status (existing functionality)
            return task_manager.get_task_status(task_id)
        
        except Exception as e:
            error_msg = f"Error in restaurant scraping workflow: {str(e)}"
            self.publish_log(task_id, error_msg, "error")
            
            # Stop real-time publishing on error (WebSocket enhancement)
            await self._stop_realtime_publishing(task_id)
            
            task_manager.update_task_status(
                task_id,
                status="failed",
                error=error_msg
            )
            return task_manager.get_task_status(task_id)

# Create a global orchestrator instance (existing functionality preserved)
orchestrator = ApolloOrchestrator()