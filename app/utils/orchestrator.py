from datetime import datetime
import os
import logging
import time
import traceback
from typing import Dict, Any, List, Optional
from bson import ObjectId

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

# Import database models
from app.models.database.database_models import (
    CrawlResult, ClusterDocument, YearDocument, CrawlData, ProcessedLinks,
    CrawlSummary, ProcessSummary, ClusterSummary, YearExtractionSummary,
    DomainCluster, Cluster
)
from app.utils.database import get_database

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class ApolloOrchestrator:
    """
    A class to orchestrate the entire Apollo workflow with full database integration:
    1. Crawling -> Database
    2. Link processing -> Database
    3. URL clustering -> Database
    4. Year extraction -> Database
    5. Content scraping (file-based for now)
    6. File downloading (file-based for now)
    """
    
    def __init__(self, base_directory: str = None):
        """
        Initialize the orchestrator.
        
        Args:
            base_directory: Base directory for storing scraping outputs (still needed for scraping/downloading)
        """
        self.base_directory = base_directory or DATA_DIR
        self.logger = logger
        
        # Create directory structure (only needed for scraping/downloading now)
        os.makedirs(self.base_directory, exist_ok=True)
        self.scrape_dir = os.path.join(self.base_directory, "scraped")
        self.download_dir = os.path.join(self.base_directory, "downloads")
        self.metadata_dir = os.path.join(self.base_directory, "metadata")
        
        for directory in [self.scrape_dir, self.download_dir, self.metadata_dir]:
            os.makedirs(directory, exist_ok=True)

    def publish_log(self, task_id: str, message: str, level: str = "info"):
        """
        Publish a log message to the task manager for real-time updates.
    
        Args:
            task_id: The ID of the task
            message: The log message
            level: Log level (debug, info, warning, error)
        """
        # Log to task manager
        from app.utils.task_manager import task_manager
        task_manager.publish_log(task_id, message, level)
            
        # Also log to regular logger
        if level == "debug":
            self.logger.debug(message)
        elif level == "info":
            self.logger.info(message)
        elif level == "warning":
            self.logger.warning(message)
        elif level == "error":
            self.logger.error(message)

    async def save_crawl_data_to_database(
        self,
        task_id: str,
        crawl_result_id: str,
        crawl_results: Dict[str, Any]
    ) -> None:
        """
        Save raw crawl data to database.
        """
        try:
            # Delete existing crawl data for this crawl result
            await CrawlData.find(CrawlData.crawl_result_id == crawl_result_id).delete()
            
            # Create new crawl data document
            crawl_data = CrawlData(
                crawl_result_id=crawl_result_id,
                task_id=task_id,
                all_links=crawl_results.get('all_links', []),
                document_links=crawl_results.get('direct_document_links', []),
                not_found_urls=crawl_results.get('404_urls', []),
                error_urls=crawl_results.get('error_urls', {})
            )
            
            await crawl_data.save()
            self.publish_log(task_id, f"Raw crawl data saved to database", "info")
            
        except Exception as e:
            self.publish_log(task_id, f"Error saving crawl data to database: {str(e)}", "error")
            raise

    async def save_crawl_to_database(
        self,
        task_id: str,
        crawl_id: str,
        crawl_summary: Dict[str, Any],
        process_summary: Dict[str, Any] = None,
        cluster_summary: Dict[str, Any] = None,
        year_extraction_summary: Dict[str, Any] = None,
        clusters_data: Dict[str, Any] = None,
        year_data: Dict[str, Any] = None
    ) -> str:
        """
        Save crawl results to database.
        
        Returns:
            The database document ID
        """
        try:
            # Create or update crawl result document
            crawl_result = await CrawlResult.find_one(CrawlResult.task_id == task_id)
            
            # Ensure crawl_summary has all required fields
            if crawl_summary and 'crawl_date' not in crawl_summary:
                crawl_summary['crawl_date'] = datetime.now().isoformat()
            
            if not crawl_result:
                crawl_result = CrawlResult(
                    task_id=task_id,
                    crawl_id=crawl_id,
                    crawl_summary=CrawlSummary(**crawl_summary) if crawl_summary else None
                )
            else:
                crawl_result.updated_at = datetime.utcnow()
                if crawl_summary:
                    crawl_result.crawl_summary = CrawlSummary(**crawl_summary)
            
            # Update completion flags and summaries
            if process_summary:
                crawl_result.process_complete = True
                crawl_result.process_summary = ProcessSummary(**process_summary)
            
            if cluster_summary:
                crawl_result.cluster_complete = True
                crawl_result.cluster_summary = ClusterSummary(**cluster_summary)
            
            if year_extraction_summary:
                crawl_result.year_extraction_complete = True
                crawl_result.year_extraction_summary = YearExtractionSummary(**year_extraction_summary)
            
            # Save the main crawl result
            await crawl_result.save()
            crawl_result_id = str(crawl_result.id)
            
            # Save clusters data if provided
            if clusters_data and cluster_summary:
                await self.save_clusters_to_database(crawl_result_id, task_id, clusters_data)
            
            # Save year data if provided
            if year_data and year_extraction_summary:
                await self.save_years_to_database(crawl_result_id, task_id, year_data)
            
            self.publish_log(task_id, f"Crawl results saved to database with ID: {crawl_result_id}", "info")
            return crawl_result_id
            
        except Exception as e:
            self.publish_log(task_id, f"Error saving crawl results to database: {str(e)}", "error")
            raise

    async def save_clusters_to_database(
        self,
        crawl_result_id: str,
        task_id: str,
        clusters_data: Dict[str, Any]
    ):
        """Save clusters data to database."""
        try:
            # Delete existing clusters for this crawl result
            await ClusterDocument.find(ClusterDocument.crawl_result_id == crawl_result_id).delete()
            
            # Save new clusters
            cluster_documents = []
            for domain, domain_data in clusters_data.get("clusters", {}).items():
                # Convert cluster data to the expected format
                clusters_list = []
                for cluster in domain_data.get("clusters", []):
                    clusters_list.append(Cluster(
                        id=cluster.get("id"),
                        path=cluster.get("path"),
                        url_count=cluster.get("url_count"),
                        urls=cluster.get("urls", [])
                    ))
                
                domain_cluster = DomainCluster(
                    id=domain_data.get("id"),
                    count=domain_data.get("count"),
                    clusters=clusters_list
                )
                
                cluster_doc = ClusterDocument(
                    crawl_result_id=crawl_result_id,
                    task_id=task_id,
                    domain=domain,
                    cluster_data=domain_cluster
                )
                cluster_documents.append(cluster_doc)
            
            if cluster_documents:
                await ClusterDocument.insert_many(cluster_documents)
                self.publish_log(task_id, f"Saved {len(cluster_documents)} domain clusters to database", "info")
        
        except Exception as e:
            self.publish_log(task_id, f"Error saving clusters to database: {str(e)}", "error")
            raise

    async def save_years_to_database(
        self,
        crawl_result_id: str,
        task_id: str,
        year_data: Dict[str, List[str]]
    ):
        """Save year data to database."""
        try:
            # Delete existing year data for this crawl result
            await YearDocument.find(YearDocument.crawl_result_id == crawl_result_id).delete()
            
            # Save new year data
            year_documents = []
            for year, files in year_data.items():
                year_doc = YearDocument(
                    crawl_result_id=crawl_result_id,
                    task_id=task_id,
                    year=year,
                    files=files,
                    files_count=len(files)
                )
                year_documents.append(year_doc)
            
            if year_documents:
                await YearDocument.insert_many(year_documents)
                self.publish_log(task_id, f"Saved {len(year_documents)} year clusters to database", "info")
        
        except Exception as e:
            self.publish_log(task_id, f"Error saving years to database: {str(e)}", "error")
            raise
    
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
        Run the complete crawling workflow (crawl, process, cluster) with full database integration.
        
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
        # Handle None values by replacing with infinity
        if max_links_to_scrape is None:
            max_links_to_scrape = float("inf")
        if max_pages_to_scrape is None:
            max_pages_to_scrape = float("inf")
        if depth_limit is None:
            depth_limit = float("inf")
        
        # Update task status
        task_manager.update_task_status(
            task_id,
            status="running",
            progress=0.0
        )
        
        # Log start of crawling workflow
        self.publish_log(task_id, f"Starting crawl workflow for {base_url}", "info")
        
        # If stop_scraper is True, handle stopping logic
        if stop_scraper:
            self.publish_log(task_id, "Stop signal received. Checking for running crawlers...", "info")
            running_tasks = task_manager.list_tasks(task_type="crawl", status="running")
            for task in running_tasks:
                if task['id'] == task_id:
                    continue
                self.publish_log(task_id, f"Stopping crawler task {task['id']}...", "info")
            
            return {"status": "stopped", "message": "Stop signal sent to all running crawlers"}
        
        try:
            # Step 1: Crawling
            task_manager.update_task_status(
                task_id,
                status="crawling",
                progress=5.0
            )
            
            # Generate unique identifiers
            timestamp = time.strftime("%Y%m%d-%H%M%S")
            crawl_id = f"{timestamp}_{base_url.replace('://', '_').replace('/', '_')[:30]}"
            
            # Log the limits being used
            self.publish_log(task_id, f"Starting crawler for {base_url} with limits: max_links={max_links_to_scrape}, max_pages={max_pages_to_scrape}, depth_limit={depth_limit}", "info")
            
            # Define a callback for Apollo status updates
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
            
            # Create the crawler (no output file needed)
            crawler = Apollo(
                base_url=base_url,
                output_file="/tmp/temp_crawl.json",  # Temporary, will be ignored
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

            # Store the crawler instance for potential stopping
            setattr(self, f"crawler_{task_id}", crawler)
            
            # Register the status callback
            crawler.register_status_callback(status_callback)
            
            # Start the crawler
            self.publish_log(task_id, f"Starting crawler for {base_url}", "info")
            crawl_result = crawler.start()
            
            # Clean up temp file
            if os.path.exists("/tmp/temp_crawl.json"):
                os.remove("/tmp/temp_crawl.json")
            
            # Log completion of crawling
            self.publish_log(
                task_id,
                f"Crawling completed. Found {crawl_result['summary']['total_links_found']} links, scraped {crawl_result['summary']['total_pages_scraped']} pages.",
                "info"
            )
            
            # Create crawl result in database first to get ID
            crawl_result_id = await self.save_crawl_to_database(
                task_id=task_id,
                crawl_id=crawl_id,
                crawl_summary=crawl_result["summary"]
            )
            
            # Save raw crawl data to database
            await self.save_crawl_data_to_database(task_id, crawl_result_id, crawl_result)
            
            # Update progress
            task_manager.update_task_status(
                task_id,
                progress=40.0,
                result={"crawl_complete": True, "crawl_results": crawl_result["summary"], "crawl_result_id": crawl_result_id}
            )
            
            # Step 2: Link processing (Database-based)
            task_manager.update_task_status(
                task_id,
                status="processing",
                progress=45.0
            )
            
            # Create the link processor (database-based)
            self.publish_log(task_id, "Processing links from database...", "info")
            processor = LinkProcessor(
                crawl_result_id=crawl_result_id,
                task_id=task_id,
                num_workers=CRAWLER_NUM_WORKERS,
                file_extensions=FILE_EXTENSIONS,
                social_media_keywords=SOCIAL_MEDIA_KEYWORDS,
                bank_keywords=BANK_KEYWORDS
            )
            
            # Process links
            process_result = await processor.process()
            
            # Log link processing results
            self.publish_log(
                task_id,
                f"Link processing completed. Categorized {process_result['summary']['total_links']} links into {process_result['summary']['file_links_count']} file links, {process_result['summary']['bank_links_count']} bank links, {process_result['summary']['social_media_links_count']} social media links, and {process_result['summary']['misc_links_count']} miscellaneous links.",
                "info"
            )
            
            # Update crawl result with process summary
            await self.save_crawl_to_database(
                task_id=task_id,
                crawl_id=crawl_id,
                crawl_summary=crawl_result["summary"],
                process_summary=process_result["summary"]
            )
            
            # Update progress
            task_manager.update_task_status(
                task_id,
                progress=60.0,
                result={
                    **task_manager.get_task_status(task_id)["result"],
                    "process_complete": True,
                    "process_results": process_result["summary"]
                }
            )
            
            # Step 3: URL clustering (Database-based)
            task_manager.update_task_status(
                task_id,
                status="clustering",
                progress=65.0
            )
            
            # Create the URL clusterer (database-based)
            self.publish_log(task_id, "Clustering URLs from database...", "info")
            clusterer = URLClusterer(
                crawl_result_id=crawl_result_id,
                task_id=task_id,
                min_cluster_size=CLUSTER_MIN_SIZE,
                path_depth=CLUSTER_PATH_DEPTH,
                similarity_threshold=CLUSTER_SIMILARITY_THRESHOLD
            )
            
            # Cluster URLs
            cluster_result = await clusterer.cluster()

            if hasattr(self, f"crawler_{task_id}"):
                delattr(self, f"crawler_{task_id}")
            
            # Log URL clustering results
            self.publish_log(
                task_id,
                f"URL clustering completed. Identified {cluster_result['summary']['total_domains']} domains and {cluster_result['summary']['total_clusters']} clusters across {cluster_result['summary']['total_urls']} URLs.",
                "info"
            )
            
            # Update progress
            task_manager.update_task_status(
                task_id,
                progress=80.0,
                result={
                    **task_manager.get_task_status(task_id)["result"],
                    "cluster_complete": True,
                    "cluster_results": cluster_result["summary"]
                }
            )
            
            # Step 4: Year extraction (Database-based)
            task_manager.update_task_status(
                task_id,
                status="year_extraction",
                progress=85.0
            )
            
            # Create the year extractor (database-based)
            self.publish_log(task_id, "Extracting years from database...", "info")
            year_extractor = YearExtractor(
                crawl_result_id=crawl_result_id,
                task_id=task_id
            )
            
            # Extract years
            year_result = await year_extractor.process()
            
            # Log year extraction results
            self.publish_log(
                task_id,
                f"Year extraction completed. Identified {len(year_result)} distinct years across {sum(len(files) for files in year_result.values())} files.",
                "info"
            )
            
            # Step 5: Save Final Results to Database
            task_manager.update_task_status(
                task_id,
                status="saving_to_database",
                progress=90.0
            )
            
            self.publish_log(task_id, "Saving final results to database...", "info")
            
            # Save all results to database
            final_crawl_result_id = await self.save_crawl_to_database(
                task_id=task_id,
                crawl_id=crawl_id,
                crawl_summary=crawl_result["summary"],
                process_summary=process_result["summary"],
                cluster_summary=cluster_result["summary"],
                year_extraction_summary={
                    "total_years": len(year_result),
                    "total_files": sum(len(files) for files in year_result.values())
                },
                clusters_data=cluster_result,
                year_data=year_result
            )
            
            # Update final progress
            task_manager.update_task_status(
                task_id,
                status="completed",
                progress=100.0,
                result={
                    "crawl_complete": True,
                    "process_complete": True,
                    "cluster_complete": True,
                    "year_extraction_complete": True,
                    "database_saved": True,
                    "crawl_result_id": final_crawl_result_id,
                    "crawl_results": crawl_result["summary"],
                    "process_results": process_result["summary"],
                    "cluster_results": cluster_result["summary"],
                    "year_extraction_results": {
                        "total_years": len(year_result),
                        "total_files": sum(len(files) for files in year_result.values())
                    }
                }
            )
            
            self.publish_log(task_id, f"Crawl workflow completed successfully for {base_url}. Results saved to database.", "info")
            
            # Return final status
            return task_manager.get_task_status(task_id)
        
        except Exception as e:
            self.publish_log(task_id, f"Error in crawl workflow: {str(e)}", "error")
            self.publish_log(task_id, traceback.format_exc(), "error")
            task_manager.update_task_status(
                task_id,
                status="failed",
                error=str(e)
            )
            return task_manager.get_task_status(task_id)
    
    def stop_crawl(self, task_id: str) -> Dict[str, Any]:
        """
        Stop a running crawl process gracefully with proper cleanup.
        """
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
                self.publish_log(task_id, "No active crawler instance found, updating task status to stopped", "info")
                
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
            
            task_manager.update_task_status(
                task_id,
                status="failed",
                error=error_msg
            )
            
            return {"success": False, "message": error_msg}

    # Database-based methods for retrieving clusters and years
    async def get_available_clusters(self, crawl_result_id: str = None) -> List[Dict[str, Any]]:
        """
        Get available clusters from database.
        
        Args:
            crawl_result_id: Specific crawl result ID, if None gets from latest crawl
            
        Returns:
            List of dictionaries with cluster information
        """
        try:
            if crawl_result_id:
                # Get clusters for specific crawl result
                cluster_docs = await ClusterDocument.find(
                    ClusterDocument.crawl_result_id == crawl_result_id
                ).to_list()
            else:
                # Get clusters from most recent completed crawl
                latest_crawl = await CrawlResult.find(
                    CrawlResult.cluster_complete == True
                ).sort([("created_at", -1)]).first()
                
                if not latest_crawl:
                    return []
                
                cluster_docs = await ClusterDocument.find(
                    ClusterDocument.crawl_result_id == str(latest_crawl.id)
                ).to_list()
            
            clusters_info = []
            
            for cluster_doc in cluster_docs:
                domain_data = cluster_doc.cluster_data
                
                # Add domain level cluster
                clusters_info.append({
                    "id": domain_data.id,
                    "name": cluster_doc.domain,
                    "type": "domain",
                    "url_count": domain_data.count
                })
                
                # Add sub-clusters
                for sub_cluster in domain_data.clusters:
                    clusters_info.append({
                        "id": sub_cluster.id,
                        "name": f"{cluster_doc.domain} - {sub_cluster.path}",
                        "type": "path",
                        "url_count": sub_cluster.url_count
                    })
            
            return clusters_info
        
        except Exception as e:
            logger.error(f"Error getting available clusters from database: {str(e)}")
            return []
    
    async def get_available_years(self, crawl_result_id: str = None) -> List[Dict[str, Any]]:
        """
        Get available years from database.
        
        Args:
            crawl_result_id: Specific crawl result ID, if None gets from latest crawl
            
        Returns:
            List of dictionaries with year information
        """
        try:
            if crawl_result_id:
                # Get years for specific crawl result
                year_docs = await YearDocument.find(
                    YearDocument.crawl_result_id == crawl_result_id
                ).to_list()
            else:
                # Get years from most recent completed crawl
                latest_crawl = await CrawlResult.find(
                    CrawlResult.year_extraction_complete == True
                ).sort([("created_at", -1)]).first()
                
                if not latest_crawl:
                    return []
                
                year_docs = await YearDocument.find(
                    YearDocument.crawl_result_id == str(latest_crawl.id)
                ).to_list()
            
            years_info = []
            
            for year_doc in year_docs:
                years_info.append({
                    "year": year_doc.year,
                    "files_count": year_doc.files_count
                })
            
            # Sort by year (newest first, but "No Year" at the end)
            return sorted(
                years_info,
                key=lambda y: (y["year"] == "No Year", y["year"]),
                reverse=True
            )
        
        except Exception as e:
            logger.error(f"Error getting available years from database: {str(e)}")
            return []
        
    async def get_cluster_by_id(self, cluster_id: str, crawl_result_id: str = None) -> Optional[Dict[str, Any]]:
        """Get cluster by ID from database."""
        try:
            if crawl_result_id:
                cluster_docs = await ClusterDocument.find(
                    ClusterDocument.crawl_result_id == crawl_result_id
                ).to_list()
            else:
                # Get from latest crawl
                latest_crawl = await CrawlResult.find(
                    CrawlResult.cluster_complete == True
                ).sort([("created_at", -1)]).first()
                
                if not latest_crawl:
                    return None
                
                cluster_docs = await ClusterDocument.find(
                    ClusterDocument.crawl_result_id == str(latest_crawl.id)
                ).to_list()
            
            for cluster_doc in cluster_docs:
                domain_data = cluster_doc.cluster_data
                
                # Check if it's a domain-level cluster
                if domain_data.id == cluster_id:
                    return {
                        "id": domain_data.id,
                        "name": cluster_doc.domain,
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
                
                # Check sub-clusters
                for cluster in domain_data.clusters:
                    if cluster.id == cluster_id:
                        return {
                            "id": cluster.id,
                            "name": f"{cluster_doc.domain} - {cluster.path}",
                            "type": "path",
                            "url_count": cluster.url_count,
                            "urls": cluster.urls
                        }
            
            return None
        
        except Exception as e:
            logger.error(f"Error getting cluster by ID from database: {str(e)}")
            return None
        
    async def get_year_by_id(self, year: str, crawl_result_id: str = None) -> Optional[Dict[str, Any]]:
        """Get year data by ID from database."""
        try:
            if crawl_result_id:
                year_doc = await YearDocument.find_one(
                    YearDocument.crawl_result_id == crawl_result_id,
                    YearDocument.year == year
                )
            else:
                # Get from latest crawl
                latest_crawl = await CrawlResult.find(
                    CrawlResult.year_extraction_complete == True
                ).sort([("created_at", -1)]).first()
                
                if not latest_crawl:
                    return None
                
                year_doc = await YearDocument.find_one(
                    YearDocument.crawl_result_id == str(latest_crawl.id),
                    YearDocument.year == year
                )
            
            if year_doc:
                return {
                    "year": year_doc.year,
                    "files_count": year_doc.files_count,
                    "files": year_doc.files
                }
            
            return None
        
        except Exception as e:
            logger.error(f"Error getting year by ID from database: {str(e)}")
            return None

    async def mark_scraping_complete(self, crawl_result_id: str) -> None:
        """Mark scraping as complete for a crawl result."""
        try:
            crawl_result = await CrawlResult.get(crawl_result_id)
            if crawl_result:
                crawl_result.scraping_complete = True
                crawl_result.updated_at = datetime.utcnow()
                await crawl_result.save()
                logger.info(f"Marked scraping complete for crawl result {crawl_result_id}")
        except Exception as e:
            logger.error(f"Error marking scraping complete: {str(e)}")

    # Keep the scrape/download methods as they are for now (file-based)
    async def run_scrape_download(
        self,
        task_id: str,
        cluster_ids: List[str],
        years: List[str] = None,
        url_clusters_file: str = None,
        year_clusters_file: str = None
    ) -> Dict[str, Any]:
        """
        Run the scraping and downloading workflow with enhanced progress tracking.
        Still uses file-based approach for now.
        
        Args:
            task_id: Task ID for tracking progress
            cluster_ids: IDs of clusters to scrape
            years: Years of files to download
            url_clusters_file: Path to the URL clusters file
            year_clusters_file: Path to the year clusters file
            
        Returns:
            Dictionary with scraping and downloading results
        """
        # Initialize task status
        task_manager.update_task_status(
            task_id,
            status="initializing",
            progress=0.0
        )
        
        # Log start of the workflow
        self.publish_log(task_id, "Starting scrape and download workflow", "info")
        
        # Get crawl_result_id from task params
        task_status = task_manager.get_task_status(task_id)
        task_params = task_status.get("params", {}) if task_status else {}
        crawl_result_id = task_params.get("crawl_result_id")
        
        try:
            # === INITIALIZATION PHASE (0-5%) ===
            # Update task status
            task_manager.update_task_status(
                task_id,
                status="checking_files",
                progress=1.0
            )
            
            # Use the latest files if not specified
            if not url_clusters_file and cluster_ids:
                # Find the most recent URL clusters file
                cluster_files = [f for f in os.listdir(self.clusters_dir) if f.endswith("_url_clusters.json")]
                if not cluster_files:
                    error_msg = "No URL clusters file found"
                    self.publish_log(task_id, error_msg, "error")
                    raise FileNotFoundError(error_msg)
                
                cluster_files.sort(reverse=True)  # Sort by name (which includes timestamp)
                url_clusters_file = os.path.join(self.clusters_dir, cluster_files[0])
                self.publish_log(task_id, f"Using most recent URL clusters file: {os.path.basename(url_clusters_file)}", "info")
            
            if not year_clusters_file and years:
                # Find the most recent year clusters file
                year_files = [f for f in os.listdir(self.clusters_dir) if f.endswith("_year_clusters.json")]
                if not year_files:
                    error_msg = "No year clusters file found"
                    self.publish_log(task_id, error_msg, "error")
                    raise FileNotFoundError(error_msg)
                
                year_files.sort(reverse=True)  # Sort by name (which includes timestamp)
                year_clusters_file = os.path.join(self.clusters_dir, year_files[0])
                self.publish_log(task_id, f"Using most recent year clusters file: {os.path.basename(year_clusters_file)}", "info")
            
            # Update progress after file detection
            task_manager.update_task_status(
                task_id,
                status="preparing",
                progress=3.0
            )
            
            # === SCRAPING PHASE (5-60%) ===
            if cluster_ids:
                task_manager.update_task_status(
                    task_id,
                    status="scraping",
                    progress=5.0
                )
                
                # Define scraper output directory
                timestamp = time.strftime("%Y%m%d-%H%M%S")
                scrape_output_dir = os.path.join(self.scrape_dir, f"scrape_{timestamp}")
                metadata_output_dir = os.path.join(self.metadata_dir, f"metadata_{timestamp}")
                
                # Create the scraper with task ID for progress tracking
                self.publish_log(task_id, f"Preparing to scrape {len(cluster_ids)} clusters", "info")
                scraper = ClusterScraper(
                    json_file_path=url_clusters_file,
                    output_dir=scrape_output_dir,
                    metadata_dir=metadata_output_dir,
                    expiry_days=EXPIRY_DAYS
                )
                
                # Scrape the clusters with task ID for continuous progress updates
                self.publish_log(task_id, f"Starting scraping of clusters: {cluster_ids}", "info")
                scrape_result = scraper.scrape_clusters(cluster_ids, task_id=task_id)
                
                # Log scraping results
                self.publish_log(
                    task_id,
                    f"Scraping completed. Scraped {scrape_result['pages_scraped']} pages from {len(scrape_result['clusters_scraped'])} clusters.",
                    "info"
                )
                
                # Update progress to 60%
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
            
            # === TRANSITION PHASE (60-65%) ===
            # Add a brief "preparing download" phase for smoother transition
            if years:
                task_manager.update_task_status(
                    task_id,
                    status="preparing_download",
                    progress=62.0
                )
                
                # Log the transition
                self.publish_log(task_id, "Scraping phase complete. Preparing for download phase...", "info")
                
                # Short sleep to ensure status updates are visible
                time.sleep(0.5)
            
            # === DOWNLOADING PHASE (65-90%) ===
            if years:
                task_manager.update_task_status(
                    task_id,
                    status="downloading",
                    progress=65.0
                )
                
                # Define download output directory
                timestamp = time.strftime("%Y%m%d-%H%M%S")
                download_output_dir = os.path.join(self.download_dir, f"download_{timestamp}")
                
                # Create the downloader with task ID for progress tracking
                self.publish_log(task_id, f"Preparing to download files for years: {years}", "info")
                downloader = FileDownloader(
                    max_workers=MAX_DOWNLOAD_WORKERS,
                    timeout=CRAWLER_TIMEOUT
                )
                
                # Download the files with task ID for continuous progress updates
                self.publish_log(task_id, f"Starting download of files for years: {years}", "info")
                download_result = downloader.download_files_by_year(
                    json_file=year_clusters_file,
                    years_to_download=years,
                    base_folder=download_output_dir,
                    task_id=task_id
                )
                
                # Log downloading results
                self.publish_log(
                    task_id,
                    f"File downloading completed. Successfully downloaded {download_result['successful']} files, failed to download {download_result['failed']} files.",
                    "info"
                )
                
                # Update progress to 90%
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
            
            # === FINALIZATION PHASE (90-100%) ===
            task_manager.update_task_status(
                task_id,
                status="finalizing",
                progress=95.0
            )
            
            self.publish_log(task_id, "Finalizing workflow and generating summary...", "info")
            
            # Mark scraping as complete in database if we have crawl_result_id and scraping was done
            if crawl_result_id and cluster_ids:
                try:
                    await self.mark_scraping_complete(crawl_result_id)
                    self.publish_log(task_id, f"Marked scraping as complete for crawl result {crawl_result_id}", "info")
                except Exception as e:
                    self.publish_log(task_id, f"Warning: Could not mark scraping as complete: {str(e)}", "warning")
            
            # Add a brief delay to show the finalizing step
            time.sleep(0.5)
            
            # Update status to completed
            task_manager.update_task_status(
                task_id,
                status="completed",
                progress=100.0
            )
            
            self.publish_log(task_id, "Scrape and download workflow completed successfully", "info")
            
            # Return final status
            return task_manager.get_task_status(task_id)
        
        except Exception as e:
            error_msg = f"Error in scrape/download workflow: {str(e)}"
            self.publish_log(task_id, error_msg, "error")
            self.publish_log(task_id, traceback.format_exc(), "error")
            
            task_manager.update_task_status(
                task_id,
                status="failed",
                error=error_msg
            )
            return task_manager.get_task_status(task_id)

    async def get_all_crawl_results(self, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Get all crawl results from database.
        
        Args:
            limit: Maximum number of results to return
            
        Returns:
            List of crawl results
        """
        try:
            crawl_results = await CrawlResult.find().sort([("created_at", -1)]).limit(limit).to_list()
            
            results = []
            for crawl_result in crawl_results:
                result_data = {
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
                
                # Add summaries if available
                if crawl_result.crawl_summary:
                    result_data["crawl_summary"] = crawl_result.crawl_summary.dict()
                
                if crawl_result.process_summary:
                    result_data["process_summary"] = crawl_result.process_summary.dict()
                
                if crawl_result.cluster_summary:
                    result_data["cluster_summary"] = crawl_result.cluster_summary.dict()
                
                if crawl_result.year_extraction_summary:
                    result_data["year_extraction_summary"] = crawl_result.year_extraction_summary.dict()
                
                results.append(result_data)
            
            return results
        
        except Exception as e:
            logger.error(f"Error getting all crawl results: {str(e)}")
            return []

# Create a global orchestrator instance
orchestrator = ApolloOrchestrator()