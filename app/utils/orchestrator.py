import os
import logging
import time
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

# Import from services instead of crawler
from app.services.apollo import Apollo
from app.services.link_processor import LinkProcessor
from app.services.url_clusterer import URLClusterer
from app.services.year_extractor import YearExtractor
from app.services.scraper import ClusterScraper
from app.services.downloader import FileDownloader

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class ApolloOrchestrator:
    """
    A class to orchestrate the entire Apollo workflow:
    1. Crawling
    2. Link processing
    3. URL clustering
    4. Year extraction
    5. Content scraping
    6. File downloading
    """
    
    def __init__(self, base_directory: str = None):
        """
        Initialize the orchestrator.
        
        Args:
            base_directory: Base directory for storing all data and output
        """
        self.base_directory = base_directory or DATA_DIR
        
        # Create directory structure
        os.makedirs(self.base_directory, exist_ok=True)
        self.crawl_dir = os.path.join(self.base_directory, "crawl")
        self.process_dir = os.path.join(self.base_directory, "process")
        self.clusters_dir = os.path.join(self.base_directory, "clusters")
        self.scrape_dir = os.path.join(self.base_directory, "scraped")
        self.download_dir = os.path.join(self.base_directory, "downloads")
        self.metadata_dir = os.path.join(self.base_directory, "metadata")
        
        for directory in [self.crawl_dir, self.process_dir, self.clusters_dir, 
                          self.scrape_dir, self.download_dir, self.metadata_dir]:
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
    
    def run_crawl(
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
        Run the complete crawling workflow (crawl, process, cluster).
        
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
        
        # If stop_scraper is True, check if there's a running crawler and stop it
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
            
            return {"status": "stopped", "message": "Stop signal sent to all running crawlers"}
        
        try:
            # Step 1: Crawling
            task_manager.update_task_status(
                task_id,
                status="crawling",
                progress=5.0
            )
            
            # Generate unique filenames for this crawl
            timestamp = time.strftime("%Y%m%d-%H%M%S")
            crawl_id = f"{timestamp}_{base_url.replace('://', '_').replace('/', '_')[:30]}"
            
            # Define file paths
            all_links_file = os.path.join(self.crawl_dir, f"{crawl_id}_all_links.json")
            categorized_file = os.path.join(self.process_dir, f"{crawl_id}_categorized.json")
            url_clusters_file = os.path.join(self.clusters_dir, f"{crawl_id}_url_clusters.json")
            year_clusters_file = os.path.join(self.clusters_dir, f"{crawl_id}_year_clusters.json")
            
            # Log the limits being used
            self.publish_log(task_id, f"Starting crawler for {base_url} with limits: max_links={max_links_to_scrape}, max_pages={max_pages_to_scrape}, depth_limit={depth_limit}", "info")
            
            # Define a callback for Apollo status updates
            def status_callback(status_data):
                # Calculate appropriate progress value (adjust as needed)
                # Crawling is ~40% of the total workflow
                progress = min(40.0, (status_data.get('progress', 0) / 100) * 40.0)
                
                crawl_result = {
                    "crawl_results": {
                        "total_links_found": status_data.get('links_found', 0),
                        "total_pages_scraped": status_data.get('pages_scraped', 0),
                        "execution_time_seconds": status_data.get('execution_time_seconds', 0)
                    }
                }
                
                # Update task status with the latest data from crawler
                task_manager.update_task_status(
                    task_id,
                    progress=progress,
                    result=crawl_result
                )
                
                # Log progress updates periodically
                if status_data.get('pages_scraped', 0) % 10 == 0:  # Every 10 pages
                    self.publish_log(
                        task_id,
                        f"Crawl progress: {status_data.get('pages_scraped', 0)} pages scraped, {status_data.get('links_found', 0)} links found",
                        "info"
                    )
            
            # Create the crawler
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
            
            # Register the status callback
            crawler.register_status_callback(status_callback)
            
            # Start the crawler
            self.publish_log(task_id, f"Starting crawler for {base_url}", "info")
            crawl_result = crawler.start()
            
            # Log completion of crawling
            self.publish_log(
                task_id,
                f"Crawling completed. Found {crawl_result['summary']['total_links_found']} links, scraped {crawl_result['summary']['total_pages_scraped']} pages.",
                "info"
            )
            
            # Update progress
            task_manager.update_task_status(
                task_id,
                progress=40.0,
                result={"crawl_complete": True, "crawl_results": crawl_result["summary"]}
            )
            
            # Step 2: Link processing
            task_manager.update_task_status(
                task_id,
                status="processing",
                progress=45.0
            )
            
            # Create the link processor
            self.publish_log(task_id, "Processing links...", "info")
            processor = LinkProcessor(
                input_file=all_links_file,
                output_file=categorized_file,
                num_workers=CRAWLER_NUM_WORKERS,
                file_extensions=FILE_EXTENSIONS,
                social_media_keywords=SOCIAL_MEDIA_KEYWORDS,
                bank_keywords=BANK_KEYWORDS
            )
            
            # Process links
            process_result = processor.process()
            
            # Log link processing results
            self.publish_log(
                task_id,
                f"Link processing completed. Categorized {process_result['summary']['total_links']} links into {process_result['summary']['file_links_count']} file links, {process_result['summary']['bank_links_count']} bank links, {process_result['summary']['social_media_links_count']} social media links, and {process_result['summary']['misc_links_count']} miscellaneous links.",
                "info"
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
            
            # Step 3: URL clustering
            task_manager.update_task_status(
                task_id,
                status="clustering",
                progress=65.0
            )
            
            # Create the URL clusterer
            self.publish_log(task_id, "Clustering URLs...", "info")
            clusterer = URLClusterer(
                input_file=categorized_file,
                output_file=url_clusters_file,
                min_cluster_size=CLUSTER_MIN_SIZE,
                path_depth=CLUSTER_PATH_DEPTH,
                similarity_threshold=CLUSTER_SIMILARITY_THRESHOLD
            )
            
            # Cluster URLs
            cluster_result = clusterer.cluster()
            
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
            
            # Step 4: Year extraction
            task_manager.update_task_status(
                task_id,
                status="year_extraction",
                progress=85.0
            )
            
            # Create the year extractor
            self.publish_log(task_id, "Extracting years from file URLs...", "info")
            year_extractor = YearExtractor(
                input_file=categorized_file,
                output_file=year_clusters_file
            )
            
            # Extract years
            year_result = year_extractor.process()
            
            # Log year extraction results
            self.publish_log(
                task_id,
                f"Year extraction completed. Identified {len(year_result)} distinct years across {sum(len(files) for files in year_result.values())} files.",
                "info"
            )
            
            # Update progress
            task_manager.update_task_status(
                task_id,
                status="completed",
                progress=100.0,
                result={
                    **task_manager.get_task_status(task_id)["result"],
                    "year_extraction_complete": True,
                    "year_extraction_results": {
                        "total_years": len(year_result),
                        "total_files": sum(len(files) for files in year_result.values())
                    },
                    "output_files": {
                        "all_links_file": all_links_file,
                        "categorized_file": categorized_file,
                        "url_clusters_file": url_clusters_file,
                        "year_clusters_file": year_clusters_file
                    }
                }
            )
            
            self.publish_log(task_id, f"Crawl workflow completed successfully for {base_url}", "info")
            
            # Return final status
            return task_manager.get_task_status(task_id)
        
        except Exception as e:
            self.publish_log(task_id, f"Error in crawl workflow: {str(e)}", "error")
            task_manager.update_task_status(
                task_id,
                status="failed",
                error=str(e)
            )
            return task_manager.get_task_status(task_id)
    
    def run_scrape_download(
        self,
        task_id: str,
        cluster_ids: List[str],
        years: List[str] = None,
        url_clusters_file: str = None,
        year_clusters_file: str = None
    ) -> Dict[str, Any]:
        """
        Run the scraping and downloading workflow.
        
        Args:
            task_id: Task ID for tracking progress
            cluster_ids: IDs of clusters to scrape
            years: Years of files to download
            url_clusters_file: Path to the URL clusters file
            year_clusters_file: Path to the year clusters file
            
        Returns:
            Dictionary with scraping and downloading results
        """
        # Update task status
        task_manager.update_task_status(
            task_id,
            status="running",
            progress=0.0
        )
        
        # Log start of the scraping workflow
        self.publish_log(task_id, "Starting scrape and download workflow", "info")
        
        try:
            # Use the latest files if not specified
            if not url_clusters_file:
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
            
            # Step 1: Content scraping
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
                
                # Create the scraper
                self.publish_log(task_id, f"Scraping clusters: {cluster_ids}", "info")
                scraper = ClusterScraper(
                    json_file_path=url_clusters_file,
                    output_dir=scrape_output_dir,
                    metadata_dir=metadata_output_dir,
                    expiry_days=EXPIRY_DAYS
                )
                
                # Scrape the clusters
                scrape_result = scraper.scrape_clusters(cluster_ids)
                
                # Log scraping results
                self.publish_log(
                    task_id,
                    f"Scraping completed. Scraped {scrape_result['pages_scraped']} pages from {len(scrape_result['clusters_scraped'])} clusters.",
                    "info"
                )
                
                # Update progress
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
            
            # Step 2: File downloading
            if years:
                task_manager.update_task_status(
                    task_id,
                    status="downloading",
                    progress=65.0
                )
                
                # Define download output directory
                timestamp = time.strftime("%Y%m%d-%H%M%S")
                download_output_dir = os.path.join(self.download_dir, f"download_{timestamp}")
                
                # Create the downloader
                self.publish_log(task_id, f"Downloading files for years: {years}", "info")
                downloader = FileDownloader(
                    max_workers=MAX_DOWNLOAD_WORKERS,
                    timeout=CRAWLER_TIMEOUT
                )
                
                # Download the files
                download_result = downloader.download_files_by_year(
                    json_file=year_clusters_file,
                    years_to_download=years,
                    base_folder=download_output_dir
                )
                
                # Log downloading results
                self.publish_log(
                    task_id,
                    f"File downloading completed. Successfully downloaded {download_result['successful']} files, failed to download {download_result['failed']} files.",
                    "info"
                )
                
                # Update progress
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
            self.publish_log(task_id, f"Error in scrape/download workflow: {str(e)}", "error")
            task_manager.update_task_status(
                task_id,
                status="failed",
                error=str(e)
            )
            return task_manager.get_task_status(task_id)
    
    def get_available_clusters(self, url_clusters_file: str = None) -> List[Dict[str, Any]]:
        """
        Get available clusters for scraping.
        
        Args:
            url_clusters_file: Path to the URL clusters file
            
        Returns:
            List of dictionaries with cluster information
        """
        # Use the latest file if not specified
        if not url_clusters_file:
            # Find the most recent URL clusters file
            cluster_files = [f for f in os.listdir(self.clusters_dir) if f.endswith("_url_clusters.json")]
            if not cluster_files:
                return []
            
            cluster_files.sort(reverse=True)  # Sort by name (which includes timestamp)
            url_clusters_file = os.path.join(self.clusters_dir, cluster_files[0])
        
        # Load the clusters file
        try:
            with open(url_clusters_file, 'r') as f:
                clusters_data = json.load(f)
            
            # Create a list of available clusters
            clusters_info = []
            
            # Go through domains
            for domain, domain_data in clusters_data.get("clusters", {}).items():
                # Add domain level clusters
                clusters_info.append({
                    "id": domain_data.get("id"),
                    "name": domain,
                    "type": "domain",
                    "url_count": domain_data.get("count", 0)
                })
                
                # Add sub-clusters
                for sub_cluster in domain_data.get("clusters", []):
                    clusters_info.append({
                        "id": sub_cluster.get("id"),
                        "name": f"{domain} - {sub_cluster.get('path', 'unknown-path')}",
                        "type": "path",
                        "url_count": sub_cluster.get("url_count", 0)
                    })
            
            return clusters_info
        
        except Exception as e:
            logger.error(f"Error getting available clusters: {str(e)}")
            return []
    
    def get_available_years(self, year_clusters_file: str = None) -> List[Dict[str, Any]]:
        """
        Get available years for downloading.
        
        Args:
            year_clusters_file: Path to the year clusters file
            
        Returns:
            List of dictionaries with year information
        """
        # Use the latest file if not specified
        if not year_clusters_file:
            # Find the most recent year clusters file
            year_files = [f for f in os.listdir(self.clusters_dir) if f.endswith("_year_clusters.json")]
            if not year_files:
                return []
            
            year_files.sort(reverse=True)  # Sort by name (which includes timestamp)
            year_clusters_file = os.path.join(self.clusters_dir, year_files[0])
        
        # Load the year clusters file
        try:
            with open(year_clusters_file, 'r') as f:
                year_data = json.load(f)
            
            # Create a list of available years
            years_info = []
            
            # Go through years
            for year, files in year_data.items():
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

# Create a global orchestrator instance
orchestrator = ApolloOrchestrator()